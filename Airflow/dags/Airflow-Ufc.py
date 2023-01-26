from spotipy.oauth2 import SpotifyOAuth
from spotipy.client import Spotify
import mysql.connector
from Classes.SpotifyFunctions import SpotifyFunctions as sf
from Classes.SqlFunctions import SqlFunctions
from Classes.EmailSender import EmailSender
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import datetime



CLIENT_ID = ""
CLIENT_SECRET = ""

mydb = mysql.connector.connect(
  host="us-cdbr-east-06.cleardb.net",
  user="",
  password="",
  database=""
)
mycursor = mydb.cursor()

def create_spotify_oauth():
    """
    Connect to Spotify OAUTH.
    """
    return SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri="http://localhost:5000/redirect",
        scope="user-library-read user-read-recently-played user-top-read user-read-email"
    )

def update():
    """
    Update the user refresh token and send them a monthly an email update of their most
    listened to songs, artists, and genres. Also create a new Spotify playlist based
    on their recent listens.
    """
    mycursor.execute(f"SELECT email, display_name, refresh_token FROM Users")
    users = mycursor.fetchall()
    for user in users:
        email = user[0]
        display_name = user[1]
        refresh_token = user[2]

        sp_oauth = create_spotify_oauth()
        token_info = sp_oauth.refresh_access_token(refresh_token)
        sp = Spotify(auth=token_info["access_token"])

        sql = "UPDATE Users SET refresh_token = %s WHERE email=%s"
        val = (token_info['refresh_token'], email)
        mycursor.execute(sql, val)
        mydb.commit()
        
        sql = SqlFunctions()
        es = EmailSender()

        user_info = sp.current_user()
        user_id = user_info["id"]

        top_tracks, seed_tracks = sf.get_top_tracks(sp)
        top_artists, seed_artists = sf.get_top_artists(sp)
        top_genres = sf.get_top_genres(top_artists)
        recommendations = sf.get_recommendations(seed_artists, top_genres, seed_tracks, sp)
        es.send_email(top_tracks, top_artists, top_genres, email, display_name)
        created_playlist = sf.create_playlist(sp, user_id)
        playlist_id = created_playlist["id"]
        sf.add_songs(sp, user_id, playlist_id, recommendations)
        return "Success"


with DAG(
    dag_id="Airflow-Spotify",
    schedule_interval="@monthly",
    catchup=False,
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime.datetime(2023, 2, 1)
    }) as f:
    main = PythonOperator(
        task_id="main",
        python_callable=update
    )
