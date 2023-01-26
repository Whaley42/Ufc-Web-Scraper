import datetime
from collections import Counter

class SpotifyFunctions:
    def get_top_tracks(sp):
        """
        Get the users top 10 tracks of the last month. Returns the top tracks
        with the song name and artist. Also returns the seed_tracks for recommendations.
        """
        track_items = sp.current_user_top_tracks(time_range='short_term', limit=10)["items"] 
        top_tracks = [(item["name"],[artist["name"] for artist in item["artists"]]) for item in track_items]
        seed_tracks = [item["id"] for item in track_items]
       
        return top_tracks, seed_tracks

    def get_top_artists(sp):
        """
        Returns the top 10 listened to artists as a list of artists and their genre(s). Also returns 
        the artist ids for recommendations.
        """
        artist_items = sp.current_user_top_artists(time_range='short_term', limit=10)["items"]
        top_artists = [(item["name"], item["genres"]) for item in artist_items]
        artist_ids= [item["id"] for item in artist_items]
      
        return top_artists, artist_ids

    
    def get_top_genres(top_artists):
        """
        Uses the genres from top artists to find the top 5 most listened to genres of
        the last month.
        """
        artist_genres = [artist[1] for artist in top_artists if len(artist) > 1]
        top_genres = Counter([genre for list_genres in artist_genres for genre in list_genres])
        top_5_genres = top_genres.most_common(5)

        return top_5_genres
    
    def get_recommendations(seed_artists, top_genres, seed_tracks, sp):
        """
        Returns the track uris of songs that are recommened based on last month's songs,artists 
        and genres.
        """
        seed_genres = [genre[0] for genre in top_genres]
        tracks = sp.recommendations(seed_artists=seed_artists, seed_genres=seed_genres, seed_tracks=seed_tracks, limit=50)["tracks"]
        recommendations = [track["uri"] for track in tracks]
        
        return recommendations
    
    def create_playlist(sp, user_id):
        """
        Create a playlist for the user.
        """
        playlist_name = str(datetime.date.today()) + " Recommendations"
        created_playlist = sp.user_playlist_create(user=user_id, name=playlist_name)
        
        return created_playlist
    
    def add_songs(sp, user_id, playlist_id, recommendations):
        """
        Add the recommended songs into the created playlist.
        """
        sp.user_playlist_add_tracks(user=user_id, playlist_id=playlist_id, tracks=recommendations)