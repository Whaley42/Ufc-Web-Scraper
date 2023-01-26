from email.message import EmailMessage
import smtplib
class EmailSender:

    def __init__(self) -> None:
        self.msg = EmailMessage()
        self.email = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        self.username = "spotifymonthlyupdate@gmail.com"
        password = ""
        self.email.login(self.username, password)

    def send_email(self, top_tracks, top_artists, top_genres, email, display_name):
        """
        Sends an email to the user of their last months top songs, artists and genres.
        """
        message = f"Hi {display_name}\n\nYour top tracks of the month:\n"
        for i, track in enumerate(top_tracks):
            message += f"{i+1}. {track[0]} by "
            for i,artist in enumerate(track[1]):
                message += artist
                if i != len(track[1]) -1:
                    message +=", "

            message += "\n"
        message += "\nYour top artists of the month:\n"
        for i, artist in enumerate(top_artists):
            message += f"{i+1}. {artist[0]}\n"
        
        message += f"\nYour top genres of the month:\n"
        for i, genre in enumerate(top_genres):
            message += f"{i+1}. {genre[0]}\n"

        
        self.msg.set_content(message)
        self.msg['subject'] = "Monthly Update"
        self.msg['to'] = email
        self.msg['from'] = self.username
        self.email.send_message(self.msg)