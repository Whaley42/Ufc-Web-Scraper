import mysql.connector

class SqlFunctions:

    def __init__(self) -> None:
        self.mydb = mysql.connector.connect(
            host="us-cdbr-east-06.cleardb.net",
            user="",
            password="",
            database=""
            )
        self.mycursor = self.mydb.cursor()
    
    def check_user_exists(self, email):
        """
        Check if the user logging in is already in the system. 
        """
        print(email)
        self.mycursor.execute(f"SELECT COUNT(email) FROM Users WHERE email='{email}'")

        in_db = self.mycursor.fetchone()[0] #Return true if not in db
        print(in_db)
        return in_db

    def insert_user(self, email, display_name, refresh_token):
        """
        Insert a new user into the system with the given values.
        """
        sql = "INSERT INTO Users (email, display_name, refresh_token) VALUES (%s, %s, %s)"
        val = (email, display_name, refresh_token)
        self.mycursor.execute(sql, val)

        self.mydb.commit()