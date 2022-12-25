import requests
from bs4 import BeautifulSoup

from .Helper import Helper


class Connector:


    def __init__(self, url, name) -> None:
        self.url = url
        self.name = name
        self.helper = Helper()

    
    def get_response(self, page):

        curr_url = self.url + "?page=" + str(page)
        response = requests.get(curr_url)
        # print(f"Connector Response: {response.text}")
        total_fights, total_wins = self.helper.fight_stats(response, self.name)
            
        return total_fights,total_wins
        












