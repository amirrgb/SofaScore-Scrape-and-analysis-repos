import scrapy
import mysql.connector
from mainScraper.mainScraper.items import MainscraperItem
from mainScraper.mainScraper.json_handlers.match_ids_extractor import loadAndSave
from tqdm import tqdm
class PresentBaseTeamsDataOldSpider(scrapy.Spider):
    name = "present_base_teams_data_old"

    def getApiLinksFromTeamLinks(self):
        conn = mysql.connector.connect(host="localhost", user="root", password="kian1381",database="updated_teams_links")
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM teams_link')
        links = cursor.fetchall()
        apiLinks = []
        for link in links:
            team_Id = link[0].split('/')[-1]
            url = f'https://api.sofascore.com/api/v1/team/{team_Id}/events/last/0'
            apiLinks.append(url)
        return apiLinks

    def __init__(self):
        print("collecting old base teams data :")
        self.start_urls = self.getApiLinksFromTeamLinks()

    def start_requests(self):
        with tqdm(total=len(self.start_urls)) as pbar:
            for url in self.start_urls:
                pbar.update(1)
                yield scrapy.Request(url, callback=self.parse, dont_filter=False)


    def parse(self, response):
        item = MainscraperItem()
        mainJson = loadAndSave(response.text, response.url)
        team_Id = str(response.url).split("/")[-4]
        if mainJson is not None:
            item['onePageOfMainJson'] = mainJson['events']
            item['team_Id'] = team_Id
            yield item











