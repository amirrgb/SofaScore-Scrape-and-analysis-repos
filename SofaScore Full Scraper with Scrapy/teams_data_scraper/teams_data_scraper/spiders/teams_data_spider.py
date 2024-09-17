import scrapy
import mysql.connector
from ..items import TeamsDataScraperItem
import json
from datetime import datetime

# complete this
def getApiLinksFromTeamLinks():
    conn = mysql.connector.connect(host="localhost", user="root", password="kian1381", database="updated_teams_links")
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM teams_link')
    links = cursor.fetchall()
    apiLinks = []
    for link in links:
        team_Id = link[0].split('/')[-1]
        url = f'https://api.sofascore.com/api/v1/team/{team_Id}/events/last/0'
        apiLinks.append(url)

    return apiLinks


def loadAndSave(source,url):
    try:
        if '"code":404,"message":"Not Found"' in source:
            return None
        mainJson = json.loads(str(source).encode('utf-8'))
        return mainJson
    except Exception as e:
        print(datetime.now(),'error in loadJsonFile',url, e, file=open('D://bet/datas/new_teams_data3/error.txt', 'a'))


def isLastPage(mainjson):
    if 'hasNextPage' in mainjson:
        if not mainjson["hasNextPage"]:
            return True
    return False


class TeamsDataSpiderSpider(scrapy.Spider):
    name = 'teams_data_spider'

    def __init__(self):
        self.start_urls = getApiLinksFromTeamLinks()

    def parse(self, response):
        team_Id = str(response.url).split("/")[-4]
        for page in range(36):
            url = f'https://api.sofascore.com/api/v1/team/{team_Id}/events/last/{page}'
            yield response.follow(url, callback=self.parse_pages, priority=-page, dont_filter=False)

    def parse_pages(self, response):
        item = TeamsDataScraperItem()
        mainJson = loadAndSave(response.text,response.url)
        team_Id = str(response.url).split("/")[-4]
        if mainJson is not None:
            item['team_Id'] = team_Id
            item['pageNumber'] = response.url.split('/')[-1]
            item['onePageOfMainJson'] = mainJson['events']
            yield item
