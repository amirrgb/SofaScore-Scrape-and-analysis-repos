import scrapy
import simplejson as json
from ..items import IncidentsScraperItem
import mysql.connector
from datetime import datetime

user, password, host = 'root', 'kian1381', 'localhost'
teamsDataBase, teamsLinksDataBase, leaguesDataBase = "teams_datas", "updated_teams_links", "leagues_datas"


def team_links_from_db():
    cnx = mysql.connector.connect(
        user=user, password=password, host=host, database=teamsLinksDataBase)
    con = cnx.cursor(buffered=True)
    con.execute("SELECT * FROM teams_link")
    teamsLinks = con.fetchall()
    all_teams_Ids = [team_link[0].split("/")[-1] for team_link in teamsLinks]
    print(len(all_teams_Ids), *all_teams_Ids[:10])
    return all_teams_Ids


def IDExtractor(team_Id):
    IDList = []
    with open('D://bet/datas/new_teams_data3/team%s/team%s.json' % (team_Id, team_Id), 'r+') as f:
        mainjson = json.loads(str(f.read()).encode('utf-8').decode('utf-8-sig'))
        for match in mainjson['onePageOfMainJson']:
            IDList.append(match['id'])
    return IDList


def allID():
    IDList = []
    for i in team_links_from_db():
        IDList += IDExtractor(i)
    print("length of ids is: ", len(IDList))
    return IDList


def loadAndSave(source, url):
    try:
        if '"code":404,"message":"Not Found"' in source:
            return "None"
        mainJson = json.loads(str(source).encode('utf-8'))
        if len(mainJson['incidents']) < 1:
            print("no incidents", url)
        return mainJson['incidents']
    except Exception as e:
        print(datetime.now(), 'error in loadJsonFile', url, e,
              file=open('D://bet/datas/incidentsBackups/error.txt', 'a'))


class IncidentsSpiderSpider(scrapy.Spider):
    name = 'incidents_spider'

    def __init__(self):
        self.counter = 0
        idList = allID()
        self.start_urls = ['https://api.sofascore.com/api/v1/event/' + str(i) + '/incidents' for i in idList]
        print("length of start_urls is: ", len(self.start_urls))

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, dont_filter=False)

    def parse(self, response):
        item = IncidentsScraperItem()
        self.counter += 1
        id = response.url.split('/')[-2]
        item["matchId"] = id
        item["incidents"] = loadAndSave(response.text, response.url)
        if self.counter % 500 == 0:
            print(datetime.now().ctime(), "counter is: ", self.counter)
        yield item
