import scrapy
import simplejson as json
from ..items import OddsScraperItem
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
    print(len(all_teams_Ids),"teams links loaded from db")
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
    for i in team_links_from_db()[3500:]:
        IDList += IDExtractor(i)
    print("length of ids is: ", len(IDList))
    return IDList


def loadAndSave(source, url):
    try:
        if '"code":404,"message":"Not Found"' in source:
            return "None"
        mainJson = json.loads(str(source).encode('utf-8'))
        if not len(mainJson['featured']):
            print("no featured", url)
        return mainJson['featured']['fullTime']
    except Exception as e:
        print(datetime.now(), 'error in loadJsonFile', url, e,
              file=open('D://bet/datas/oddsBackups/error.txt', 'a'))


class OddsSpiderSpider(scrapy.Spider):
    name = 'odds_spider'

    def __init__(self):
        self.counter = 0
        idList = allID()
        self.start_urls = ['https://api.sofascore.com/api/v1/event/' + str(i) + '/odds/1/featured' for i in idList]
        print("length of start_urls is: ", len(self.start_urls))

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, dont_filter=False)

    def parse(self, response):
        item = OddsScraperItem()
        self.counter += 1
        id = response.url.split('/')[-4]
        item["matchId"] = id
        item["featured"] = loadAndSave(response.text, response.url)
        if self.counter % 1000 == 0:
            print(datetime.now().ctime(), "counter is: ", self.counter)
        yield item
