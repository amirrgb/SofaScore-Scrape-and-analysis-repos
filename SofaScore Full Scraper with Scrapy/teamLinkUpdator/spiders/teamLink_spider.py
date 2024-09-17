import scrapy
import mysql.connector
from datetime import datetime


global updatedLinks
updatedLinks = []
class TeamlinkSpiderSpider(scrapy.Spider):
    name = 'teamLink_spider'

    def getLinksFromTeamLinks(self):
        conn = mysql.connector.connect(host="localhost",user="root",password="kian1381",database="teams_links")
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM teams_link')
        links=["https://www.sofascore.com/team/football/iskenderunspor-as/170408"]
        for row in cursor[:10]:
            links.append(row[0])
        links.append("https://www.sofascore.com/team/football/fc-leonzio-1909/391555")
        print("len Links before update : ",len(links))
        return links

    def __init__(self):
        print(str(datetime.now())+" TeamLinkUpdator Started")
        self.start_urls = self.getLinksFromTeamLinks()

    def parse(self, response):
        print(response.url)
        if response.request.meta.get('redirect_urls') is not None:
            print(response.request.meta.get('redirect_urls'),end="",file=open("teamLinkUpdator/teamLinkUpdator/spiders/log.txt","w"))
            print(response.url,file=open("teamLinkUpdator/teamLinkUpdator/spiders/log.txt","a"))
        if "https://www.sofascore.com/team/football/fc-leonzio-1909/391555" == response.url:
            print(str(datetime.now()) + " TeamLinkUpdator Ended")