import scrapy
from datetime import datetime
import mysql.connector
from ..items import UpdateLinkItem


def getLinksFromTeamLinks():
    conn = mysql.connector.connect(host="localhost", user="root", password="kian1381", database="teams_links")
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM teams_link')
    listt = cursor.fetchall()
    links = []
    for row in listt:
        links.append(row[0])
    links.append("https://www.sofascore.com/team/football/fc-leonzio-1909/391555")
    print("len Links before update : ", len(links))
    return links


class UpdateSpiderSpider(scrapy.Spider):
    name = 'update_spider'

    def __init__(self):
        print(str(datetime.now()) + " TeamLinkUpdator Started")
        self.start_urls = getLinksFromTeamLinks()

    def parse(self, response):
        item = UpdateLinkItem()
        if response.request.meta.get('redirect_urls') is not None:
            # create new link
            link = 'https://www.sofascore.com/team/football/' + str(response.url).split("/")[-2] + '/' + \
                   str(response.url).split("/")[-1]
            # log
            print(response.request.meta.get('redirect_urls')[0], " >>>>>", link,file=open("D://bet/notInUpdatedLinks.txt", "a"))
            print(response.request.meta.get('redirect_urls')[0], " >>>>>", link)
        else:
            link = response.url
        item['link'] = link
        yield item
