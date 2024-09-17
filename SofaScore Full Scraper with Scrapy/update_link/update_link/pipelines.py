# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector


def insertToNewDB(link):
    conn = mysql.connector.connect(host="localhost", user="root", password="kian1381")
    cursor = conn.cursor()
    existSql = 'SELECT * FROM updated_teams_links.teams_link WHERE TeamLink = "%s"'%link
    cursor.execute(existSql)
    if cursor.fetchone() is None:
        sql = 'INSERT INTO updated_teams_links.teams_link (TeamLink , IsCollected) VALUES ("%s","%s")' % (link, "False")
        cursor.execute(sql)
        conn.commit()
    else:
        print("This link is already in database : ", link)
        print("This link is already in database : ", link,file=open("D://bet/notInUpdatedLinks.txt", "a"))



class UpdateLinkPipeline:

    def process_item(self, item, spider):
        link = item['link']
        insertToNewDB(link)
        return
