# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import os


def isThereFolder(team_Id):
    dir_path = 'D://bet/datas/new_teams_data3/team%s/' % team_Id
    return os.path.isdir(dir_path)


def insertToJsonFiles(item: dict):
    try:
        team_Id = item['team_Id']
        if not isThereFolder(team_Id):
            os.mkdir('D://bet/datas/new_teams_data3/team%s/' % team_Id)
        path = 'D://bet/datas/new_teams_data3/team%s/page%s.json' % (team_Id, item['pageNumber'])
        print(path)
        with open(path, 'w') as f:
            f.write(json.dumps(item, indent=5, ensure_ascii=True))
    except Exception as e:
        print('error in insertToJsonFiles', e, file=open('D://bet/datas/new_teams_data3/error.txt', 'a'))


class TeamsDataScraperPipeline:
    def process_item(self, item, spider):
        insertToJsonFiles(dict(item))
