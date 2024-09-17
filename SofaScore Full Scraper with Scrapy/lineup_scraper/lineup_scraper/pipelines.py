# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import simplejson as json
from datetime import datetime

# useful for handling different item types with a single interface

class LineupScraperPipeline:
    def process_item(self, item, spider):
        try:
            with open('D://bet/datas/lineupBackups2/match%s.json' % item['matchId'], 'w') as f:
                f.write(json.dumps(dict(item), indent=5))
        except Exception as e:
            print(datetime.now(), 'error in loadJsonFile', item['matchId'], e,
                  file=open('D://bet/datas/lineupBackups2/error.txt', 'a'))