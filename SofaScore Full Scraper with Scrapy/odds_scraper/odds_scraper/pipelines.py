# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import simplejson as json
from datetime import datetime

class OddsScraperPipeline:
    def process_item(self, item, spider):
        try:
            with open('D://bet/datas/oddsBackups/match%s.json' % item['matchId'], 'w') as f:
                f.write(json.dumps(dict(item), indent=5))
        except Exception as e:
            print(datetime.now(), 'error in loadJsonFile', item['matchId'], e,
                  file=open('D://bet/datas/oddsBackups/error.txt', 'a'))