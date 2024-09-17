# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
from itemadapter import ItemAdapter
import scrapy


class LineupScraperItem(scrapy.Item):
    # define the fields for your item here like:
    matchId = scrapy.Field()
    lineups = scrapy.Field()
