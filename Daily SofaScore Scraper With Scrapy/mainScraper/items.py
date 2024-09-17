# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

#write item for all spiders

class MainscraperItem(scrapy.Item):
    team_Id = scrapy.Field()
    onePageOfMainJson = scrapy.Field()
    matchId = scrapy.Field()
    incidents = scrapy.Field()
    featured = scrapy.Field()
    lineups = scrapy.Field()


