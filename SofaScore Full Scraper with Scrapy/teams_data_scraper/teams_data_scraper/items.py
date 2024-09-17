# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TeamsDataScraperItem(scrapy.Item):
    # define the fields for your item here like:
    team_Id = scrapy.Field()
    onePageOfMainJson = scrapy.Field()
    pageNumber = scrapy.Field()

