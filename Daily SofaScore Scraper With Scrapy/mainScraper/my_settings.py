import scrapy.settings


class MY_SETTINGS(scrapy.settings.Settings):
    def __init__(self):
        super().__init__()
        self.setmodule('scrapy.settings.default_settings')
        self['LOG_LEVEL'] = 'ERROR'
        self['USER_AGENT'] = 'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/W.X.Y.Z Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
        self['CONCURRENT_REQUESTS'] = 32
        self['DOWNLOADER_MIDDLEWARES'] = {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 400
        }
        self['REQUEST_FINGERPRINTER_IMPLEMENTATION'] = "2.7"
        self['DOWNLOAD_FAIL_ON_DATALOSS'] = False
        self['RETRY_TIMES'] = 150
        self['ITEM_PIPELINES'] = {
            "mainScraper.mainScraper.pipelines.MainscraperPipeline": 300,
        }

# this is my config
