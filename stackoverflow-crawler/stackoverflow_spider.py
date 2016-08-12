import scrapy

class StackOverflowSpider(scrapy.Spider):
    name = 'stackoverflow'
    custom_settings = {
        "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        "ITEM_PIPELINES": {
            'scrapy_redis.pipelines.RedisPipeline': 300
        },
        "REDIS_HOST": "redis"
    }

    start_urls = ['http://stackoverflow.com/questions?sort=votes']

    def parse(self, response):
        self.logger.warning('parse url: %s', response.url)

        next_page = response.xpath('//*[@id="mainbar"]/div[4]/a[@rel="next"]/@href').extract() 
        if next_page:
            next_url = response.urljoin(next_page[0])
            yield scrapy.Request(next_url, callback=self.parse)
            
        for href in response.css('.question-summary h3 a::attr(href)'):
            full_url = response.urljoin(href.extract())
            yield scrapy.Request(full_url, callback=self.parse_question)



    def parse_question(self, response):
        yield {
            'title': response.css('h1 a::text').extract_first(),
            'votes': response.css('.question .vote-count-post::text').extract_first(),
            'body': response.css('.question .post-text').extract_first(),
            'tags': response.css('.question .post-tag::text').extract(),
            'link': response.url,
        }
