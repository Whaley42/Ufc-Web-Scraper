from postscrape.spiders.Classes.ufc_spider import UfcSpider
from scrapy.crawler import CrawlerProcess

def run():
  process = CrawlerProcess()
  process.crawl(UfcSpider)
  process.start()