from Classes.ufc_spider import UfcSpider
from scrapy.crawler import CrawlerProcess

if __name__ == "__main__":
  process = CrawlerProcess()
  process.crawl(UfcSpider)
  process.start()