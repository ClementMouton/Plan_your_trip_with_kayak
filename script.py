import os
import pandas as pd
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
import random

# Chargement du fichier CSV des meilleures villes
cities_file_path = 'files/top_5city.csv'
cities_df = pd.read_csv(cities_file_path)

class RandomUserAgentMiddleware:
    def __init__(self, user_agents):
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.getlist('USER_AGENTS'))

    def process_request(self, request, spider):
        if self.user_agents:
            request.headers['User-Agent'] = random.choice(self.user_agents)

class Booking_Kayak(scrapy.Spider):
    name = "booking"
    start_urls = ['https://www.booking.com/']

    custom_settings = {
        'DOWNLOAD_DELAY': 3,  # Délai de 3 secondes entre chaque requête
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # Activation du délai aléatoire
        'LOG_LEVEL': logging.INFO,  # Niveau de log INFO
        'FEEDS': {
            'files/booking.json': {
                'format': 'json',
                'overwrite': True,
                'indent': 4,
            },
        },
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
        },
    }

    def start_requests(self):
        for city in cities_df['name']:
            url = f'https://www.booking.com/searchresults.fr.html?ss={city}'
            self.logger.info(f'Requesting URL: {url}')
            yield scrapy.Request(url=url, callback=self.after_search, cb_kwargs={'city': city})

    def after_search(self, response, city):
        hotel_links = response.css('a.hotel_name_link.url::attr(href)').getall()  # Mettez à jour le sélecteur ici
        self.logger.info(f'Found {len(hotel_links)} hotel links for city: {city}')
        for link in hotel_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(url=full_url, callback=self.parse_review, cb_kwargs={'city': city})

def parse_review(self, response, city):
    hotel_name = response.xpath("//h2[@class='d2fee87262 pp-header__title']/text()").get()
    hotel_address = response.xpath('//span[contains(@class, "hp_address_subtitle")]/text()').get()
    coordinates = response.xpath('//a[@id="hotel_address"]/@data-atlas-latlng').get()
    rating = response.xpath('//*[@class="a3b8729ab1 d86cee9b25"]/text()').get()
    number_of_reviews = response.xpath('//*[@class="abf093bdfe f45d8e4c32 d935416c47"]/text()').get()
    facilities = response.xpath('//span[@class="a5a5a75131"]/text()').getall()
    description = response.xpath("//p[@data-testid='property-description']/text()").get()

    items = {
        'city': city,
        'hotel_name': hotel_name.strip() if hotel_name else None,
        'hotel_address': hotel_address.strip() if hotel_address else None,
        'coordinates': coordinates.strip() if coordinates else None,
        'rating': rating.strip() if rating else None,
        'number_of_reviews': number_of_reviews.strip() if number_of_reviews else None,
        'facilities': [facility.strip() for facility in facilities] if facilities else None,
        'description': description.strip() if description else None,
        'url': response.url,
    }

    self.logger.info(f'Extracted data: {items}')
    yield items


# Supprimer le fichier JSON précédent s'il existe
output_file_path = 'files/booking.json'
if os.path.exists(output_file_path):
    os.remove(output_file_path)

# Configurer et exécuter le processus de crawling
process = CrawlerProcess(settings={
    'USER_AGENTS': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1.2 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'
    ],
    'LOG_LEVEL': logging.INFO,
    'DOWNLOADER_MIDDLEWARES': {
        '__main__.RandomUserAgentMiddleware': 400,
        'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    },
    'FEEDS': {
        output_file_path: {
            'format': 'json',
            'overwrite': True,
            'indent': 4,
        },
    },
})

process.crawl(Booking_Kayak)
process.start()
