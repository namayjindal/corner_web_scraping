import csv
import requests
import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse

# OpenStreetMap Nominatim API URL
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MyScraper/1.0)"}

def get_osm_data(place_name, neighborhood):
    params = {
        "q": f"{place_name}, {neighborhood}",
        "format": "json",
        "addressdetails": 1,
        "extratags": 1
    }
    response = requests.get(NOMINATIM_URL, params=params, headers=HEADERS)
    if response.status_code == 200 and response.json():
        return response.json()[0]  # Return the first match
    return {}

class BusinessSpider(scrapy.Spider):
    name = "business_spider"
    
    def __init__(self, urls):
        self.start_urls = urls

    def parse(self, response):
        yield {
            "url": response.url,
            "title": response.css("title::text").get(),
            "meta_description": response.css('meta[name="description"]::attr(content)').get(),
            "meta_keywords": response.css('meta[name="keywords"]::attr(content)').get(),
            "business_hours": response.css(".hours, .opening-hours, .business-hours::text").get()
        }

# Read CSV and gather OSM data
places_data = []
with open("places.csv", newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        osm_data = get_osm_data(row["name"], row["neighborhood"])
        row.update(osm_data)
        places_data.append(row)

# Write updated data to a new CSV
with open("places_with_osm.csv", "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = list(places_data[0].keys())
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(places_data)

# Scrape additional data from websites
urls = [row["website"] for row in places_data if row["website"]]
process = CrawlerProcess(settings={"FEEDS": {"scraped_data.json": {"format": "json"}}})
process.crawl(BusinessSpider, urls=urls)
process.start()
