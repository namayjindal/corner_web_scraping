import csv
import requests
import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse
from typing import Dict, List
import json
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenStreetMap Nominatim API URL
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MyScraper/1.0)"}

def get_osm_data(place_name: str, neighborhood: str) -> dict:
    """Fetch data from OpenStreetMap for a given place"""
    try:
        params = {
            "q": f"{place_name}, {neighborhood}",
            "format": "json",
            "addressdetails": 1,
            "extratags": 1
        }
        response = requests.get(NOMINATIM_URL, params=params, headers=HEADERS)
        if response.status_code == 200 and response.json():
            logger.info(f"Successfully fetched OSM data for {place_name}")
            return response.json()[0]
        logger.warning(f"No OSM data found for {place_name}")
        return {}
    except Exception as e:
        logger.error(f"Error fetching OSM data for {place_name}: {str(e)}")
        return {}

class IncrementalCSVWriter:
    """Helper class to write CSV data incrementally"""
    def __init__(self, filename: str):
        self.filename = filename
        self.file_exists = os.path.exists(filename)
        self.processed_ids = set()
        
        # Load existing IDs if file exists
        if self.file_exists:
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.processed_ids = {row['corner_place_id'] for row in reader}
                self.fieldnames = reader.fieldnames
        else:
            self.fieldnames = None

    def write_row(self, row: Dict):
        if not row['corner_place_id'] in self.processed_ids:
            mode = 'a' if self.file_exists else 'w'
            with open(self.filename, mode, newline='', encoding='utf-8') as f:
                if not self.fieldnames:
                    self.fieldnames = list(row.keys())
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                if not self.file_exists:
                    writer.writeheader()
                writer.writerow(row)
                self.file_exists = True
                self.processed_ids.add(row['corner_place_id'])
                logger.info(f"Added row for place ID {row['corner_place_id']}")

class IncrementalJSONWriter:
    """Helper class to write JSON data incrementally"""
    def __init__(self, filename: str):
        self.filename = filename
        self.processed_ids = set()
        
        # Load existing data if file exists
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    self.processed_ids = {item['corner_place_id'] for item in data}
                except json.JSONDecodeError:
                    data = []
        else:
            data = []
        self.data = data

    def write_item(self, item: Dict):
        if not item['corner_place_id'] in self.processed_ids:
            self.data.append(item)
            self.processed_ids.add(item['corner_place_id'])
            with open(self.filename, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            logger.info(f"Added website data for place ID {item['corner_place_id']}")

class BusinessSpider(scrapy.Spider):
    name = "business_spider"
    
    def __init__(self, places_data: List[Dict], **kwargs):
        super().__init__(**kwargs)
        self.places_data = {place['website']: place for place in places_data if place['website']}
        self.start_urls = list(self.places_data.keys())
        self.json_writer = IncrementalJSONWriter('scraped_data.json')

    def parse(self, response):
        url = response.url
        place_data = self.places_data.get(url)
        
        if place_data:
            item = {
                "corner_place_id": place_data['corner_place_id'],
                "url": url,
                "title": response.css("title::text").get(),
                "meta_description": response.css('meta[name="description"]::attr(content)').get(),
                "meta_keywords": response.css('meta[name="keywords"]::attr(content)').get(),
                "business_hours": response.css(".hours, .opening-hours, .business-hours::text").get()
            }
            self.json_writer.write_item(item)
            yield item

def main():
    # Initialize writers
    osm_writer = IncrementalCSVWriter("places_with_osm.csv")
    
    # Read and process places
    logger.info("Starting data collection...")
    places_data = []
    
    with open("places.csv", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        total_places = sum(1 for row in reader)
        csvfile.seek(0)
        next(reader)  # Skip header
        
        for i, row in enumerate(reader, 1):
            logger.info(f"Processing place {i}/{total_places}: {row['name']}")
            
            # Get OSM data and save incrementally
            osm_data = get_osm_data(row["name"], row["neighborhood"])
            row.update(osm_data)
            osm_writer.write_row(row)
            places_data.append(row)

    # Configure and start the crawler
    process = CrawlerProcess(
        settings={
            "USER_AGENT": HEADERS["User-Agent"],
            "DOWNLOAD_DELAY": 1,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
            "HTTPERROR_ALLOW_ALL": True,
            "LOG_LEVEL": "INFO"
        }
    )
    
    process.crawl(BusinessSpider, places_data=places_data)
    process.start()

if __name__ == "__main__":
    main()