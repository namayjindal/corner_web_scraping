import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import logging
import time
from typing import Dict, Optional, List
import re
import json
from pathlib import Path

class VenueWebsiteScraper:
    def __init__(self, concurrency_limit: int = 3, output_file: str = 'scraped_data.csv'):
        self.concurrency_limit = concurrency_limit
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.output_file = output_file
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    async def initialize(self):
        """Initialize the aiohttp session"""
        self.session = aiohttp.ClientSession(headers=self.headers)

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()

    def clean_text(self, text: str) -> str:
        """Clean extracted text content"""
        if not text:
            return ""
        # Remove extra whitespace and newlines
        text = re.sub(r'\s+', ' ', text)
        # Remove non-breaking spaces and other special characters
        text = text.replace('\xa0', ' ').strip()
        return text

    async def fetch_page(self, url: str) -> Optional[str]:
        """Fetch a single page with error handling"""
        if not url or 'instagram.com' in url:
            return None
            
        # Clean up URL
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        try:
            async with self.session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                self.logger.warning(f"Failed to fetch {url} with status {response.status}")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            return None

    def extract_info(self, html: str, url: str) -> Dict:
        """Extract relevant information from HTML content"""
        if not html:
            return {}
            
        soup = BeautifulSoup(html, 'lxml')
        
        # Remove script and style elements
        for element in soup(['script', 'style']):
            element.decompose()
            
        info = {
            'hours': self._extract_hours(soup),
            'price_range': self._extract_price(soup),
            'description': self._extract_description(soup),
            'menu_url': self._extract_menu_url(soup, url),
            'phone': self._extract_phone(soup),
            'cuisine_type': self._extract_cuisine_type(soup),
            'features': self._extract_features(soup),
        }
        
        # Clean all string values
        return {k: self.clean_text(v) if isinstance(v, str) else v 
               for k, v in info.items() if v}

    def _extract_hours(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract business hours"""
        hours_patterns = [
            r'\b(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*[\s-]*(?:\d{1,2}(?::\d{2})?(?:am|pm|AM|PM)[-–]?\d{1,2}(?::\d{2})?(?:am|pm|AM|PM))',
            r'\b\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)\s*[-–]\s*\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM)'
        ]
        
        hours_elements = soup.find_all(['div', 'p', 'section'], 
                                     string=lambda text: text and any(
                                         re.search(pattern, text, re.I) 
                                         for pattern in hours_patterns
                                     ))
        
        if hours_elements:
            hours = []
            for element in hours_elements:
                text = element.get_text().strip()
                if len(text) < 200:  # Avoid capturing large blocks of text
                    hours.append(text)
            return ' | '.join(hours)
        return None

    def _extract_price(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract price indicators"""
        # Look for dollar signs
        price_patterns = [
            r'[\$]{1,4}',
            r'(?:price range|average price)[:\s]*(?:\$\d+)',
        ]
        
        for pattern in price_patterns:
            matches = []
            for element in soup.find_all(['div', 'p', 'span']):
                text = element.get_text().strip()
                match = re.search(pattern, text, re.I)
                if match:
                    matches.append(match.group(0))
            
            if matches:
                # Count $ signs in the first match
                dollars = matches[0].count('$')
                if dollars > 0:
                    return '$' * min(dollars, 4)
                
        return None

    def _extract_phone(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract phone number"""
        phone_pattern = r'\b(?:\+?1[-.]?)?\s*\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b'
        
        for element in soup.find_all(['div', 'p', 'span', 'a']):
            text = element.get_text().strip()
            match = re.search(phone_pattern, text)
            if match:
                return match.group(0)
        return None

    def _extract_cuisine_type(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract cuisine type"""
        cuisine_keywords = [
            'italian', 'japanese', 'chinese', 'thai', 'mexican', 'indian',
            'french', 'american', 'mediterranean', 'korean', 'vietnamese'
        ]
        
        for element in soup.find_all(['meta', 'div', 'p'], {'name': 'description'}):
            text = element.get_text().lower() if element.name != 'meta' else element.get('content', '').lower()
            found_cuisines = [cuisine for cuisine in cuisine_keywords if cuisine in text]
            if found_cuisines:
                return found_cuisines[0].title()
        return None

    def _extract_features(self, soup: BeautifulSoup) -> List[str]:
        """Extract restaurant features"""
        feature_keywords = [
            'delivery', 'takeout', 'outdoor seating', 'reservations',
            'wheelchair accessible', 'vegan', 'vegetarian', 'gluten-free',
            'full bar', 'wine', 'beer', 'cocktails'
        ]
        
        found_features = set()
        for element in soup.find_all(['div', 'p', 'span']):
            text = element.get_text().lower()
            for feature in feature_keywords:
                if feature in text:
                    found_features.add(feature)
        
        return list(found_features)

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract venue description"""
        # Try meta description first
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content']
            
        # Look for main content areas
        content_selectors = [
            'main p', '.about', '.description', '#about',
            '[class*="about"]', '[class*="description"]'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text().strip()
                if len(text) > 50:
                    return text[:500]  # Limit length
                    
        return None

    def _extract_menu_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """Extract menu URL"""
        menu_links = soup.find_all('a', href=True, string=lambda t: t and 'menu' in t.lower())
        if menu_links:
            menu_url = menu_links[0]['href']
            if not urlparse(menu_url).netloc:
                base_parts = urlparse(base_url)
                menu_url = f"{base_parts.scheme}://{base_parts.netloc}{menu_url}"
            return menu_url
        return None

    async def scrape_venue(self, venue_data: Dict) -> Dict:
        """Scrape a single venue's website"""
        url = venue_data.get('website')
        html = await self.fetch_page(url)
        if html:
            scraped_data = self.extract_info(html, url)
            # Combine with original venue data
            return {**venue_data, **scraped_data}
        return venue_data

    def save_results(self, results: Dict):
        """Save results to CSV file"""
        df = pd.DataFrame([results])
        
        if Path(self.output_file).exists():
            df.to_csv(self.output_file, mode='a', header=False, index=False)
        else:
            df.to_csv(self.output_file, index=False)

    async def scrape_venues(self, venues_df: pd.DataFrame, batch_size: int = 5):
        """Scrape venues in batches"""
        total_venues = len(venues_df)
        processed = 0
        
        for i in range(0, total_venues, batch_size):
            batch = venues_df.iloc[i:i + batch_size]
            
            # Process batch
            tasks = [self.scrape_venue(venue) for _, venue in batch.iterrows()]
            batch_results = await asyncio.gather(*tasks)
            
            # Save results
            for result in batch_results:
                if result:
                    self.save_results(result)
            
            processed += len(batch)
            self.logger.info(f"Processed {processed}/{total_venues} venues")
            
            # Small delay between batches
            await asyncio.sleep(1)

async def main():
    # Read venues from CSV
    df = pd.read_csv('places.csv')
    
    # Initialize scraper
    scraper = VenueWebsiteScraper(output_file='scraped_venues.csv')
    await scraper.initialize()
    
    try:
        start_time = time.time()
        await scraper.scrape_venues(df)
        print(f"\nScraping completed in {time.time() - start_time:.2f} seconds")
    
    finally:
        await scraper.close()

if __name__ == "__main__":
    asyncio.run(main())