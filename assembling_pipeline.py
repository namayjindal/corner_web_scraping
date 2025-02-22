import pandas as pd
import json
from pathlib import Path
import logging
from datetime import datetime
import traceback
from typing import Dict, List, Any, Optional
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_integration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataIntegrationPipeline:
    def __init__(self, raw_data_dir: str = 'raw_data'):
        """Initialize the data integration pipeline."""
        self.raw_data_dir = Path(raw_data_dir)
        self.raw_data_dir.mkdir(exist_ok=True)
        
        # Track processing statistics
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'errors': 0,
            'missing_data': {}
        }

    def _save_raw_data(self, data: Any, source: str):
        """Save raw data files with timestamp."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{source}_{timestamp}.json"
        
        with open(self.raw_data_dir / filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _parse_reviews(self, reviews_str: Any) -> List[str]:
        """Parse reviews from string or handle NaN values."""
        if pd.isna(reviews_str):
            return []
            
        if isinstance(reviews_str, str):
            try:
                # Handle string representation of list
                # Remove literal list string indicators and split on commas
                clean_str = reviews_str.strip('[]').replace("'", '"')
                if clean_str:
                    return json.loads(f"[{clean_str}]")
            except json.JSONDecodeError:
                logger.warning(f"Could not parse reviews string: {reviews_str[:100]}...")
                return []
                
        if isinstance(reviews_str, list):
            return reviews_str
            
        return []

    def _merge_reviews(self, google_reviews: Any, opentable_reviews: Any) -> List[Dict[str, str]]:
        """Merge and deduplicate reviews from different sources."""
        merged_reviews = []
        
        # Parse and add Google reviews
        google_reviews_list = self._parse_reviews(google_reviews)
        if google_reviews_list:
            merged_reviews.extend([{
                'text': review,
                'source': 'google'
            } for review in google_reviews_list if review])
            
        # Parse and add OpenTable reviews
        opentable_reviews_list = self._parse_reviews(opentable_reviews)
        if opentable_reviews_list:
            merged_reviews.extend([{
                'text': review,
                'source': 'opentable'
            } for review in opentable_reviews_list if review])
                
        return merged_reviews

    def _format_hours(self, hours_data: Any) -> Dict[str, str]:
        """Standardize hours format from various sources."""
        if not hours_data:
            return None
            
        if isinstance(hours_data, str):
            try:
                hours_data = json.loads(hours_data.replace("'", '"'))
            except:
                return {'raw_hours': hours_data}
                
        if isinstance(hours_data, dict):
            return {
                day: hours for day, hours in hours_data.items()
                if day and hours and isinstance(hours, str)
            }
            
        return None

    def _extract_price_range(self, 
                           google_price: Optional[str], 
                           opentable_price: Optional[str]) -> str:
        """Normalize price range from different sources."""
        if google_price and isinstance(google_price, str):
            return google_price
        if opentable_price and isinstance(opentable_price, str):
            return opentable_price
        return None

    def _ensure_string(self, value: Any) -> Optional[str]:
        """Convert value to string if it exists and isn't None."""
        if pd.isna(value) or value is None:
            return None
        return str(value)

    def _get_safe_value(self, data: Any, key: str, default: Any = None) -> Any:
        """Safely get value from data object which might be a float/string/dict."""
        if isinstance(data, (float, str)) or pd.isna(data):
            return default
        return data.get(key, default)

    def _process_single_venue(self, 
                            base_data: Dict[str, Any],
                            google_data: Dict[str, Any],
                            opentable_data: Dict[str, Any],
                            osm_data: Dict[str, Any],
                            website_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and merge data for a single venue."""
        try:
            # Get descriptions and ensure they're strings
            descriptions = [
                self._ensure_string(google_data.get('description')),
                self._ensure_string(opentable_data.get('description')),
                self._ensure_string(website_data.get('meta_description'))
            ]
            
            # Extract OSM data safely
            osm_address = self._get_safe_value(osm_data, 'address', {})
            osm_extratags = self._get_safe_value(osm_data, 'extratags', {})
            osm_lat = self._get_safe_value(osm_data, 'lat')
            osm_lon = self._get_safe_value(osm_data, 'lon')
            
            processed = {
                'corner_place_id': base_data['corner_place_id'],
                'name': base_data['name'],
                'google_id': base_data['google_id'],
                'neighborhood': base_data['neighborhood'],
                
                # Category (prioritize Google's category)
                'category': (
                    google_data.get('category') or 
                    opentable_data.get('cuisine') or
                    (base_data.get('tags', [])[0] if base_data.get('tags') else None)
                ),
                
                # Hours
                'hours': self._format_hours(google_data.get('hours')),
                
                # Price range
                'price_range': self._extract_price_range(
                    google_data.get('price'),
                    opentable_data.get('price_range')
                ),
                
                # Description
                'description': ' '.join(filter(None, descriptions)),
                
                # Reviews
                'reviews': self._merge_reviews(
                    google_data.get('reviews', []),
                    opentable_data.get('reviews', [])
                ),
                
                # Rating
                'rating': google_data.get('rating'),
                
                # Location
                'location': {
                    'address': osm_address,
                    'lat': float(osm_lat) if osm_lat and not pd.isna(osm_lat) else None,
                    'lon': float(osm_lon) if osm_lon and not pd.isna(osm_lon) else None
                },
                
                # Metadata
                'metadata': {
                    'website': base_data.get('website'),
                    'instagram': base_data.get('instagram_handle'),
                    'tags': base_data.get('tags', []),
                    'cuisines': [
                        cuisine for cuisine in [
                            opentable_data.get('cuisine'),
                            self._get_safe_value(osm_extratags, 'cuisine')
                        ] if cuisine
                    ],
                    'opening_hours_url': website_data.get('business_hours'),
                    'keywords': website_data.get('meta_keywords')
                }
            }
            
            return processed
                
        except Exception as e:
            logger.error(f"Error processing venue {base_data['name']}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _get_matching_record(self, df: pd.DataFrame, match_col: str, match_value: str) -> dict:
        """Safely get matching record from DataFrame."""
        if df.empty:
            return {}
            
        matching_records = df[df[match_col] == match_value].to_dict('records')
        return matching_records[0] if matching_records else {}

    def process_data(self,
                    base_csv: str,
                    google_data_csv: str,
                    opentable_csv: str,
                    osm_csv: str,
                    website_data_json: str,
                    output_file: str) -> None:
        """
        Main method to process and integrate all data sources.
        """
        try:
            logger.info("Starting data integration process...")
            
            # Load all data sources
            base_df = pd.read_csv(base_csv)
            google_df = pd.read_csv(google_data_csv) if Path(google_data_csv).exists() else pd.DataFrame()
            opentable_df = pd.read_csv(opentable_csv) if Path(opentable_csv).exists() else pd.DataFrame()
            osm_df = pd.read_csv(osm_csv) if Path(osm_csv).exists() else pd.DataFrame()
            
            with open(website_data_json, 'r', encoding='utf-8') as f:
                website_data = json.load(f)
            website_data = {item['url']: item for item in website_data}
            
            # Save raw data
            self._save_raw_data({
                'google': google_df.to_dict('records'),
                'opentable': opentable_df.to_dict('records'),
                'osm': osm_df.to_dict('records'),
                'website': website_data
            }, 'raw_data')
            
            # Process each venue
            processed_venues = []
            total_venues = len(base_df)
            
            for idx, base_venue in base_df.iterrows():
                try:
                    logger.info(f"Processing venue {idx + 1}/{total_venues}: {base_venue['name']}")
                    
                    # Get corresponding data from each source
                    google_venue = self._get_matching_record(google_df, 'google_id', base_venue['google_id'])
                    opentable_venue = self._get_matching_record(opentable_df, 'corner_place_id', base_venue['corner_place_id'])
                    osm_venue = self._get_matching_record(osm_df, 'corner_place_id', base_venue['corner_place_id'])
                    website_venue = website_data.get(base_venue['website'], {}) if base_venue['website'] else {}
                    
                    # Process venue
                    processed_venue = self._process_single_venue(
                        base_venue.to_dict(),
                        google_venue,
                        opentable_venue,
                        osm_venue,
                        website_venue
                    )
                    
                    if processed_venue:
                        processed_venues.append(processed_venue)
                        self.stats['successful'] += 1
                    else:
                        self.stats['errors'] += 1
                        
                    self.stats['total_processed'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing venue {base_venue['name']}: {str(e)}")
                    logger.error(traceback.format_exc())
                    self.stats['errors'] += 1
                    self.stats['total_processed'] += 1
                    continue
            
            # Save processed data
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(processed_venues, f, ensure_ascii=False, indent=2)
            
            # Log statistics
            logger.info("\nProcessing completed!")
            logger.info(f"Total processed: {self.stats['total_processed']}")
            logger.info(f"Successful: {self.stats['successful']}")
            logger.info(f"Errors: {self.stats['errors']}")
            
            if processed_venues:
                # Create a summary DataFrame
                summary_df = pd.DataFrame(processed_venues)
                logger.info("\nData Summary:")
                logger.info(f"Total venues: {len(summary_df)}")
                logger.info(f"Venues with reviews: {sum(1 for v in processed_venues if v.get('reviews'))}")
                logger.info(f"Venues with hours: {sum(1 for v in processed_venues if v.get('hours'))}")
                logger.info(f"Venues with price range: {sum(1 for v in processed_venues if v.get('price_range'))}")
            else:
                logger.warning("No venues were successfully processed!")
            
        except Exception as e:
            logger.error(f"Critical error in data integration: {str(e)}")
            logger.error(traceback.format_exc())
            raise

if __name__ == "__main__":
    pipeline = DataIntegrationPipeline()
    pipeline.process_data(
        base_csv='places.csv',
        google_data_csv='places_with_google_data.csv',
        opentable_csv='opentable_results.csv',
        osm_csv='places_with_osm.csv',
        website_data_json='scraped_data.json',
        output_file='integrated_data.json'
    )