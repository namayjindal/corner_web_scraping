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
        
    def _parse_reviews(self, reviews: Any) -> List[Dict[str, str]]:
        """Parse reviews ensuring no NaN values and handling string or list inputs."""
        if pd.isna(reviews) or reviews is None:
            return []
        
        # If reviews is already a list of dictionaries with 'text' and 'source'
        if isinstance(reviews, list) and all(isinstance(r, dict) and 'text' in r and 'source' in r for r in reviews if r):
            return [r for r in reviews if r]
        
        # If reviews is a string, try to parse it
        if isinstance(reviews, str):
            try:
                # First, try to parse as JSON
                parsed_reviews = json.loads(reviews)
                if isinstance(parsed_reviews, list):
                    return [{'text': str(review), 'source': 'unknown'} for review in parsed_reviews if review]
                return [{'text': reviews, 'source': 'unknown'}] if reviews else []
            except (json.JSONDecodeError, TypeError):
                # If not JSON, treat as a single review string
                return [{'text': reviews, 'source': 'unknown'}] if reviews else []
        
        # If reviews is a list (but not of dictionaries with the right structure)
        if isinstance(reviews, list):
            return [{'text': str(review), 'source': 'unknown'} for review in reviews if review and not pd.isna(review)]
        
        # For any other type, convert to string if not NaN
        return [{'text': str(reviews), 'source': 'unknown'}] if not pd.isna(reviews) else []

    def _is_nan_value(self, value: Any) -> bool:
        """Check if a value is NaN, handling various types."""
        if value is None:
            return True
        if isinstance(value, (float, int, bool, str)):
            return pd.isna(value)
        if isinstance(value, np.ndarray):
            return np.isnan(value).all()
        if isinstance(value, list):
            return len(value) == 0
        return False

    def _merge_reviews(self, google_reviews: Any, opentable_reviews: Any) -> List[Dict[str, str]]:
        """Combine reviews from Google and OpenTable."""
        merged_reviews = []
        
        # Parse Google reviews
        if google_reviews is not None and not self._is_nan_value(google_reviews):
            google_parsed = self._parse_reviews(google_reviews)
            for review in google_parsed:
                if isinstance(review, dict) and 'text' in review:
                    review['source'] = 'google'
                    merged_reviews.append(review)
                elif isinstance(review, str) or not pd.isna(review):
                    merged_reviews.append({'text': str(review), 'source': 'google'})

        # Parse OpenTable reviews
        if opentable_reviews is not None and not self._is_nan_value(opentable_reviews):
            opentable_parsed = self._parse_reviews(opentable_reviews)
            for review in opentable_parsed:
                if isinstance(review, dict) and 'text' in review:
                    review['source'] = 'opentable'
                    merged_reviews.append(review)
                elif isinstance(review, str) or not pd.isna(review):
                    merged_reviews.append({'text': str(review), 'source': 'opentable'})
                
        return merged_reviews

    def _format_hours(self, google_hours: Any, osm_hours: Any) -> Optional[Dict[str, Any]]:
        """Format business hours, falling back to OSM if Google is missing."""
        if pd.isna(google_hours) and pd.isna(osm_hours):
            return {}
            
        if not google_hours and osm_hours and not pd.isna(osm_hours):
            return {'source': 'osm', 'hours': osm_hours}
            
        return {'source': 'google', 'hours': google_hours} if google_hours and not pd.isna(google_hours) else {}

    def _ensure_string(self, value: Any) -> Optional[str]:
        """Ensure value is a string, handling NaN properly."""
        return str(value) if pd.notna(value) and value is not None else None

    def _get_safe_value(self, data: Any, key: str, default: Any = None) -> Any:
        """Safely retrieve values from dictionaries."""
        if not isinstance(data, dict) or pd.isna(data):
            return default
        return data.get(key, default)

    def _process_single_venue(self, base_data: Dict[str, Any], google_data: Dict[str, Any], opentable_data: Dict[str, Any], osm_data: Dict[str, Any], website_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and merge data for a single venue."""
        try:
            # Gather descriptions
            descriptions = [
                self._ensure_string(google_data.get('description')),
                self._ensure_string(opentable_data.get('description')),
                self._ensure_string(website_data.get('meta_description'))
            ]

            # Extract OpenStreetMap data
            osm_hours = self._get_safe_value(osm_data, 'opening_hours', '')
            osm_address = self._get_safe_value(osm_data, 'address', {})
            osm_lat = self._get_safe_value(osm_data, 'lat')
            osm_lon = self._get_safe_value(osm_data, 'lon')

            tags = base_data.get('tags', [])

            # Ensure tags is a list
            if isinstance(tags, str):
                try:
                    # Try to parse JSON string (like "{tag1,tag2}")
                    tags = json.loads(tags.replace('{', '[').replace('}', ']'))
                except:
                    tags = [tags]
            elif tags is None:
                tags = []

            meta_keywords = website_data.get('meta_keywords')

            # Ensure meta_keywords is a list
            if isinstance(meta_keywords, str):
                meta_keywords = [k.strip() for k in meta_keywords.split(',') if k.strip()]
            elif meta_keywords is None:
                meta_keywords = []

            # Now safely extend tags
            if isinstance(meta_keywords, list):
                tags = tags + meta_keywords if isinstance(tags, list) else meta_keywords

            # Handle price range
            price_range = google_data.get('price') or opentable_data.get('price_range') or ''

            # Handle cuisine
            cuisine = []
            if opentable_data.get('cuisine'):
                cuisine_str = opentable_data.get('cuisine')
                if isinstance(cuisine_str, str):
                    cuisine = [c.strip() for c in cuisine_str.split(',') if c.strip()]
                else:
                    cuisine = [cuisine_str] if cuisine_str else []

            # Handle hours
            hours = self._format_hours(google_data.get('hours'), osm_hours) or {}

            processed = {
                'corner_place_id': base_data['corner_place_id'],
                'name': base_data['name'],
                'google_id': base_data['google_id'],
                'neighborhood': base_data['neighborhood'],
                'category': google_data.get('category') or opentable_data.get('cuisine') or '',
                'hours': hours,
                'price_range': price_range,
                'description': ' '.join(filter(None, descriptions)),
                'reviews': self._merge_reviews(google_data.get('reviews', []), opentable_data.get('reviews', [])),
                'location': {
                    'address': osm_address if osm_address else '',
                    'lat': float(osm_lat) if osm_lat and not pd.isna(osm_lat) else None,
                    'lon': float(osm_lon) if osm_lon and not pd.isna(osm_lon) else None
                },
                'metadata': {
                    'website': base_data.get('website', ''),
                    'instagram': base_data.get('instagram_handle', ''),
                    'tags': tags,
                    'cuisines': cuisine
                }
            }
            return processed
        except Exception as e:
            logger.error(f"Error processing venue {base_data['name']}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _get_matching_record(self, df: pd.DataFrame, match_col: str, match_value: str) -> dict:
        """Retrieve matching record safely from DataFrame."""
        if df.empty:
            return {}
        matching_records = df[df[match_col] == match_value].to_dict('records')
        return matching_records[0] if matching_records else {}

    def clean_processed_data(self, data: List[Dict]) -> List[Dict]:
        """Clean the processed data to handle NaN and other issues."""
        
        # Function to recursively clean NaN values
        def clean_value(value):
            try:
                if isinstance(value, np.ndarray):
                    value = value.tolist()

                if isinstance(value, list):
                    return [clean_value(item) for item in value if not (isinstance(item, np.ndarray) and np.isnan(item).all()) and not pd.isna(item)]
                elif isinstance(value, dict):
                    return {k: clean_value(v) for k, v in value.items() if not (isinstance(v, np.ndarray) and np.isnan(v).all()) and not pd.isna(v)}
                elif isinstance(value, bool):
                    return value
                elif pd.isna(value):
                    return '' if isinstance(value, float) else []
                return value
            except Exception as e:
                logger.error(f"Error cleaning value: {value}")
                logger.error(f"Error details: {str(e)}")
                return value if not pd.isna(value) else ''

        # Clean each venue
        cleaned_data = []
        for venue in data:
            if not isinstance(venue, dict):
                continue
                
            cleaned_venue = clean_value(venue)
            
            # Ensure reviews is a list of dictionaries with text and source
            if 'reviews' in cleaned_venue:
                if not isinstance(cleaned_venue['reviews'], list):
                    cleaned_venue['reviews'] = []
                else:
                    # Ensure each review is properly structured
                    cleaned_reviews = []
                    for review in cleaned_venue['reviews']:
                        if isinstance(review, dict) and 'text' in review and 'source' in review:
                            cleaned_reviews.append(review)
                        elif isinstance(review, str):
                            cleaned_reviews.append({'text': review, 'source': 'unknown'})
                    cleaned_venue['reviews'] = cleaned_reviews
            
            # Ensure metadata exists and has valid cuisines
            if 'metadata' in cleaned_venue:
                if not isinstance(cleaned_venue['metadata'], dict):
                    cleaned_venue['metadata'] = {}
                    
                if 'cuisines' in cleaned_venue['metadata']:
                    if not isinstance(cleaned_venue['metadata']['cuisines'], list):
                        cleaned_venue['metadata']['cuisines'] = []
                    else:
                        cleaned_venue['metadata']['cuisines'] = [c for c in cleaned_venue['metadata']['cuisines'] if c]
                        
                if 'tags' in cleaned_venue['metadata']:
                    if not isinstance(cleaned_venue['metadata']['tags'], list):
                        cleaned_venue['metadata']['tags'] = []
            
            # Ensure hours is a dictionary
            if 'hours' in cleaned_venue and not isinstance(cleaned_venue['hours'], dict):
                cleaned_venue['hours'] = {}
                
            # Ensure location is a dictionary
            if 'location' in cleaned_venue and not isinstance(cleaned_venue['location'], dict):
                cleaned_venue['location'] = {}
                
            cleaned_data.append(cleaned_venue)
            
        return cleaned_data

    def process_data(self, base_csv: str, google_data_csv: str, opentable_csv: str, osm_csv: str, website_data_json: str, output_file: str) -> None:
        """Process and integrate all data sources."""
        try:
            logger.info("Starting data integration process...")
            
            # Load datasets
            base_df = pd.read_csv(base_csv)
            google_df = pd.read_csv(google_data_csv) if Path(google_data_csv).exists() else pd.DataFrame()
            opentable_df = pd.read_csv(opentable_csv) if Path(opentable_csv).exists() else pd.DataFrame()
            osm_df = pd.read_csv(osm_csv) if Path(osm_csv).exists() else pd.DataFrame()

            with open(website_data_json, 'r', encoding='utf-8') as f:
                website_data = json.load(f)
            website_data = {item['url']: item for item in website_data}

            processed_venues = []
            for idx, base_venue in base_df.iterrows():
                logger.info(f"Processing venue {idx + 1}/{len(base_df)}: {base_venue['name']}")
                
                google_venue = self._get_matching_record(google_df, 'google_id', base_venue['google_id'])
                opentable_venue = self._get_matching_record(opentable_df, 'corner_place_id', base_venue['corner_place_id'])
                osm_venue = self._get_matching_record(osm_df, 'corner_place_id', base_venue['corner_place_id'])
                website_venue = website_data.get(base_venue['website'], {}) if base_venue['website'] else {}

                processed_venue = self._process_single_venue(base_venue.to_dict(), google_venue, opentable_venue, osm_venue, website_venue)

                if processed_venue:
                    processed_venues.append(processed_venue)

            # Clean the processed data
            logger.info("Cleaning processed data...")
            cleaned_venues = self.clean_processed_data(processed_venues)

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(cleaned_venues, f, ensure_ascii=False, indent=2)

            logger.info(f"Processing complete: {len(cleaned_venues)} venues processed and saved to {output_file}")
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
        output_file='cleaned_integrated_data.json'
    )