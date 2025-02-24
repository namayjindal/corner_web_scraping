import pandas as pd
import json
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import logging
from typing import Dict, List, Any
import re
import os
import ast

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataMigrator:
    def __init__(self, db_config: Dict[str, str]):
        """Initialize database connection and prepare for migration"""
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        self.combined_data = []
        self.use_pgvector = True  # Flag to control pgvector usage

    def setup_database(self):
        """Create database schema with fallback if pgvector isn't available"""
        try:
            # Try to create pgvector extension
            self.cur.execute("CREATE EXTENSION IF NOT EXISTS pgvector;")
            self.conn.commit()
            logger.info("pgvector extension created successfully")
        except Exception as e:
            logger.warning(f"Could not create pgvector extension: {str(e)}")
            logger.warning("Continuing without vector search capabilities")
            self.use_pgvector = False
            self.conn.rollback()

        # Create places table - removed lat and lon fields
        places_table = """
        CREATE TABLE IF NOT EXISTS places (
            id SERIAL PRIMARY KEY,
            corner_place_id VARCHAR(255) UNIQUE NOT NULL,
            google_id VARCHAR(255),
            name VARCHAR(255) NOT NULL,
            neighborhood VARCHAR(255),
            website VARCHAR(255),
            instagram_handle VARCHAR(255),
            price_range VARCHAR(50),
            combined_description TEXT,
            tags TEXT[],
            address TEXT,
            hours JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_places_corner_id ON places(corner_place_id);
        CREATE INDEX IF NOT EXISTS idx_places_neighborhood ON places(neighborhood);
        """
        
        self.cur.execute(places_table)
        self.conn.commit()
        
        # Create reviews table
        reviews_table = """
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            place_id INTEGER REFERENCES places(id),
            source VARCHAR(50),
            review_text TEXT,
            posted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_reviews_place_id ON reviews(place_id);
        """
        
        self.cur.execute(reviews_table)
        self.conn.commit()
        
        # Create embeddings table only if pgvector is available
        if self.use_pgvector:
            embeddings_table = """
            CREATE TABLE IF NOT EXISTS embeddings (
                id SERIAL PRIMARY KEY,
                place_id INTEGER REFERENCES places(id),
                embedding vector(1536),
                content_type VARCHAR(50),
                last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.cur.execute(embeddings_table)
            self.conn.commit()
            
        logger.info("Database schema created successfully")

    def combine_descriptions(self, place_data: Dict[str, Any]) -> str:
        """Combine descriptions from different sources"""
        descriptions = []
        
        # Google description
        if 'description' in place_data and place_data['description']:
            descriptions.append(("Google Maps", place_data['description']))
            
        # Resy description (including why_we_like_it and about sections)
        if 'resy_data' in place_data and place_data['resy_data']:
            resy = place_data['resy_data']
            if isinstance(resy, dict):
                if resy.get('why_we_like_it'):
                    descriptions.append(("Resy Highlight", resy['why_we_like_it']))
                if resy.get('about'):
                    descriptions.append(("About", resy['about']))
                
        # Website description
        if 'meta_description' in place_data and place_data['meta_description']:
            descriptions.append(("Website", place_data['meta_description']))
            
        # OpenTable description
        if 'opentable_description' in place_data and place_data['opentable_description']:
            descriptions.append(("OpenTable", place_data['opentable_description']))

        # Combine all descriptions with source attribution
        combined = "\n\n".join([f"{source}: {desc}" for source, desc in descriptions if desc])
        return combined if combined else None

    def parse_reviews_list(self, reviews_data):
        """Parse reviews that might be in various formats"""
        if not reviews_data:
            return []
            
        if isinstance(reviews_data, list):
            return reviews_data
            
        if isinstance(reviews_data, str):
            # Try to parse as JSON or list literal
            try:
                return json.loads(reviews_data)
            except:
                try:
                    return ast.literal_eval(reviews_data)
                except:
                    # If single string, return as single item list
                    return [reviews_data]
        
        # If we can't parse it, return empty list
        return []

    def combine_reviews(self, place_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Combine reviews from different sources"""
        combined_reviews = []
        
        # Google reviews
        if 'reviews' in place_data and place_data['reviews']:
            reviews = self.parse_reviews_list(place_data['reviews'])
            for review in reviews:
                if review:
                    combined_reviews.append({
                        'source': 'google',
                        'review_text': str(review),
                        'posted_at': datetime.now()
                    })
                
        # OpenTable reviews
        if 'opentable_reviews' in place_data and place_data['opentable_reviews']:
            reviews = self.parse_reviews_list(place_data['opentable_reviews'])
            for review in reviews:
                if review:
                    combined_reviews.append({
                        'source': 'opentable',
                        'review_text': str(review),
                        'posted_at': datetime.now()
                    })
                
        return combined_reviews

    def clean_unicode(self, text):
        """Remove or replace unwanted Unicode characters"""
        if not text:
            return text
            
        # Replace special Unicode spaces, dashes and quotes
        text = text.replace('\u200b', '')  # zero-width space
        text = text.replace('\u2009', ' ')  # thin space
        text = text.replace('\u2013', '-')  # en dash
        text = text.replace('\u2014', '-')  # em dash
        text = text.replace('\u201c', '"')  # left double quote
        text = text.replace('\u201d', '"')  # right double quote
        text = text.replace('\u2018', "'")  # left single quote
        text = text.replace('\u2019', "'")  # right single quote
        text = text.replace('\u2026', '...')  # ellipsis
        text = text.replace('\u200e', '')  # left-to-right mark
        text = text.replace('\u200f', '')  # right-to-left mark
        text = text.replace('\ufeff', '')  # zero width no-break space
        
        # Replace Unicode dollars with ASCII
        text = text.replace('\u0024', '$')  # dollar sign
        text = text.replace('\ufe69', '$')  # small dollar sign
        text = text.replace('\uff04', '$')  # fullwidth dollar sign
        
        # Clean common Unicode ranges but preserve basic punctuation
        return text

    def extract_tags(self, place_data: Dict[str, Any]) -> List[str]:
        """Extract and combine tags from different sources"""
        tags = set()
        
        # Extract from original tags
        if 'tags' in place_data and place_data['tags']:
            if isinstance(place_data['tags'], str):
                # Handle JSON string format like "{tag1,tag2}"
                tag_str = place_data['tags'].strip('{}')
                tags.update([t.strip('"').strip("'").strip() for t in tag_str.split(',') if t.strip()])
            elif isinstance(place_data['tags'], list):
                for tag in place_data['tags']:
                    if isinstance(tag, str):
                        # Clean up tags that might contain JSON or braces
                        clean_tag = tag.replace('{', '').replace('}', '').replace("'", '').strip()
                        if clean_tag and clean_tag not in ('', ','):
                            tags.add(clean_tag)
            
        # Extract from OSM data
        if 'extratags' in place_data and place_data['extratags']:
            extratags = place_data['extratags']
            
            if isinstance(extratags, str):
                try:
                    # Try to parse as JSON string
                    extratags = json.loads(extratags.replace("'", '"'))
                except:
                    # If not valid JSON, try simple parsing
                    if ":" in extratags:
                        parts = extratags.split(":")
                        tags.add(parts[0].strip("'").strip())
            
            if isinstance(extratags, dict):
                # Extract relevant tags from OSM extras
                relevant_keys = ['cuisine', 'amenity', 'shop', 'leisure']
                for key in relevant_keys:
                    if key in extratags and extratags[key]:
                        if isinstance(extratags[key], str):
                            # Split values that might contain multiple tags
                            for value in extratags[key].split(';'):
                                tags.add(value.strip())
                        
        # Add Google category as a tag
        if 'category' in place_data and place_data['category']:
            tags.add(place_data['category'])
                        
        # Clean tags
        cleaned_tags = [tag.strip().lower() for tag in tags if tag and not tag.startswith("{")]
        return list(set(cleaned_tags))  # Remove duplicates

    def clean_hours_dict(self, hours_dict):
        """Clean Unicode characters from hours dictionary"""
        if not hours_dict:
            return hours_dict
            
        cleaned_hours = {}
        for day, hours in hours_dict.items():
            if isinstance(hours, str):
                # Clean the hours string
                clean_hours = self.clean_unicode(hours)
                
                # Standardize the format (e.g., $10-20 -> $10-$20)
                clean_hours = clean_hours.replace('–', '-')  # Standardize dash
                clean_hours = re.sub(r'(\d+)\s*-\s*(\d+)', r'\1-\2', clean_hours)  # Remove spaces around dash
                
                cleaned_hours[day] = clean_hours
            else:
                cleaned_hours[day] = hours
                
        return cleaned_hours

    def process_hours(self, place_data: Dict[str, Any]) -> Dict:
        """Process and standardize hours from different sources"""
        if 'hours' in place_data and place_data['hours']:
            hours_data = place_data['hours']
            
            if isinstance(hours_data, dict):
                return self.clean_hours_dict(hours_data)
            elif isinstance(hours_data, str):
                # Try to parse string format
                try:
                    hours_dict = json.loads(hours_data.replace("'", '"'))
                    return self.clean_hours_dict(hours_dict)
                except:
                    return {'raw_hours': self.clean_unicode(hours_data)}
        
        # Check for the business_hours field from website scraping
        if 'business_hours' in place_data and place_data['business_hours']:
            return {'business_hours': self.clean_unicode(place_data['business_hours'])}
            
        return None

    def clean_price_range(self, price_range):
        """Clean and standardize price range format"""
        if not price_range:
            return None
            
        # Clean Unicode
        price = self.clean_unicode(str(price_range))
        
        # Replace Unicode characters
        price = price.replace('\u2013', '-')  # en dash
        price = price.replace('\u2014', '-')  # em dash
        price = price.replace('\u201c', '"')  # left double quote
        price = price.replace('\u201d', '"')  # right double quote
        
        return price

    def read_scraped_website_data(self):
        """Read scraped website data if available"""
        try:
            with open('scraped_data.json', 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not read scraped website data: {str(e)}")
            return []

    def fix_nan_values(self, df):
        """Replace NaN values with None for better JSON serialization"""
        return df.where(pd.notna(df), None)

    def migrate_data(self):
        """Main migration function"""
        try:
            # Read all data sources
            places_df = pd.read_csv('places.csv')
            places_df = self.fix_nan_values(places_df)
            
            # Read Google data if available
            try:
                google_df = pd.read_csv('places_with_google_data.csv')
                google_df = self.fix_nan_values(google_df)
                # Create a dictionary for faster lookup
                google_data = {}
                for _, row in google_df.iterrows():
                    corner_id = str(row['corner_place_id'])
                    google_data[corner_id] = row.to_dict()
                    
                logger.info(f"Loaded Google data for {len(google_data)} places")
            except Exception as e:
                logger.warning(f"Could not read Google data: {str(e)}")
                google_data = {}
            
            # Read OSM data if available
            try:
                osm_df = pd.read_csv('places_with_osm.csv')
                osm_df = self.fix_nan_values(osm_df)
                # Create a dictionary for faster lookup
                osm_data = {}
                for _, row in osm_df.iterrows():
                    corner_id = str(row['corner_place_id'])
                    osm_data[corner_id] = row.to_dict()
                    
                logger.info(f"Loaded OSM data for {len(osm_data)} places")
            except Exception as e:
                logger.warning(f"Could not read OSM data: {str(e)}")
                osm_data = {}
            
            # Read OpenTable data if available
            try:
                opentable_df = pd.read_csv('opentable_results.csv')
                opentable_df = self.fix_nan_values(opentable_df)
                # Create a dictionary for faster lookup
                opentable_data = {}
                for _, row in opentable_df.iterrows():
                    if row['found']:  # Only include found places
                        corner_id = str(row['corner_place_id'])
                        opentable_data[corner_id] = row.to_dict()
                        
                logger.info(f"Loaded OpenTable data for {len(opentable_data)} places")
            except Exception as e:
                logger.warning(f"Could not read OpenTable data: {str(e)}")
                opentable_data = {}
            
            # Read Resy data if available
            resy_dict = {}
            try:
                with open('resy_data.json', 'r') as f:
                    resy_data = json.load(f)
                for item in resy_data:
                    if 'corner_place_id' in item:
                        resy_dict[str(item['corner_place_id'])] = item
                        
                logger.info(f"Loaded Resy data for {len(resy_dict)} places")
            except Exception as e:
                logger.warning(f"Could not read Resy data: {str(e)}")
                
            # Read website scraping data
            website_data = self.read_scraped_website_data()
            website_dict = {}
            for item in website_data:
                if 'corner_place_id' in item:
                    website_dict[str(item['corner_place_id'])] = item
                    
            logger.info(f"Loaded website data for {len(website_dict)} places")
            
            # Process each place
            for _, place in places_df.iterrows():
                corner_id = str(place['corner_place_id'])
                logger.info(f"Processing place {corner_id}: {place['name']}")
                
                # Gather all data for this place
                place_data = {
                    'corner_place_id': corner_id,
                    'google_id': place['google_id'],
                    'name': place['name'],
                    'neighborhood': place.get('neighborhood'),
                    'website': place.get('website'),
                    'instagram_handle': place.get('instagram_handle'),
                    'tags': place.get('tags'),
                }
                
                # Add Google data
                if corner_id in google_data:
                    google_row = google_data[corner_id]
                    place_data.update({
                        'description': google_row.get('description'),
                        'reviews': google_row.get('reviews'),
                        'price_range': self.clean_price_range(google_row.get('price')),
                        'hours': google_row.get('hours'),
                        'category': google_row.get('category')
                    })
                    logger.info(f"Added Google data for {place['name']}")
                
                # Add OSM data
                if corner_id in osm_data:
                    osm_row = osm_data[corner_id]
                    place_data.update({
                        'address': osm_row.get('display_name'),
                        'extratags': osm_row.get('extratags'),
                    })
                    logger.info(f"Added OSM data for {place['name']}")
                
                # Add OpenTable data
                if corner_id in opentable_data:
                    ot_row = opentable_data[corner_id]
                    place_data.update({
                        'opentable_reviews': ot_row.get('reviews'),
                        'opentable_description': ot_row.get('description'),
                        'price_range': place_data.get('price_range') or self.clean_price_range(ot_row.get('price_range')),
                        'cuisine': ot_row.get('cuisine')
                    })
                    logger.info(f"Added OpenTable data for {place['name']}")
                
                # Add Resy data
                place_data['resy_data'] = resy_dict.get(corner_id, {})
                if corner_id in resy_dict:
                    logger.info(f"Added Resy data for {place['name']}")
                
                # Add website scraping data
                if corner_id in website_dict:
                    for key, value in website_dict[corner_id].items():
                        if key not in ['corner_place_id', 'url']:
                            place_data[key] = value
                    logger.info(f"Added website data for {place['name']}")
                
                # Process combined fields
                place_data['combined_description'] = self.combine_descriptions(place_data)
                place_data['reviews'] = self.combine_reviews(place_data)
                place_data['tags'] = self.extract_tags(place_data)
                place_data['hours'] = self.process_hours(place_data)
                
                # Save to database
                self.save_to_db(place_data)
                
                # Add to combined JSON (with simplified reviews for better readability)
                json_data = place_data.copy()
                if 'reviews' in json_data and json_data['reviews']:
                    json_data['reviews'] = [review['review_text'] for review in json_data['reviews']]
                # Remove rating field completely
                if 'rating' in json_data:
                    del json_data['rating']
                self.combined_data.append(json_data)
                
            # Save combined JSON
            with open('combined_data.json', 'w') as f:
                json.dump(self.combined_data, f, indent=2, ensure_ascii=False)
                
            logger.info("Data migration completed successfully")
            
        except Exception as e:
            logger.error(f"Error during migration: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            self.conn.rollback()
            raise
        finally:
            self.cur.close()
            self.conn.close()

    def save_to_db(self, place_data: Dict[str, Any]):
        """Save processed data to database - removed lat and lon fields"""
        try:
            # Insert place - removed lat and lon from the query
            place_query = """
            INSERT INTO places (
                corner_place_id, google_id, name, neighborhood, website, instagram_handle,
                price_range, combined_description, tags, address, hours
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (corner_place_id) DO UPDATE SET
                google_id = EXCLUDED.google_id,
                name = EXCLUDED.name,
                neighborhood = EXCLUDED.neighborhood,
                website = EXCLUDED.website,
                instagram_handle = EXCLUDED.instagram_handle,
                price_range = EXCLUDED.price_range,
                combined_description = EXCLUDED.combined_description,
                tags = EXCLUDED.tags,
                address = EXCLUDED.address,
                hours = EXCLUDED.hours,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
            """
            
            # Convert hours to JSON string if it's a dict
            hours_json = None
            if place_data.get('hours'):
                if isinstance(place_data['hours'], dict):
                    hours_json = json.dumps(place_data['hours'])
                elif isinstance(place_data['hours'], str):
                    hours_json = place_data['hours']
            
            self.cur.execute(place_query, (
                place_data['corner_place_id'],
                place_data.get('google_id'),
                place_data['name'],
                place_data.get('neighborhood'),
                place_data.get('website'),
                place_data.get('instagram_handle'),
                place_data.get('price_range'),
                place_data.get('combined_description'),
                place_data.get('tags'),
                place_data.get('address'),
                hours_json
            ))
            
            place_id = self.cur.fetchone()[0]
            
            # Insert reviews
            if place_data.get('reviews'):
                review_query = """
                INSERT INTO reviews (place_id, source, review_text, posted_at)
                VALUES (%s, %s, %s, %s);
                """
                
                review_data = [
                    (place_id, review['source'], review['review_text'], review['posted_at'])
                    for review in place_data['reviews']
                ]
                
                execute_batch(self.cur, review_query, review_data)
            
            self.conn.commit()
            logger.info(f"Saved place: {place_data['name']} (ID: {place_id})")
            
        except Exception as e:
            logger.error(f"Error saving data for {place_data['name']}: {str(e)}")
            self.conn.rollback()
            raise


if __name__ == "__main__":
    # Use your macOS username instead of "postgres"
    db_config = {
        "dbname": "corner_db",
        "user": "namayjindal",  # Your macOS username - adjust if needed
        "password": "",         # Usually empty on macOS
        "host": "localhost"
    }
    
    migrator = DataMigrator(db_config)
    migrator.setup_database()
    migrator.migrate_data()