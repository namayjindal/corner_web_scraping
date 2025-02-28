import os
import json
import logging
import psycopg2
import pandas as pd
import time
import re
import hashlib
from psycopg2.extras import execute_batch
from openai import OpenAI
from datetime import datetime
from dotenv import load_dotenv
import traceback

# Import the location extraction functionality
from location_extraction import extract_location_from_query, get_adjacent_neighborhoods

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("embeddings.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EmbeddingGenerator:
    def __init__(self, db_config):
        """Initialize database configuration and OpenAI client"""
        self.db_config = db_config
        
        # Set up OpenAI client
        openai_api_key = os.getenv("OPENAI_KEY")
        if not openai_api_key:
            raise ValueError("OPENAI_KEY environment variable not set")
        
        self.client = OpenAI(api_key=openai_api_key)
        self.model = "text-embedding-ada-002"  # Default embedding model
        
        # Keep track of tokens used for cost estimation
        self.total_tokens = 0
        self.has_pgvector = self._check_pgvector()
    
    def _connect_db(self):
        """Create and return a new database connection and cursor"""
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        return conn, cur
    
    def _check_pgvector(self):
        """Check if pgvector extension is installed"""
        conn, cur = self._connect_db()
        try:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
            has_pgvector = bool(cur.fetchone())
            if not has_pgvector:
                logger.warning("pgvector extension not installed. Embeddings will not be stored.")
            return has_pgvector
        except Exception as e:
            logger.error(f"Error checking pgvector: {str(e)}")
            return False
        finally:
            cur.close()
            conn.close()

    def clean_price_range(self, price_range):
        """Clean and standardize price range format"""
        if not price_range:
            return None
            
        # If already a string, clean it
        if isinstance(price_range, str):
            # Remove Unicode characters
            price = price_range.replace('\u2013', '-')  # en dash
            price = price.replace('\u2014', '-')  # em dash
            price = price.replace('\u201c', '"')  # left double quote
            price = price.replace('\u201d', '"')  # right double quote
            price = price.replace('\u2018', "'")  # left single quote
            price = price.replace('\u2019', "'")  # right single quote
            
            # Standardize format
            price = re.sub(r'\s+', ' ', price).strip()  # Remove extra spaces
            
            return pricep
        
        # If it's a number, format it
        if isinstance(price_range, (int, float)):
            return f"${price_range}"
        
        return None

    def process_price_range(self, price_range):
        """Process and add semantic meaning to price range indicators"""
        if not price_range:
            return None
            
        # Clean the price range
        price = self.clean_price_range(price_range)
        if not price:
            return None
        
        # Extract dollar signs if present
        dollar_count = price.count('$')
        if dollar_count > 0:
            price_level = dollar_count
        else:
            # Try to extract numerical ranges (e.g. $10-20, $30-50)
            match = re.search(r'\$?(\d+)(?:[^\d]+)(\d+)', price)
            if match:
                low, high = int(match.group(1)), int(match.group(2))
                avg_price = (low + high) / 2
                if avg_price < 15:
                    price_level = 1
                elif avg_price < 30:
                    price_level = 2
                elif avg_price < 60:
                    price_level = 3
                else:
                    price_level = 4
            else:
                # Try to extract single values
                match = re.search(r'\$?(\d+)', price)
                if match:
                    value = int(match.group(1))
                    if value < 15:
                        price_level = 1
                    elif value < 30:
                        price_level = 2
                    elif value < 60:
                        price_level = 3
                    else:
                        price_level = 4
                else:
                    # If we can't determine, assume mid-range
                    price_level = 2
        
        # Map price levels to descriptive text
        price_descriptions = {
            1: "Budget-friendly, inexpensive, affordable",
            2: "Moderately priced, mid-range",
            3: "Higher-end, upscale, expensive",
            4: "Fine dining, premium, luxury, high-end"
        }
        
        return {
            "original": price,
            "level": price_level,
            "description": price_descriptions.get(price_level, "")
        }

    def process_business_hours(self, hours_data):
        """Process business hours to extract meaningful patterns"""
        if not hours_data:
            return None
            
        parsed_hours = self.parse_hours(hours_data)
        if not parsed_hours:
            return None
        
        hour_patterns = {
            "open_late": False,
            "open_early": False,
            "open_weekends": False,
            "open_breakfast": False,
            "open_lunch": False,
            "open_dinner": False,
            "open_24h": False,
            "closed_mondays": False,
            "days_open": []
        }
        
        # Convert hours to a standardized format for analysis
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_abbrevs = {"Mon": "Monday", "Tue": "Tuesday", "Wed": "Wednesday", "Thu": "Thursday", 
                    "Fri": "Friday", "Sat": "Saturday", "Sun": "Sunday"}
        
        if isinstance(parsed_hours, dict):
            for day, hours_str in parsed_hours.items():
                if hours_str == "Closed":
                    continue
                    
                # Standardize day name
                day_name = day
                for abbrev, full_name in day_abbrevs.items():
                    if abbrev in day or abbrev.lower() in day.lower():
                        day_name = full_name
                        break
                
                hour_patterns["days_open"].append(day_name)
                
                # Check for specific patterns in hours
                if "24" in hours_str:
                    hour_patterns["open_24h"] = True
                    continue
                    
                # Parse actual opening and closing times
                time_patterns = [
                    # 12-hour format: 10 AM to 10 PM
                    r'(\d+(?::\d+)?)\s*([aApP][mM])\s*(?:to|[-–—])\s*(\d+(?::\d+)?)\s*([aApP][mM])',
                    # 24-hour format: 10:00-22:00
                    r'(\d+):(\d+)\s*(?:to|[-–—])\s*(\d+):(\d+)',
                    # Simple format: 10-22
                    r'(\d+)\s*(?:to|[-–—])\s*(\d+)'
                ]
                
                for pattern in time_patterns:
                    match = re.search(pattern, hours_str)
                    if match:
                        # 12-hour format
                        if len(match.groups()) == 4 and match.group(2) and match.group(4):
                            open_hour = int(match.group(1).split(':')[0])
                            close_hour = int(match.group(3).split(':')[0])
                            
                            # Adjust for PM
                            if match.group(2).lower() == 'pm' and open_hour < 12:
                                open_hour += 12
                            if match.group(4).lower() == 'pm' and close_hour < 12:
                                close_hour += 12
                            
                        # 24-hour format
                        elif len(match.groups()) == 4:
                            open_hour = int(match.group(1))
                            close_hour = int(match.group(3))
                            
                        # Simple format
                        else:
                            open_hour = int(match.group(1))
                            close_hour = int(match.group(2))
                        
                        # Check time patterns
                        if open_hour <= 8:
                            hour_patterns["open_early"] = True
                        if open_hour <= 10:
                            hour_patterns["open_breakfast"] = True
                        if open_hour <= 12 and close_hour >= 14:
                            hour_patterns["open_lunch"] = True
                        if close_hour >= 17:
                            hour_patterns["open_dinner"] = True
                        if close_hour >= 22 or close_hour <= 4:  # Late night or early morning closing
                            hour_patterns["open_late"] = True
                        break
        
        # Check weekend operation
        if "Saturday" in hour_patterns["days_open"] or "Sunday" in hour_patterns["days_open"]:
            hour_patterns["open_weekends"] = True
        
        # Check if closed Mondays
        hour_patterns["closed_mondays"] = "Monday" not in hour_patterns["days_open"]
        
        # Generate descriptive text
        descriptions = []
        if hour_patterns["open_early"]:
            descriptions.append("Opens early")
        if hour_patterns["open_late"]:
            descriptions.append("Open late")
        if hour_patterns["open_breakfast"]:
            descriptions.append("Serves breakfast")
        if hour_patterns["open_lunch"]:
            descriptions.append("Open for lunch")
        if hour_patterns["open_dinner"]:
            descriptions.append("Open for dinner")
        if hour_patterns["open_24h"]:
            descriptions.append("Open 24 hours")
        if hour_patterns["open_weekends"]:
            descriptions.append("Open on weekends")
        if hour_patterns["closed_mondays"]:
            descriptions.append("Closed on Mondays")
        
        return {
            "original": parsed_hours,
            "patterns": hour_patterns,
            "description": ", ".join(descriptions)
        }
        
    def fetch_places_needing_embeddings(self):
        """Fetch places that need embeddings generated or updated"""
        logger.info("Fetching places that need embeddings...")
        
        conn, cur = self._connect_db()
        try:
            # First, check for places that have no embeddings at all
            query = """
            SELECT 
                p.id, 
                p.name, 
                p.combined_description, 
                p.tags, 
                p.corner_place_id,
                p.neighborhood,
                p.price_range,
                p.address,
                p.hours
            FROM places p
            LEFT JOIN embeddings e ON p.id = e.place_id
            WHERE e.id IS NULL
            """
            cur.execute(query)
            places = cur.fetchall()
            
            # For places with existing embeddings, check if content has changed
            query = """
            SELECT 
                p.id, 
                p.name, 
                p.combined_description, 
                p.tags, 
                p.corner_place_id,
                p.neighborhood,
                p.price_range,
                p.address,
                p.hours,
                e.id as embedding_id, 
                e.last_updated
            FROM places p
            JOIN embeddings e ON p.id = e.place_id
            WHERE p.updated_at > e.last_updated
            """
            cur.execute(query)
            updated_places = cur.fetchall()
            
            # Fetch reviews for all places that need embeddings
            all_place_ids = [place[0] for place in places] + [place[0] for place in updated_places]
            
            place_reviews = {}
            if all_place_ids:
                placeholders = ','.join(['%s'] * len(all_place_ids))
                review_query = f"""
                SELECT place_id, review_text
                FROM reviews
                WHERE place_id IN ({placeholders})
                """
                cur.execute(review_query, all_place_ids)
                review_results = cur.fetchall()
                
                # Group reviews by place_id
                for place_id, review_text in review_results:
                    if place_id not in place_reviews:
                        place_reviews[place_id] = []
                    place_reviews[place_id].append(review_text)
            
            logger.info(f"Found {len(places)} places without embeddings and {len(updated_places)} places with outdated embeddings")
            
            return places, updated_places, place_reviews
        
        except Exception as e:
            logger.error(f"Error fetching places: {str(e)}")
            conn.rollback()
            return [], [], {}
        finally:
            cur.close()
            conn.close()
    
    def fetch_resy_data(self, corner_place_id):
        """Fetch Resy data for a place from the combined_data.json file"""
        try:
            with open('combined_data.json', 'r') as f:
                combined_data = json.load(f)
                
            for place in combined_data:
                if str(place.get('corner_place_id')) == str(corner_place_id):
                    return place.get('resy_data', {})
            
            return {}
        except Exception as e:
            logger.warning(f"Error fetching Resy data: {str(e)}")
            return {}
    
    def validate_text(self, text):
        """Validate text before generating embeddings"""
        if not text:
            return False, "Text is empty"
        
        if not isinstance(text, str):
            return False, f"Text is not a string (got {type(text)})"
        
        if len(text) < 10:
            return False, "Text is too short"
        
        # Check for common meaningless text patterns
        low_info_patterns = [
            "not available", "n/a", "none", "unknown", "null", 
            "undefined", "to be added", "coming soon"
        ]
        
        text_lower = text.lower()
        for pattern in low_info_patterns:
            if pattern in text_lower and len(text) < 100:
                return False, f"Text contains low-information pattern: {pattern}"
        
        return True, "Text is valid"
    
    def parse_tags(self, tags_data):
        """Parse tags from various formats"""
        if not tags_data:
            return []
        
        # If already a list, just return it
        if isinstance(tags_data, list):
            return [str(tag).strip() for tag in tags_data if tag]
        
        # If it's a string, try different formats
        if isinstance(tags_data, str):
            # Check if it's a JSON array string
            if tags_data.startswith('[') and tags_data.endswith(']'):
                try:
                    return [str(tag).strip() for tag in json.loads(tags_data) if tag]
                except:
                    pass
            
            # Check if it's a PostgreSQL array format like {tag1,tag2}
            if tags_data.startswith('{') and tags_data.endswith('}'):
                tags = tags_data.strip('{}').split(',')
                return [tag.strip(' "\'') for tag in tags if tag.strip()]
            
            # Check if it's a comma-separated string
            if ',' in tags_data:
                return [tag.strip() for tag in tags_data.split(',') if tag.strip()]
            
            # Just return as a single tag
            return [tags_data.strip()]
        
        return []
    
    def parse_hours(self, hours_data):
        """Parse hours data from various formats"""
        if not hours_data:
            return None
        
        # If it's already a dictionary, return it
        if isinstance(hours_data, dict):
            return hours_data
        
        # If it's a JSON string, parse it
        if isinstance(hours_data, str):
            try:
                return json.loads(hours_data.replace("'", '"'))
            except:
                # Just return as is
                return hours_data
        
        return None
    
    def extract_resy_details(self, resy_data):
        """Extract useful information from Resy data"""
        if not resy_data or not isinstance(resy_data, dict):
            return ""
        
        resy_text = ""
        
        if resy_data.get('why_we_like_it'):
            resy_text += f"Why Resy likes it: {resy_data['why_we_like_it']}\n\n"
        
        if resy_data.get('about'):
            resy_text += f"About: {resy_data['about']}\n\n"
        
        if resy_data.get('need_to_know'):
            resy_text += f"Need to know: {resy_data['need_to_know']}"
            
        return resy_text.strip()
    
    def prepare_text_for_embedding(self, place, reviews):
        """Prepare and validate text for embedding generation including all data sources"""
        # Extract relevant place data
        place_id, name, description = place[0], place[1], place[2]
        tags_data, corner_id = place[3], place[4]
        neighborhood, price_range = place[5] if len(place) > 5 else None, place[6] if len(place) > 6 else None
        address, hours = place[7] if len(place) > 7 else None, place[8] if len(place) > 8 else None
        
        # Start with the basic info
        content_parts = [f"Name: {name}"]
        
        if neighborhood:
            content_parts.append(f"Neighborhood: {neighborhood}")
        
        # Process price range
        processed_price = self.process_price_range(price_range)
        if processed_price:
            content_parts.append(f"Price Range: {processed_price['original']}")
            content_parts.append(f"Price Category: {processed_price['description']}")
        
        if address:
            content_parts.append(f"Address: {address}")
        
        # Process hours
        processed_hours = self.process_business_hours(hours)
        if processed_hours:
            hours_text = []
            if isinstance(processed_hours['original'], dict):
                for day, time in processed_hours['original'].items():
                    hours_text.append(f"{day}: {time}")
                if hours_text:
                    content_parts.append(f"Hours: {', '.join(hours_text)}")
            
            if processed_hours['description']:
                content_parts.append(f"Hours Info: {processed_hours['description']}")
        
        # Add description if available
        if description:
            is_valid, msg = self.validate_text(description)
            if is_valid:
                content_parts.append(f"Description: {description}")
        
        # Add tags if available
        tags = self.parse_tags(tags_data)
        if tags:
            content_parts.append(f"Tags: {', '.join(tags)}")
        
        # Fetch and add Resy data
        resy_data = self.fetch_resy_data(corner_id)
        resy_text = self.extract_resy_details(resy_data)
        if resy_text:
            content_parts.append(f"From Resy: {resy_text}")
        
        # Add reviews
        place_reviews = reviews.get(place_id, [])
        if place_reviews:
            # Limit the number of reviews to avoid token limits
            max_reviews = min(5, len(place_reviews))
            selected_reviews = place_reviews[:max_reviews]
            reviews_text = "\n".join([f"- {review[:300]}" for review in selected_reviews])
            content_parts.append(f"Reviews:\n{reviews_text}")
        
        # Join all content parts
        content = "\n\n".join(content_parts)
        
        # Check if we have enough valid content
        if not content or len(content) < 50:
            logger.warning(f"Not enough valid content for place {name} (ID: {place_id})")
            return None, None
        
        # Calculate content hash for detecting changes
        content_hash = hashlib.md5(content.encode()).hexdigest()
        
        return content, content_hash
    
    def generate_embedding(self, text):
        """Generate embedding using OpenAI API"""
        max_retries = 3
        retry_delay = 2
        
        # Safeguard against overly long texts (token limit is around 8191 for text-embedding-ada-002)
        max_chars = 25000  # Approximate character limit for safety
        if len(text) > max_chars:
            logger.warning(f"Text too long ({len(text)} chars), truncating to {max_chars} chars")
            text = text[:max_chars] + "..."
        
        for attempt in range(max_retries):
            try:
                # Generate embedding
                response = self.client.embeddings.create(
                    input=text,
                    model=self.model
                )
                
                # Extract embedding vector
                embedding = response.data[0].embedding
                
                # Track token usage
                tokens_used = response.usage.total_tokens
                self.total_tokens += tokens_used
                
                logger.info(f"Generated embedding successfully. Used {tokens_used} tokens.")
                return embedding, tokens_used
                
            except Exception as e:
                # Implement exponential backoff
                wait_time = retry_delay * (2 ** attempt)
                logger.warning(f"Error generating embedding (attempt {attempt+1}/{max_retries}): {str(e)}")
                logger.warning(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
        
        # If we get here, all retries failed
        logger.error(f"Failed to generate embedding after {max_retries} attempts")
        return None, 0
    
    def store_embedding(self, place_id, embedding, content_type="combined"):
        """Store embedding in the database"""
        if not self.has_pgvector:
            logger.warning("pgvector extension not available, skipping embedding storage")
            return False
        
        conn, cur = self._connect_db()
        try:
            # Check if embedding already exists for this place
            cur.execute(
                "SELECT id FROM embeddings WHERE place_id = %s AND content_type = %s",
                (place_id, content_type)
            )
            
            result = cur.fetchone()
            if result:
                # Update existing embedding
                embedding_id = result[0]
                cur.execute(
                    """
                    UPDATE embeddings 
                    SET embedding = %s::vector, last_updated = CURRENT_TIMESTAMP 
                    WHERE id = %s
                    """,
                    (embedding, embedding_id)
                )
                logger.info(f"Updated embedding {embedding_id} for place {place_id}")
            else:
                # Insert new embedding
                cur.execute(
                    """
                    INSERT INTO embeddings (place_id, embedding, content_type, last_updated)
                    VALUES (%s, %s::vector, %s, CURRENT_TIMESTAMP)
                    """,
                    (place_id, embedding, content_type)
                )
                logger.info(f"Created new embedding for place {place_id}")
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error storing embedding for place {place_id}: {str(e)}")
            conn.rollback()
            return False
        finally:
            cur.close()
            conn.close()
    
    def update_embedding_status(self, place_id, status, message=None):
        """Update the place with embedding status metadata"""
        conn, cur = self._connect_db()
        try:
            # Add metadata about embedding status
            query = """
            UPDATE places 
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{embedding_status}',
                %s::jsonb
            )
            WHERE id = %s
            """
            
            status_data = json.dumps({
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "message": message
            })
            
            cur.execute(query, (status_data, place_id))
            conn.commit()
            
        except Exception as e:
            logger.warning(f"Failed to update embedding status: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    def process_all_places(self):
        """Process all places that need embeddings"""
        try:
            # Fetch places that need embeddings
            new_places, updated_places, place_reviews = self.fetch_places_needing_embeddings()
            
            if not new_places and not updated_places:
                logger.info("No places need embeddings. All up to date!")
                return
            
            # Process new places
            total_places = len(new_places) + len(updated_places)
            processed = 0
            
            for place in new_places:
                place_id, name = place[0], place[1]
                logger.info(f"Processing new place: {name} (ID: {place_id}) - {processed+1}/{total_places}")
                
                # Prepare text and validate
                content, content_hash = self.prepare_text_for_embedding(place, place_reviews)
                
                if not content:
                    self.update_embedding_status(place_id, "failed", "No valid content for embedding")
                    processed += 1
                    continue
                
                # Generate embedding
                embedding, tokens = self.generate_embedding(content)
                
                if embedding:
                    # Store embedding
                    success = self.store_embedding(place_id, embedding)
                    
                    if success:
                        self.update_embedding_status(place_id, "success", f"Used {tokens} tokens")
                    else:
                        self.update_embedding_status(place_id, "failed", "Failed to store embedding")
                else:
                    self.update_embedding_status(place_id, "failed", "Failed to generate embedding")
                
                processed += 1
                
                # Add a small delay between calls to avoid rate limiting
                time.sleep(0.5)
            
            # Process updated places
            for place in updated_places:
                place_id, name = place[0], place[1]
                embedding_id = place[9] if len(place) > 9 else None
                
                logger.info(f"Processing updated place: {name} (ID: {place_id}) - {processed+1}/{total_places}")
                
                # Prepare text and validate
                content, content_hash = self.prepare_text_for_embedding(place, place_reviews)
                
                if not content:
                    self.update_embedding_status(place_id, "failed", "No valid content for embedding")
                    processed += 1
                    continue
                
                # Generate embedding
                embedding, tokens = self.generate_embedding(content)
                
                if embedding:
                    # Store embedding
                    success = self.store_embedding(place_id, embedding)
                    
                    if success:
                        self.update_embedding_status(place_id, "updated", f"Used {tokens} tokens")
                    else:
                        self.update_embedding_status(place_id, "failed", "Failed to update embedding")
                else:
                    self.update_embedding_status(place_id, "failed", "Failed to generate embedding")
                
                processed += 1
                
                # Add a small delay between calls to avoid rate limiting
                time.sleep(0.5)
            
            # Log summary
            logger.info(f"Embedding generation complete.")
            logger.info(f"Processed {total_places} places.")
            logger.info(f"Total tokens used: {self.total_tokens}")
            logger.info(f"Estimated cost: ${(self.total_tokens / 1000) * 0.0001:.4f} (at $0.0001 per 1K tokens)")
            
            return self.total_tokens
            
        except Exception as e:
            logger.error(f"Error processing places: {str(e)}")
            logger.error(traceback.format_exc())
            return 0
    
    def search_places_with_location(self, query, neighborhood=None, limit=5, location_boost=1.5):
        """
        Enhanced search combining semantic similarity with location filtering
        
        Args:
            query: The search query
            neighborhood: Optional specific neighborhood to filter by
            limit: Maximum number of results to return
            location_boost: Boost factor for matching neighborhood
            
        Returns:
            List of matching places
        """
        if not self.has_pgvector:
            logger.warning("pgvector extension not available, cannot perform search")
            return []
        
        # Extract location from query if not explicitly provided
        if not neighborhood:
            clean_query, extracted_neighborhood = extract_location_from_query(query)
            if extracted_neighborhood:
                neighborhood = extracted_neighborhood
                query = clean_query  # Use the cleaned query without location
                logger.info(f"Extracted location '{neighborhood}' from query. Modified query: '{query}'")
        
        conn, cur = self._connect_db()
        try:
            # Generate embedding for query
            query_embedding, _ = self.generate_embedding(query)
            
            if not query_embedding:
                logger.error("Failed to generate embedding for search query")
                return []
            
            # If neighborhood specified, use location-boosted search
            if neighborhood:
                # Query with location boost for places in the target neighborhood
                search_query = """
                SELECT 
                    p.id, p.name, p.neighborhood, p.tags, p.price_range,
                    p.combined_description,
                    CASE 
                        WHEN p.neighborhood ILIKE %s THEN (1 - (e.embedding <=> %s::vector)) * %s
                        ELSE 1 - (e.embedding <=> %s::vector)
                    END as adjusted_similarity
                FROM places p
                JOIN embeddings e ON p.id = e.place_id
                ORDER BY adjusted_similarity DESC
                LIMIT %s
                """
                
                neighborhood_pattern = f'%{neighborhood}%'
                
                cur.execute(search_query, (
                    neighborhood_pattern, 
                    query_embedding, 
                    location_boost,
                    query_embedding,
                    limit
                ))
            else:
                # Standard vector search without location filtering
                search_query = """
                SELECT p.id, p.name, p.neighborhood, p.tags, p.price_range,
                       p.combined_description,
                       1 - (e.embedding <=> %s::vector) as similarity
                FROM places p
                JOIN embeddings e ON p.id = e.place_id
                ORDER BY similarity DESC
                LIMIT %s
                """
                
                cur.execute(search_query, (query_embedding, limit))
            
            results = cur.fetchall()
            
            # If we got very few results with neighborhood filtering, try expanding to adjacent neighborhoods
            if neighborhood and len(results) < 3:
                logger.info(f"Few results ({len(results)}) with neighborhood filter '{neighborhood}'. Expanding to adjacent neighborhoods.")
                
                # Try adjacent neighborhoods from our mapping
                adjacent_neighborhoods = get_adjacent_neighborhoods(neighborhood)
                if adjacent_neighborhoods:
                    adjacent_results = []
                    for adjacent in adjacent_neighborhoods:
                        # Query with adjacent neighborhood
                        adjacent_pattern = f'%{adjacent}%'
                        cur.execute(search_query, (
                            adjacent_pattern, 
                            query_embedding, 
                            1.2,  # Lower boost for adjacent neighborhoods
                            query_embedding,
                            3  # Limit per adjacent neighborhood
                        ))
                        adjacent_results.extend(cur.fetchall())
                    
                    # Combine results, prioritizing any that were in the original results
                    all_ids = set(r[0] for r in results)  # IDs from original results
                    
                    # Add results from adjacent neighborhoods that weren't in original
                    for res in adjacent_results:
                        if res[0] not in all_ids:
                            results.append(res)
                            all_ids.add(res[0])
                
                # If still too few, fall back to unfiltered search 
                if len(results) < 3:
                    logger.info(f"Still too few results with adjacent neighborhoods. Trying without location filter.")
                    
                    # Query without location filter
                    cur.execute(
                        """
                        SELECT p.id, p.name, p.neighborhood, p.tags, p.price_range,
                               p.combined_description,
                               1 - (e.embedding <=> %s::vector) as similarity
                        FROM places p
                        JOIN embeddings e ON p.id = e.place_id
                        ORDER BY similarity DESC
                        LIMIT %s
                        """, 
                        (query_embedding, limit)
                    )
                    unfiltered_results = cur.fetchall()
                    
                    # Add unfiltered results that aren't already in our results
                    for res in unfiltered_results:
                        if res[0] not in all_ids:
                            results.append(res)
            
            # Sort results by similarity (or adjusted_similarity)
            if neighborhood:
                results.sort(key=lambda x: x[6], reverse=True)  # Sort by adjusted_similarity
            else:
                results.sort(key=lambda x: x[6], reverse=True)  # Sort by similarity
            
            # Limit to the requested number of results
            results = results[:limit]
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching places: {str(e)}")
            logger.error(traceback.format_exc())
            conn.rollback()
            return []
        finally:
            cur.close()
            conn.close()
    
    def test_vector_search(self, query, limit=5):
        """Test vector search with a sample query"""
        logger.info(f"Testing vector search with query: '{query}'")
        
        # First try with location extraction
        clean_query, location = extract_location_from_query(query)
        if location:
            logger.info(f"Extracted location '{location}' from query. Modified query: '{clean_query}'")
            results = self.search_places_with_location(clean_query, neighborhood=location, limit=limit)
        else:
            results = self.search_places_with_location(query, limit=limit)
            
        if not results:
            logger.warning("No results found.")
            return []
            
        logger.info(f"Found {len(results)} results:")
        for i, result in enumerate(results, 1):
            id, name, neighborhood, tags, price, desc, similarity = result
            logger.info(f"{i}. {name} ({neighborhood}) - {similarity:.4f}")
            
        return results
    
    def add_missing_metadata_column(self):
        """Add metadata JSONB column if it doesn't exist"""
        conn, cur = self._connect_db()
        try:
            # Check if metadata column exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'places' AND column_name = 'metadata'
            """)
            
            if not cur.fetchone():
                logger.info("Adding metadata column to places table")
                cur.execute("ALTER TABLE places ADD COLUMN metadata JSONB")
                conn.commit()
                logger.info("Added metadata column to places table")
            else:
                logger.info("Metadata column already exists")
                
        except Exception as e:
            logger.error(f"Error adding metadata column: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()


def main():
    # Database configuration
    db_config = {
        "dbname": "corner_db",
        "user": os.getenv("DB_USER", "namayjindal"),
        "password": os.getenv("DB_PASSWORD", ""),
        "host": os.getenv("DB_HOST", "localhost")
    }
    
    generator = EmbeddingGenerator(db_config)
    
    # Add metadata column if needed
    generator.add_missing_metadata_column()
    
    # Process all places
    tokens_used = generator.process_all_places()
    
    # Test vector search with enhanced query set
    logger.info("\nTesting vector search functionality...")
    
    test_queries = [
        # General location queries
        "cozy coffee shop in Brooklyn",
        "authentic thai food with good reviews",
        "romantic restaurant for date night in West Village",
        
        # Price-focused queries
        "cheap eats in Chinatown",
        "budget-friendly pizza",
        "expensive fine dining",
        "mid-range italian restaurant",
        "affordable breakfast spots",
        
        # Hours-focused queries
        "restaurants open late in East Village",
        "breakfast places open early",
        "cafes open on weekends",
        "restaurants open for lunch on Mondays",
        "dinner spots open until midnight",
        "places for Sunday brunch",
        
        # Combined queries
        "affordable Italian open late",
        "upscale sushi bar open for lunch",
        "cheap breakfast place open early in Brooklyn",
        "mid-priced restaurants with outdoor seating open on Sundays",
        
        # Original queries
        "casual pizza place that's open late",
        "cocktail bar with unique drinks",
        "restaurants near Soho with outdoor seating",
        "affordable brunch spots in East Village",
        "Japanese restaurants with good vegetarian options"
    ]
    
    for query in test_queries:
        generator.test_vector_search(query, limit=3)
        print("-" * 40)

if __name__ == "__main__":
    main()