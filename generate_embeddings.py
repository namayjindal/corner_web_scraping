import os
import json
import logging
import psycopg2
import pandas as pd
import time
import re
from psycopg2.extras import execute_batch
from openai import OpenAI
from datetime import datetime
from dotenv import load_dotenv
import traceback
import hashlib

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
        
        if price_range:
            content_parts.append(f"Price Range: {price_range}")
        
        if address:
            content_parts.append(f"Address: {address}")
        
        # Add description if available
        if description:
            is_valid, msg = self.validate_text(description)
            if is_valid:
                content_parts.append(f"Description: {description}")
        
        # Add tags if available
        tags = self.parse_tags(tags_data)
        if tags:
            content_parts.append(f"Tags: {', '.join(tags)}")
        
        # Add hours if available
        parsed_hours = self.parse_hours(hours)
        if isinstance(parsed_hours, dict):
            hours_text = []
            for day, time in parsed_hours.items():
                hours_text.append(f"{day}: {time}")
            if hours_text:
                content_parts.append(f"Hours: {', '.join(hours_text)}")
        elif isinstance(parsed_hours, str) and len(parsed_hours) > 5:
            content_parts.append(f"Hours: {parsed_hours}")
        
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
    
    def test_vector_search(self, query, limit=5):
        """Test vector search to validate embeddings"""
        if not self.has_pgvector:
            logger.warning("pgvector extension not available, cannot test vector search")
            return []
        
        conn, cur = self._connect_db()
        try:
            # Generate embedding for query
            query_embedding, _ = self.generate_embedding(query)
            
            if not query_embedding:
                logger.error("Failed to generate embedding for test query")
                return []
            
            # Perform vector search
            search_query = """
            SELECT p.id, p.name, p.neighborhood, p.tags, p.price_range,
                   1 - (e.embedding <=> %s::vector) as similarity
            FROM places p
            JOIN embeddings e ON p.id = e.place_id
            ORDER BY similarity DESC
            LIMIT %s
            """
            
            cur.execute(search_query, (query_embedding, limit))
            results = cur.fetchall()
            
            return results
            
        except Exception as e:
            logger.error(f"Error testing vector search: {str(e)}")
            conn.rollback()
            return []
        finally:
            cur.close()
            conn.close()
    
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
        "user": "namayjindal",  # Your username - adjust if needed
        "password": "",         # Usually empty on macOS
        "host": "localhost"
    }
    
    generator = EmbeddingGenerator(db_config)
    
    # Add metadata column if needed
    generator.add_missing_metadata_column()
    
    # Process all places
    tokens_used = generator.process_all_places()
    
    # Test vector search with various queries
    logger.info("Testing vector search...")
    
    test_queries = [
        "cozy coffee shop in Brooklyn",
        "authentic thai food with good reviews",
        "romantic restaurant for date night in West Village",
        "casual pizza place that's open late",
        "cocktail bar with unique drinks",
        "restaurants near Soho with outdoor seating",
        "affordable brunch spots in East Village",
        "Japanese restaurants with good vegetarian options",
        "Matcha"
    ]
    
    for query in test_queries:
        logger.info(f"\nTesting query: '{query}'")
        results = generator.test_vector_search(query, limit=3)
        
        if results:
            logger.info("Top results:")
            for i, (id, name, neighborhood, tags, price, similarity) in enumerate(results, 1):
                tags_formatted = ", ".join(generator.parse_tags(tags)) if tags else "N/A"
                logger.info(f"{i}. {name} ({neighborhood}) - ${price} - Tags: {tags_formatted} - Similarity: {similarity:.4f}")
        else:
            logger.info("No results found.")

if __name__ == "__main__":
    main()