import re
import spacy
import logging
from typing import Optional, Tuple, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load SpaCy model for NER
try:
    nlp = spacy.load("en_core_web_sm")
    logger.info("Loaded SpaCy model successfully")
except Exception as e:
    logger.error(f"Failed to load SpaCy model: {str(e)}")
    logger.info("Installing SpaCy model...")
    import subprocess
    subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
    nlp = spacy.load("en_core_web_sm")

# Define a mapping of common location terms and variations to standardized neighborhood names
NEIGHBORHOOD_MAPPING = {
        # Boroughs and citywide
        "nyc": "New York City",
        "new york": "New York City",
        "new york city": "New York City",
        "manhattan": "Manhattan",
        "brooklyn": "Brooklyn",
        "queens": "Queens",
        "bronx": "Bronx",
        "staten island": "Staten Island",
        
        # Manhattan neighborhoods
        "soho": "SoHo",
        "greenpoint": "Greenpoint",
        "east village": "East Village",
        "west village": "West Village",
        "lower east side": "Lower East Side",
        "les": "Lower East Side",
        "upper east side": "Upper East Side",
        "ues": "Upper East Side",
        "upper west side": "Upper West Side",
        "uws": "Upper West Side",
        "chelsea": "Chelsea District",
        "chinatown": "Chinatown",
        "tribeca": "Tribeca",
        "little italy": "Little Italy",
        "nolita": "Little Italy",  # Mapping similar neighborhoods
        "midtown": "Midtown",
        "flatiron": "Flatiron District",
        "gramercy": "Gramercy",
        "noho": "NoHo", 
        "fidi": "Financial District",
        "financial district": "Financial District",
        "alphabet city": "Alphabet City",
        "downtown": "Manhattan",  # General mapping
        "hell's kitchen": "Hell's Kitchen",
        "hells kitchen": "Hell's Kitchen",
        
        # Brooklyn neighborhoods
        "williamsburg": "Brooklyn",
        "dumbo": "Brooklyn",
        
        # Landmarks and parks (mapped to containing neighborhoods)
        "central park": "Upper East Side",  # Could also map to "Manhattan" or specific nearby neighborhoods
        "bryant park": "Midtown",
        "washington square park": "Greenwich Village",
        "times square": "Midtown",
        "union square": "Flatiron District",
        "hudson yards": "Chelsea District",
        "high line": "Chelsea District"
    }

def extract_location_from_query(query: str) -> Tuple[str, Optional[str]]:
    """
    Extract location information from a user query and return the modified query
    and the standardized location if found.
    
    Args:
        query: The user search query
        
    Returns:
        Tuple containing:
            - Modified query with location removed
            - Standardized location name if found, None otherwise
    """
    query_lower = query.lower()
    
    # Special handling for queries that start with landmarks
    landmark_prefixes = ["central park", "bryant park", "washington square park", "times square"]
    for landmark in landmark_prefixes:
        if query_lower.startswith(landmark):
            # Keep the query as is but return the mapped neighborhood
            if landmark in NEIGHBORHOOD_MAPPING:
                return query, NEIGHBORHOOD_MAPPING[landmark]
    
    # First, look for explicit location patterns ("in X", "near X", "around X", etc.)
    location_patterns = [
        r'in\s+([a-zA-Z\s\']+)(?:\s|$|\.)',
        r'near\s+([a-zA-Z\s\']+)(?:\s|$|\.)',
        r'around\s+([a-zA-Z\s\']+)(?:\s|$|\.)',
        r'at\s+([a-zA-Z\s\']+)(?:\s|$|\.)',
        r'by\s+([a-zA-Z\s\']+)(?:\s|$|\.)',
        r'within\s+([a-zA-Z\s\']+)(?:\s|$|\.)',
    ]
    
    for pattern in location_patterns:
        match = re.search(pattern, query_lower)
        if match:
            location_text = match.group(1).strip()
            
            # Check if the extracted text is in our neighborhood mapping
            for loc_key, std_name in NEIGHBORHOOD_MAPPING.items():
                if loc_key in location_text:
                    # Remove the location part from the query
                    modified_query = re.sub(pattern, '', query).strip()
                    return modified_query, std_name
    
    # If no explicit pattern found, use SpaCy NER to look for locations
    doc = nlp(query)
    for ent in doc.ents:
        if ent.label_ == "GPE":  # Geopolitical entity
            for loc_key, std_name in NEIGHBORHOOD_MAPPING.items():
                if loc_key in ent.text.lower():
                    # Remove the location from the query
                    modified_query = query.replace(ent.text, "").strip()
                    return modified_query, std_name
    
    # Finally, check for direct mentions of neighborhoods without prepositions
    for loc_key, std_name in NEIGHBORHOOD_MAPPING.items():
        if loc_key in query_lower:
            # Make sure it's a full word/phrase by checking boundaries
            pattern = r'\b' + re.escape(loc_key) + r'\b'
            if re.search(pattern, query_lower):
                modified_query = re.sub(pattern, '', query_lower, flags=re.IGNORECASE).strip()
                return modified_query, std_name
    
    # No location found
    return query, None

def expand_neighborhood_mapping():
    """Add variations to the neighborhood mapping"""
    variations = {}
    for key, value in list(NEIGHBORHOOD_MAPPING.items()):
        # Add variations with punctuation
        variations[key.replace(" ", "-")] = value
        variations[key.replace(" ", ".")] = value
        
        # Add variations with different capitalizations
        variations[key.title()] = value
        
    NEIGHBORHOOD_MAPPING.update(variations)
    logger.info(f"Expanded neighborhood mapping to {len(NEIGHBORHOOD_MAPPING)} entries")

def test_extraction():
    """Test the location extraction function with various queries"""
    test_queries = [
        "romantic restaurants in West Village",
        "coffee shops near SoHo",
        "best pizza in Brooklyn",
        "cocktail bars in the East Village",
        "breakfast places in UES", 
        "late night food lower east side",
        "sushi restaurants",
        "manhattan whiskey bars",
        "cozy cafes in williamsburg",
        "NoHo diner with outdoor seating",
        "Chinese food Chinatown",
        "best bagels in NYC", 
        "museums in upper east side",
        "vegan restaurants in Hell's Kitchen",
        "Central Park picnic spots",
        "coffee shop with wifi in Greenpoint",
        "Matcha lattes in East Village",
        "DUMBO art galleries",
        "best croissants in SoHo",
        "Flatiron District steakhouses"
    ]
    
    print("\nTesting location extraction from queries:")
    print("=" * 60)
    
    for query in test_queries:
        modified_query, location = extract_location_from_query(query)
        print(f"Original: '{query}'")
        print(f"Modified: '{modified_query}'")
        print(f"Location: {location}")
        print("-" * 60)

if __name__ == "__main__":
    # Expand neighborhood mapping with variations
    expand_neighborhood_mapping()
    
    # Test extraction
    test_extraction()