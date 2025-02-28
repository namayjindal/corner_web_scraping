import re
import logging
import spacy
from typing import Tuple, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define neighborhood mapping for location extraction
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
    
    # Additional aliases for better coverage
    "downtown brooklyn": "Brooklyn",
    "bk heights": "Brooklyn Heights",
    "boerum hill": "Brooklyn",
    "fort greene": "Brooklyn",
    "park slope": "Brooklyn",
    "cobble hill": "Brooklyn",
    "prospect heights": "Brooklyn",
    "ktown": "Koreatown",
    "korea town": "Koreatown",
    "midtown east": "Midtown East",
    "midtown west": "Midtown West",
    "theatre district": "Theater District",
    "theater district": "Theater District",
    "hudson yards": "Chelsea District",
    "meatpacking": "Meatpacking District",
    "meatpacking district": "Meatpacking District",
    "meat packing": "Meatpacking District",
    
    # Landmarks and parks (mapped to containing neighborhoods)
    "central park": "Upper East Side",  # Could also map to "Manhattan" or specific nearby neighborhoods
    "bryant park": "Midtown",
    "washington square park": "Greenwich Village",
    "times square": "Midtown",
    "union square": "Flatiron District",
    "hudson yards": "Chelsea District",
    "high line": "Chelsea District"
}

# Define mapping of adjacent neighborhoods for expanding searches
ADJACENT_NEIGHBORHOODS = {
    "SoHo": ["NoHo", "Little Italy", "Tribeca", "Greenwich Village"],
    "East Village": ["NoHo", "Lower East Side", "Alphabet City", "Gramercy"],
    "West Village": ["Greenwich Village", "Chelsea District", "Meatpacking District"],
    "Lower East Side": ["East Village", "Chinatown", "Little Italy"],
    "Tribeca": ["SoHo", "Financial District", "Chinatown"],
    "Midtown": ["Chelsea District", "Midtown East", "Midtown West", "Theater District"],
    "Brooklyn": ["Williamsburg", "Brooklyn Heights", "Greenpoint"],
    "Chinatown": ["Little Italy", "Lower East Side", "Financial District"],
    "Chelsea District": ["West Village", "Midtown", "Meatpacking District"],
    "Greenwich Village": ["West Village", "SoHo", "NoHo"],
    # Add more mappings as needed
}

# Try to load SpaCy model for advanced location extraction
try:
    nlp = spacy.load("en_core_web_sm")
    logger.info("Loaded SpaCy model successfully")
except Exception as e:
    logger.warning(f"SpaCy model not available: {str(e)}. Location extraction will use pattern matching only.")
    nlp = None

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
    if not query:
        return query, None
        
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
    
    # If no explicit pattern found, try using SpaCy NER if available
    if nlp:
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

def get_adjacent_neighborhoods(neighborhood: str) -> list:
    """
    Return a list of adjacent neighborhoods for a given neighborhood.
    
    Args:
        neighborhood: The neighborhood to find adjacents for
        
    Returns:
        List of adjacent neighborhood names
    """
    return ADJACENT_NEIGHBORHOODS.get(neighborhood, [])

# Helper function to install the SpaCy model if it's not available
def install_spacy_model():
    try:
        import subprocess
        logger.info("Installing SpaCy model...")
        subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"], check=True)
        global nlp
        nlp = spacy.load("en_core_web_sm")
        logger.info("SpaCy model installed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to install SpaCy model: {str(e)}")
        return False

if __name__ == "__main__":
    # Test the location extraction
    test_queries = [
        "coffee shops in SoHo",
        "best pizza near Brooklyn",
        "cocktail bars in the East Village",
        "restaurants with outdoor seating in West Village",
        "cheap eats Chinatown",
        "brunch places central park",
        "Italian food Midtown"
    ]
    
    print("Testing location extraction:")
    for query in test_queries:
        modified_query, location = extract_location_from_query(query)
        print(f"Original: '{query}'")
        print(f"Modified: '{modified_query}'")
        print(f"Location: {location}")
        print("-" * 40)