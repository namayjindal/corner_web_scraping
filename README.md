# Corner Project - Restaurant Data Scraper

This project collects and processes restaurant data from multiple sources including Google Places, OpenTable, Resy, OpenStreetMap, and individual restaurant websites. It combines the data and stores it in a PostgreSQL database with vector embeddings for semantic search capabilities.

## Features

- Multi-source data collection (Google, OpenTable, Resy, OSM)
- Automatic metadata extraction from restaurant websites
- PostgreSQL database storage with proper schema
- Vector embeddings for powerful semantic search
- One-click execution with a simple shell script

## Prerequisites

Before running the script, make sure you have:

- Python 3.7 or higher
- PostgreSQL installed and running
- Chrome/Chromium (for web scraping)
- Git (to clone this repository)

## Quick Start

1. Clone this repository:
   ```
   git clone https://github.com/namayjindal/corner-project.git
   cd corner-project
   ```

2. Make the script executable:
   ```
   chmod +x scrape_all.sh
   ```

3. Run the script:
   ```
   ./scrape_all.sh
   ```

4. The script will:
   - Check for required dependencies and install them
   - Create a PostgreSQL database named `corner_db`
   - Run all scrapers sequentially
   - Process and combine the data
   - Generate embeddings for semantic search
   - Validate the final data

## Configuration

The first time you run the script, it will create a `.env` file template. You'll need to edit this file to include:

- Your OpenAI API key (for generating embeddings)
- PostgreSQL credentials

Example `.env` file:
```
# API Keys
OPENAI_KEY=your_openai_key_here

# Database Configuration
DB_USER=your_postgres_username
DB_PASSWORD=your_postgres_password
DB_HOST=localhost
DB_NAME=corner_db
```

## Output Files

After running the script, you'll have several data files:

- `places_with_google_data.csv`: Data from Google Places
- `opentable_results.csv`: Restaurant data from OpenTable
- `resy_data.json`: Restaurant data from Resy
- `places_with_osm.csv`: Location data from OpenStreetMap
- `scraped_data.json`: Website metadata
- `combined_data.json`: All data merged together

## Exploring the Data

To explore the database after the script has run:

```
psql corner_db
```

You can then run SQL queries to explore the data. For example:

```sql
-- Get all restaurants
SELECT name, neighborhood, price_range FROM places LIMIT 10;

-- Get reviews for a specific place
SELECT r.review_text, r.source FROM reviews r 
JOIN places p ON r.place_id = p.id 
WHERE p.name = 'Restaurant Name' LIMIT 5;
```

## Troubleshooting

- If the script fails at any point, check the error message and the logs in the `logs/` directory
- If web scraping fails, it might be due to website changes or blocking. Try running just that specific scraper again later.
- For database connection errors, make sure PostgreSQL is running and the credentials in your `.env` file are correct

## Project Structure

```
.
├── google_places/           # Google Places scraper
│   └── google_scraper.py
├── opentable/               # OpenTable scraper  
│   └── scrape.py
├── resy/                    # Resy scraper
│   └── scrape.py
├── osm/                     # OpenStreetMap scraper
│   └── scrape.py
├── location_extraction.py   # Location parsing utilities
├── postgres_migration.py    # Database migration script
├── generate_embeddings.py   # Vector embedding generation
├── data_validation.py       # Data validation utilities
├── scrape_all.sh            # Main pipeline script
└── README.md                # This file
```

## Next Steps

After collecting the data, you might want to:

1. Perform more advanced data analysis
2. Create visualizations of the data
3. Build a simple search interface using the vector embeddings
