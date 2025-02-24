import psycopg2
import pandas as pd

# Connect to database
conn = psycopg2.connect(
    dbname="corner_db",
    user="namayjindal",  # Adjust this if needed
    password="",
    host="localhost"
)
cur = conn.cursor()

# Check if tables exist
cur.execute("""
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
""")

tables = cur.fetchall()
print("Tables in database:")
for table in tables:
    print(f"- {table[0]}")

# Get table schemas
def print_table_schema(table_name):
    cur.execute(f"""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """)
    columns = cur.fetchall()
    print(f"\nSchema for {table_name}:")
    for col in columns:
        print(f"- {col[0]}: {col[1]} (Nullable: {col[2]})")

for table in tables:
    print_table_schema(table[0])

# Close connection
cur.close()
conn.close()

conn = psycopg2.connect(
    dbname="corner_db",
    user="namayjindal",
    password="",
    host="localhost"
)
cur = conn.cursor()

# Count records in each table
cur.execute("SELECT COUNT(*) FROM places")
places_count = cur.fetchone()[0]
print(f"Number of places: {places_count}")

cur.execute("SELECT COUNT(*) FROM reviews")
reviews_count = cur.fetchone()[0]
print(f"Number of reviews: {reviews_count}")

if 'embeddings' in [t[0] for t in tables]:
    cur.execute("SELECT COUNT(*) FROM embeddings")
    embeddings_count = cur.fetchone()[0]
    print(f"Number of embeddings: {embeddings_count}")

# Close connection
cur.close()
conn.close()

conn = psycopg2.connect(
    dbname="corner_db",
    user="namayjindal",
    password="",
    host="localhost"
)

# Get a few sample places
sample_places = pd.read_sql("""
    SELECT id, name, neighborhood, website, price_range, 
           tags, combined_description, hours
    FROM places
    LIMIT 5
""", conn)
print("\nSample places:")
print(sample_places)

# Get associated reviews for one place
if places_count > 0:
    place_id = sample_places.iloc[0]['id']
    sample_reviews = pd.read_sql(f"""
        SELECT source, review_text
        FROM reviews
        WHERE place_id = {place_id}
        LIMIT 3
    """, conn)
    print(f"\nSample reviews for {sample_places.iloc[0]['name']}:")
    print(sample_reviews)

# Check if any places are missing critical data
missing_data = pd.read_sql("""
    SELECT name, neighborhood 
    FROM places 
    WHERE combined_description IS NULL
       OR tags IS NULL
    LIMIT 10
""", conn)
print("\nPlaces missing critical data:")
print(missing_data)

conn.close()