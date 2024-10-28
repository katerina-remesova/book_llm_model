import sqlite3
import csv
import os
import time

conn = sqlite3.connect('imdb.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS title_basics (
        tconst TEXT PRIMARY KEY,
        titleType TEXT,
        primaryTitle TEXT,
        originalTitle TEXT,
        isAdult INTEGER,
        startYear INTEGER,
        endYear INTEGER,
        runtimeMinutes INTEGER,
        genres TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS name_basics (
        nconst TEXT PRIMARY KEY,
        primaryName TEXT,
        birthYear INTEGER,
        deathYear INTEGER,
        primaryProfession TEXT,
        knownForTitles TEXT
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS title_akas (
        titleId TEXT,
        ordering INTEGER,
        title TEXT,
        region TEXT,
        language TEXT,
        types TEXT,
        attributes TEXT,
        isOriginalTitle INTEGER,
        FOREIGN KEY (titleId)
            REFERENCES title_basics (tconst)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS title_episode (
        tconst TEXT,
        parentTconst TEXT,
        seasonNumber INTEGER,
        episodeNumber INTEGER,
        FOREIGN KEY (tconst)
            REFERENCES title_basics (tconst),
        FOREIGN KEY (parentTconst)
            REFERENCES title_basics (tconst)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS title_crew (
        tconst TEXT,
        directors TEXT,
        writers TEXT,
        FOREIGN KEY (tconst)
            REFERENCES title_basics (tconst),
        FOREIGN KEY (directors)
            REFERENCES name_basics (nconst),
        FOREIGN KEY (writers)
            REFERENCES name_basics (nconst)        
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS title_principals (
        tconst TEXT,
        ordering INTEGER,
        nconst TEXT,
        category TEXT,
        job TEXT,
        characters TEXT,
        FOREIGN KEY (tconst)
            REFERENCES title_basics (tconst),
        FOREIGN KEY (nconst)
            REFERENCES name_basics (nconst)
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS title_ratings (
        tconst TEXT,
        averageRating REAL,
        numVotes INTEGER,
        FOREIGN KEY (tconst)
            REFERENCES title_basics (tconst)
    )
''')

# List of data files and corresponding tables
tsv_files = [
    ('title.basics.tsv', 'title_basics'),
    ('title.ratings.tsv', 'title_ratings'),
    ('title.principals.tsv', 'title_principals'),
    ('title.crew.tsv', 'title_crew'),
    ('title.episode.tsv', 'title_episode'),
    ('title.akas.tsv', 'title_akas'),
    ('name.basics.tsv', 'name_basics')
]

file_name = 'title.akas.tsv'
table_name = 'title_akas'

def get_table_columns(table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [col[1] for col in cursor.fetchall()]

def insert_data(file_path, table_name):
    table_columns = get_table_columns(table_name)
    
    # INSERT
    columns = ', '.join(table_columns)
    placeholders = ', '.join(['?' for _ in table_columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    chunk_size = 10000  # Change in chunk size improves performance in loading data to db
    total_rows = 0
    start_time = time.time()
    
    try:
        # Enable WAL mode for better write performance
        cursor.execute('PRAGMA journal_mode=WAL')
        
        # Start a transaction
        cursor.execute('BEGIN TRANSACTION')
        
        with open(file_path, 'r', encoding='utf-8') as tsv_file:
            csv_reader = csv.reader(tsv_file, delimiter='\t')
            headers = next(csv_reader)  # Skip the header row
            
            chunk = []
            for row in csv_reader:
                # Process the row, handling multi-value fields
                processed_row = [None if field == r"\N" else field for field in row]
                
                # Pad the row with None if it's shorter than the table columns
                processed_row.extend([None] * (len(table_columns) - len(processed_row)))
                
                chunk.append(processed_row[:len(table_columns)])
                
                if len(chunk) == chunk_size:
                    cursor.executemany(insert_query, chunk)
                    total_rows += len(chunk)
                    
                    # Commit every 1 million rows
                    if total_rows % 1000000 == 0:
                        conn.commit()
                        cursor.execute('BEGIN TRANSACTION')
                        elapsed_time = time.time() - start_time
                        print(f"Inserted {total_rows:,} rows... ({elapsed_time:.2f} seconds)")
                    
                    chunk = []
            
            # Insert any remaining rows
            if chunk:
                cursor.executemany(insert_query, chunk)
                total_rows += len(chunk)
        
        # Commit the final transaction
        conn.commit()
        
        elapsed_time = time.time() - start_time
        print(f"Inserted a total of {total_rows:,} rows into {table_name}")
        print(f"Total time: {elapsed_time:.2f} seconds")
        print(f"Average speed: {total_rows / elapsed_time:.2f} rows/second")
    
    except IOError as e:
        print(f"Error reading file: {e}")
        conn.rollback()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Unexpected error: {e}")
        conn.rollback()
    finally:
        # Disable WAL mode
        cursor.execute('PRAGMA journal_mode=DELETE')

# Process the file
file_path = os.path.join('data', file_name)
print(f"Processing {file_path}...")
if os.path.exists(file_path):
    insert_data(file_path, table_name)
else:
    print(f"File not found: {file_path}")

conn.close()
print("Insert successfull.")