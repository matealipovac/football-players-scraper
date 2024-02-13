import psycopg2
import sys
import import_csv_data
import scrape_from_urls


if __name__ == "__main__":
    if len(sys.argv) > 1:
        url_file = sys.argv[1]
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(
            dbname="football_players",
            user="postgres",
            password="12345678",
            host="localhost",
            port="5432"
        )

        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS football_players (
             url TEXT PRIMARY KEY,
             name TEXT,
             full_name TEXT,
             date_of_birth DATE,
             age INTEGER,
             place_of_birth TEXT,
             country_of_birth TEXT,
             positions TEXT,
             current_club TEXT,
             national_team TEXT,
             number_of_appearances_in_current_club INTEGER,
             goals_in_current_club INTEGER,
             scraping_timestamp TIMESTAMP
         )
        """)
        conn.commit()
        cursor.close()
        # Call the import_data function with the CSV file and database connection
        import_csv_data.import_csv_data("playersData.csv", conn)
        
        scrape_from_urls.scrape_url_data(url_file, conn)

        # Close the database connection
        conn.close()
    else:
        print("No path for urls file provided.")
    
