# Football Players Data Scraper and Analyzer
This is a simple web scraper and data analyzer application designed to scrape data from a specified website (Wikipedia) containing football players' information. The scraped data is then stored in a SQL database for further analysis.

## Features:
- **Web Scraper:** The application includes a web scraper that extracts data from a designated website. The scraper retrieves information such as player names, full names, date of birth, age, place of birth, country of birth, positions, current club, national team, number of appearances in the current club, and goals scored in the current club.

- **SQL Database:** Scraped data is stored in a PostgreSQL database for efficient storage and retrieval. The database schema includes tables to store player information along with relevant metrics.

- **Data Enrichment:** SQL queries are provided to enrich the scraped data. These queries calculate specific metrics such as the average age, average number of appearances, total number of players by club, and more.

- **Metrics Calculation:** The application offers SQL queries to calculate various metrics based on the scraped data. These metrics can help analyze player performance, club statistics, and other relevant insights.

## Setup Instructions:
- **Install Dependencies:** Ensure that Python 3.x and the psycopg2 library are installed on your system. You can install psycopg2 using pip:

`pip install psycopg2`
- **Set up PostgreSQL:** Install PostgreSQL on your system. Create a database named "football_players" and configure the connection parameters (dbname, user, password, host, port) in the Python script (playersScraper.py) accordingly.

- **Run the Scraper:** Execute the Python script playersScraper.py to initiate the web scraping process. This script scrapes data from the specified website and inserts it into the PostgreSQL database.

- **Execute SQL Queries:** Use the provided SQL queries to analyze the scraped data. These queries can be executed directly in a PostgreSQL client or through the application code.

## Example Queries:

- Calculate the average age, average number of appearances, and total number of players by club.

- Determine the count of younger players in other clubs who have a higher number of appearances than players from a specific club.
- Enrich player data with additional columns such as age category and goals per club game.
