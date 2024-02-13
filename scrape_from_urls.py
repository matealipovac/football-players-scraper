import psycopg2
import csv
from datetime import datetime
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import sql
from dateutil.parser import parse
import re


def can_cast_to_int(s):
    try:
        int(s)
        return True
    except:
        return False
    
# Function to scrape player data from a Wikipedia page
def scrape_player_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        player_data = {}
        
        # Scrape required player data
        player_data['url'] = url
        player_name = soup.find('h1', class_='firstHeading').text.strip()
        player_data['name'] = re.sub(r'\[\d+\]', '', player_name)
        infobox = soup.find('table', class_='infobox')
        if infobox:
            rows = infobox.find_all('tr')
            for row in rows:
                headers = row.find_all('th')
                if len(headers) == 1:
                    header = headers[0].text.strip().lower()
                    if header == 'full name':
                        full_name = row.find('td').text.strip()
                        player_data['full_name'] = re.sub(r'\[\d+\]', '', full_name)
                    elif header == 'date of birth':
                        date_of_birth_str = row.find('td').text.strip()
                        date_of_birth_match = re.search(r'\(([^)]+)\)', date_of_birth_str)
                        if date_of_birth_match:
                            date_of_birth_str = date_of_birth_match.group(1)  # Extract only the date
                            date_of_birth = parse(date_of_birth_str, fuzzy=True)  # Parse date
                            player_data['date_of_birth'] = date_of_birth.strftime('%Y-%m-%d') if date_of_birth else None
                            if date_of_birth:
                                player_data['age'] = (datetime.now() - date_of_birth).days // 365
                    elif header == 'place of birth':
                        birth_data = row.find('td').text.strip().split(", ")
                        place_of_birth = ', '.join(birth_data[:-1])
                        country_of_birth = birth_data[-1]
                        player_data['place_of_birth'] = re.sub(r'\[\d+\]', '', place_of_birth)
                        player_data['country_of_birth'] = re.sub(r'\[\d+\]', '', country_of_birth)
                    elif header == 'position(s)':
                        positions = row.find('td').text.strip()
                        player_data['positions'] = re.sub(r'\[\d+\]', '', positions)
                    elif header == 'current team':
                        current_club = row.find('td').text.strip()
                        player_data['current_club'] = re.sub(r'\[\d+\]', '', current_club)
                        if current_club:
                            data_list = []
                            # Iterate through all elements with classes 'infobox-data-a', 'infobox-data-b', 'infobox-data-c'
                            for element in soup.find_all(class_=re.compile(r'infobox-data-[abc]')):
                                element_text = element.get_text().strip()
                                data_list.append(element_text)
                            try:
                                index = data_list.index(current_club)
                                appearances = data_list[index + 1]
                                goals = data_list[index + 2]
                            except :
                                #print(f"'{current_club}' is not found in the list")
                                appearances = None
                                goals = None
                            if appearances:
                                player_data['number_of_appearances_in_current_club'] = int(appearances) if can_cast_to_int(appearances) else None
                            if goals:
                                goals_data = goals.replace("(", "").replace(")", "") 
                                player_data['goals_in_current_club'] = goals_data if can_cast_to_int(goals_data) else None
                            else:
                                player_data['number_of_appearances_in_current_club'] = None
                                player_data['goals_in_current_club'] = None
                    elif header == 'international career':
                        national_teams = soup.find_all('a', title=lambda value: value and 'national football team' in value.lower() )
                        if national_teams:
                            national_team = national_teams[0].text.strip()
                            player_data['national_team'] = re.sub(r'\[\d+\]', '', national_team)
                    
        return player_data
    else:
        print(f"Failed to fetch {url}")
        return None
    
# Function to write player data to PostgreSQL
def write_to_postgres(data, conn):
    cursor = conn.cursor()

    try:
        insert_query = sql.SQL("""  
            INSERT INTO football_players (url, name, full_name, date_of_birth, age, place_of_birth, country_of_birth,
                         positions, current_club, national_team, number_of_appearances_in_current_club,
                         goals_in_current_club, scraping_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            ON CONFLICT (url) DO UPDATE
            SET
                name = COALESCE(EXCLUDED.name, football_players.name),
                full_name = COALESCE(EXCLUDED.full_name, football_players.full_name),
                date_of_birth = COALESCE(EXCLUDED.date_of_birth, football_players.date_of_birth),
                age = COALESCE(EXCLUDED.age, football_players.age),
                place_of_birth = COALESCE(EXCLUDED.place_of_birth, football_players.place_of_birth),
                country_of_birth = COALESCE(EXCLUDED.country_of_birth, football_players.country_of_birth),
                positions = COALESCE(EXCLUDED.positions, football_players.positions),
                current_club = COALESCE(EXCLUDED.current_club, football_players.current_club),
                national_team = COALESCE(EXCLUDED.national_team, football_players.national_team),
                number_of_appearances_in_current_club = COALESCE(EXCLUDED.number_of_appearances_in_current_club, football_players.number_of_appearances_in_current_club),
                goals_in_current_club = COALESCE(EXCLUDED.goals_in_current_club, football_players.goals_in_current_club),
                scraping_timestamp = COALESCE(EXCLUDED.scraping_timestamp, football_players.scraping_timestamp);
        """)

        cursor.execute(insert_query, (
            data.get('url'),
            data.get('name'),
            data.get('full_name'),
            data.get('date_of_birth'),
            data.get('age'),
            data.get('place_of_birth'),
            data.get('country_of_birth'),
            data.get('positions'),
            data.get('current_club'),
            data.get('national_team'),
            data.get('number_of_appearances_in_current_club'),
            data.get('goals_in_current_club'),
            datetime.now()
        ))
        conn.commit()
        print(f"Inserted or updated data for player: {data['name']}")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
    finally:
        cursor.close()
    
def scrape_url_data(urlFile, db_connection):
    with open(urlFile, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header row
        for row in reader:
            player_url = row[0]
            player_data = scrape_player_data(player_url)
            if player_data:
                write_to_postgres(player_data, db_connection)
