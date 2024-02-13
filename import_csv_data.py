import csv
from datetime import datetime

def import_csv_data(csv_file, db_connection):
    # Open a cursor
    cursor = db_connection.cursor()
    # Read data from CSV and insert into the database
    with open(csv_file, 'r', newline='', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            # Map CSV columns to database columns
            if row.get('No data') == '1':
                #return everything null except url
                data = {
                    'url': row.get('URL', ''),
                    'name': '',
                    'full_name': '',
                    'date_of_birth': None,
                    'age': None,
                    'place_of_birth': '',
                    'country_of_birth': '',
                    'positions': '',
                    'current_club': '',
                    'national_team': '',
                    'number_of_appearances_in_current_club': None,  # Initialize as NULL
                    'goals_in_current_club': None,  # Initialize as NULL
                    'scraping_timestamp': None # Timestamp for scraping
                }
            else:
                data = {
                'url': row.get('URL', ''),
                'name': row.get('Name', ''),
                'full_name': row.get('Full name', ''),
                'date_of_birth': row.get('Date of birth', None),
                'age': row.get('Age', None),
                'place_of_birth': row.get('City of birth', ''),
                'country_of_birth': row.get('Country of birth', ''),
                'positions': row.get('Position', ''),
                'current_club': row.get('Current club', ''),
                'national_team': row.get('National_team', ''),
                'number_of_appearances_in_current_club': None,  # Initialize as NULL
                'goals_in_current_club': None,  # Initialize as NULL
                'scraping_timestamp': None  # Timestamp for scraping
                }
                

                if data['date_of_birth']:
                    if data['date_of_birth'] != None:
                        data['date_of_birth'] = datetime.strptime(row.get('Date of birth'), '%d.%m.%Y').date() if row.get('Date of birth') else None
                else:
                    data['date_of_birth'] = None
                    
                if data['age'] is not None and data['age'] != '':
                    data['age'] = int(data['age'])
                else:
                    data['age'] = None


            # Insert data into the database
            cursor.execute("""
            INSERT INTO football_players (url, name, full_name, date_of_birth, age, place_of_birth, country_of_birth,
                                 positions, current_club, national_team, number_of_appearances_in_current_club,
                                 goals_in_current_club, scraping_timestamp)
            VALUES (%(url)s, %(name)s, %(full_name)s, %(date_of_birth)s, %(age)s, %(place_of_birth)s,
                    %(country_of_birth)s, %(positions)s, %(current_club)s, %(national_team)s,
                    %(number_of_appearances_in_current_club)s, %(goals_in_current_club)s, %(scraping_timestamp)s)
            ON CONFLICT (url) DO UPDATE
            SET
                name = EXCLUDED.name,
                full_name = EXCLUDED.full_name,
                date_of_birth = EXCLUDED.date_of_birth,
                age = EXCLUDED.age,
                place_of_birth = EXCLUDED.place_of_birth,
                country_of_birth = EXCLUDED.country_of_birth,
                positions = EXCLUDED.positions,
                current_club = EXCLUDED.current_club,
                national_team = EXCLUDED.national_team,
                number_of_appearances_in_current_club = EXCLUDED.number_of_appearances_in_current_club,
                goals_in_current_club = EXCLUDED.goals_in_current_club,
                scraping_timestamp = EXCLUDED.scraping_timestamp;
            """, data)

    # Commit the transaction
    db_connection.commit()
    print("Table football_players created successfully!")
    cursor.close()