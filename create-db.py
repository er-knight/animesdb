import json
import lzma
import sqlite3

from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

jsondb_path = Path(__file__).parent / 'anime-offline-database-minified.json'

with jsondb_path.open('rb') as f:
    data = json.load(f)['data']

sqlitedb_path = Path(__file__).parent / 'anime.db'

with sqlite3.connect(str(sqlitedb_path)) as connection:

    cursor = connection.cursor()

    def stage_one():
        cursor.execute(
            '''
            DROP TABLE IF EXISTS animes
            '''
        )

        cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS animes (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                type TEXT,
                episodes INTEGER,
                status TEXT,
                airing_period TEXT,
                image_url TEXT,
                rank INTEGER,
                rating REAL
            )
            '''
        )

        for anime in data:
            anime_id = None
            for source in anime['sources']:
                if 'myanimelist' in source:
                    anime_id = int(source.split('/')[-1])
            if anime_id:
                cursor.execute(
                    '''
                    INSERT INTO animes (id, title, type, episodes, status, image_url)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        anime_id,
                        anime['title'],
                        anime['type'] if anime['type'] in (
                            'TV', 'OVA', 'ONA') else anime['type'].title(),
                        anime['episodes'],
                        anime['status'].title(),
                        anime['picture']
                    )
                )

            connection.commit()

    def stage_two():
        compressed_files_dir = Path(__file__).parent / 'compressed-webpages'
        for filepath in compressed_files_dir.iterdir():
            if filepath.is_file():
                with lzma.open(filepath, 'r') as f:
                    soup = BeautifulSoup(f.read(), 'html.parser')

                    for row in soup.find_all('tr', {'class': 'ranking-list'}):
                        anime_id = int(row.find('a', {'class': 'hoverinfo_trigger'}).get(
                            'href').split('/')[4])
                        rank = (lambda s: int(s) if s.isdigit() else None)(
                            row.find(
                                'span', {'class': 'top-anime-rank-text'}).text
                        )
                        rating = (lambda s: float(s) if s.lower() != 'n/a' else None)(
                            row.find('span', {'class': 'score-label'}).text
                        )

                        parts = [
                            part.strip() for part in (row.find(
                                'div', {'class': 'information'}
                            ).text.strip().split('\n')[1].strip().split('-')) if part.strip()
                        ]

                        airing_period = None

                        if parts:
                            if len(parts) == 1:
                                if parts[0].isdigit():
                                    airing_period = parts[0]
                                else:
                                    airing_period = datetime.strptime(
                                        parts[0], '%b %Y').strftime('%m/%Y')
                            elif len(parts) == 2:
                                if parts[0].isdigit() and parts[1].isdigit():
                                    if parts[0] == parts[1]:
                                        airing_period = parts[0]
                                    else:
                                        airing_period = f'{parts[0]} - {parts[1]}'
                                elif parts[0].isdigit() or parts[1].isdigit():
                                    parts[0] = parts[0][-4:]
                                    parts[1] = parts[1][-4:]
                                    if parts[0] == parts[1]:
                                        airing_period = parts[0]
                                    else:
                                        airing_period = f'{parts[0]} - {parts[1]}'
                                else:
                                    parts[0] = datetime.strptime(
                                        parts[0], '%b %Y').strftime('%m/%Y')
                                    parts[1] = datetime.strptime(
                                        parts[1], '%b %Y').strftime('%m/%Y')
                                    if parts[0] == parts[1]:
                                        airing_period = parts[0]
                                    else:
                                        airing_period = f'{parts[0]} - {parts[1]}'

                        try:
                            cursor.execute(
                                '''
                                UPDATE animes
                                SET rank = ?,
                                    rating = ?, 
                                    airing_period = ?
                                WHERE id = ?
                                ''', (rank, rating, airing_period, anime_id)
                            )
                        except Exception as e:
                            print(f'[{id}]', e)

            connection.commit()

    stage_one()
    stage_two()

    cursor.close()
