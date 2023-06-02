import asyncio
import os
import re
import time
from csv import reader
from datetime import datetime
from hashlib import sha256
from io import StringIO
from typing import List

import requests
from asyncpg import create_pool
from dotenv import load_dotenv
from requests import get


class checkForUpdate():
    def __init__(self) -> None:
        self._file_link = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update.txt"
        self._load_env()
        self._postcode_re = re.compile("^(?:(?P<a1>[Gg][Ii][Rr])(?P<d1>) (?P<s1>0)(?P<u1>[Aa]{2}))|(?:(?:(?:(?P<a2>[A-Za-z])(?P<d2>[0-9]{1,2}))|(?:(?:(?P<a3>[A-Za-z][A-Ha-hJ-Yj-y])(?P<d3>[0-9]{1,2}))|(?:(?:(?P<a4>[A-Za-z])(?P<d4>[0-9][A-Za-z]))|(?:(?P<a5>[A-Za-z][A-Ha-hJ-Yj-y])(?P<d5>[0-9]?[A-Za-z]))))) (?P<s2>[0-9])(?P<u2>[A-Za-z]{2}))$", flags=re.IGNORECASE)
        self._areas = ["postcode", "street", "town", "district", "county", "outcode", "area", "sector"]

    async def _connect_db(self):
        self._pool = await create_pool(f"postgresql://{self._USERNAME}:{self._PASSWORD}@{self._HOST}/{self._DB}", max_size=450)

    def _load_env(self):
        # Loads enviroment variables
        load_dotenv()
        self._DB = self.manage_sensitive("DBNAME", "house_data")
        self._USERNAME = self.manage_sensitive("POSTGRES_USER")
        self._PASSWORD = self.manage_sensitive("POSTGRES_PASSWORD")
        self._HOST = self.manage_sensitive("POSTGRES_HOST")

    def manage_sensitive(self, name, default = None):
        v1 = os.environ.get(name)

        secret_fpath = f'/run/secrets/{name}'
        existence = os.path.exists(secret_fpath)

        if v1 is not None:
            return v1

        if existence:
            v2 = open(secret_fpath).read().rstrip('\n')
            return v2

        if all([v1 is None, not existence]) and default is None:
            raise KeyError(f'{name} environment variable is not defined')
        elif default is not None:
            return default

    async def _fetch_file(self):
        await self._connect_db()
        file = get(self._file_link).content  # Download monthly file from land registry
        file_hash = sha256(file).hexdigest()  # Calculate hash of file
        async with self._pool.acquire() as connection:
            prev_hash = await connection.fetchrow("SELECT data FROM settings WHERE name='update_hash';")

        if prev_hash is not None:
            prev_hash = prev_hash[0]
        if file_hash != prev_hash:  # Compare hash to hash of old file
            print("New file being uploaded")
            await self._update_database(file, file_hash)
            return True
        else:
            print("No new file yet")
            return True

    async def _update_database(self, file, file_hash):
        # await self._send_file_db(file)
        await self._update_hash(file_hash)

    async def _update_hash(self, file_hash) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute("""UPDATE settings SET data = $1
                            WHERE name='update_hash';""",
                            file_hash)
                    
            await connection.execute("""UPDATE settings SET data = $1
                            WHERE name='last_updated';""",
                            str(time.time()))

    async def _send_file_db(self, file):
        batch_counter = 0
        counter = 0
        tasks = []
        csv_file = reader(StringIO(file.decode("UTF-8")))
        csv_file = map(lambda x: [x[0][1:-1]] + [i for i in x[1:]], csv_file)  # Remove braces from tui
        while True:  # While is quicker than for loop
            try:
                data = next(csv_file) 
                tasks.append(asyncio.create_task(self._insert_sale(data)))
                counter += 1
                if counter >= 1000:
                    for task in tasks:
                        await task
                    print(batch_counter)
                    tasks = []
                    counter = 0
                    batch_counter += 1
            except StopIteration:
                for task in tasks:
                    await task
                break

    async def _insert_sale(self, sale: List) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                if sale[-1] in ["C", "D"]:
                    await connection.execute("DELETE FROM sales WHERE tui=$1", sale[0]) # Delete sale
                if sale[-1] in ["A", "C"]:
                    postcode_parts = self.extract_parts(sale[3])  # Fetches the postcode parts
                    await self._insert_areas(sale, postcode_parts, connection)

                    houseID = str(sale[7]) + str(sale[8]) + str(sale[3])
                    await connection.execute("INSERT INTO postcodes \
                                    (postcode, street, town, district, county, outcode, area, sector) \
                                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (postcode) DO NOTHING;", 
                                    sale[3], sale[9], sale[11], sale[12], sale[13], postcode_parts[0], 
                                    postcode_parts[1], postcode_parts[2])  # Insert into postcode table

                    await connection.execute("INSERT INTO houses (houseID, PAON, SAON, type, postcode) \
                                    VALUES ($1,$2,$3,$4,$5) ON CONFLICT (houseID) DO NOTHING;", 
                                    houseID, sale[7], sale[8], sale[4], sale[3])  # Insert into house table

                    new = True if sale[5] == "Y" else False # Convets to boolean type
                    freehold = True if sale[6] == "F" else False  # Converts to boolean type
                    date = datetime.strptime(sale[2], "%Y-%m-%d %H:%M")  # Converts string to datetime object

                    await connection.execute("INSERT INTO sales (tui, price, date, new, freehold, ppd_cat, houseID) \
                                VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tui) DO NOTHING;", 
                                sale[0], int(sale[1]), date, new, freehold, sale[14], houseID) # Insert into sales table

    async def _insert_areas(self, sale: List, postcode_parts: List[str], conn):
        areas = [sale[3], sale[9], sale[11], sale[12], sale[13],
                 postcode_parts[0], postcode_parts[1], postcode_parts[2]] # Extracts areas values from sale
        values = []
        for idx, area_type in enumerate(self._areas):
            area_data = (area_type, areas[idx])
            values.append(area_data)
        await conn.executemany("""INSERT INTO areas (area_type, area) 
                                VALUES ($1,$2) ON CONFLICT (area_type, area) DO NOTHING;""",values)

    def extract_parts(self, postcode: str) -> List[str]:
        try:
            if (parts := self._postcode_re.findall(postcode)[0]) != None:  # splits postcode & checks if it is valid
                parts = list(filter(lambda x : x != '', parts))  # Removes empty parts from postcode
                outcode = parts[0] + parts[1]
                area = parts[0]
                sector = parts[0] + parts[1] + " " + parts[2]
                return [outcode, area, sector]  # Returns the parts of the postcode
            else:
                return ["","",""]
        except IndexError:
            return ["","",""]

    async def _aggregate_counties(self):
        async with self._pool.acquire() as connection:
            counties = await connection.fetch("SELECT area FROM areas WHERE area_type = 'area' AND area <> '';")
        result = []
        for county in counties:
            county = county['area']
            resp = requests.get(f"https://api.housestats.co.uk/api/v1/analyse/area/{county}")
            result.append(resp.json()["result"])
            print(resp.status_code, county)

        for res in result:
            while True:
                resp = requests.get(res)
                if resp.status_code < 400:
                    if resp.json()["status"] == "SUCCESS":
                        break
                    else:
                        time.sleep(5)
                else:
                    time.sleep(10)

    def run(self):
        asyncio.run(self.do_everything())

    async def do_everything(self):
        updated = await self._fetch_file()
        if updated:
            await self._aggregate_counties()

if __name__ == "__main__":
    x = checkForUpdate()
    x.run()

