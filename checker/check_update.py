import time
from csv import reader
from hashlib import sha256
from io import StringIO
from os import environ
from pickle import dumps

import requests
import schedule
from confluent_kafka import Producer
from dotenv import load_dotenv
from psycopg2 import connect
from requests import get


class checkForUpdate():
    def __init__(self) -> None:
        self._file_link = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update.txt"
        self._load_env()
        self._conn = connect(f"dbname={self._DB} \
            user={self._USERNAME} password={self._PASSWORD} \
            host={self._HOST}")  # Connect to the database
        self._cur = self._conn.cursor()
        self._producer = Producer({"bootstrap.servers": self._KAFKA})  # Connect to Kafka cluster

    def _load_env(self):
        # Loads enviroment variables
        load_dotenv()
        self._DB = environ.get("DBNAME", "house_data")
        self._USERNAME = environ.get("POSTGRES_USER")
        self._PASSWORD = environ.get("POSTGRES_PASSWORD")
        self._HOST = environ.get("POSTGRES_HOST")
        self._KAFKA = environ.get("KAFKA")

    def _fetch_file(self):
        print("fetching file")
        file = get(self._file_link).content  # Download monthly file from land registry
        file_hash = sha256(file).hexdigest()  # Calculate hash of file
        self._cur.execute("SELECT data FROM settings WHERE name='update_hash';")
        prev_hash = self._cur.fetchone()  # Check to see if file has been inserted already
        if prev_hash is not None:
            prev_hash = prev_hash[0]
        if file_hash != prev_hash:  # Compare hash to hash of old file
            print("New file being uploaded")
            self._update_database(file, file_hash)
        else:
            print("No new file yet")

    def _update_database(self, file, file_hash):
        self._send_file_db(file)
        self._cur.execute("""UPDATE settings SET data = %s
                          WHERE name='update_hash' returning name;""",
                          (file_hash,))
        # Changes previous hash to most recent one

        if self._cur.fetchone() is None:
            self._cur.execute("""INSERT INTO settings (name, data)
                              VALUES ('update_hash', %s)""",
                              (file_hash,))
            # Inserts update_hash row if it doesn't exist
        self._conn.commit()

    def _send_file_db(self, file):
        csv_file = reader(StringIO(file.decode("UTF-8")))
        csv_file = map(lambda x: dumps([x[0][1:-1]] + [i for i in x[1:]]), csv_file)  # Remove braces from tui
        while True:  # While is quicker than for loop
            try:
                list_bytes = next(csv_file) # Converts list to byte array
                while True:
                    try:
                        self._producer.produce("new_sales", list_bytes)  # Send each sale as string to kafka
                        self._producer.poll(0)
                        break
                    except BufferError:
                        print(time.time(), "Flushing")
                        self._producer.flush()
                        print(time.time(), "Finished flush")
            except StopIteration:
                self._producer.flush()
                break
        self._cur.execute("""UPDATE settings SET data = %s
                          WHERE name='last_updated';""",
                          (time.time(),))
        self._aggregate_counties()

    def aggregate_counties(self):
        self._cur.execute("SELECT * FROM settings WHERE name = 'last_updated' OR name = 'last_aggregated_counties' ORDER BY name DESC;")
        times = self._cur.fetchall()
        print(times)
        if float(times[0][1]) > float(times[1][1]):
            self._cur.execute("SELECT * FROM settings WHERE data = 'WAITING';")
            res = self._cur.fetchall()
            if len(res) == 4:
                self._cur.execute("SELECT area FROM areas WHERE area_type = 'county';")
                counties = self._cur.fetchall()
                self._cur.execute("""UPDATE settings SET data = 'true'
                                    WHERE name='agregating_counties';""")
                self._conn.commit()
                print(len(counties))
                for county in counties:
                    county = county[0]
                    print(county)
                    resp = requests.get(f"https://api.housestats.co.uk/api/v1/analyse/county/{county}")
                    print(resp.json())
                    if county == counties[-1][0]:
                        url = resp.json()["result"]
                        while True:
                            resp = requests.get(url)
                            if resp.json()["status"] == "SUCCESS":
                                self._cur.execute("UPDATE settings SET data = %s WHERE name='last_aggregated_counties';",
                                                (time.time(),))
                                self._cur.execute("UPDATE settings SET data = 'false' WHERE name='agregating_counties';")
                                self._conn.commit()
                                break
                            else:
                                time.sleep(5)

    def run(self):
        schedule.every(5).minutes.do(self._fetch_file)
        while True:
            schedule.run_pending()
            time.sleep(30)

if __name__ == "__main__":
    x = checkForUpdate()
    x.aggregate_counties()
    # x.run()
