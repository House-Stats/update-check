import time
from csv import reader
from hashlib import sha256
from io import StringIO
import os
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
        self._DB = self.manage_sensitive("DBNAME", "house_data")
        self._USERNAME = self.manage_sensitive("POSTGRES_USER")
        self._PASSWORD = self.manage_sensitive("POSTGRES_PASSWORD")
        self._HOST = self.manage_sensitive("POSTGRES_HOST")
        self._KAFKA = self.manage_sensitive("KAFKA")

    def manage_sensitive(self, name, default= None):
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
        self.aggregate_counties()
        resp = requests.get("https://api.housestats.co.uk/api/v1/analyse/COUNTRY/ALL")
        while True:
            checker = requests.get(resp.json()["result"])
            if checker["status"] != "PENDING":
                break
            else:
                time.sleep(5)


    def aggregate_counties(self):
        self._cur.execute("SELECT * FROM settings WHERE name = 'last_updated' OR name = 'last_aggregated_counties' ORDER BY name DESC;")
        times = self._cur.fetchall()

        if float(times[0][1]) > float(times[1][1]):
            self._cur.execute("SELECT * FROM settings WHERE data = 'WAITING';")
            res = self._cur.fetchall()
            if len(res) == 4:
                self._cur.execute("SELECT area FROM areas WHERE area_type = 'area';")
                counties = self._cur.fetchall()
                self._cur.execute("""UPDATE settings SET data = 'true'
                                    WHERE name='agregating_counties';""")
                self._conn.commit()
                for county in counties:
                    county = county[0] if county != ('',) else 'CH'
                    resp = requests.get(f"https://api.housestats.co.uk/api/v1/analyse/area/{county}")
                    print(county, resp.json()["status"])
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
