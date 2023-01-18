import sqlite3
import os
import datetime
import pandas as pd

DB_SERVER = os.getenv("DB_SERVER_NAME", "status-database.sqlite3")
DB_PATH = os.path.join("data", DB_SERVER)


class DB:

    @staticmethod
    def create_status(comid: int):
        create_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        query = "DELETE FROM DataStatus WHERE COMID=?"
        values = (comid,)
        cur.execute(query, values)
        query = "INSERT INTO DataStatus(COMID, status, create_date) VALUES(?,?,?)"
        values = (comid, "STARTED", create_timestamp)
        cur.execute(query, values)
        conn.commit()
        conn.close()

    @staticmethod
    def update_status(comid: int, status: str, message: str, file_path: str = None,
                      start_date: str = None, end_date: str = None):
        update_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        query = "Update DataStatus SET status=?, message=?, update_date=?, file_path=?, start_date=?, end_date=? WHERE COMID=?"
        values = (status, message, update_timestamp, file_path, start_date, end_date, comid)
        cur.execute(query, values)
        conn.commit()
        conn.close()

    @staticmethod
    def add_missing(comid: int, nearest: int = None):
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        query = "INSERT INTO Missing(COMID, nearest) VALUES(?,?)"
        values = (comid, nearest)
        cur.execute(query, values)
        conn.commit()
        conn.close()

    @staticmethod
    def add_huc8(comid: int, huc8: str):
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        query = "INSERT INTO HUC8(HUC8, COMID) VALUES(?,?)"
        values = (huc8, comid)
        cur.execute(query, values)
        conn.commit()
        conn.close()

    @staticmethod
    def check_comid(comid: int):
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        query = "SELECT status from DataStatus WHERE COMID=?"
        values = (comid,)
        cur.execute(query, values)
        results = cur.fetchone()
        conn.commit()
        conn.close()
        if results in ("COMPLETED", "STARTED"):
            return True
        else:
            return False

    @staticmethod
    def cleanup():
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        query = "DELETE FROM DataStatus WHERE status !='COMPLETED'"
        cur.execute(query)
        conn.commit()
        conn.close()


