import sqlite3
import pymysql
from database import MySQL, SQLite


def select_db(db_type, config):
    if db_type.upper() == "SQL":
        return Database(pymysql.connect(**config))
    return Database(sqlite3.connect(**config))
