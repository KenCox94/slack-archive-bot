import sqlite3
import pymysql.cursors as pymysql
from database import Database, DatabaseInterface


def select_db(db_type, config) -> DatabaseInterface:
    if db_type.upper() == "SQL":
        return Database(pymysql, config)
    return Database(sqlite3, config)
