import json

from abc import ABC, abstractmethod


class DatabaseInterface(ABC):
    """
    Interface for data access
    Currently using DragonFire but initial development, the Integration Team/ Hydra
    was using local disk storage.
    KSQL could be a potential for the future, just makes the code more flexible 
    """

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration

    @ abstractmethod
    def connect(self):
        raise NotImplementedError("connect method needs implementation")

    @ abstractmethod
    def query(self, query: str):
        raise NotImplementedError("query method needs implementation")

    @ abstractmethod
    def close(self):
        raise NotImplementedError("close method needs implementation")

    @ abstractmethod
    def insert(self, data: dict):
        raise NotImplementedError("insert method needs implementation")


class Database(DatabaseInterface):

    def __init__(self, impl, config):
        self.impl = impl
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        if self.connection is None:
            self.connection = self.impl.connect(**self.config)
        self.cursor = self.connection.cursor()

    def query(self, query: str, query_args: list):
        self.cursor.execute(query, query_args)

    def fetchmany(self, limits: int):
        return self.cursor.fetchmany(limits)

    def fetchone(self):
        return self.fetchone()

    def execute(self, query, query_args):
        self.cursor.execute(query, query_args)

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.connection.commit()
        if self.connection:
            self.connection.close()

    def update_users(self, users: list):
        self.cursor.executemany(
            "INSERT INTO users(name, id, avatar) VALUES(?,?,?)", users)

    def update_channels(self, channel_args, member_args):
        self.cursor.executemany(
            "INSERT INTO channels(name, id, is_private) VALUES(?,?,?)", channel_args
        )
        self.cursor.executemany(
            "INSERT INTO members(channel, user) VALUES(?,?)", member_args)

    def migrate_db(self):
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                message TEXT,
                user TEXT,
                channel TEXT,
                timestamp TEXT,
                UNIQUE(channel, timestamp) ON CONFLICT REPLACE
            )
        """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                name TEXT,
                id TEXT,
                avatar TEXT,
                UNIQUE(id) ON CONFLICT REPLACE
        )"""
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                name TEXT,
                id TEXT,
                is_private BOOLEAN NOT NULL CHECK (is_private IN (0,1)),
                UNIQUE(id) ON CONFLICT REPLACE
        )"""
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS members (
                channel TEXT,
                user TEXT,
                FOREIGN KEY (channel) REFERENCES channels(id),
                FOREIGN KEY (user) REFERENCES users(id)
            )
        """
        )
        # Add `is_private` to channels for dbs that existed in v0.1
        try:
            self.cursor.execute(
                """
                ALTER TABLE channels
                ADD COLUMN is_private BOOLEAN default 1
                NOT NULL CHECK (is_private IN (0,1))
            """
            )
        except:
            pass

    def insert_channels(self, channel_id, channel_name, channel_is_private):
        self.cursor.execute(
            "INSERT INTO channels(name, id, is_private) VALUES(?,?,?)",
            (channel_id, channel_name, channel_is_private),
        )

    def insert_members(self, members):
        self.cursor.executemany(
            "INSERT INTO members(channel, user) VALUES(?,?)", members)
