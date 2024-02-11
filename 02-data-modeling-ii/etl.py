import glob
import json
import os
from typing import List

from cassandra.cluster import Cluster

table_drop_actors = "DROP TABLE IF EXISTS actors"
table_drop_repos = "DROP TABLE IF EXISTS repos"
table_drop_events = "DROP TABLE IF EXISTS events"

table_create_actors = """
    CREATE TABLE IF NOT EXISTS actors
    (
        id text,
        login text,
        public boolean,
        PRIMARY KEY (
            id
        )
    )
"""

table_create_repos = """
    CREATE TABLE IF NOT EXISTS repo
    (
        id text,
        type text,
        public boolean,
        PRIMARY KEY (
            id
        )
    )
"""
table_create_events = """
    CREATE TABLE IF NOT EXISTS events
    (
        id text,
        type text,
        actor_id text,
        actor_login text,
        repo_id text,
        repo_name text,
        created_at timestamp,
        public boolean,
        PRIMARY KEY (
            id,
            type
        )
    )
"""

create_table_queries = [
    table_create_actors,
    table_create_repos,
    table_create_events
]
drop_table_queries = [
    table_drop_actors,
    table_drop_repos,
    table_drop_events,
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                print(each["id"], each["type"], each["actor"]["login"])

                #Insert data into tables here
                query = f"""
                   INSERT INTO events (
                        id,
                        type,
                        actor_id,
                        actor_login,
                        repo_id,
                        repo_name,
                        created_at,
                        public) 
                    VALUES ('{each["id"]}', '{each["type"]}', 
                            '{each["actor"]["id"]}','{each["actor"]["login"]}',
                            '{each["repo"]["id"]}', '{each["repo"]["name"]}', 
                            '{each["created_at"]}',{each["public"]})
                """
                session.execute(query)

def insert_sample_data(session):
    query = f"""
    INSERT INTO events (id, type, public) VALUES ('23487929637', 'IssueCommentEvent', true)
    """
    session.execute(query)


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)

    process(session, filepath="../data")

    query = """
    SELECT * from events
    """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()