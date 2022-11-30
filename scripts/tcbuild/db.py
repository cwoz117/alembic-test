#!/usr/bin/env python3
import argparse
import toml
import logging
import database


def create_db(env, dbName):
    dev = database.Database(env)
    if not dev.exists(dbName):
        logging.info(f"{dbName} does not exist!")
        dev.create_database(dbName)
        print(f"{dbName} created!")
    else:
        print(f"{dbName} exists!")

def delete_db(env, dbName):
    dev = database.Database(env)
    if dev.exists(dbName):
        logging.info(f"{dbName} is slated for deletion!")
        dev.drop_database(dbName)
        print(f"{dbName} deleted")
    else:
        logging.info(f"{dbName} does not exist!")

def backup_db(env, dbName):
    db = database.Database(env)
    logging.info(f"saving {db.database}")
    db.backup_data(env.get('backup_bucket').get('name'), env.get('backup_bucket').get('iam'))
    print(f"{db.database} saved")
  
def restore_db(env, dbName):
    db = database.Database(env)
    logging.info(f"restore db from s3")
    iam    = env.get('backup_bucket').get('iam')
    bucket = env.get('backup_bucket').get('name')
    db.restore_data(bucket, iam)
    print(f"{db.database} restored from s3")
    dbName = env.get('redshift').get('sandbox').get('database')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TC Energy database development utility")
    parser.add_argument(
            'command', choices=['create', 'backup', 'restore'],
            help='What are we doing to Redshift?')
    parser.add_argument(
            '-d', '--debug', help='Log level: DEBUG',
            action="store_const", dest="loglevel", 
            const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument(
            '-v', '--verbose',
            help="Be verbose",
            action="store_const", dest="loglevel", const=logging.INFO)
    parser.add_argument(
            '-d', '--database',
            help="Database name",
            default="")

    
    args = parser.parse_args()
    env = toml.load("env.toml")
    logging.basicConfig(level=args.loglevel)

    dispatch = {
        "backup": backup_db,
        "create": create_db,
        "restore": restore_db,
        "sync": sync_db,
        "drop": delete_db,
    }
    dispatch[args.command](env, args.database)
