from batch_backup_data import run_redshift
DEFAULT_DB = 'tc-test-database'

def check_db_exists(env:dict,dbname:str):
    dbs, _= run_redshift(env=env,dbname = DEFAULT_DB, \
            query='SELECT datname FROM pg_database;')
   
    return dbname in [db[0] for db in dbs]

def create_database(env:dict,dbname:str):
    _, error = run_redshift(env=env,dbname = DEFAULT_DB, \
            query=f'create database {dbname};')
    return not bool(error)

def drop_database(env:dict,dbname:str):
    _, error = run_redshift(env=env,dbname = DEFAULT_DB, \
            query=f'drop database {dbname};')
    return not bool(error)

import toml, os
env = toml.load(f"{os.getcwd()}\scripts\env.toml")
# print(check_db_exists(env,'dev_alii'))
# print(create_database(env,'test1'))
# print(drop_database(env,'test1'))
