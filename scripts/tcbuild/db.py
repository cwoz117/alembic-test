#!/usr/bin/env python3
import argparse
import toml
import database


# TODO
# Don't have a good spot to put this as its 1am, not pinging teams with it:
# https://stackoverflow.com/questions/22178339/is-it-possible-to-store-the-alembic-connect-string-outside-of-alembic-ini/55190497#55190497
#
# this shows how to use the env.py file to update the sqlalchemy.url variable
# in the alembic.ini file using code.
# So we don't need to add to gitignore, can just make the database be dynamic

def create_db(env):
    db = database.Database(env, "personal")
    if not db.exists(db.database):
        #TODO exists function doesn't match properly, result does contain
        # a found database, but then says it does not exist
        # logic error here but too tired to think 'bout it atm
        print(f"{db.database} does not exist!")
    else:
        print(f"{db.database} exists!")

def backup_db(env):
    db = database.Database(env, "dev")
    print(f"saving {db.database}")

def restore_db(env):
    if args.name == "int_sol":
        raise argparse.ArgumentTypeError(f"{args.name} is the dev database, don't mess with it.")
    print(f"restore to {db.database}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TC Energy database development utility")
    parser.add_argument('command', choices=['create', 'backup', 'restore'],help='What are we doing to Redshift?')
    
    args = parser.parse_args()
    env = toml.load("env.toml")

    dispatch = {
        "backup": backup_db,
        "create": create_db,
        "restore": restore_db
    }
    dispatch[args.command](env)
