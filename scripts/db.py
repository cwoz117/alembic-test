#!/usr/bin/env python3
import argparse
import toml
import tcbuild

def create_db(env, args):
    if not tcbuild.check_db_exists(env, args.name):
        #tcbuild.create_database(env, args.name)
        print(f"{args.name} does not exist!")
    else:
        print(f"{args.name} exists!")

def rebuild(env, args):
    # drop schema check to make sure its only a dev account
    print(f"drop {args.name} schema")

def backup_db(env, args):
    print(f"saving {args.name}")

def restore_db(env, args):
    if args.name == "int_sol":
        raise argparse.ArgumentTypeError(f"{args.name} is the dev database, don't mess with it.")
    print(f"restore to {args.name}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TC Energy database development utility")
    parser.add_argument('command', choices=['create', 'backup', 'restore'],help='What are we doing to the database?')
    parser.add_argument('name',help='The database in question')
    
    args = parser.parse_args()
    env = toml.load("env.toml")

    dispatch = {
        "backup": backup_db,
        "create": create_db,
        "restore": restore_db
    }
    dispatch[args.command](env, args)
