#!/usr/bin/env python3
import argparse
import tcbuild

def create_db(args):
    #if not tcbuild.check_db_exists(args.name):
    #    tcbuild.create_database(args.name)
    tcbuild.check_db_exists()

def rebuild(args):
    # drop schema check to make sure its only a dev account
    print(f"drop {args.name} schema")

def backup_db(args):
    print(f"saving {args.name}")

def restore_db(args):
    if args.name == "int_sol":
        raise argparse.ArgumentTypeError(f"{args.name} is the dev database, don't mess with it.")
    print(f"restore to {args.name}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="TC Energy database development utility")
    parser.add_argument('command', choices=['create', 'backup', 'restore'],help='What are we doing to the database?')
    parser.add_argument('name',help='The database in question')
    args = parser.parse_args()
    dispatch = {
        "backup": backup_db,
        "create": create_db,
        "restore": restore_db
    }
    dispatch[args.command](args)