import argparse
from .db_manager import DbManager

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--command", type = str)
    parser.add_argument("-t", "--table", type = str)
    parser.add_argument("-d", "--database", type = str)

    args = parser.parse_args()

    match args.command:
        case "tables":
            print(DbManager.load(args.database).show_tables())
        case "show_table":
            print(DbManager.load(args.database).show_table_structure(args.table))
        case _:
            raise ValueError(f"Unknown command {args.command}")

