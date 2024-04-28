import os
import sys
import argparse
import time
import pyodbc
from natsort import os_sorted

# TODO Fill in
server = ''
database = ''
username = ''
password = ''

SQL_DRIVER = 'ODBC Driver 17 for SQL Server'

DEFAULT_PREFIX_LIST = ['Prefix1', 'Prefix2']
FILE_PREFIX_SEPARATOR = r'_'
ALLOWED_SUFFIX_TUPLE = (".txt", ".sql")
PROCESSED_FILE_SUFFIX = '.done'
NO_ERRORS_MSG = 'No errors\n'

# Not using python's logging feature is a deliberate choice
error_file_name = "ImportErrors.txt"


def error_file_write(error_message):
    with open(error_file_name, 'a') as error_file:
        error_file.write(error_message + '\n')


def execute_sql_files(file_prefix, working_directory, simulation=True):
    with pyodbc.connect(f"DRIVER={SQL_DRIVER};SERVER={server};DATABASE={database};UID={username};PWD={password}") as connection:
        connection.autocommit = False
        with connection.cursor() as cursor:
            has_error = False
            try:
                error_file_write(f'{file_prefix} {"simulation errors (rollback)" if simulation else "DB Transaction errors"}:')
                print(f'\n{file_prefix} {"simulation" if simulation else "COMMITING!"}')
                for root, directory, files in os.walk(working_directory):
                    files = os_sorted(files)
                    for file in files:
                        if not file.startswith(f'{file_prefix}{FILE_PREFIX_SEPARATOR}') or not file.lower().endswith(ALLOWED_SUFFIX_TUPLE):
                            continue
                        file_process_start = time.time()
                        file_path = os.path.join(root, file)
                        base_name = os.path.basename(file_path)
                        print(f"Executing SQL statements from {base_name}...")
                        with open(file_path, 'r') as sql_file:
                            sql_statements = sql_file.readlines()
                        line = 0
                        lines_modified = 0
                        for i, statement in enumerate(sql_statements):
                            line = i + 1
                            statement = statement.strip()
                            try:
                                cursor.execute(statement)
                                row_count = cursor.rowcount
                                if row_count > 0:
                                    lines_modified += row_count
                                    if row_count > 1:
                                        error_file_write(f"{base_name} line {line} statement modified {row_count} records:\n{statement}\n")
                                else:
                                    error_file_write(f"{base_name} line {line} statement modified no records:\n{statement}\n")
                                    has_error = True
                            except Exception as e:
                                error_file_write(f"{base_name} line {line}\n{statement}\n{e}\n")
                                has_error = True
                        if line != lines_modified:
                            error_file_write(f"{base_name}: {lines_modified} lines modified out of {line} lines read.\n---\n")
                            has_error = True
                        if not simulation:
                            if has_error:
                                error_file_write(f"{base_name} file statements not commited. Errors were found.\n")
                            else:
                                connection.commit()
                                os.rename(file_path, f'{file_path}{PROCESSED_FILE_SUFFIX}')
                        print(f'{line} lines read; {lines_modified} lines modified. Elapsed: {round(time.time() - file_process_start, 2)} s.')
                if simulation:
                    connection.rollback()
                    if has_error:
                        sys.exit(1)
                    else:
                        error_file_write(NO_ERRORS_MSG)
                        execute_sql_files(file_prefix, working_directory, simulation=False)
                else:
                    if not has_error:
                        error_file_write(NO_ERRORS_MSG)
            except Exception as e:
                connection.rollback()
                print(f"Transaction rolled back. Error details: {e}", file=sys.stderr)
                raise


if __name__ == "__main__":
    start = time.time()

    arg_parser = argparse.ArgumentParser(description="Execute SQL files with specified prefixes")
    arg_parser.add_argument("directory", help="Directory containing SQL files")
    arg_parser.add_argument("prefixes", nargs="*", default=DEFAULT_PREFIX_LIST, help="Prefixes to search for")
    args = arg_parser.parse_args()

    try:
        with open(error_file_name, 'w') as create_error_file:
            pass

        expanded_directory = os.path.expanduser(args.directory)
        if not os.path.exists(expanded_directory):
            print(f"Error: Directory '{expanded_directory}' does not exist.")
            sys.exit(1)
        if not os.path.isdir(expanded_directory):
            print(f"Error: '{expanded_directory}' is not a directory.")
            sys.exit(1)

        for prefix in args.prefixes:
            execute_sql_files(prefix, expanded_directory)
    except Exception as e:
        print(f"An error occurred. Error details: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        print(f"\nTotal Elapsed: {round(time.time() - start, 2)} seconds.")
