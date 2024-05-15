import re
import csv
import sys
import os
import time
import signal

def count_total_rows(file_path):
    total_rows = 0
    with open(file_path, 'r') as infile:
        for line in infile:
            if line.strip().upper().startswith('INSERT INTO'):
                matches = re.findall(r'\(([^)]+)\)', line)
                total_rows += len(matches)
    return total_rows

def extract_values_from_sql_line(sql_line):
    matches = re.findall(r'\(([^)]+)\)', sql_line)
    values = [value.split(',') for value in matches]
    values = [[v.strip().strip("'") for v in group] for group in values]
    return values

def write_values_to_csv(writer, values, total_rows, processed_rows):
    for value in values:
        writer.writerow(value)
        processed_rows += 1
        print_progress(processed_rows, total_rows)
    return processed_rows

def extract_table_structure(file_path):
    with open(file_path, 'r') as infile:
        create_table_sql = ""
        for line in infile:
            if line.strip().upper().startswith('CREATE TABLE'):
                create_table_sql += line
                while not line.strip().endswith(';'):
                    line = next(infile)
                    create_table_sql += line
                break
    if not create_table_sql:
        raise ValueError("No 'CREATE TABLE' statement found in the input file.")
    
    table_name_match = re.search(r'CREATE TABLE (\S+)', create_table_sql)
    if not table_name_match:
        raise ValueError("No table name found in the 'CREATE TABLE' statement.")
    table_name = table_name_match.group(1)
    
    columns = re.findall(r'\(\s*(.*)\s*\)', create_table_sql, re.DOTALL)
    if not columns:
        raise ValueError("No columns found in the 'CREATE TABLE' statement.")
    
    columns = columns[0].split(',')
    num_columns = len(columns)
    return table_name, num_columns

def print_progress(iteration, total):
    bar_length = 50
    progress = float(iteration) / total
    arrow = '-' * int(round(progress * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write(f'\rProgress: [{arrow + spaces}] {iteration}/{total} Rows - Estimated Time Remaining: {calculate_time_remaining(iteration, total)}')
    sys.stdout.flush()

def calculate_time_remaining(iteration, total):
    if iteration <= total * 0.05:
        return '--:--:--'

    elapsed_time = time.time() - start_time
    time_per_iteration = elapsed_time / iteration
    remaining_iterations = total - iteration
    remaining_time = time_per_iteration * remaining_iterations
    return time.strftime('%H:%M:%S', time.gmtime(remaining_time))

def process_sql_file(input_sql_file, output_csv_file, total_rows, column_names):
    global start_time
    start_time = time.time()
    try:
        with open(input_sql_file, 'r') as infile, open(output_csv_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(column_names)
            processed_rows = 0

            for line in infile:
                if line.strip().upper().startswith('INSERT INTO'):
                    values = extract_values_from_sql_line(line)
                    processed_rows = write_values_to_csv(writer, values, total_rows, processed_rows)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\nFinished processing and writing {processed_rows} rows in {elapsed_time:.2f} seconds.")
    except KeyboardInterrupt:
        print("\nUser exit handled, export canceled!")

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

def signal_handler(signal, frame):
    print("\nUser exit handled, export canceled!")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    clear_terminal()

    if len(sys.argv) != 3:
        print(f"Missing arguments!\nThis script helps export data from an SQL table dump to a CSV file.\nUsage: {os.path.basename(__file__)} <input_sql_file> <output_csv_file>")
        sys.exit(1)

    input_sql_file = sys.argv[1]
    output_csv_file = sys.argv[2]

    if not os.path.isfile(input_sql_file):
        print(f"Error: Input file '{input_sql_file}' does not exist or is not a file.")
        sys.exit(1)

    if not input_sql_file.lower().endswith('.sql'):
        print(f"Error: Input file '{input_sql_file}' is not an SQL file.")
        sys.exit(1)
    print(f"This script helps export data from an SQL table dump to a CSV file.\n\nCopyright (C) 2024 < Author : github.com/dalymr >\n\nThis program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License or any later version.\n\n ")
    time.sleep(2)
    print(f"*** Reading from file: {input_sql_file}")

    try:
        table_name, num_columns = extract_table_structure(input_sql_file)
        print(f"*** Detected table dump: {table_name}")
        print(f"*** Number of columns found: {num_columns}")
    except Exception as e:
        print(f"Error extracting table structure: {e}")
        sys.exit(1)

    try:
        column_names = ['column' + str(i) for i in range(1, num_columns + 1)]
        total_rows = count_total_rows(input_sql_file)
        if total_rows == 0:
            print("Error: No valid 'INSERT INTO' statements found in the input file.")
            sys.exit(1)
        print(f"Total rows to process: {total_rows}\n\n")
    except Exception as e:
        print(f"Error processing input file: {e}")
        sys.exit(1)
    print("Buffering data from SQL file... \n\n\n")

    try:
        process_sql_file(input_sql_file, output_csv_file, total_rows, column_names)
        print(f"Data written to file: {output_csv_file}")
    except Exception as e:
        print(f"Error writing to output file: {e}")
        sys.exit(1)
