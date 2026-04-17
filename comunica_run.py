import os
import subprocess
import argparse
import datetime
import time
import sys

def list_query_files(directory_path):
    """
    Return sorted filenames for files directly inside a query directory.
    """
    query_files = []
    for filename in os.listdir(directory_path):
        full_path = os.path.join(directory_path, filename)
        if os.path.isfile(full_path):
            query_files.append(filename)
    return sorted(query_files)

def execute_queries(name, directory_path, output_base_path):
    """
    Iterate through all files in a directory, read each file as a query,
    and execute a CLI command with that query.

    Parameters:
    - directory_path: Path to the directory containing query files.
    - cli_command_template: Command with '{}' as placeholder for the query.
    """

    output_path = os.path.join(os.getcwd(), "experiments", name)
    batch_name = os.path.basename(os.path.normpath(directory_path))
    output_results_file =  os.path.join(output_path, f"{name}-{batch_name}.txt")

    # checks if specified output path is valid
    if not os.path.isdir(output_path):
        os.makedirs(output_path, exist_ok=False)
        
    query_files = list_query_files(directory_path)
    total_queries = len(query_files)
    if total_queries == 0:
        print(f"No query files found in {directory_path}. Skipping batch.")
        return

    # Initialize the log file before anything else
    with open(output_results_file, "w", encoding="utf-8") as results_file:
        results_file.write(f"Experiment log for: {name}\nExperiment {name} began at {datetime.datetime.now().isoformat()}\n\n")
    
    # record results and write them to the log file
    n = 1
    for filename in query_files:
        output_log_file = os.path.join(output_path, f"{filename}.log")
        file_path = os.path.join(directory_path, filename)
        with open(file_path, "r", encoding="utf-8") as query_file:
            sources = getSources(query_file)
        # Format the CLI command
        base_command = f"node comunica/engines/query-sparql/bin/query-dynamic.js "
        for source in sources:
            if source != "":
                fixed_source = source.replace('\n', '')
                base_command += f"{fixed_source} "
        print(f"Processing query {n}/{str(total_queries)}: {filename}")
        base_command += f"-f {file_path} -t 'application/sparql-results+json' -l debug 2> {output_log_file} --httpRetryCount=2"
        start_time = datetime.datetime.now()
        with open(output_results_file, "a", encoding="utf-8") as results_file:
            results_file.write(f"Executing: {base_command}\n")
            results_file.write(f"Timestamp (start): {start_time.isoformat()}\n")
            try:
                result = subprocess.run(base_command, shell=True, check=True, text=True, capture_output=True)
                results_file.write("Output:\n" + result.stdout)
            except subprocess.CalledProcessError as e:
                results_file.write(f"Error executing command for {filename}: {e.stderr}\n")
            end_time = datetime.datetime.now()
            results_file.write(f"Timestamp (end): {end_time.isoformat()}\n\n")
        print(f"Finished with query {n}/{str(total_queries)}: {filename}")
        n += 1
        if n < total_queries + 1:
            print("\nShort 1 second break between queries\n")
            time.sleep(1)

    # end of the experiment
    with open(output_results_file, "a", encoding="utf-8") as results_file:
        results_file.write(f"Experiment {name} completed at {datetime.datetime.now().isoformat()}\n")


def getSources(query_file):
    """
    Function to get the sources for the CLI command.
    This is a placeholder and should be replaced with actual logic to retrieve sources.
    """
    f = query_file.readlines()
    return f[0].split("# Datasources: ")[1].split(' ')

def get_batch_directories(input_directory):
    """
    If a directory contains subdirectories, treat each subdirectory as a batch.
    Otherwise, treat the directory itself as a single batch.
    """
    subdirectories = []
    for item in os.listdir(input_directory):
        full_path = os.path.join(input_directory, item)
        if os.path.isdir(full_path):
            subdirectories.append(full_path)

    if subdirectories:
        return sorted(subdirectories)
    return [input_directory]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Comunica tests script.")
    parser.add_argument("-n", "--name", type=str, required=True, help="Name of the experiment run (please avoid using spaces)")
    parser.add_argument("-o", "--output", type=str, default="queries", help="The base directory for output files.")
    parser.add_argument("-t", "--type", type=str, required=True, help="The directory of queries to execute.")
    args = parser.parse_args()

    if " " in args.name:
        print("Invalid experiment name. Please avoid spaces.")
        sys.exit(1)
    else:
        print(f"Experiment Name: {args.name}\n")


    requested_type = args.type.strip()
    if requested_type.lower() == "service":
        input_directory = os.path.join(os.getcwd(), "queries", "service")
    else:
        normalized_type = requested_type.strip("/\\")
        input_directory = os.path.join(os.getcwd(), "queries", normalized_type)

    if not os.path.isdir(input_directory):
        print(f"Input directory does not exist: {input_directory}")
        sys.exit(1)

    batch_directories = get_batch_directories(input_directory)
    total_batches = len(batch_directories)

    for index, batch_directory in enumerate(batch_directories, start=1):
        if total_batches > 1:
            batch_name = os.path.basename(os.path.normpath(batch_directory))
            print(f"Running batch {index}/{total_batches}: {batch_name}")
        execute_queries(args.name, batch_directory, args.output)
        if total_batches > 1 and index < total_batches:
            print("\nMoving to next batch...\n")

    print(f"\nQuery execution completed, results can be found in experiments/{args.name}/.")
