import os
import subprocess
import argparse
import datetime
import time
import sys

def execute_queries(name, directory_path):
    """
    Iterate through all files in a directory, read each file as a query,
    and execute a CLI command with that query.

    Parameters:
    - directory_path: Path to the directory containing query files.
    - cli_command_template: Command with '{}' as placeholder for the query.
    """

    output_log_path = os.path.join(os.getcwd(), "experiments")
    output_log_file = os.path.join(output_log_path, f"{name}.log")

    # checks if specified output path is valid
    if not os.path.isdir(output_log_path):
        os.makedirs(output_log_path, exist_ok=False)
        
    # Initialize the log file before anything else
    with open(output_log_file, "w", encoding="utf-8") as log_file:
        log_file.write(f"Experiment log for: {name}\nExperiment {name} began at {datetime.datetime.now().isoformat()}\n\n")
    
    # record results and write them to the log file
    n = 1
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        sources = getSources(open(file_path, 'r'))
        # Format the CLI command
        base_command = f"node comunica/engines/query-sparql/bin/query-dynamic.js "
        for source in sources:
            if source != "":
                fixed_source = source.replace('\n', '')
                base_command += f"{fixed_source} "
        print(f"Processing query {n}/{str(len(os.listdir(directory_path)))}: {filename}")
        base_command += f"-f {file_path} -t 'application/sparql-results+json' --httpRetryCount=2"
        start_time = datetime.datetime.now()
        with open(output_log_file, "a", encoding="utf-8") as log_file:
            log_file.write(f"Executing: {base_command}\n")
            log_file.write(f"Timestamp (start): {start_time.isoformat()}\n")
            try:
                result = subprocess.run(base_command, shell=True, check=True, text=True, capture_output=True)
                log_file.write("Output:\n" + result.stdout + "\n")
            except subprocess.CalledProcessError as e:
                log_file.write(f"Error executing command for {filename}: {e.stderr}\n")
            end_time = datetime.datetime.now()
            log_file.write(f"Timestamp (end): {end_time.isoformat()}\n\n")
        print(f"Finished with query {n}/{str(len(os.listdir(directory_path)))}: {filename}")
        n += 1
        if n < len(os.listdir(directory_path)):
            time.sleep(1)
            print("\nShort 1 second break between queries\n")
    
    # end of the experiment
    with open(output_log_file, "a", encoding="utf-8") as log_file:
        log_file.write(f"Experiment {name} completed at {datetime.datetime.now().isoformat()}\n")


def getSources(query_file):
    """
    Function to get the sources for the CLI command.
    This is a placeholder and should be replaced with actual logic to retrieve sources.
    """
    f = query_file.readlines()
    return f[0].split("# Datasources: ")[1].split(' ')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Comunica tests script.")
    parser.add_argument("-n", "--name", type=str, required=True, help="Name of the experiment run (please avoid using spaces)")    
    parser.add_argument("-t", "--type", type=str, required=True, help="Type of queries to execute (SERVICE or NO SERVICE).")
    args = parser.parse_args()

    if " " in args.name:
        print("Invalid experiment name. Please avoid spaces.")
        sys.exit(1)
    else:
        print(f"Experiment Name: {args.name}\n")

    if args.type.lower() not in ["service", "noservice", "no-service", "no service"]:
        print("Invalid query type. Please use 'service' or 'no service'.")
        sys.exit(1)
    else:
        if args.type.lower() == "service":
            input_directory = os.path.join(os.getcwd(), "queries", "service")
        else:
            input_directory = os.path.join(os.getcwd(), "queries", "no-service")

    execute_queries(args.name, input_directory)

    print(f"\nQuery execution completed, results can be found in experiments/{args.name}.log.")