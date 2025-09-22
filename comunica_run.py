import os
import subprocess
import argparse
import datetime
import time
import sys

def execute_queries(name, directory_path, output_base_path):
    """
    Iterate through all files in a directory, read each file as a query,
    and execute a CLI command with that query.

    Parameters:
    - directory_path: Path to the directory containing query files.
    - cli_command_template: Command with '{}' as placeholder for the query.
    """

    output_path = os.path.join(os.getcwd(), "experiments", name)
    batch_name = directory_path.split("/")[-2]
    output_results_file =  os.path.join(output_path, f"{name}-{batch_name}.txt")

    # checks if specified output path is valid
    if not os.path.isdir(output_path):
        os.makedirs(output_path, exist_ok=False)
        
    # Initialize the log file before anything else
    with open(output_results_file, "w", encoding="utf-8") as results_file:
        results_file.write(f"Experiment log for: {name}\nExperiment {name} began at {datetime.datetime.now().isoformat()}\n\n")
    
    # record results and write them to the log file
    n = 1
    for filename in os.listdir(directory_path):
        output_log_file = os.path.join(output_path, f"{filename}.log")
        file_path = os.path.join(directory_path, filename)
        sources = getSources(open(file_path, 'r'))
        # Format the CLI command
        base_command = f"node comunica/engines/query-sparql/bin/query-dynamic.js "
        for source in sources:
            if source != "":
                fixed_source = source.replace('\n', '')
                base_command += f"{fixed_source} "
        print(f"Processing query {n}/{str(len(os.listdir(directory_path)))}: {filename}")
        base_command += f"-f {file_path} -t 'application/sparql-results+json' -l debug 2> {output_log_file} --httpRetryCount=50"
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
        print(f"Finished with query {n}/{str(len(os.listdir(directory_path)))}: {filename}")
        n += 1
        if n < len(os.listdir(directory_path)) + 1:
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


    if args.type.lower() == "service":
        input_directory = os.path.join(os.getcwd(), "queries", "service")
    else:
        input_directory = os.path.join(os.getcwd(), "queries", args.type.lower())

    execute_queries(args.name, input_directory, args.output)

    print(f"\nQuery execution completed, results can be found in experiments/{args.name}.log.")