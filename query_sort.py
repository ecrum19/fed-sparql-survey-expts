import os
import json
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="Generate individual query files in designated directory.")
    parser.add_argument("-i", "--input", type=str, required=True, help="The input JSON file.")
    parser.add_argument("-t", "--type", type=str, required=True, help="The type of queries you want generated (SERVICE or NOSERVICE).")
    args = parser.parse_args()
    input_file = args.input
    service_type = args.type.lower()
    if service_type not in ["service", "noservice", "no-service", "no service"]:
        print("Sort option not supported, please use 'service or 'noservice' as third argument")
        sys.exit(1)

    if service_type != "service":
        service_type = "no-service"
    else:
        service_type = "service"
    
    input_file_path = os.path.join(os.getcwd(), input_file)
    # checks input file validity
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except Exception as e:
        print(f"Error reading {input_file}: {e}")
        sys.exit(1)

    working_output_dir = os.path.join(os.getcwd(), "queries", service_type)
    # checks if specified output path is valid
    if not os.path.isdir(working_output_dir):
        print(f"Output directory {working_output_dir} does not exist. Creating it...")
        try:
            os.makedirs(working_output_dir, exist_ok=False)
        except Exception as e:
            print(f"Directory {working_output_dir} already exists")

    # Make sure the JSON has a top-level "data" key.
    if "data" not in json_data:
        print("JSON file does not contain a top-level 'data' key.")
        sys.exit(1)

    # generate queries    
    if service_type == "service":
        withService(json_data, working_output_dir)
    elif service_type == "no-service":
        withoutService(json_data, working_output_dir)
    

def withService(data, out_directory):

    excluded = [
        "13", # server-side error
        "14", # server-side error
        "20", # server-side error
        "46", # server-side error
        "99_uniprot_identifiers_org_translation",   # server-side error
        "90_uniprot_affected_by_metabolic_diseases_using_MeSH", # server-side error
    ]
    total = 0
    past_names = []
    for item_key, item_value in data["data"].items():
        s_query_text = item_value.get("query").split('\n')
        fix_prefix_s_query_text = ''
        for i in s_query_text:
            if i.strip() != '':
                fix_prefix_s_query_text += f"\n{i}"
        
        s_query_source = item_value.get("target")

        if s_query_text is None:
            print(f"Skipping item '{item_key}': no 'query' property found.")
            continue
        
        # Generate a safe filename.
        # Here we use os.path.basename to extract the last part of the URL.
        base_name = os.path.basename(item_key)

        # In case the key does not have a proper basename, replace unsafe characters.
        if not base_name:
            base_name = item_key.replace('https://', '00').replace('/', '_')
        
        # Case where file name is repeated
        if base_name in past_names:
            base_name += "a"
        past_names.append(base_name)

        # Append the .rq extension.
        s_output_filename = f"{base_name}.rq"
        s_full_output_path = os.path.join(out_directory, s_output_filename)

        if base_name not in excluded:
            total += 1
            # for with SERVICE descriptions
            try:
                with open(s_full_output_path, 'w', encoding='utf-8') as out_file:
                    out_file.write("# Datasources: %s%s" % (s_query_source, fix_prefix_s_query_text))
                # print(f"Created file: {output_filename}")
            except Exception as e:
                print(f"Error writing {s_output_filename}: {e}")
    print("Total with service queries:", total)



def withoutService(data, out_directory):
    """
    Splits no-service queries into four batches:
    1. Bgee queries (dir: ns_get)
    2. Batch 1 (dir: ns_batch1)
    3. Batch 2 (dir: ns_batch2)
    4. Batch 3 (dir: ns_batch3)
    """
    excluded = [
        "14", # server-side error
        "20", # server-side error
        "46", # server-side error
        "99_uniprot_identifiers_org_translation",   # server-side error
        "90_uniprot_affected_by_metabolic_diseases_using_MeSH", # server-side error
        "70_enzymes_interacting_with_molecules_similar_to_dopamine", # IDSM
        "71_enzymes_interacting_with_molecules_similar_to_dopamine_with_variants_related_to_disease", # IDSM
        "52",  # IDSM
        "54",  # IDSM
        "60",  # IDSM
        "002", # IDSM
        "18a", # IDSM
        # "29", # UniProt stress
        # "36", # UniProt stress
        # "19", # UniProt stress
        # "42", # UniProt stress
        # #"13", # UniProt stress
        # "40", # UniProt stress
        # "43", # UniProt stress
        # "50", # stack-trace
        # "19", # weird UniProt error
        # "27", # stack-trace
        # "45", # UniProt stress
        # "19_draft_human_metabolome", # UniProt stress
        # "48", # UniProt stress
        # "38", # UniProt stress
        # "11", # UniProt stress
        # "53", # UniProt stress
        # "27", # UniProt stress
        # "12", # UniProt stress
        # "26", # UniProt stress
        # "16", # UniProt stress
        # biosoda problems
        # "emi#examples019b",
        # "emi#examples018",
        # "emi#examples012",
        # "emi#examples019a",
        # "emi#examples011a",
        # "emi#examples011b",
        # "emi#examples013",
        # "007",
        # "006",
        # "009",
        # "017",
        # "016",
        # "emi#examples021",
        # "emi#examples015",
        # "001",
        "38", # uniprot broken query
        "49", # uniprot broken query
    ]
    included = [
        "42",
        "117_biosodafrontend_glioblastoma_orthologs_rat",
        "118_biosodafrontend_rat_brain_human_cancer",
        "109_uniprot_transporter_in_liver",
        "43",
        "53",
        "29",
        "50"
    ]
    bgee = [
        "50",
        "49",
        "109_uniprot_transporter_in_liver",
        "20",
        "16",
        "17",
        "19",
        "18",
        "117_biosodafrontend_glioblastoma_orthologs_rat",
        "118_biosodafrontend_rat_brain_human_cancer",
        "028-biosodafrontend",
        "027-biosodafrontend"
    ]

    # Collect all eligible queries
    all_queries = []
    for item_key, item_value in data["data"].items():
        base_name = os.path.basename(item_key)
        if not base_name:
            base_name = item_key.replace('https://', '00').replace('/', '_')
        if base_name in excluded:
            continue
        all_queries.append((item_key, item_value))
    # Separate Bgee queries
    bgee_queries = [q for q in all_queries if os.path.basename(q[0]) in bgee or q[0] in bgee]
    other_queries = [q for q in all_queries if not (os.path.basename(q[0]) in bgee or q[0] in bgee)]
    # Split other queries into 3 roughly equal batches
    batch_size = (len(other_queries) + 2) // 3  # ensures all queries are included
    batches = [
        other_queries[0:batch_size],
        other_queries[batch_size:2*batch_size],
        other_queries[2*batch_size:]
    ]
    batch_dirs = ["ns_batch1", "ns_batch2", "ns_batch3"]
    # Create output directories
    ns_get_dir = os.path.join(out_directory, "ns_get")
    os.makedirs(ns_get_dir, exist_ok=True)
    total = 0
    for bd in batch_dirs:
        os.makedirs(os.path.join(out_directory, bd), exist_ok=True)
    # Write Bgee queries
    print(f"Writing {len(bgee_queries)} Bgee queries to {ns_get_dir}")
    total = total + len(bgee_queries)
    write_no_service_queries(bgee_queries, ns_get_dir)
    # Write other batches
    for i, batch in enumerate(batches):
        total = total + len(batch)
        batch_dir = os.path.join(out_directory, batch_dirs[i])
        print(f"Writing {len(batch)} queries to {batch_dir}")
        write_no_service_queries(batch, batch_dir)
    print("\nTotal no-service queries:", total)

def write_no_service_queries(queries, out_directory):
    past_names = []
    for item_key, item_value in queries:
        s_query_text = item_value.get("query")
        ns_query_source = item_value.get("target")
        if s_query_text is None:
            print(f"Skipping item '{item_key}': no 'query' property found.")
            continue
        # generate no SERVICE description query
        split_query = s_query_text.split("\n")
        ns_query_text = ""
        brace_count = 0
        curr_service = False
        for line in split_query:
            if "SERVICE" in line:
                tab_count = line.count("\t")
                tabs = ""
                for i in range(tab_count+1):
                    tabs += "\t"
                line_s = line.split("<")
                for s in line_s:
                    if ">" in s:
                        source = s.split(">")[0]
                        if "{" in source:
                            source = source[:-1].strip()
                if source not in ns_query_source:
                    ns_query_source += " %s" % source
                brace_count += 1
                curr_service = True
                ns_query_text += (tabs + "{\n")
            elif "{" in line and "}" in line and curr_service:
                ns_query_text += "%s\n" % line
            elif "{" in line and curr_service:
                brace_count += 1
                ns_query_text += "%s\n" % line
            elif "}}" in line:
                tab_count = line.count("\t")
                tabs = ""
                for i in range(tab_count):
                    tabs += "\t"
                rm_one_bracket = line.replace("}}", "}")
                ns_query_text += tabs + "}\n" + tabs[:-1] + "%s\n" % rm_one_bracket
            else:
                if line.strip() != '':
                    ns_query_text += "%s\n" % line
        base_name = os.path.basename(item_key)
        if not base_name:
            base_name = item_key.replace('https://', '00').replace('/', '_')
        if base_name in past_names:
            base_name += "a"
        past_names.append(base_name)
        ns_output_filename = f"{base_name}_ns.rq"
        ns_full_output_path = os.path.join(out_directory, ns_output_filename)
        try:
            with open(ns_full_output_path, 'w', encoding='utf-8') as out_file:
                out_file.write("# Datasources: %s\n%s" % (ns_query_source, ns_query_text.rstrip('\n')))
        except Exception as e:
            print(f"Error writing {ns_output_filename}: {e}")


    # # Iterate over each item in the "data" dictionary.
    # total = 0
    # past_names = []
    # for item_key, item_value in data["data"].items():
    #     s_query_text = item_value.get("query")
    #     ns_query_source = item_value.get("target")

    #     if s_query_text is None:
    #         print(f"Skipping item '{item_key}': no 'query' property found.")
    #         continue

        
    #     # generate no SERVICE description query
    #     split_query = s_query_text.split("\n")
    #     ns_query_text = ""
    #     brace_count = 0
    #     curr_service = False
    #     for line in split_query:
    #         # remove SERVICE description line
    #         if "SERVICE" in line:
    #             tab_count = line.count("\t")
    #             tabs = ""
    #             for i in range(tab_count+1):
    #                 tabs += "\t"
    #             line_s = line.split("<")
    #             for s in line_s:
    #                 if ">" in s:
    #                     source = s.split(">")[0]
    #                     if "{" in source:
    #                         source = source[:-1].strip()
    #             # avoid duplicate sources
    #             if source not in ns_query_source:
    #                 ns_query_source += " %s" % source
    #             brace_count += 1
    #             curr_service = True
    #             ns_query_text += (tabs + "{\n")
            
    #         # case where both brackets are in same line
    #         elif "{" in line and "}" in line and curr_service:
    #             ns_query_text += "%s\n" % line

    #         # count bracket offset
    #         elif "{" in line and curr_service:
    #             brace_count += 1
    #             ns_query_text += "%s\n" % line

    #         # find closing bracket for SERVICE clause
    #         # elif "}" in line and curr_service:
    #         #     brace_count -= 1
    #         #     if brace_count > 0:
    #         #         ns_query_text += "%s\n" % line
    #         #     else:
    #         #         curr_service = False
    #         #         brace_count = 0
            
    #         # fix double bracket syntax
    #         elif "}}" in line:
    #             tab_count = line.count("\t")
    #             tabs = ""
    #             for i in range(tab_count):
    #                 tabs += "\t"
    #             rm_one_bracket = line.replace("}}", "}")
    #             ns_query_text += tabs + "}\n" + tabs[:-1] + "%s\n" % rm_one_bracket

    #         # for normal query lines
    #         else:
    #             if line.strip() != '':
    #                 ns_query_text += "%s\n" % line


    #     # Generate a safe filename.
    #     # Here we use os.path.basename to extract the last part of the URL.
    #     base_name = os.path.basename(item_key)

    #     # In case the key does not have a proper basename, replace unsafe characters.
    #     if not base_name:
    #         base_name = item_key.replace('https://', '00').replace('/', '_')
        
    #     # Case where file name is repeated
    #     if base_name in past_names:
    #         base_name += "a"
    #     past_names.append(base_name)

    #     # Append the .rq extension.
    #     ns_output_filename = f"{base_name}_ns.rq"
    #     ns_full_output_path = os.path.join(out_directory, ns_output_filename)

    #     if str(base_name) in bgee:
    #         total += 1
    #         # for without SERVICE descriptions
    #         try:
    #             with open(ns_full_output_path, 'w', encoding='utf-8') as out_file:
    #                 out_file.write("# Datasources: %s\n%s" % (ns_query_source, ns_query_text.rstrip('\n')))
    #             # print(f"Created file: {output_filename}")
    #         except Exception as e:
    #             print(f"Error writing {ns_output_filename}: {e}")
    # print("Total no-service queries:", total)


if __name__ == "__main__":
    main()
