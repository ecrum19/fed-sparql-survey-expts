# FederatedSparqlSurveyExperiments

This repository contains scripts, configurations, and workflows for running federated SPARQL query experiments over biological SPARQL endpoints. The goal of this survey is to evaluate the performance, reliability, and scalability of federated SPARQL engines using a variety of query federation approaches, data sources, and real-world queries.

## Dependencies
To run the experiments, you will need:
- **Python 3.8+**
- **Node.js (v18+)**
- **Git**
- Python packages: `argparse`, `json`, `os`, `sys`, `shutil`
- These packages are detailed in "requirements.txt"

### Installing Comunica
Navigate to the `comunica/` directory and run:
```bash
bash install_comunica.sh
```

### Installing Python dependencies [TODO]
If a `requirements.txt` is present:
```bash
pip install -r requirements.txt
```

## Workflow for Replicating Experiments
1. **Prepare Query Files**
```bash
python3 query_sort.py -i sib-swiss-federated-queries.json -t [service/noservice]
```

2. **Configure Experiment Parameters**
```bash
python3 comunica_configuration.py -e [experiment]
```

3. **Run Experiment**
```bash
python3 comunica_run.py -n [DATE-EX#] -t [service/noservice]
```

4. **Analyze Results**
```bash
python3 interpret_results.py -c [experiments/experiment_run_#]
```

## Reproducibility
- All scripts are designed to be run from the command line
- Configuration files and experiment options are versioned for transparency
- Output directories are created automatically if missing

## Citation
If you use this repository for published research, please cite the included `CITATION.cff` file.

## Contact
For questions or collaboration, please contact the repository owner or open an issue.
