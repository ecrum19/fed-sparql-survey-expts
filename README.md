# FederatedSparqlSurveyExperiments

This repository contains scripts, configurations, and workflows for running federated SPARQL query experiments over biological SPARQL endpoints. The goal of this survey is to evaluate the performance, reliability, and scalability of federated SPARQL engines using a variety of query federation approaches, data sources, and real-world queries.

## Dependencies
To run the experiments, you will need:
- **Python 3.8+**
- **Node.js (v18+)**
- **Git**
- **Yarn**
- Python standard-library modules such as `argparse`, `json`, `os`, `sys`, `shutil` (already included with Python)
- Third-party Python packages listed in `requirements.txt` (`pandas`, `numpy`, `matplotlib`)

### Installing dependencies on Ubuntu
Run:
```bash
bash install_ubuntu_dependencies.sh
source .venv/bin/activate
```

### Installing Comunica
If needed, install directly by navigating to the `comunica/` directory and run:
```bash
bash install_comunica.sh
```

### Installing Python dependencies manually
If needed, install directly with:
```bash
pip install -r requirements.txt
```

## Query Catalogue Versions
| File | What it is |
|---|---|
| `sib-swiss-federated-queries.json` | **Original catalogue (frozen).** 67 federated queries from [sib-swiss/sparql-examples](https://github.com/sib-swiss/sparql-examples) at commit `c40a905` (2025-03-17). Used for all experiments run so far. Kept unchanged for transparency. |
| `sib-swiss-federated-queries.updated-2026-11.json` | **Updated catalogue for future studies.** 63 of the 67 queries, with the same keys and minimal endpoint fixes. The BioSODA proxy is deprecated, so EMI queries now target the EarthMetabolome QLever endpoint. QLever Wikidata moved to `qlever.dev`. OMA uses its canonical `/sparql/` URL. The EMI queries also get the `PREFIX` namespaces the new EMI knowledge graph uses (`emi:` → `https://w3id.org/emi#`, plus `qudt:` in two queries). Four queries that changed in the upstream SIB catalogue (`emi#examples012`, `emi#examples019a`, UniProt `36` and `67_draft_human_metabolome`) are replaced with their upstream version, and eight queries have a small documented fix: four that returned no results (UniProt 48 and 53, OrthoDB 17, OMA 15), three SwissLipids queries whose result was a cross product (26, 27, 34; upstream's join fix) and EMI 018 (a variable missing from a subquery). The results of these twelve are not comparable with earlier experiments. Four queries are left out: UniProt 46, OrthoDB 18 and OMA 14 because they use dead endpoints (EEA, neXtProt, NIBB), and `emi#examples019b` because upstream dropped it. Query logic is otherwise unchanged and no newer upstream queries were added. |
| `sib-swiss-federated-queries.updated-2026-11.md` | Generated change log: the endpoint and prefix changes with reasons, the replaced, fixed (with a diff) and removed queries, every check of each query (HTTP checks, runs by hand, computed counts), how each failing query compares with upstream, the other versions that were tried (with their query text) and what could still be tried, and the expected results. |
| `sib-swiss-federated-queries.updated-2026-11.expected-results.json` | **Expected results for future testing.** For each of the 63 queries: the expected row count, 0 rows, or the reason it fails (`expected`), the checks of the current query text behind it (`checks`), and the results of other versions such as the upstream one (`otherVersions`). As of 2026-11-20: 46 queries return rows; SwissLipids 26 errors when run (its stream breaks part way), so its expected count (136,755) is computed from aggregate queries, a computation that gave exactly the observed counts of 27 and 34; 2 queries return 0 rows and 14 fail on today's endpoints. Counts are snapshots and can change with the endpoints' data. |
| `update_query_catalogue.py` | Script that builds the updated file from the original (`build`), checks each query once against its live endpoint (`verify`), writes the change log (`report`) and the expected results (`expected`). The runs by hand, other versions tried and upstream notes are kept in its `QUERY_NOTES`. |
| `count_results.py` | Counts the rows in a downloaded result file (`.srj`/`.json` SPARQL JSON results or `.csv`), for queries run by hand. |

Each entry in the updated file keeps the original fields (`query`, `description`, `federatesWith`, `target`) and adds:
- `changeStatus`: `unchanged`, `endpoint-updated`, `prefix-updated`, `endpoint-and-prefix-updated`, `query-fixed` or `replaced-with-upstream`
- `changes`: list of `{field, from, to, reason}` (`field` is `query`, `target`, `federatesWith`, `queryPrefix` for a `PREFIX` namespace change, which also names the `prefix`, `queryAndDescription` for a query replaced with its upstream version, or `queryFix` for a small fix, which lists its exact text replacements)
- `status`: `working`, `needs-manual-fix` or `unverified`
- `manualFixNotes`: what still needs attention
- `verification`: date, run number, HTTP status, row count, response size, duration and any error from the latest check
- `previousVerifications`: earlier checks of the same query, kept when a query is re-checked
- `otherChecks` (some queries): results outside the verification runs: runs by hand (`by-hand`, with the result as written down), single HTTP requests (`probe`) and counts computed from aggregate queries (`computed`). `version` says which text was run: `catalogue` (`currentQuery` is true while its `queryHash` matches the entry's query), `upstream` (`upstreamFile` at sparql-examples@45e5b53) or `variant` (text in `query`)
- `upstreamComparison`, `failureSummary`, `openOptions` (some queries): how the query compares with upstream, why it fails today, and what could still be tried

`metadata.verificationRuns` lists every verification run with its settings and notes. `metadata.upstreamReplacements` lists the queries replaced with their upstream version, `metadata.queryFixes` the queries with a small fix (with the reason), and `metadata.removedQueries` lists the queries left out, with the reason and their last checks.

`query_sort.py` ignores the extra fields, so either file can be used in step 1 below.

To refresh the updated catalogue (for example after editing the endpoint or prefix map in the script):
```bash
python3 update_query_catalogue.py build --force
python3 update_query_catalogue.py verify    # slow on purpose: one query at a time, pauses between requests, no retries
python3 update_query_catalogue.py report
python3 update_query_catalogue.py expected
```
To record a query you ran by hand, add the result to `QUERY_NOTES` in the script (for the catalogue text, copy the entry's `verification.queryHash`; `count_results.py` gives the row count of a downloaded result), then run `build --force`, `report` and `expected`.
`build --force` keeps earlier checks and the run history: a check stays current if the query did not change, otherwise it moves to `previousVerifications` and `verify` checks that query again. Add `--discard-checks` to start from scratch. `verify` saves progress after every query and skips queries that were already checked, unless you pass `--force`. Use `--only <id> ...` to re-check single queries after fixing them by hand. To give slow or very large queries another chance with higher limits (results are counted while they stream, so a large limit does not need much memory):
```bash
python3 update_query_catalogue.py verify --recheck timeout too-large --timeout 900 --max-response-mb 2048 --label "higher limits"
```

## Workflow for Replicating Experiments
1. **Prepare Query Files**
```bash
python3 query_sort.py -i sib-swiss-federated-queries.json -t [service/noservice]
# or, for new studies on the updated endpoints:
python3 query_sort.py -i sib-swiss-federated-queries.updated-2026-11.json -t [service/noservice]
```

2. **Configure Experiment Parameters**
```bash
python3 comunica_configuration.py -e [experiment]
```

3. **Run Experiment**
```bash
python3 comunica_run.py -n [DATE-EX#] -t [service/noservice]
python3 comunica_run.py -n EX1-17-9-25 -t no-service/ns_batch1
```

4. **Analyze Results**
```bash
bash localize_results.sh /path/to/source /path/to/dest
bash localize_results.sh /experiments/testbatch1 ../fed-survey-results/EX1-09-25
```
```bash
python3 organize_data.py /path/to/dir -o summary.json --csv
python3 organize_data.py ../fed-survey-results/EX1-09-25 -o summary.json --csv
```

## Reproducibility
- All scripts are designed to be run from the command line
- Configuration files and experiment options are versioned for transparency
- Output directories are created automatically if missing

## Citation
If you use this repository for published research, please cite the included `CITATION.cff` file.

## Contact
For questions or collaboration, please contact the repository owner or open an issue.
