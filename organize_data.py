from __future__ import annotations
import re, json, os, argparse, io, sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import zipfile
from collections import deque
from pathlib import Path

ISO_RE = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?"
URL_RE = r"https?://[^\s']+"
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
ERROR_PATTERNS = [
    "FATAL ERROR: Reached heap limit Allocation failed",
    "fetch failed",
    "DEBUG: Server reported client-side error",
    "DEBUG: Server-side error encountered",
]

def _parse_iso(ts: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unrecognized timestamp: {ts}")

def _read_text_maybe_zipped(path, member: Optional[str] = None) -> str:
    """Read UTF-8 text from a plain file or a .zip/.txt.zip without extracting."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path, "r") as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            if not names:
                raise FileNotFoundError("Zip archive contains no files.")

            if member is None:
                txts = [n for n in names if n.lower().endswith(".txt")]
                if len(txts) == 1:
                    member = txts[0]
                elif len(txts) == 0 and len(names) == 1:
                    member = names[0]
                else:
                    raise ValueError(
                        "Zip contains multiple files; specify 'member'. "
                        f"Candidates: {txts or names}"
                    )
            elif member not in names:
                raise FileNotFoundError(f"Member '{member}' not found in zip. Candidates: {names}")

            with zf.open(member, "r") as f:
                return io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="").read()
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

def parse_batch_log(path: str) -> Dict[str, Any]:
    """Parse a batch log like your example and return a structured summary.

    Returns:
        {
          "run_start": str,
          "run_end": str,
          "run_duration_seconds": float,
          "entries": [
            {
              "query_name": "018_ns.rq",
              "sources": ["https://www.bgee.org/sparql", "https://sparql.omabrowser.org/sparql"],
              "start": "2025-09-02T10:17:00.718579",
              "end":   "2025-09-02T10:17:25.782577",
              "duration_seconds": 25.063998,
              "http_requests": 15,
              "produced_results": False,
              "results_count": 0,
              "error": "…error text if any…",
            },
            ...
          ]
        }
    """
    working_path = Path(path)
    parent_dir = working_path.parent

    text = _read_text_maybe_zipped(working_path)

    # Overall run window from the first and last sections
    blocks = re.split(r"\n{2,}", text.strip())
    if len(blocks) < 3:
        raise ValueError("Unexpected log structure: fewer than 3 sections after split on blank lines.")
    first_ts = re.findall(ISO_RE, blocks[0])
    last_ts  = re.findall(ISO_RE, blocks[-1])
    if not first_ts or not last_ts:
        raise ValueError("Could not find ISO timestamps in the first/last sections.")
    run_start = _parse_iso(first_ts[0])
    run_end   = _parse_iso(last_ts[-1])
    run_duration_s = (run_end - run_start).total_seconds()
    http_count = 0
    
    # Per-query sections start with "Executing:"
    sections = re.split(r"\n(?=Executing: )", text.strip(), flags=re.MULTILINE)
    entries: List[Dict[str, Any]] = []

    for sec in sections:
        if not sec.startswith("Executing: "):
            continue

        # Query file (after -f)
        qfile_match = re.search(r"-f\s+([^\s]+)", sec)
        query_name = os.path.basename(qfile_match.group(1)) if qfile_match else None

        # Sources = all http(s) URLs in the exec line before -f
        exec_line = sec.splitlines()[0]
        pre_f = exec_line.split("-f")[0]
        urls = [u.rstrip("/") for u in re.findall(URL_RE, pre_f) if u.startswith("http")]
        # Deduplicate, preserve order
        seen, sources = set(), []
        for u in urls:
            if u not in seen:
                seen.add(u)
                sources.append(u)

        # Per-query timestamps & duration
        start_m = re.search(r"Timestamp \(start\):\s*("+ISO_RE+")", sec)
        end_m   = re.search(r"Timestamp \(end\):\s*("+ISO_RE+")", sec)
        start_ts = _parse_iso(start_m.group(1)) if start_m else None
        end_ts   = _parse_iso(end_m.group(1))   if end_m   else None
        q_duration = (end_ts - start_ts).total_seconds() if (start_ts and end_ts) else None

        # Inspect the content between start/end for Output JSON or Error lines
        mid = ""
        if start_m and end_m:
            mid = sec[start_m.end():end_m.start()]

        # Try to parse "Output: { ... }" JSON and count results
        produced_results, results_count, error_text = False, 0, None
        out_m = re.search(r"Output:\s*(\{.*)", mid, flags=re.DOTALL)
        if out_m:
            # Grab JSON up to the last closing brace before the end
            json_text = out_m.group(1)
            last_brace = json_text.rfind("}")
            if last_brace != -1:
                json_text = json_text[:last_brace+1]
            try:
                data = json.loads(json_text)
                bindings = data.get("results", {}).get("bindings")
                if isinstance(bindings, list):
                    results_count = len(bindings)
                    produced_results = results_count >= 0
            except Exception:
                # If JSON fails, we still know there *was* Output (but unknown count)
                pass

        # If results, get HTTP requests count
        if produced_results:
            if file_exists(parent_dir, query_name + ".log"):
                http_count, _ = getLogDataFromFileOrZipped(os.path.join(parent_dir, query_name + ".log"), has_error=False)
            elif file_exists(parent_dir, query_name + ".log.zip"):
                http_count, _ = getLogDataFromFileOrZipped(os.path.join(parent_dir, query_name + ".log.zip"), has_error=False)
            else:
                http_count = 0

        # If no results, look for error line(s)
        elif not produced_results:
            em = re.search(r"Error executing command for\s+[^\n:]+:\s*(.*)", sec)
            if em:
                if file_exists(parent_dir, query_name + ".log"):
                    http_count, error_text = getLogDataFromFileOrZipped(os.path.join(parent_dir, query_name + ".log"), has_error=True)
                elif file_exists(parent_dir, query_name + ".log.zip"):
                    http_count, error_text = getLogDataFromFileOrZipped(os.path.join(parent_dir, query_name + ".log.zip"), has_error=True)
                else:
                    error_text = "Unknown Error"

        entries.append({
            "query_name": query_name,
            "sources": sources,
            "start": start_ts.isoformat() if start_ts else "None",
            "end": end_ts.isoformat() if end_ts else "None",
            "duration_seconds": q_duration,
            "http_requests": http_count,
            "produced_results": produced_results,
            "results_count": results_count if produced_results else 0,
            "error": "None" if produced_results else error_text,
        })

    return {
        "run_start": run_start.isoformat(),
        "run_end": run_end.isoformat(),
        "run_duration_seconds": run_duration_s,
        "entries": entries,
    }

def get_general_stats(summary: Dict[str, Any], input_dirc) -> Dict[str, Any]:
    entries = summary["entries"]
    run_start = _parse_iso(summary["general_stats"]["run_start"])
    run_end = _parse_iso(summary["general_stats"]["run_end"])
    run_duration_s = summary["general_stats"]["run_duration_seconds"]
    # Compute aggregate counts across all queries (BEFORE inserting the summary row)
    num_with_num_results = sum(1 for e in entries if e.get("results_count", 0) > 0)
    num_with_results = sum(1 for e in entries if e.get("produced_results", False))
    num_errors = sum(1 for e in entries if not e.get("produced_results", False))

    # Build the "general" summary row and put it at index 0
    general_row = {
        "query_name": str(os.path.basename(os.path.normpath(str(input_dirc)))),
        "sources": "None",                           # record "None" as a string
        "start": run_start.isoformat(),
        "end": run_end.isoformat(),
        "duration_seconds": run_duration_s,         # total run duration (seconds)
        "http_requests": None,  
        "produced_results": num_with_results,       # number of queries with results
        "results_count": num_with_num_results,      # number of queries with results set >0
        "error": num_errors,                        # number of queries that produced an error
    }
    entries.insert(0, general_row)

    return {
        "general_stats": summary["general_stats"],
        "entries": entries
    }


def write_csv(summary: {Dict[str, Any]}, out_path: str):
    df = pd.DataFrame(summary['entries'])
    df.to_csv(out_path, index=False)


def _strip_ansi(s: str) -> str:
    """Strip ANSI escape sequences using global ANSI_ESCAPE if present, else a local regex."""
    try:
        return ANSI_ESCAPE.sub("", s)  # uses your existing compiled regex if defined elsewhere
    except NameError:
        return re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", s)

def getLogDataFromFileOrZipped(
    file_path: str,
    member: Optional[str] = None,
    has_error: bool = True,
) -> Tuple[int, Optional[str]]:
    """
    Reads either a plain text file or a zipped file and returns:
        (count_of_info_requesting_lines, error_or_last_line_or_none)

    Parameters
    ----------
    file_path : str
        Path to a text file or .zip archive
    member : str, optional
        Member filename inside zip (if not provided, picks automatically)
    has_error : bool, optional
        If False, skip error-finding and only count 'INFO: Requesting' lines.
        In that case, the second return value is None.

    Returns
    -------
    tuple (int, Optional[str])
        - int: number of lines containing 'INFO: Requesting'
        - str or None:
            * If has_error=True:
                → the detected error string, or last line (cleaned)
                → or 'Unknown Error' if last line has '['
            * If has_error=False:
                → None
    """
    info_count = 0
    found_error: Optional[str] = None
    last_line = ""

    def process_lines(line_iterable):
        nonlocal info_count, found_error, last_line
        for raw_line in line_iterable:
            line = raw_line.rstrip("\r\n")
            if "INFO: Requesting" in line:
                info_count += 1
            if has_error and found_error is None:
                for pat in ERROR_PATTERNS:
                    if pat in line:
                        found_error = pat
                        break
            last_line = line

    # --- Handle zipped file ---
    if zipfile.is_zipfile(file_path):
        with zipfile.ZipFile(file_path, "r") as zf:
            # Choose member automatically if not specified
            if member is None:
                files = [n for n in zf.namelist() if not n.endswith("/")]
                if not files:
                    raise FileNotFoundError("Zip archive contains no files.")
                if len(files) > 1:
                    raise ValueError(
                        f"Zip contains multiple files; specify one via 'member'. Candidates: {files}"
                    )
                member = files[0]
            with zf.open(member, "r") as f:
                text_stream = io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline="")
                process_lines(text_stream)
    else:
        # --- Handle normal text file ---
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            process_lines(f)

    # --- Return results ---
    if not has_error:
        return (int(info_count), None)

    if last_line == "":
        return (int(info_count), "")

    if found_error:
        return (int(info_count), found_error)

    cleaned_last = _strip_ansi(last_line)
    if "[" in cleaned_last:
        return (int(info_count), "Unknown Error")
    return (int(info_count), cleaned_last)


from pathlib import Path

def file_exists(directory: str, filename: str) -> bool:
    """
    Check if a file exists inside a given directory.
    Args:
        directory: Path to the directory
        filename: Name of the file to check for
    Returns:
        True if file exists, False otherwise
    """
    return (Path(directory) / filename).is_file()


def main():
    parser = argparse.ArgumentParser(description="Summarize all .txt query run logs in a directory.")
    parser.add_argument("input_dir", help="Directory containing .txt files to summarize.")
    parser.add_argument("-o", "--output", default="summary.json",
                        help="Path to write combined JSON summary (default: summary.json)")
    parser.add_argument("-c", "--csv", action="store_true",
                        help="Also write a CSV file version of the summary")
    args = parser.parse_args()

    # Collect all .txt files (or zipped .txt files) in the directory (non-recursive)
    input_dir = Path(args.input_dir)
    if not input_dir.is_dir():
        print(f"[ERROR] Input directory does not exist: {input_dir}", file=sys.stderr)
        sys.exit(1)
    inputs = sorted(input_dir.rglob("*.txt*"))
    if not inputs:
        print(f"[WARN] No .txt files found in {input_dir}")
        sys.exit(0)

    print(f"[INFO] Running summary generation for {len(inputs)} batch file(s)...")

    # Process each file and combine results
    overall_summary = {
        "general_stats": {
            "run_start": None,
            "run_end": None,
            "run_duration_seconds": 0.0,
        },
        "entries": []
    }
    earliest: Optional[datetime] = None
    latest: Optional[datetime] = None
    total_duration: float = 0.0

    for inp in inputs:
        working_summary = parse_batch_log(str(inp))

        # general stats aggergation
        if not earliest:
            earliest = _parse_iso(working_summary["run_start"])
        else:
            earliest = min(earliest, _parse_iso(working_summary["run_start"]))
        if not latest:
            latest = _parse_iso(working_summary["run_end"])
        else:
            latest = max(latest, _parse_iso(working_summary["run_end"]))
        total_duration += working_summary.get("run_duration_seconds", 0.0)
        
        # record general stats
        overall_summary["general_stats"]["run_start"] = earliest.isoformat() if earliest else None
        overall_summary["general_stats"]["run_end"] = latest.isoformat() if latest else None
        overall_summary["general_stats"]["run_duration_seconds"] = total_duration

        # entries aggregation
        overall_summary["entries"].extend(working_summary["entries"])

    added_general_stats_row = get_general_stats(overall_summary, input_dir)

    # Always write JSON
    with open(input_dir / args.output, "w", encoding="utf-8") as f:
        json.dump(added_general_stats_row, f, ensure_ascii=False, indent=2)
    print(f"[OK] Wrote combined JSON summary for {len(inputs)} file(s) to {args.output}.")

    # Also write a CSV of entries if there are any
    if args.csv:
        csv_path = Path(input_dir / args.output).with_suffix(".csv")
        write_csv(added_general_stats_row, str(csv_path))
        print(f"[OK] Wrote {len(overall_summary['entries']) - 1} total query records to {csv_path}.")
    else:
        print("[INFO] Did not write summary to CSV, include '--csv' if you wish for this file to be created.")

if __name__ == "__main__":
    main()

