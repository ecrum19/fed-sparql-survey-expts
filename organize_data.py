from __future__ import annotations
import re, json, os, argparse, io
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd
import zipfile
from collections import deque
from pathlib import Path

ISO_RE = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?"
URL_RE = r"https?://[^\s']+"
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

def _parse_iso(ts: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unrecognized timestamp: {ts}")

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
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

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

        # If no results, look for error line(s)
        if not produced_results:
            em = re.search(r"Error executing command for\s+[^\n:]+:\s*(.*)", sec)
            if em:
                if file_exists(parent_dir, query_name + ".log"):
                    error_text = getErrorFromNorm(os.path.join(parent_dir, query_name + ".log"))
                elif file_exists(parent_dir, query_name + ".log.zip"):
                    error_text = getErrorFromZipped(os.path.join(parent_dir, query_name + ".log.zip"))
                else:
                    error_text = "Unknown Error"

        entries.append({
            "query_name": query_name,
            "sources": sources,
            "start": start_ts.isoformat() if start_ts else "None",
            "end": end_ts.isoformat() if end_ts else "None",
            "duration_seconds": q_duration,
            "produced_results": produced_results,
            "results_count": results_count if produced_results else 0,
            "error": "None" if produced_results else error_text,
        })

    # Compute aggregate counts across all queries (BEFORE inserting the summary row)
    num_with_num_results = sum(1 for e in entries if e.get("results_count", 0) > 0)
    num_with_results = sum(1 for e in entries if e.get("produced_results", False))
    num_errors = sum(1 for e in entries if not e.get("produced_results", False))

    # Build the "general" summary row and put it at index 0
    general_row = {
        "query_name": str(os.path.basename(os.path.normpath(str(parent_dir)))),
        "sources": "None",                           # record "None" as a string
        "start": run_start.isoformat(),
        "end": run_end.isoformat(),
        "duration_seconds": run_duration_s,         # total run duration (seconds)
        "produced_results": num_with_results,       # number of queries with results
        "results_count": num_with_num_results,      # number of queries with results set >0
        "error": num_errors,                        # number of queries that produced an error
    }
    entries.insert(0, general_row)

    return {
        "run_start": run_start.isoformat(),
        "run_end": run_end.isoformat(),
        "run_duration_seconds": run_duration_s,
        "entries": entries,
    }

def write_csv(summary: Dict[str, Any], out_path: str):
    df = pd.DataFrame(summary["entries"])
    df.to_csv(out_path, index=False)
    print(f"[INFO] Wrote {len(df)} query records to {out_path}")


def getErrorFromZipped(file, member: Optional[str] = None) -> str:
    """
    Return the last line of a text file contained in a .zip, without extracting to disk.

    Args:
        zip_path: Path to the .zip file
        member:   Optional specific member filename inside the zip; if None, picks the only file
                if there is exactly one, otherwise raises an error
        encoding: Text encoding for decoding the member bytes

    Returns:
        The last line (rstripped and ANSI-cleaned). Returns "" if the file is empty.
    """
    with zipfile.ZipFile(file, "r") as zf:
        # Determine which member to open
        if member is None:
            # Ignore directories
            files = [n for n in zf.namelist() if not n.endswith("/")]
            if not files:
                raise FileNotFoundError("Zip archive contains no files.")
            if len(files) > 1:
                raise ValueError(
                    f"Zip contains multiple files; specify one via 'member'. Candidates: {files}"
                )
            member = files[0]

        # Stream and keep only the last line
        with zf.open(member, "r") as f:
            text_stream = io.TextIOWrapper(f, encoding="utf-8",errors="replace", newline="")
            tail = deque(text_stream, maxlen=1)

    if not tail:
        return ""

    last_line = tail[0].rstrip("\r\n")
    # Strip ANSI escape sequences if present
    last_line = ANSI_ESCAPE.sub("", last_line)
    if "[" in last_line:
        return "Unknown Error"
    return last_line

def getErrorFromNorm(file):
    with open(file, "r", encoding="utf-8") as f:
        # Read all lines, strip trailing whitespace from the last one
        last_line = ANSI_ESCAPE.sub('', f.readlines()[-1].rstrip())
        if "[" in last_line:
            return "Unknown Error"
        return last_line


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
    parser = argparse.ArgumentParser(description="Parse batch log and output CSV summary.")
    parser.add_argument("--input", help="Path to input batch log file")
    parser.add_argument("--output", help="Name of output CSV file")
    args = parser.parse_args()

    summary = parse_batch_log(args.input)
    write_csv(summary, args.output)

if __name__ == "__main__":
    main()


# TODO: Add tracking of HTTP requests