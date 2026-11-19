#!/usr/bin/env python3
"""
Derive an *updated* copy of the federated query catalogue without touching the
original file used in past experiments.

The original catalogue (sib-swiss-federated-queries.json) is a frozen snapshot of
sib-swiss/sparql-examples at commit c40a905 (2025-03-17). Since then some
endpoints were deprecated or moved. This script applies a minimal, documented
set of endpoint fixes and (optionally) checks every query once against its live
target endpoint.

Subcommands
-----------
build   Apply ENDPOINT_MAP and PREFIX_MAP to the original catalogue and write the
        updated file. Query text is only changed where it names a moved endpoint
        (SERVICE IRIs) or declares a PREFIX whose namespace changed with the
        target endpoint's data; no query logic is changed and no new upstream
        queries are added. Rebuilding an existing file keeps its checks: a check
        stays current if the query did not change, otherwise it moves to
        previousVerifications and the query is checked again by `verify`.

verify  Run each query (with its SERVICE clauses) once against its target
        endpoint and record the outcome in the updated file. Designed to be
        gentle on public endpoints:
          * strictly one request at a time, never in parallel
          * a fixed pause between requests and a longer minimum gap per host
          * no retries; a failure is recorded and flagged for manual follow-up
          * redirects are not followed (they are recorded as a finding)
          * a host that answers 429/503, or fails several times in a row, is
            not contacted again in that run
        Progress is saved after every query, so an interrupted run resumes
        without re-sending queries that were already checked. Earlier results
        are kept in previousVerifications; every run is listed in
        metadata.verificationRuns. Use --recheck to re-check queries by outcome
        (e.g. --recheck timeout too-large) with a higher --timeout and
        --max-response-mb.

report  Write a Markdown change log / manual-fix list from the updated file.

Usage
-----
  python3 update_query_catalogue.py build  -i sib-swiss-federated-queries.json \
                                           -o sib-swiss-federated-queries.updated-2026-11.json
  python3 update_query_catalogue.py verify -c sib-swiss-federated-queries.updated-2026-11.json
  python3 update_query_catalogue.py report -c sib-swiss-federated-queries.updated-2026-11.json \
                                           -o sib-swiss-federated-queries.updated-2026-11.md
"""

import argparse
import codecs
import datetime as dt
import hashlib
import html
import json
import os
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict, defaultdict

# ---------------------------------------------------------------------------
# Endpoint changes (checked by hand on 2026-11-18; see the generated report)
# ---------------------------------------------------------------------------

UPSTREAM_REPO = "https://github.com/sib-swiss/sparql-examples"
UPSTREAM_COMMIT_CHECKED = "45e5b53a2d59a121221cd66a8596ef868cc58021"  # 2026-10-15

ENDPOINT_MAP = OrderedDict([
    ("https://biosoda.unil.ch/emi/sparql", {
        "to": "https://qlever.earthmetabolome.org/api/metrin-kg",
        "reason": (
            "BioSODA federation proxy is deprecated: the URL now 301-redirects to an HTML "
            "page (https://biosoda.unil.ch/pdn-agent). Upstream sparql-examples@45e5b53 "
            "lists the EMI endpoint as https://kg.earthmetabolome.org/metrin/api/, which "
            "302-redirects to this URL; the direct URL is used because 302 redirects drop "
            "POST bodies in most SPARQL clients."
        ),
    }),
    ("https://qlever.cs.uni-freiburg.de/api/wikidata", {
        "to": "https://qlever.dev/api/wikidata",
        "reason": (
            "QLever's public Wikidata endpoint moved: the old URL answers 308 Permanent "
            "Redirect to this URL. Upstream sparql-examples made the same change "
            "(2025-10-28, 'update qlever public endpoint URL')."
        ),
    }),
    ("https://sparql.omabrowser.org/sparql", {
        "to": "https://sparql.omabrowser.org/sparql/",
        "reason": (
            "OMA answers 301 Moved Permanently from /sparql to /sparql/; most clients "
            "turn a redirected POST into a GET without the query, so the canonical URL "
            "with the trailing slash is used."
        ),
    }),
])

# PREFIX namespaces that changed together with an endpoint's data, keyed by the
# (updated) target endpoint. Only the namespace IRI in the PREFIX declaration of
# queries sent to that endpoint is changed; the rest of the query is untouched.
PREFIX_MAP = OrderedDict([
    ("https://qlever.earthmetabolome.org/api/metrin-kg", [
        OrderedDict([
            ("prefix", "emi"),
            ("from", "https://purl.org/emi#"),
            ("to", "https://w3id.org/emi#"),
            ("reason", (
                "The EMI knowledge graph behind the new endpoint uses the namespace "
                "https://w3id.org/emi#; with the old one every EMI query returned 0 rows "
                "(run 1, 2026-11-18). Upstream sparql-examples@45e5b53 examples/emi/ made "
                "the same change."
            )),
        ]),
        OrderedDict([
            ("prefix", "qudt"),
            ("from", "https://qudt.org/2.1/schema/qudt#"),
            ("to", "https://qudt.org/schema/qudt/"),
            ("reason", (
                "Namespace used for qudt:hasUnit in the corresponding upstream EMI queries "
                "(sparql-examples@45e5b53 examples/emi/012.ttl and 014.ttl)."
            )),
        ]),
    ]),
])

# Endpoints that did not answer as SPARQL endpoints on 2026-11-18 and have no
# known successor. Queries using them are left unchanged and flagged.
UNAVAILABLE_ENDPOINTS = OrderedDict([
    ("https://sparql.nextprot.org/",
     "neXtProt is retired: the endpoint 301-redirects to https://www.expasy.org/archives/nextprot (HTML). No replacement endpoint known."),
    ("http://sparql.nibb.ac.jp/sparql",
     "No response (connection failed) on 2026-11-18. Was already failing with HTTP 500 in the 2025 control runs."),
    ("https://semantic.eea.europa.eu/sparql",
     "No response (connection failed) on 2026-11-18. Was already failing with HTTP 404 in the 2025 control runs."),
])

# Known endpoint behaviour worth recording for people running the queries.
ENDPOINT_NOTES = OrderedDict([
    ("https://www.bgee.org/sparql",
     "Bgee answers 403 to POST requests (with or without trailing slash); GET works. "
     "It sits behind Cloudflare, which ends requests that run longer than ~125 s with HTTP 524."),
    ("https://qlever.earthmetabolome.org/api/metrin-kg",
     "The EMI knowledge graph behind the new endpoint uses the namespace https://w3id.org/emi# "
     "instead of https://purl.org/emi#; the PREFIX is updated in this catalogue (see the prefix "
     "changes). Upstream sparql-examples@45e5b53 also reworked and renumbered the EMI examples "
     "011-023 (e.g. names are read with rdfs:label); that rework was not applied here. "
     "Its gateway (nginx) ends requests that run longer than 60 s with HTTP 504 (seen on 2026-11-19)."),
])

# Hints added to a query's manualFixNotes when its check did not succeed,
# keyed by target endpoint.
FAILURE_HINTS = {
    "https://qlever.earthmetabolome.org/api/metrin-kg":
        "Hint: HTTP 504 after 60 s comes from the endpoint's gateway, so run the query by hand "
        "or ask the maintainers. For 0 rows: the PREFIX namespaces are already updated, but "
        "upstream sparql-examples@45e5b53 reworked and renumbered the EMI examples "
        "(examples/emi/); compare with the closest upstream query.",
    "https://www.bgee.org/sparql":
        "Hint: HTTP 524 means Cloudflare ended the request after ~125 s; the query itself may still be valid.",
    "https://www.bgee.org/sparql/":
        "Hint: HTTP 524 means Cloudflare ended the request after ~125 s; the query itself may still be valid.",
}

USER_AGENT = "FederatedSparqlSurveyExperiments-catalogue-check/1.0 (python-urllib)"
ACCEPT = "application/sparql-results+json"
MAX_GET_URL = 7000          # longer queries are sent with POST
DEFAULT_MAX_RESPONSE_MB = 20   # stop reading (and close the connection) after this much


def now_iso():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f, object_pairs_hook=OrderedDict)


def save_json_atomic(obj, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, path)


def short_id(key):
    """Same file stem query_sort.py derives from a catalogue key."""
    base = os.path.basename(key)
    return base or key.replace("https://", "00").replace("/", "_")


def query_hash(entry):
    h = hashlib.sha256()
    h.update(entry["target"].encode("utf-8"))
    h.update(b"\0")
    h.update(entry["query"].encode("utf-8"))
    return h.hexdigest()[:16]


def all_endpoints(entry):
    """Every endpoint an entry refers to: target, federatesWith and SERVICE IRIs."""
    eps = [entry["target"]] + list(entry.get("federatesWith", []))
    eps += re.findall(r"SERVICE\s+(?:SILENT\s+)?<([^>]+)>", entry["query"], re.I)
    return eps


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

def map_endpoint(url):
    return ENDPOINT_MAP[url]["to"] if url in ENDPOINT_MAP else url


def build(args):
    previous = None
    if os.path.exists(args.output):
        if not args.force:
            sys.exit(f"{args.output} already exists (it may hold verification results). "
                     f"Use --force to rebuild it; earlier checks are kept unless you also "
                     f"pass --discard-checks.")
        if not args.discard_checks:
            previous = load_json(args.output)

    original = load_json(args.input)
    data = original["data"]
    out_data = OrderedDict()
    counts = defaultdict(int)

    for key, entry in data.items():
        new = OrderedDict()
        changes = []

        # 1. query text: only rewrite IRIs that name a moved endpoint, e.g.
        #    SERVICE <old> -> SERVICE <new>. Data IRIs and PREFIXes are untouched.
        query = entry["query"]
        for old, spec in ENDPOINT_MAP.items():
            token = f"<{old}>"
            n = query.count(token)
            if n:
                query = query.replace(token, f"<{spec['to']}>")
                changes.append(OrderedDict([
                    ("field", "query"), ("from", old), ("to", spec["to"]),
                    ("occurrences", n), ("reason", spec["reason"]),
                ]))

        # 2. target
        target = map_endpoint(entry["target"])
        if target != entry["target"]:
            changes.append(OrderedDict([
                ("field", "target"), ("from", entry["target"]), ("to", target),
                ("reason", ENDPOINT_MAP[entry["target"]]["reason"]),
            ]))

        # 3. federatesWith (keep order, drop duplicates created by the mapping)
        fed, seen = [], set()
        for ep in entry.get("federatesWith", []):
            mapped = map_endpoint(ep)
            if mapped != ep:
                changes.append(OrderedDict([
                    ("field", "federatesWith"), ("from", ep), ("to", mapped),
                    ("reason", ENDPOINT_MAP[ep]["reason"]),
                ]))
            if mapped not in seen:
                fed.append(mapped)
                seen.add(mapped)

        # 4. PREFIX namespaces that changed with the target endpoint's data
        prefix_changed = False
        for spec in PREFIX_MAP.get(target, []):
            pattern = re.compile(r"(?im)^(\s*PREFIX\s+%s:\s*)<%s>" % (re.escape(spec["prefix"]),
                                                                     re.escape(spec["from"])))
            query, n = pattern.subn(lambda m: f"{m.group(1)}<{spec['to']}>", query)
            if n:
                prefix_changed = True
                changes.append(OrderedDict([
                    ("field", "queryPrefix"), ("prefix", spec["prefix"]), ("from", spec["from"]),
                    ("to", spec["to"]), ("occurrences", n), ("reason", spec["reason"]),
                ]))

        notes = []
        for ep in sorted(set(all_endpoints(OrderedDict(query=query, target=target, federatesWith=fed)))):
            if ep in UNAVAILABLE_ENDPOINTS:
                notes.append(f"Uses unavailable endpoint {ep}: {UNAVAILABLE_ENDPOINTS[ep]}")

        new["query"] = query
        new["description"] = entry["description"]
        new["federatesWith"] = fed
        new["target"] = target
        endpoint_changed = any(c["field"] != "queryPrefix" for c in changes)
        new["changeStatus"] = {(False, False): "unchanged", (True, False): "endpoint-updated",
                               (False, True): "prefix-updated",
                               (True, True): "endpoint-and-prefix-updated"}[
                                   (endpoint_changed, prefix_changed)]
        new["changes"] = changes
        new["status"] = "unverified"
        new["manualFixNotes"] = notes
        new["verification"] = None
        out_data[key] = new
        counts[new["changeStatus"]] += 1

    meta = OrderedDict()
    meta["catalogueVersion"] = args.version
    meta["generated"] = now_iso()
    meta["generatedBy"] = "update_query_catalogue.py build"
    meta["derivedFrom"] = OrderedDict([
        ("file", os.path.basename(args.input)),
        ("metadata", original.get("metadata")),
        ("note", "The original file is kept unchanged for transparency about past experiments."),
    ])
    meta["upstream"] = OrderedDict([
        ("repository", UPSTREAM_REPO),
        ("commitConsultedForEndpointUrls", UPSTREAM_COMMIT_CHECKED),
        ("note", "Only endpoint URLs and changed PREFIX namespaces were taken from upstream; "
                 "query logic was not synced and no newer upstream queries were added."),
    ])
    meta["policy"] = ("Minimal fixes only: endpoint URLs in target, federatesWith and SERVICE "
                      "clauses, and PREFIX namespaces that changed together with an endpoint's "
                      "data (see prefixMap). Query keys are unchanged so results can be joined "
                      "with earlier experiments.")
    meta["endpointMap"] = [OrderedDict([("from", k), ("to", v["to"]), ("reason", v["reason"])])
                           for k, v in ENDPOINT_MAP.items()]
    meta["prefixMap"] = [OrderedDict([("endpoint", ep)] + list(spec.items()))
                         for ep, specs in PREFIX_MAP.items() for spec in specs]
    meta["unavailableEndpoints"] = [OrderedDict([("endpoint", k), ("note", v)])
                                    for k, v in UNAVAILABLE_ENDPOINTS.items()]
    meta["endpointNotes"] = [OrderedDict([("endpoint", k), ("note", v)])
                             for k, v in ENDPOINT_NOTES.items()]
    meta["number_of_queries"] = len(out_data)
    meta["ignored_endpoints"] = (original.get("metadata") or {}).get("ignored_endpoints", [])
    meta["verificationRuns"] = []

    kept = carry_over_checks(previous, out_data, meta) if previous else None
    save_json_atomic(OrderedDict([("data", out_data), ("metadata", meta)]), args.output)
    print(f"Wrote {args.output}: {len(out_data)} queries ("
          + ", ".join(f"{n} {k}" for k, n in sorted(counts.items())) + ")")
    if kept:
        print(f"Kept earlier checks: {kept['current']} still current."
              + (f" {kept['stale']} queries changed, so their check moved to previousVerifications "
                 f"and they are unverified." if kept["stale"] else ""))


def carry_over_checks(previous, out_data, meta):
    """Keep check results and run history from an earlier build of the same file.

    A check stays current only if the query text and target are unchanged (same
    queryHash); otherwise it is kept in previousVerifications and the query is
    marked unverified, so `verify` checks it again.
    """
    meta["verificationRuns"] = migrate_runs(previous["metadata"])
    kept = {"current": 0, "stale": 0}
    for key, entry in out_data.items():
        old = previous["data"].get(key)
        if not old:
            continue
        history = list(old.get("previousVerifications") or [])
        v = old.get("verification")
        if v and v.get("queryHash") == query_hash(entry):
            entry["verification"] = v
            kept["current"] += 1
        elif v and v.get("outcome") != "not-run":
            history.append(v)
            kept["stale"] += 1
        if history:
            entry["previousVerifications"] = history
        classify(entry)
    return kept


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # surface 3xx as HTTPError instead of following it


def ssl_context():
    """python.org builds on macOS ship without CA certificates; fall back to
    certifi or the system bundle so TLS verification still works."""
    if os.environ.get("SSL_CERT_FILE"):
        return ssl.create_default_context(cafile=os.environ["SSL_CERT_FILE"])
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    if os.path.exists("/etc/ssl/cert.pem"):
        return ssl.create_default_context(cafile="/etc/ssl/cert.pem")
    return ssl.create_default_context()


OPENER = urllib.request.build_opener(
    NoRedirect, urllib.request.HTTPSHandler(context=ssl_context()))


class TooLarge(Exception):
    """Raised when a response grows past the configured size limit."""


def count_sparql_json(resp, max_bytes, chunk_size=1 << 20):
    """Count the rows of a SPARQL JSON response while streaming it.

    Bindings are decoded one at a time, so even very large results are never
    held in memory. Returns (rows, bytes_read). Raises TooLarge once more than
    max_bytes were read, and ValueError if the body is not SPARQL JSON results.
    """
    decoder = json.JSONDecoder()
    utf8 = codecs.getincrementaldecoder("utf-8")("replace")
    nbytes = 0
    buf = ""

    def more():
        nonlocal buf, nbytes
        data = resp.read(chunk_size)
        if not data:
            return False
        nbytes += len(data)
        if nbytes > max_bytes:
            raise TooLarge(nbytes)
        buf += utf8.decode(data)
        return True

    # 1. find the start of the bindings array (or the answer of an ASK query)
    start = re.compile(r'"bindings"\s*:\s*\[|"boolean"\s*:\s*(true|false)')
    while True:
        m = start.search(buf)
        if m:
            break
        if not more():
            raise ValueError(f"no 'bindings' array found in: {squash(buf[:1000])}")
    if m.group(1):
        return (1 if m.group(1) == "true" else 0), nbytes

    # 2. decode one binding object at a time
    buf, idx, rows = buf[m.end():], 0, 0
    while True:
        while idx < len(buf) and buf[idx] in " \t\r\n,":
            idx += 1
        if idx >= len(buf):
            buf, idx = "", 0
            if not more():
                raise ValueError("response ended inside the bindings array")
            continue
        if buf[idx] == "]":
            buf = ""
            while more():  # read the short remainder so the byte count is complete
                buf = ""
            return rows, nbytes
        try:
            _, idx = decoder.raw_decode(buf, idx)
            rows += 1
        except json.JSONDecodeError:
            buf, idx = buf[idx:], 0  # binding split across chunks: read more
            if not more():
                raise ValueError("invalid JSON inside the bindings array")


def run_query(endpoint, query, timeout, max_bytes):
    """Send one SPARQL SELECT request. Returns a dict describing the outcome."""
    params = urllib.parse.urlencode({"query": query})
    get_url = endpoint + ("&" if "?" in endpoint else "?") + params
    headers = {"Accept": ACCEPT, "User-Agent": USER_AGENT}
    if len(get_url) <= MAX_GET_URL:
        method = "GET"
        req = urllib.request.Request(get_url, headers=headers, method="GET")
    else:
        method = "POST"
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(endpoint, data=params.encode("utf-8"),
                                     headers=headers, method="POST")

    res = OrderedDict([("date", now_iso()), ("endpoint", endpoint), ("method", method)])
    start = time.monotonic()
    try:
        with OPENER.open(req, timeout=timeout) as resp:
            res["httpStatus"] = resp.status
            res["contentType"] = resp.headers.get("Content-Type", "")
            if "json" not in res["contentType"].lower():
                snippet = resp.read(256 * 1024).decode("utf-8", "replace")
                res["outcome"] = "error"
                res["error"] = (f"HTTP {res['httpStatus']} but content type is "
                                f"'{res['contentType']}', not SPARQL JSON results: {squash(snippet)}")
            else:
                rows, nbytes = count_sparql_json(resp, max_bytes)
                res["resultCount"] = rows
                res["responseBytes"] = nbytes
                res["outcome"] = "ok" if rows > 0 else "empty"
    except urllib.error.HTTPError as e:
        res["httpStatus"] = e.code
        res["contentType"] = e.headers.get("Content-Type", "") if e.headers else ""
        try:
            text = e.read(256 * 1024).decode("utf-8", "replace")  # error pages can be long HTML
        except Exception:
            text = ""
        res["outcome"] = "error"
        if 300 <= e.code < 400:
            res["error"] = f"HTTP {e.code} redirect to {e.headers.get('Location')} (not followed)"
        else:
            res["error"] = f"HTTP {e.code}: {squash(text)}"
    except TooLarge as e:
        res["outcome"] = "too-large"
        res["responseBytes"] = e.args[0]
        res["error"] = f"Response larger than {max_bytes} bytes; stopped reading"
    except ValueError as e:
        res["outcome"] = "error"
        res["error"] = f"Not valid SPARQL JSON results: {e}"
    except (socket.timeout, TimeoutError):
        res["outcome"] = "timeout"
        res["error"] = f"No complete response within {timeout} s (client timeout)"
    except urllib.error.URLError as e:
        if isinstance(e.reason, (socket.timeout, TimeoutError)):
            res["outcome"] = "timeout"
            res["error"] = f"No complete response within {timeout} s (client timeout)"
        else:
            res["outcome"] = "error"
            res["error"] = f"Connection error: {e.reason}"
    except Exception as e:  # e.g. connection reset mid-body
        res["outcome"] = "error"
        res["error"] = f"{type(e).__name__}: {e}"
    res["durationSeconds"] = round(time.monotonic() - start, 1)
    return res

def squash(text, limit=600):
    """Turn an (HTML) error page into a short readable message."""
    text = re.sub(r"(?is)<(script|style|svg|head)\b.*?</\1>", " ", text)
    text = re.sub(r"data:[^\s\"'>)]+", "", text)  # inline images
    text = re.sub(r"<[^>]+>", " ", text)       # strip HTML tags from error pages
    text = html.unescape(text)                 # &quot; -> " etc. (after stripping tags)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit] + ("…" if len(text) > limit else "")


def classify(entry):
    """Set status / manualFixNotes from the build notes and the verification result."""
    notes = [n for n in entry.get("manualFixNotes", []) if n.startswith("Uses unavailable endpoint")]
    v = entry.get("verification")
    if not v:
        entry["status"] = "unverified"
    elif v["outcome"] == "not-run":
        entry["status"] = "unverified"
        notes.append(f"Not checked: {v.get('error')}. Re-run `verify` later or check by hand.")
    elif v["outcome"] == "ok" and not notes:
        entry["status"] = "working"
    else:
        entry["status"] = "needs-manual-fix"
        if v["outcome"] == "ok":
            pass  # returned rows, but depends on an unavailable endpoint (see notes)
        elif v["outcome"] == "empty":
            notes.append("Returned 0 results: check by hand whether that is expected.")
        elif v["outcome"] == "timeout":
            notes.append(f"Timed out: {v.get('error')}. Run by hand with a longer timeout.")
        elif v["outcome"] == "too-large":
            notes.append(f"{v.get('error')}. The query probably works but returns a very large "
                         f"result; confirm by hand.")
        else:
            notes.append(f"Failed: {v.get('error')}")
        hint = FAILURE_HINTS.get(entry["target"])
        if hint and v["outcome"] != "ok":
            notes.append(hint)
    entry["manualFixNotes"] = notes


def host_of(url):
    return urllib.parse.urlsplit(url).netloc


def interleave_by_host(items):
    """Round-robin over target hosts so consecutive requests go to different servers."""
    buckets = OrderedDict()
    for key, entry in items:
        buckets.setdefault(host_of(entry["target"]), []).append((key, entry))
    ordered = []
    while any(buckets.values()):
        for host in list(buckets):
            if buckets[host]:
                ordered.append(buckets[host].pop(0))
    return ordered


def migrate_runs(meta):
    """Early files kept a single 'verificationRun'; every run is now kept in a list."""
    if "verificationRuns" not in meta:
        old = meta.pop("verificationRun", None)
        meta["verificationRuns"] = []
        if old:
            run = OrderedDict([("id", 1)])
            run.update((k, v) for k, v in old.items() if k != "id")
            meta["verificationRuns"].append(run)
    meta.pop("verificationRun", None)
    return meta["verificationRuns"]


def record(entry, res):
    """Store a new check result, keeping the earlier one in previousVerifications."""
    prev = entry.get("verification")
    if prev and prev.get("outcome") != "not-run":
        entry.setdefault("previousVerifications", []).append(prev)
    entry["verification"] = res
    classify(entry)


def verify(args):
    cat = load_json(args.catalogue)
    data, meta = cat["data"], cat["metadata"]
    runs = migrate_runs(meta)
    max_bytes = int(args.max_response_mb * 1024 * 1024)
    recheck = set(args.recheck or [])

    todo = []
    for key, entry in data.items():
        if args.only and short_id(key) not in args.only and key not in args.only:
            continue
        v = entry.get("verification")
        if (not args.force and v and v.get("queryHash") == query_hash(entry)
                and v.get("outcome") != "not-run" and v.get("outcome") not in recheck):
            continue
        todo.append((key, entry))
    todo = interleave_by_host(todo)

    print(f"{len(todo)} queries to check. Pause {args.delay}s between requests, "
          f"≥{args.host_gap}s between requests to the same host, timeout {args.timeout}s, "
          f"response limit {args.max_response_mb:g} MB, no retries.", flush=True)
    if args.dry_run:
        for key, entry in todo:
            prev = (entry.get("verification") or {}).get("outcome", "unverified")
            print(f"  {short_id(key):<60} -> {entry['target']}  (last: {prev})")
        return

    run_id = max([r.get("id", 0) for r in runs] + [0]) + 1
    run_meta = OrderedDict([
        ("id", run_id), ("label", args.label),
        ("started", now_iso()), ("finished", None),
        ("queriesSelected", len(todo)),
        ("settings", OrderedDict([
            ("delaySeconds", args.delay), ("perHostGapSeconds", args.host_gap),
            ("timeoutSeconds", args.timeout), ("retries", 0),
            ("failureCooldownSeconds", args.failure_cooldown),
            ("maxConsecutiveFailuresPerHost", args.max_host_failures),
            ("maxResponseBytes", max_bytes),
            ("recheckOutcomes", sorted(recheck)),
            ("only", args.only), ("force", args.force),
            ("userAgent", USER_AGENT), ("accept", ACCEPT),
        ])),
        ("notes", []),
    ])
    runs.append(run_meta)
    save_json_atomic(cat, args.catalogue)

    last_request_at = None
    host_next_ok = defaultdict(float)
    host_fail_streak = defaultdict(int)
    host_stopped = {}

    for i, (key, entry) in enumerate(todo, 1):
        host = host_of(entry["target"])
        sid = short_id(key)

        if host in host_stopped:
            prev = entry.get("verification")
            if prev and prev.get("outcome") != "not-run":
                # keep the earlier real result instead of replacing it with "not run"
                print(f"[{i}/{len(todo)}] {sid}: skipped, earlier result kept ({host_stopped[host]})",
                      flush=True)
                continue
            entry["verification"] = OrderedDict([
                ("date", now_iso()), ("endpoint", entry["target"]),
                ("outcome", "not-run"), ("error", host_stopped[host]),
                ("queryHash", query_hash(entry)), ("run", run_id),
            ])
            classify(entry)
            save_json_atomic(cat, args.catalogue)
            print(f"[{i}/{len(todo)}] {sid}: not run ({host_stopped[host]})", flush=True)
            continue

        # politeness: global pause + per-host minimum gap
        wait = 0.0
        if last_request_at is not None:
            wait = max(wait, last_request_at + args.delay - time.monotonic())
        wait = max(wait, host_next_ok[host] - time.monotonic())
        if wait > 0:
            time.sleep(wait)

        res = run_query(entry["target"], entry["query"], args.timeout, max_bytes)
        res["queryHash"] = query_hash(entry)
        res["run"] = run_id
        last_request_at = time.monotonic()
        host_next_ok[host] = last_request_at + args.host_gap

        record(entry, res)
        save_json_atomic(cat, args.catalogue)

        if res["outcome"] in ("ok", "empty"):
            extra = f"{res.get('resultCount')} rows, {res.get('responseBytes', 0) / 1e6:.1f} MB"
        else:
            extra = res.get("error", "")
        print(f"[{i}/{len(todo)}] {sid}: {res['outcome']} ({res.get('durationSeconds')}s) {extra[:200]}",
              flush=True)

        # back-off rules: only signs that the *server* is struggling count
        # (timeouts, 5xx, connection errors); a 4xx or a huge result is about
        # the individual query and does not stop the run for that host.
        status = res.get("httpStatus")
        server_trouble = (res["outcome"] == "timeout"
                          or (res["outcome"] == "error" and (status is None or status >= 500)))
        if status in (429, 503):
            host_stopped[host] = (f"stopped contacting {host} after it answered HTTP {status} "
                                  f"to an earlier query in run {run_id}")
        elif server_trouble:
            host_fail_streak[host] += 1
            host_next_ok[host] = last_request_at + max(args.host_gap, args.failure_cooldown)
            if host_fail_streak[host] >= args.max_host_failures:
                host_stopped[host] = (f"stopped contacting {host} after "
                                      f"{host_fail_streak[host]} consecutive failures in run {run_id}")
        else:
            host_fail_streak[host] = 0

    run_meta["finished"] = now_iso()
    save_json_atomic(cat, args.catalogue)
    summarize(cat)

def summarize(cat):
    counts = defaultdict(int)
    for entry in cat["data"].values():
        counts[entry.get("status")] += 1
    print("Status: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

def md_escape(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def fmt_check(v):
    parts = [f"**{v.get('outcome')}**"]
    if "resultCount" in v:
        parts.append(f"{v['resultCount']} rows")
    if v.get("httpStatus"):
        parts.append(f"HTTP {v['httpStatus']}")
    if v.get("durationSeconds") is not None:
        parts.append(f"{v['durationSeconds']} s")
    run = f"run {v['run']}, " if v.get("run") else ""
    return ", ".join(parts) + f" ({run}{v.get('date')})"


def report(args):
    cat = load_json(args.catalogue)
    data, meta = cat["data"], cat["metadata"]
    runs = meta.get("verificationRuns")
    if runs is None:
        runs = [meta["verificationRun"]] if meta.get("verificationRun") else []
    src = meta["derivedFrom"]
    src_meta = src.get("metadata") or {}

    status_counts = defaultdict(int)
    outcome_counts = defaultdict(int)
    for e in data.values():
        status_counts[e["status"]] += 1
        outcome_counts[(e.get("verification") or {}).get("outcome", "unverified")] += 1

    L = []
    L.append(f"# Query catalogue update ({meta['catalogueVersion']})")
    L.append("")
    L.append(f"Generated by `update_query_catalogue.py` from `{src['file']}` "
             f"(sparql-examples commit `{src_meta.get('commit')}`, "
             f"{len(data)} queries). The original file is unchanged.")
    L.append("")
    L.append(f"- **Policy:** {meta['policy']}")
    L.append(f"- **Upstream consulted:** {meta['upstream']['repository']} "
             f"@ `{meta['upstream']['commitConsultedForEndpointUrls'][:7]}`. "
             f"{meta['upstream']['note']}")
    L.append("- **Live checks:** in each verification run, every selected query is sent once, "
             "with its SERVICE clauses, to its `target` endpoint, one query at a time and "
             "without retries. Status and outcome below come from each query's latest check; "
             "earlier checks are kept. Settings per run are listed under *Verification runs*.")
    L.append("")
    L.append("## Summary")
    L.append("")
    L.append("| Status | Queries |")
    L.append("|---|---:|")
    for k in ("working", "needs-manual-fix", "unverified"):
        if status_counts.get(k):
            L.append(f"| {k} | {status_counts[k]} |")
    L.append("")
    L.append("| Latest outcome | Queries |")
    L.append("|---|---:|")
    for k in ("ok", "empty", "too-large", "error", "timeout", "not-run", "unverified"):
        if outcome_counts.get(k):
            L.append(f"| {k} | {outcome_counts[k]} |")
    L.append("")
    L.append("## Endpoint changes")
    L.append("")
    L.append("| From | To | Queries affected | Reason |")
    L.append("|---|---|---:|---|")
    for m in meta["endpointMap"]:
        n = sum(1 for e in data.values() if any(c["from"] == m["from"] for c in e["changes"]))
        L.append(f"| `{m['from']}` | `{m['to']}` | {n} | {md_escape(m['reason'])} |")
    L.append("")
    L.append("### Unavailable endpoints (no replacement; queries left unchanged)")
    L.append("")
    for u in meta["unavailableEndpoints"]:
        n = sum(1 for e in data.values() if u["endpoint"] in all_endpoints(e))
        L.append(f"- `{u['endpoint']}` ({n} {'query' if n == 1 else 'queries'}): {u['note']}")
    if meta.get("endpointNotes"):
        L.append("")
        L.append("### Other endpoint notes")
        L.append("")
        for u in meta["endpointNotes"]:
            L.append(f"- `{u['endpoint']}`: {u['note']}")
    L.append("")

    if meta.get("prefixMap"):
        L.append("## Prefix changes")
        L.append("")
        L.append("Only applied to queries whose target is the given endpoint.")
        L.append("")
        L.append("| Endpoint | Prefix | From | To | Queries affected | Reason |")
        L.append("|---|---|---|---|---:|---|")
        for m in meta["prefixMap"]:
            n = sum(1 for e in data.values()
                    if any(c["field"] == "queryPrefix" and c["from"] == m["from"] for c in e["changes"]))
            L.append(f"| `{m['endpoint']}` | `{m['prefix']}:` | `{m['from']}` | `{m['to']}` | {n} | "
                     f"{md_escape(m['reason'])} |")
        L.append("")
    manual = [(k, e) for k, e in data.items() if e["status"] == "needs-manual-fix"]
    L.append(f"## Queries to fix or check by hand ({len(manual)})")
    L.append("")
    if not manual:
        L.append("None.")
    for k, e in sorted(manual, key=lambda kv: (kv[1]["target"], short_id(kv[0]))):
        v = e.get("verification") or {}
        L.append(f"### `{short_id(k)}`")
        L.append("")
        L.append(f"- Key: <{k}>")
        L.append(f"- Target: `{e['target']}`"
                 + (f" ({e['changeStatus']})" if e["changeStatus"] != "unchanged" else ""))
        if v:
            L.append(f"- Check: {fmt_check(v)}")
        for pv in e.get("previousVerifications", []):
            L.append(f"- Earlier check: {fmt_check(pv)}")
        for n in e["manualFixNotes"]:
            L.append(f"- {md_escape(n)}")
        L.append("")

    unverified = [(k, e) for k, e in data.items() if e["status"] == "unverified"]
    L.append(f"## Queries not checked yet ({len(unverified)})")
    L.append("")
    if not unverified:
        L.append("None.")
    else:
        L.append("These were not sent to their endpoint (see the reason per query). "
                 "Re-run `python3 update_query_catalogue.py verify` to check only these.")
        L.append("")
        for k, e in sorted(unverified, key=lambda kv: (kv[1]["target"], short_id(kv[0]))):
            reason = (e.get("verification") or {}).get("error", "never checked")
            L.append(f"- `{short_id(k)}` → `{e['target']}`: {md_escape(reason)}")
    L.append("")

    L.append("## Verification runs")
    L.append("")
    if not runs:
        L.append("None yet.")
    for r in runs:
        s = r.get("settings", {})
        title = f"### Run {r.get('id', '?')}" + (f": {r['label']}" if r.get("label") else "")
        L.append(title)
        L.append("")
        sel = f"; {r['queriesSelected']} queries selected" if r.get("queriesSelected") is not None else ""
        L.append(f"- {r.get('started')} → {r.get('finished') or '(unfinished)'}{sel}")
        limit = (f", {s['maxResponseBytes'] / 1048576:g} MB response limit"
                 if s.get("maxResponseBytes") else "")
        L.append(f"- Settings: {s.get('delaySeconds')} s pause between requests, "
                 f"≥ {s.get('perHostGapSeconds')} s between requests to the same host, "
                 f"{s.get('timeoutSeconds')} s timeout{limit}, no retries"
                 + (f"; re-checked outcomes: {', '.join(s['recheckOutcomes'])}"
                    if s.get("recheckOutcomes") else ""))
        for n in r.get("notes") or []:
            L.append(f"- {md_escape(n)}")
        L.append("")

    L.append("## All queries")
    L.append("")
    L.append("| Query | Target | Change | Status | Outcome | Rows | Time (s) | Earlier checks |")
    L.append("|---|---|---|---|---|---:|---:|---|")
    for k, e in sorted(data.items(), key=lambda kv: (kv[1]["target"], short_id(kv[0]))):
        v = e.get("verification") or {}
        earlier = ", ".join(f"{pv.get('outcome')} (run {pv.get('run', '?')})"
                            for pv in e.get("previousVerifications", []))
        L.append(f"| `{md_escape(short_id(k))}` | `{e['target']}` | {e['changeStatus']} | "
                 f"{e['status']} | {v.get('outcome', '')} | {v.get('resultCount', '')} | "
                 f"{v.get('durationSeconds', '')} | {earlier} |")
    L.append("")

    with open(args.output, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    print(f"Wrote {args.output}")

# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="apply the endpoint map and write the updated catalogue")
    b.add_argument("-i", "--input", default="sib-swiss-federated-queries.json")
    b.add_argument("-o", "--output", default="sib-swiss-federated-queries.updated-2026-11.json")
    b.add_argument("--version", default="2026-11")
    b.add_argument("--force", action="store_true",
                   help="rebuild an existing output file (its earlier checks are kept: a check stays "
                        "current if the query did not change, otherwise it moves to previousVerifications)")
    b.add_argument("--discard-checks", action="store_true",
                   help="with --force: start without any earlier checks or run history")
    b.set_defaults(func=build)

    v = sub.add_parser("verify", help="check each query once against its live target endpoint")
    v.add_argument("-c", "--catalogue", default="sib-swiss-federated-queries.updated-2026-11.json")
    v.add_argument("--delay", type=float, default=20, help="seconds between any two requests (default 20)")
    v.add_argument("--host-gap", type=float, default=60, help="minimum seconds between requests to the same host (default 60)")
    v.add_argument("--failure-cooldown", type=float, default=180, help="seconds before contacting a host again after it failed (default 180)")
    v.add_argument("--max-host-failures", type=int, default=3, help="stop contacting a host after this many consecutive failures (default 3)")
    v.add_argument("--timeout", type=float, default=180, help="client timeout per query in seconds (default 180)")
    v.add_argument("--max-response-mb", type=float, default=DEFAULT_MAX_RESPONSE_MB,
                   help=f"stop reading a response after this many MB (default {DEFAULT_MAX_RESPONSE_MB}); "
                        "results are counted while streaming, so large limits do not need much memory")
    v.add_argument("--recheck", nargs="+", choices=["ok", "empty", "too-large", "error", "timeout"],
                   help="also re-check queries whose latest outcome is one of these "
                        "(unchecked queries are always included)")
    v.add_argument("--label", help="short free-text label stored with this verification run")
    v.add_argument("--only", nargs="+", help="only check these queries (short id such as 51 or emi#examples011a, or full key)")
    v.add_argument("--force", action="store_true", help="re-check queries that already have a result")
    v.add_argument("--dry-run", action="store_true", help="list what would be sent, send nothing")
    v.set_defaults(func=verify)

    r = sub.add_parser("report", help="write a Markdown change log and manual-fix list")
    r.add_argument("-c", "--catalogue", default="sib-swiss-federated-queries.updated-2026-11.json")
    r.add_argument("-o", "--output", default="sib-swiss-federated-queries.updated-2026-11.md")
    r.set_defaults(func=report)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
