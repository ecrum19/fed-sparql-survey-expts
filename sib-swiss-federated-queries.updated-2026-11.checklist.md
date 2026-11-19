# Manual query checks (2026-11)

Checklist for the 26 queries in `sib-swiss-federated-queries.updated-2026-11.json` that did not pass the automated checks. Created on 2026-11-19 from the catalogue; it is not regenerated, so edit it by hand: tick a box (`[x]`) when done and write what you found after **Result:**.

- **The query text** is the `query` field of the entry with the given key in the updated catalogue (or `queries/service/<id>.rq` after running `python3 query_sort.py -i sib-swiss-federated-queries.updated-2026-11.json -t service`).
- **Upstream** compares the query with the current upstream SIB catalogue ([sib-swiss/sparql-examples@45e5b53](https://github.com/sib-swiss/sparql-examples/tree/45e5b53)), ignoring PREFIX lines.
- **After fixing a query**, record a new check with `python3 update_query_catalogue.py verify --only <id>`. If you change the query text, also add the change to `update_query_catalogue.py` (for example `UPSTREAM_REPLACEMENTS`); otherwise `build --force` undoes it.

## Upstream changed the query in a way that may fix it (7)

Try the upstream version first. Upstream `.ttl` files declare only some PREFIXes in the query; add the rest from that folder's `prefixes.ttl`, or copy the expanded query from the endpoint's examples page.

- [ ] **SwissLipids 7** → `https://beta.sparql.swisslipids.org/sparql`
  - Key: `https://sparql.swisslipids.org/.well-known/sparql-examples/7`
  - Last check: empty, 0 rows, 0.5 s (run 2, 2026-11-19)
  - Upstream: typo fixed: `SWISSLIPID:Isomeric_Subspecie` → `SWISSLIPID:Isomeric_Subspecies`; SERVICE URL without trailing slash ([`examples/SwissLipids/7.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/SwissLipids/7.ttl))
  - Try: Run with the corrected class name.
  - Result:
- [ ] **UniProt 67_draft_human_metabolome** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/67_draft_human_metabolome`
  - Last check: empty, 0 rows, 0.6 s (run 2, 2026-11-19)
  - Upstream: ChEBI predicates changed (`chebislash:smiles`/`inchikey` → `chemrof:smiles_string`/`chemrof:inchi_key_string`) and `rh:underlyingChebi/rh:chebi` → `rh:underlyingChebi` ([`examples/UniProt/67_draft_human_metabolome.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/67_draft_human_metabolome.ttl))
  - Try: Run the upstream version.
  - Result:
- [ ] **Rhea 19_draft_human_metabolome** → `https://sparql.rhea-db.org/sparql`
  - Key: `https://sparql.rhea-db.org/.well-known/sparql-examples/19_draft_human_metabolome`
  - Last check: empty, 0 rows, 1.2 s (run 1, 2026-11-18)
  - Upstream: same ChEBI changes as UniProt 67 (note: upstream uses `chemrof:inchikey_string` here but `chemrof:inchi_key_string` in UniProt 67; one may be a typo); SERVICE URL without trailing slash ([`examples/Rhea/19_draft_human_metabolome.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/Rhea/19_draft_human_metabolome.ttl))
  - Try: Run the upstream version; if it is still empty, try the other `inchi_key` spelling.
  - Result:
- [ ] **UniProt 36** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/36`
  - Last check: empty, 0 rows, 0.5 s (run 2, 2026-11-19)
  - Upstream: reworked: the OrthoDB group is no longer read from UniProt's `rdfs:seeAlso` cross-reference (upstream says these can be out of date) but matched inside the OrthoDB SERVICE ([`examples/UniProt/36_orthologous_proteins_via_orthodb.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/36_orthologous_proteins_via_orthodb.ttl))
  - Try: Run the upstream version.
  - Result:
- [ ] **UniProt 117** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/117_biosodafrontend_glioblastoma_orthologs_rat`
  - Last check: error, HTTP 400, 1 s (run 2, 2026-11-19)
  - Upstream: restructured: the UniProt disease filter now runs in a subquery before the OMA and Bgee SERVICE calls (OMA and Bgee parts unchanged apart from layout) ([`examples/UniProt/117_biosodafrontend_glioblastoma_orthologs_rat.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/117_biosodafrontend_glioblastoma_orthologs_rat.ttl))
  - Try: Run the upstream version. It also calls Bgee, which may block UniProt's SERVICE call (see UniProt 49).
  - Result:
- [ ] **UniProt 118** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/118_biosodafrontend_rat_brain_human_cancer`
  - Last check: error, HTTP 400, 0.9 s (run 2, 2026-11-19)
  - Upstream: restructured like UniProt 117 (UniProt part in a subquery) and selects only `?gene ?orthologous_protein2` ([`examples/UniProt/118_biosodafrontend_rat_brain_human_cancer.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/118_biosodafrontend_rat_brain_human_cancer.ttl))
  - Try: Run the upstream version (same Bgee caveat as 117).
  - Result:
- [ ] **Bgee 028-biosodafrontend** → `https://www.bgee.org/sparql/`
  - Key: `https://www.bgee.org/sparql/.well-known/sparql-examples/028-biosodafrontend`
  - Last check: error, HTTP 524, 125.3 s (run 1, 2026-11-18)
  - Upstream: reworked: `LIMIT 10` on the Bgee genes, `up:transcribedFrom` without `rdfs:seeAlso`, and no `up:Disease_Annotation` filter any more (it returns all annotation IRIs, not disease annotation texts) ([`examples/Bgee/028-biosodafrontend.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/Bgee/028-biosodafrontend.ttl))
  - Try: Run the upstream version; the `LIMIT 10` may keep it under Cloudflare's ~125 s limit.
  - Result:

## Endpoint time limit: run it where the limit does not apply (5)

These were cut off by the endpoint or its proxy, not by our client. Try the endpoint's own web UI, a client with a long timeout, or ask the maintainers.

- [ ] **Bgee 018** → `https://www.bgee.org/sparql`
  - Key: `https://www.bgee.org/sparql/.well-known/sparql-examples/018`
  - Last check: error, HTTP 524, 125.4 s (run 1, 2026-11-18)
  - Upstream: only the SERVICE URL's trailing slash differs ([`examples/Bgee/018.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/Bgee/018.ttl))
  - Try: Cloudflare ends requests at ~125 s (HTTP 524).
  - Result:
- [ ] **Bgee 019** → `https://www.bgee.org/sparql`
  - Key: `https://www.bgee.org/sparql/.well-known/sparql-examples/019`
  - Last check: error, HTTP 524, 125.3 s (run 1, 2026-11-18)
  - Upstream: unchanged ([`examples/Bgee/019.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/Bgee/019.ttl))
  - Try: Cloudflare ends requests at ~125 s (HTTP 524).
  - Result:
- [ ] **EMI 006** → `https://qlever.earthmetabolome.org/api/metrin-kg`
  - Key: `https://biosoda.unil.ch/emi/sparql/.well-known/sparql-examples/006`
  - Last check: error, HTTP 504, 60.3 s (run 3, 2026-11-19)
  - Upstream: identical apart from the `emi:` PREFIX (already updated) ([`examples/emi/006.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/emi/006.ttl))
  - Try: The EMI gateway ends requests at 60 s (HTTP 504).
  - Result:
- [ ] **EMI emi#examples018** → `https://qlever.earthmetabolome.org/api/metrin-kg`
  - Key: `https://purl.org/emi#examples018`
  - Last check: error, HTTP 504, 60.2 s (run 3, 2026-11-19)
  - Upstream: reworked and renumbered; closest upstream queries are `examples/emi/020.ttl` and `021.ttl` ([`examples/emi/020.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/emi/020.ttl))
  - Try: The EMI gateway ends requests at 60 s (HTTP 504). If it keeps failing, consider replacing it with the upstream version like 012 and 019a.
  - Result:
- [ ] **OrthoDB 19** → `https://sparql.orthodb.org/sparql/`
  - Key: `https://sparql.orthodb.org/.well-known/sparql-examples/19`
  - Last check: timeout, 900.2 s (run 2, 2026-11-19)
  - Upstream: only the SERVICE URL's trailing slash differs ([`examples/OrthoDB/19.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/OrthoDB/19.ttl))
  - Try: No answer within 900 s from our client.
  - Result:

## A SERVICE call inside the query fails (6)

The target endpoint answered, but one of its SERVICE calls failed.

- [ ] **UniProt 49** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/49`
  - Last check: error, HTTP 400, 11 s (run 2, 2026-11-19)
  - Upstream: SERVICE URL is `https://www.bgee.org/sparql/` (with trailing slash) upstream ([`examples/UniProt/49_tissues_where_genes_metabolizing_cholestrol_are_expressed.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/49_tissues_where_genes_metabolizing_cholestrol_are_expressed.ttl))
  - Try: Bgee's Cloudflare bot check answers UniProt's SERVICE call with 403. Try the `/sparql/` URL, or run the Bgee part directly on Bgee.
  - Result:
- [ ] **UniProt 50** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/50`
  - Last check: error, HTTP 400, 0.9 s (run 2, 2026-11-19)
  - Upstream: SERVICE URL is `https://www.bgee.org/sparql/` (with trailing slash) upstream ([`examples/UniProt/50_tissues_where_genes_sphingosines_are_expressed.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/50_tissues_where_genes_sphingosines_are_expressed.ttl))
  - Try: Same as UniProt 49.
  - Result:
- [ ] **UniProt 109** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/109_uniprot_transporter_in_liver`
  - Last check: error, HTTP 400, 4.5 s (run 2, 2026-11-19)
  - Upstream: SERVICE URL is `https://www.bgee.org/sparql/` (with trailing slash) upstream ([`examples/UniProt/109_uniprot_transporter_in_liver.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/109_uniprot_transporter_in_liver.ttl))
  - Try: Same as UniProt 49.
  - Result:
- [ ] **EMI emi#examples012** → `https://qlever.earthmetabolome.org/api/metrin-kg`
  - Key: `https://purl.org/emi#examples012`
  - Last check: error, HTTP 500, 2.3 s (run 5, 2026-11-19)
  - Upstream: **changed in the upstream SIB catalogue; already replaced with the upstream version** (`examples/emi/013.ttl`) ([`examples/emi/013.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/emi/013.ttl))
  - Try: QLever's Wikidata endpoint answered the EMI server's SERVICE call with 429 Too Many Requests. Try again later.
  - Result:
- [ ] **UniProt 90** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/90_uniprot_affected_by_metabolic_diseases_using_MeSH`
  - Last check: error, HTTP 400, 1.1 s (run 2, 2026-11-19)
  - Upstream: unchanged ([`examples/UniProt/90_uniprot_affected_by_metabolic_diseases_using_MeSH.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/90_uniprot_affected_by_metabolic_diseases_using_MeSH.ttl))
  - Try: The MeSH endpoint (`https://id.nlm.nih.gov/mesh/sparql`) returned its service description instead of results, which UniProt (QLever) cannot use. Check whether MeSH still offers SPARQL results there.
  - Result:
- [ ] **OrthoDB 20** → `https://sparql.orthodb.org/sparql/`
  - Key: `https://sparql.orthodb.org/.well-known/sparql-examples/20`
  - Last check: error, HTTP 500, 5.9 s (run 1, 2026-11-18)
  - Upstream: unchanged ([`examples/OrthoDB/20.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/OrthoDB/20.ttl))
  - Try: STRING (`https://sparql.string-db.org/sparql`) answered OrthoDB's SERVICE call with 403 Forbidden (an HTML page). Check whether STRING's endpoint still accepts SPARQL requests.
  - Result:

## Very large results (3)

Each returned more than 2 GB before our client stopped reading. Check with `COUNT(*)` or a `LIMIT`.

- [ ] **SwissLipids 26** → `https://beta.sparql.swisslipids.org/sparql`
  - Key: `https://sparql.swisslipids.org/.well-known/sparql-examples/26`
  - Last check: too-large, HTTP 200, 45.2 s (run 2, 2026-11-19)
  - Upstream: `rh:underlyingChebi/rh:chebi` → `rh:underlyingChebi`; SERVICE URL without trailing slash ([`examples/SwissLipids/26.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/SwissLipids/26.ttl))
  - Result:
- [ ] **SwissLipids 27** → `https://beta.sparql.swisslipids.org/sparql`
  - Key: `https://sparql.swisslipids.org/.well-known/sparql-examples/27`
  - Last check: too-large, HTTP 200, 42.9 s (run 2, 2026-11-19)
  - Upstream: `rh:underlyingChebi/rh:chebi` → `rh:underlyingChebi`; SERVICE URL without trailing slash ([`examples/SwissLipids/27.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/SwissLipids/27.ttl))
  - Result:
- [ ] **SwissLipids 34** → `https://beta.sparql.swisslipids.org/sparql`
  - Key: `https://sparql.swisslipids.org/.well-known/sparql-examples/34`
  - Last check: too-large, HTTP 200, 45.7 s (run 2, 2026-11-19)
  - Upstream: `rh:underlyingChebi/rh:chebi` → `rh:underlyingChebi`; SERVICE URL without trailing slash ([`examples/SwissLipids/34.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/SwissLipids/34.ttl))
  - Result:

## 0 rows, no relevant upstream change (4)

Check whether an empty result is expected (data changed?) or the query needs a fix.

- [ ] **UniProt 48** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/48`
  - Last check: empty, 0 rows, 0.1 s (run 2, 2026-11-19)
  - Upstream: unchanged ([`examples/UniProt/48_glycosylation_sites_and_glycans.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/48_glycosylation_sites_and_glycans.ttl))
  - Result:
- [ ] **UniProt 53** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/53`
  - Last check: empty, 0 rows, 760.2 s (run 2, 2026-11-19)
  - Upstream: unchanged ([`examples/UniProt/53_mouse_homologs_of_sterol_enzymes_via_omabrowser.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/53_mouse_homologs_of_sterol_enzymes_via_omabrowser.ttl))
  - Result:
- [ ] **OrthoDB 17** → `https://sparql.orthodb.org/sparql/`
  - Key: `https://sparql.orthodb.org/.well-known/sparql-examples/17`
  - Last check: empty, 0 rows, 0.2 s (run 1, 2026-11-18)
  - Upstream: only the SERVICE URL's trailing slash differs ([`examples/OrthoDB/17.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/OrthoDB/17.ttl))
  - Result:
- [ ] **OMA 15-rat-TP53-biosodafrontend** → `https://sparql.omabrowser.org/sparql/`
  - Key: `https://sparql.omabrowser.org/.well-known/sparql-examples/15-rat-TP53-biosodafrontend`
  - Last check: empty, 0 rows, 0.3 s (run 1, 2026-11-18)
  - Upstream: only the SERVICE URL's trailing slash differs ([`examples/OMA/15-biosodafrontend-rat-TP53.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/OMA/15-biosodafrontend-rat-TP53.ttl))
  - Result:

## Error message missing (1)

- [ ] **UniProt 99** → `https://sparql.uniprot.org/sparql/`
  - Key: `https://sparql.uniprot.org/.well-known/sparql-examples/99_uniprot_identifiers_org_translation`
  - Last check: error, HTTP 400, 0.6 s (run 1, 2026-11-18)
  - Upstream: unchanged ([`examples/UniProt/99_uniprot_identifiers_org_translation.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/UniProt/99_uniprot_identifiers_org_translation.ttl))
  - Try: UniProt answered HTTP 400 in run 1, but the error text was cut off. Run it once to see the message.
  - Result:

## Not on this list

- **EMI emi#examples019a**: changed in the upstream SIB catalogue and replaced with [`examples/emi/019.ttl`](https://github.com/sib-swiss/sparql-examples/blob/45e5b53/examples/emi/019.ttl); it now works (ok, 1000 rows, HTTP 200, 30.3 s (run 5, 2026-11-19)).
- **UniProt 46**: Removed because it uses a dead endpoint: https://semantic.eea.europa.eu/sparql (No response (connection failed) on 2026-11-18. Was already failing with HTTP 404 in the 2025 control runs.) The query is still in the original catalogue.
- **OrthoDB 18**: Removed because it uses a dead endpoint: https://sparql.nextprot.org/ (neXtProt is retired: the endpoint 301-redirects to https://www.expasy.org/archives/nextprot (HTML). No replacement endpoint known.) The query is still in the original catalogue.
- **OMA 14**: Removed because it uses a dead endpoint: http://sparql.nibb.ac.jp/sparql (No response (connection failed) on 2026-11-18. Was already failing with HTTP 500 in the 2025 control runs.) The query is still in the original catalogue.
- **EMI emi#examples019b**: Removed because it was dropped from the upstream SIB catalogue: at sparql-examples@45e5b53 examples/emi/ has a single 019 example, the new version of 019a. This version also has a typo (emi:hasTarget ?parasitoidX where ?parasiteX is meant) and returned 0 rows on the new EMI endpoint (run 3, 2026-11-19). The query is still in the original catalogue.
