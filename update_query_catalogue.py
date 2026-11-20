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
        target endpoint's data. Exceptions: queries in UPSTREAM_REPLACEMENTS get
        their current upstream text, queries in QUERY_FIXES get small documented
        text replacements, and queries in REMOVED_QUERIES or using an
        endpoint in UNAVAILABLE_ENDPOINTS are left out (listed in the metadata).
        QUERY_NOTES (runs by hand, other versions tried, upstream comparison)
        are copied into the entries.
        No new upstream queries are added. Rebuilding an existing file keeps its checks: a check
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

report  Write a Markdown change log from the updated file: every change, every
        check, the queries that do not work yet and the expected results.

expected
        Write the expected result of each query (row count, 0 rows or the reason
        it fails) with every check behind it, for testing engines later.

Usage
-----
  python3 update_query_catalogue.py build  -i sib-swiss-federated-queries.json \
                                           -o sib-swiss-federated-queries.updated-2026-11.json
  python3 update_query_catalogue.py verify -c sib-swiss-federated-queries.updated-2026-11.json
  python3 update_query_catalogue.py report -c sib-swiss-federated-queries.updated-2026-11.json \
                                           -o sib-swiss-federated-queries.updated-2026-11.md
  python3 update_query_catalogue.py expected   # -> ...updated-2026-11.expected-results.json
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

# Queries whose text and description are replaced with their current upstream
# version, because upstream reworked them and the old version does not work on
# the new endpoint even after the endpoint and PREFIX fixes. The key stays the
# same, but results for these keys are not comparable with earlier experiments.
# Texts copied verbatim from sparql-examples@45e5b53.
UPSTREAM_REPLACEMENTS = OrderedDict([
    ("https://purl.org/emi#examples012", OrderedDict([
        ("upstreamFile", "examples/emi/013.ttl"),
        ("upstreamId", "https://kg.earthmetabolome.org/metrin/api/.well-known/examples/013"),
        ("reason", (
            "Changed in the upstream SIB catalogue: at sparql-examples@45e5b53 this query is "
            "examples/emi/013.ttl (same description), which reads species names with "
            "rdfs:label and filters seed dry mass >= 100 instead of >= 626. The old version "
            "returned 0 rows on the new EMI endpoint even with the PREFIX fix (run 3, "
            "2026-11-19), so it was replaced with the upstream text and description."
        )),
        ("description", "List all metabolites produced by species with near threatened (wd:Q719675) IUCN status (wdt:P141) and with values available (or greater than a specific value) for trait 'Seed dry mass'."),
        ("query", """PREFIX emi: <https://w3id.org/emi#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?structure_inchikey ?wd_chem ?source_wdx ?sourceName WHERE {
	# trait data
	?trySpObs sosa:isSampleOf ?trySpName ; # trait/non-trait data for trySpName (scientific name of plant species as listed in trydb)
		rdfs:label ?sourceName ;
		sosa:isFeatureOfInterestOf ?tryObId .
	?tryObId sosa:hasResult ?tryData .
	?trySpName emi:inTaxon ?source_wdx . # wikidata-ids wdx for trySpName
	?tryData rdfs:label ?tryDataLab ;
		rdf:type emi:Trait ; #retrieve data which is labelled as 'Trait' and its values
		rdf:value ?tryDataVal .
	FILTER (STR(?tryDataLab) = 'Seed dry mass' ) #filter on data label 'Seed dry mass'
	FILTER (?tryDataVal >= 100)
	SERVICE <https://qlever.dev/api/wikidata> {
        	?source_wdx wdt:P141 wd:Q719675 . # filter wikidata-ids for trySpName, which have IUCN status (wdt:P141) as near threatened (wd:Q719675)
        }
	# metabolite data
	{
		SELECT ?source_wdx ?structure_inchikey ?wd_chem WHERE {
			?material sosa:hasSample ?extract ;
		        	sosa:isSampleOf ?organe .
			?organe emi:inTaxon ?source_wdx . # filter metabolite data which is found in wikidata-ids wdx
			?extract sosa:isFeatureOfInterestOf ?lcms .
		    	?lcms sosa:hasResult ?feature_list .
			?feature_list emi:hasLCMSFeature ?feature .
			?feature emi:hasAnnotation ?sirius_annotation .
			?sirius_annotation a emi:StructuralAnnotation ;
				emi:hasChemicalStructure ?ik2d .
			?ik2d emi:hasSMILES ?smiles ;
		    		emi:isInChIKey2DOf ?structure_inchikey .
			?structure_inchikey emi:isInChIKeyOf ?wd_chem .	# wikidata-ids for metabolites
		}
	}
	UNION # union with data from lotus (integrated in wikidata)
	{
		SERVICE <https://qlever.dev/api/wikidata> {
			?wd_chem wdt:P235 ?structure_inchikey;
   				wdt:P703 ?source_wdx .
		}
	}
} LIMIT 1000"""),
    ])),
    ("https://purl.org/emi#examples019a", OrderedDict([
        ("upstreamFile", "examples/emi/019.ttl"),
        ("upstreamId", "https://kg.earthmetabolome.org/metrin/api/.well-known/examples/019"),
        ("reason", (
            "Changed in the upstream SIB catalogue: at sparql-examples@45e5b53 this query is "
            "examples/emi/019.ttl (same description), which selects the interaction type "
            "with VALUES instead of a REGEX filter and reads names with rdfs:label. The old "
            "version hit the endpoint's 60 s gateway limit (HTTP 504) even with the PREFIX "
            "fix (run 3, 2026-11-19), so it was replaced with the upstream text and description."
        )),
        ("description", 'A list of interactions depicting connections between parasatoids harmful for insects living as parasites on plants.'),
        ("query", """PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX emi: <https://w3id.org/emi#>
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX dcterms: <http://purl.org/dc/terms/>
SELECT DISTINCT ?parasitoidX_WD ?parasitoidName ?intxn2Label ?parasiteX_WD ?parasiteName ?intxn3Label ?hostPlant_WD ?hostPlantName ?study2_DOI ?study3_DOI WHERE {
	# part-1 interaction data parasite and parasitoid
	{
		SELECT DISTINCT ?parasitoidName ?parasiteName ?parasiteX_WD ?intxn2Label ?parasitoidX_WD ?study2_DOI WHERE {
			?intxn2 emi:hasSource ?parasitoidX ; # retrieve interaction pairs part-1
	    			emi:hasTarget ?parasiteX ;
	    			emi:isClassifiedWith ?intxnName2 .
			VALUES ?intxnName2 { <http://purl.obolibrary.org/obo/RO_0002208> } # keep interactions only if the interaction-id is RO_0002208 (parasitoidOf), e.g: 'parasitoidX' is 'parasitoidOf' 'parasiteX'
			?intxnName2 rdfs:label ?intxn2Label .
			?parasitoidX emi:inTaxon ?parasitoidX_WD ; # retreieve wikidata-ids for parasitoidX
			     	rdfs:label ?parasitoidName ;
			     	sosa:isSampleOf ?parasitoidSample . # retrieve scientific name of parasitoidX as listed in GloBI
			?parasiteX emi:inTaxon ?parasiteX_WD ; # retreieve wikidata-ids for parasiteX
				rdfs:label ?parasiteName ;
				sosa:isSampleOf ?parasiteSample . # retreieve scientific name for parasiteX as listed in GloBI
			OPTIONAL {
				?intxn2 dcterms:bibliographicCitation ?study2_DOI . # optionally retrieve the doi of the study for parasitoid-parasite pairs
			}
		}
	}

	# part-2 interaction data parasite and host plant
	?parasiteX1 emi:inTaxon ?parasiteX_WD .	# check pasarsiteX1 is in wikidata-id parasiteX_WD (the ones obtained from interaction-pairs part-1)
	?intxn3 emi:hasSource ?parasiteX1 ; # retrieve interaction pairs part-2
    		emi:hasTarget ?hostPlant ;
    		emi:isClassifiedWith ?intxnName3 .
	?intxnName3 rdfs:label ?intxn3Label .
	?hostPlant emi:inTaxon ?hostPlant_WD ;	# retreieve wikidata-ids for hostPlant
		rdfs:label ?hostPlantName ;
		sosa:isSampleOf ?hostPlantSample . # retreieve scientific name for hostPlant as listed in GloBI
	OPTIONAL {
		?intxn3 dcterms:bibliographicCitation ?study3_DOI .
	}
    	FILTER (!(STR(?intxn3Label) IN ('visits', 'visitsFlowersOf', 'pollinates')))	# keep interactions only if the interaction-names are not 'visits', 'visitsFlowersOf' or 'pollinates'. e.g.: 'parasiteX1' is 'pathogenOf/parasiteOf/..' 'hostPlant'
	SERVICE <https://qlever.dev/api/wikidata> {
	 	?hostPlant_WD wdt:P171* wd:Q879246 . # keep the interaction pairs part-2 only if hostPlant_WD has Kingdom Archaeplastida (wd:Q879246) in its lineage, this is to ensure the hostPlant is really a plant
	}
} LIMIT 1000"""),
    ])),
    ("https://sparql.uniprot.org/.well-known/sparql-examples/67_draft_human_metabolome", OrderedDict([
        ("upstreamFile", "examples/UniProt/67_draft_human_metabolome.ttl"),
        ("upstreamId", "https://sparql.uniprot.org/.well-known/sparql-examples/67_draft_human_metabolome"),
        ("reason", (
            "Changed in the upstream SIB catalogue: at sparql-examples@45e5b53 this query reads ChEBI's "
            "chemrof:smiles_string and chemrof:inchi_key_string (instead of chebislash:smiles and "
            "chebislash:inchikey) and follows rh:underlyingChebi straight to the ChEBI IRI (instead of "
            "rh:underlyingChebi/rh:chebi); the description is unchanged. The old version returned 0 rows "
            "(run 2, 2026-11-19); the upstream version returned 317,738 rows when run by hand "
            "(2026-11-20), so it replaced the old text."
        )),
        ("description", 'Generate a draft human metabolome'),
        ("query", """PREFIX chemrof: <https://w3id.org/chemrof/>
PREFIX rh: <http://rdf.rhea-db.org/>
PREFIX taxon: <http://purl.uniprot.org/taxonomy/>
PREFIX up: <http://purl.uniprot.org/core/>

SELECT
?uniprot ?mnemonic ?rhea ?chebi ?smiles ?inchiKey
WHERE
{
  ?uniprot up:annotation/up:catalyticActivity/up:catalyzedReaction ?rhea ;
        up:organism taxon:9606 ;
        up:mnemonic ?mnemonic .
  SERVICE <https://sparql.rhea-db.org/sparql> {
	?rhea rh:side/rh:contains/rh:compound ?compound .
    ?compound (rh:chebi|(rh:reactivePart/rh:chebi)|rh:underlyingChebi) ?chebi .
   }
   ?chebi chemrof:smiles_string ?smiles ;
          chemrof:inchi_key_string ?inchiKey .
}"""),
    ])),
    ("https://sparql.uniprot.org/.well-known/sparql-examples/36", OrderedDict([
        ("upstreamFile", "examples/UniProt/36_orthologous_proteins_via_orthodb.ttl"),
        ("upstreamId", "https://sparql.uniprot.org/.well-known/sparql-examples/36"),
        ("reason", (
            "Changed in the upstream SIB catalogue: at sparql-examples@45e5b53 this query no longer reads "
            "the OrthoDB group from UniProt's rdfs:seeAlso cross-reference (upstream notes these can be out "
            "of date) but matches the protein inside the OrthoDB SERVICE, whose URL now has a trailing "
            "slash; the description is unchanged. The old version returned 0 rows (run 2, 2026-11-19); the "
            "upstream version returned 1,100 rows when run by hand (2026-11-20), so it replaced the old "
            "text."
        )),
        ("description", 'Find the orthologous proteins for UniProtKB entry P05067 using the <a href="http://www.orthod.org">OrthoDB database</a>'),
        ("query", """PREFIX orthodb: <http://purl.orthodb.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX uniprotkb: <http://purl.uniprot.org/uniprot/>
PREFIX up: <http://purl.uniprot.org/core/>

SELECT
  ?protein
  ?orthoGroup
  ?scientificName
  ?functionComment
  ?prefferedGeneName
  ((STRLEN(?value) - ?medianLength) as ?deviationFromMedianLength)
WHERE
{
  BIND(uniprotkb:P05067 AS ?protein)
  ?protein a up:Protein ;
    up:organism/up:scientificName ?scientificName ;
    up:encodedBy/skos:prefLabel ?prefferedGeneName ;
    up:sequence/rdf:value ?value .
  # We do not ask the UniProt endpoint for the ?orthoGroup from the
  # cross-reference as these can be out of date. We trust the content
  # of the orthodb endpoint
  OPTIONAL {
    ?protein up:annotation ?functionAnnotation .
    ?functionAnnotation a up:Function_Annotation ;
      rdfs:comment ?functionComment .
  }
  SERVICE <https://sparql.orthodb.org/sparql/>{
    ?orthoGroup orthodb:ogMedianProteinLength ?medianLength .
    ?orthoGroup orthodb:hasMember ?xref .
    ?xref orthodb:xref/orthodb:xrefResource ?protein .
  }
}"""),
    ])),
])

# Small query fixes for queries that returned no results, keyed by query.
# Each fix is a list of exact text replacements, applied after the endpoint and
# PREFIX maps; the query keeps asking the same question. Found with small probe
# queries on 2026-11-20 (see the reason of each fix).
QUERY_FIXES = OrderedDict([
    ('https://sparql.uniprot.org/.well-known/sparql-examples/48', OrderedDict([
        ("query", [
            ('PREFIX glycan: <http://purl.jp/bio/12/glyco/glycan#>',
             'PREFIX glyconj: <http://purl.jp/bio/12/glyco/conjugate#>'),
            ('glycan:glycosylated_at',
             'glyconj:glycosylated_at'),
            ('glycan:glycosylates_at',
             'glyconj:glycosylates_at'),
        ]),
        ("description", []),
        ("reason", (
            'GlyConnect now publishes glycosylated_at and glycosylates_at in the namespace '
            'http://purl.jp/bio/12/glyco/conjugate# (probe 2026-11-20: 0 triples with the glycan# '
            'predicates, 5,052 matching rows for the SERVICE block with conjugate#). Only those two '
            'predicates change.'
        )),
    ])),
    ('https://sparql.orthodb.org/.well-known/sparql-examples/17', OrderedDict([
        ("query", [
            ('odbgroup:6400at314295',
             'odbgroup:17060at314295'),
        ]),
        ("description", [('6400at314295', '17060at314295')]),
        ("reason", (
            "OrthoDB group 6400at314295 no longer exists (0 genes on 2026-11-20). OrthoDB's v11 API names "
            "it 'Coiled-coil domain-containing protein 179' (Hominoidea, 7 genes in 7 species); the "
            'current group for the same family is 17060at314295 (same name, 7 genes, contains human '
            'CCDC179, UniProt H3BU77). The group ID is replaced in the query and the description.'
        )),
    ])),
    ('https://sparql.omabrowser.org/.well-known/sparql-examples/15-rat-TP53-biosodafrontend', OrderedDict([
        ("query", [
            ('PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT DISTINCT ?PROTEIN',
             'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nPREFIX sio: <http://semanticscience.org/resource/>\nSELECT DISTINCT ?PROTEIN'),
            ("\t\t?PROTEIN orth:organism/obo:RO_0002162/up:scientificName 'Rattus norvegicus' ;\n\t\t\trdfs:label 'TP53' ;\n\t\t\tlscr:xrefUniprot ?UNIPROT_XREF .\n",
             "\t\t?PROTEIN orth:organism/obo:RO_0002162/up:scientificName 'Rattus norvegicus' ;\n\t\t\tsio:SIO_010079 ?gene .\n\t\t?tp53Node sio:SIO_010079 ?gene ; # label and UniProt xref are on another protein node of the same gene\n\t\t\trdfs:label 'TP53' ;\n\t\t\tlscr:xrefUniprot ?UNIPROT_XREF .\n"),
            ('\t\t?IS_PARALOGOUS_TO_PROTEIN lscr:xrefUniprot ?PARALOG_UNIPROT_XREF .\n',
             '\t\t?IS_PARALOGOUS_TO_PROTEIN sio:SIO_010079 ?paralogGene .\n\t\t?paralogNode sio:SIO_010079 ?paralogGene ;\n\t\t\tlscr:xrefUniprot ?PARALOG_UNIPROT_XREF .\n'),
        ]),
        ("description", []),
        ("reason", (
            'In the current OMA data the protein node that has the organism and sits in the paralog '
            'clusters (rat TP53: RATNO03077) has no rdfs:label and no lscr:xrefUniprot; those are on '
            'another protein node of the same gene (RATNO03074), which has no organism and no cluster '
            '(probes 2026-11-20). The label and the UniProt cross-references are now read from a protein '
            'node of the same gene (sio:SIO_010079), for the TP53 protein and for its paralogs.'
        )),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/53', OrderedDict([
        ("query", [
            ('WHERE {\n  SERVICE <https://sparql.rhea-db.org/sparql> {',
             'WHERE {\n  { SELECT DISTINCT ?chebi ?reaction ?humanProtein WHERE {\n  SERVICE <https://sparql.rhea-db.org/sparql> {'),
            ('  ?ca up:catalyzedReaction ?reaction .\n\n  SERVICE  <https://sparql.omabrowser.org/sparql/> {',
             '  ?ca up:catalyzedReaction ?reaction .\n  } }\n\n  SERVICE  <https://sparql.omabrowser.org/sparql/> {'),
        ]),
        ("description", []),
        ("reason", (
            'The UniProt endpoint (QLever) only passes values into a SERVICE call when the SERVICE is '
            'joined directly with a result of at most 10,000 rows; otherwise OMA gets the whole block '
            "unbound, which returned 0 rows after 760 s in run 2 (most likely OMA's time limit). The Rhea "
            'and UniProt part is wrapped in a subquery (1,390 rows, 319 human proteins on 2026-11-20), so '
            "it becomes the SERVICE's direct sibling and OMA gets the human proteins as VALUES. The query "
            'asks the same question.'
        )),
    ])),
    ('https://sparql.swisslipids.org/.well-known/sparql-examples/26', OrderedDict([
        ("query", [
            ('(rh:underlyingChebi/rh:chebi)) ?metabolite',
             'rh:underlyingChebi) ?chebi'),
            ('SELECT ?swisslipid  ?organism {',
             'SELECT DISTINCT ?swisslipid  ?organism {'),
            ('             up:organism ?organism .\n',
             '             up:organism ?organism ;\n             up:reviewed true .\n'),
        ]),
        ("description", []),
        ("reason", (
            'Changed the same way in the upstream SIB catalogue (sparql-examples@45e5b53 '
            'examples/SwissLipids/26.ttl): the Rhea part binds ?chebi instead of ?metabolite, so it joins '
            'the SwissLipids part instead of forming a cross product (632,026,411,011 rows, run 6), and '
            'rh:underlyingChebi now points straight to the ChEBI IRI (the old rh:underlyingChebi/rh:chebi '
            'path matched nothing in Rhea on 2026-11-20). With the join fix alone the result would still '
            'be 89,319,800 rows, so the query also returns DISTINCT (lipid, organism) pairs and counts '
            'only reviewed (Swiss-Prot) enzymes (up:reviewed true): 136,755 rows expected (computed from '
            'aggregate queries on 2026-11-20).'
        )),
    ])),
    ('https://sparql.swisslipids.org/.well-known/sparql-examples/27', OrderedDict([
        ("query", [
            ('(rh:underlyingChebi/rh:chebi)) ?metabolite',
             'rh:underlyingChebi) ?chebi'),
            ('SELECT ?swisslipid  ?organism {',
             'SELECT DISTINCT ?swisslipid  ?organism {'),
            ('             up:organism ?organism .\n',
             '             up:organism ?organism ;\n             up:reviewed true .\n'),
        ]),
        ("description", []),
        ("reason", (
            'Changed the same way in the upstream SIB catalogue (sparql-examples@45e5b53 '
            'examples/SwissLipids/27.ttl): the Rhea part binds ?chebi instead of ?metabolite, so it joins '
            'the SwissLipids part instead of forming a cross product (80,644,283,152,110 rows, run 6), '
            'and rh:underlyingChebi now points straight to the ChEBI IRI (the old '
            'rh:underlyingChebi/rh:chebi path matched nothing in Rhea on 2026-11-20). With the join fix '
            'alone the result would still be 10,548,746,085 rows, so the query also returns DISTINCT '
            '(lipid, organism) pairs and counts only reviewed (Swiss-Prot) enzymes (up:reviewed true): '
            '6,400 rows expected (computed from aggregate queries on 2026-11-20).'
        )),
    ])),
    ('https://sparql.swisslipids.org/.well-known/sparql-examples/34', OrderedDict([
        ("query", [
            ('(rh:underlyingChebi/rh:chebi)) ?metabolite',
             'rh:underlyingChebi) ?chebi'),
        ]),
        ("description", []),
        ("reason", (
            'Changed the same way in the upstream SIB catalogue (sparql-examples@45e5b53 '
            'examples/SwissLipids/34.ttl): the Rhea part binds ?chebi instead of ?metabolite, so it joins '
            'the SwissLipids part instead of forming a cross product (1,780,126,200 rows, run 6), and '
            'rh:underlyingChebi now points straight to the ChEBI IRI (the old rh:underlyingChebi/rh:chebi '
            'path matched nothing in Rhea on 2026-11-20). Expected size with this fix: 280,679 rows '
            '(computed from aggregate queries on 2026-11-20).'
        )),
    ])),
    ('https://purl.org/emi#examples018', OrderedDict([
        ("query", [
            ('\t\tSELECT DISTINCT ?wd_chem WHERE { #retrieve metabolite data for sourceWD',
             '\t\tSELECT DISTINCT ?sourceWD ?wd_chem WHERE { #retrieve metabolite data for sourceWD'),
        ]),
        ("description", []),
        ("reason", (
            'The metabolite subquery returned only ?wd_chem, not ?sourceWD, so in the EMI branch of the '
            'UNION the producing organism was not tied to the interactions: every interaction in the '
            "graph (12.9 million) matched, and the query hit the endpoint's 60 s gateway limit (HTTP 504, "
            'run 3). The subquery now also returns ?sourceWD, as its comment intends (probe 2026-11-20: '
            '32 rows in 0.5 s).'
        )),
    ])),
])

# Tried versions of queries, kept verbatim (see QUERY_NOTES).
EMI018_Q021_ASIMICIN = """PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX emi: <https://w3id.org/emi#>
PREFIX sosa: <http://www.w3.org/ns/sosa/>
SELECT DISTINCT ?sourceWD ?sourceName ?intxnName ?intxnId ?targetWD ?targetName WHERE {
	# interaction data
	{
        	?intxn emi:hasSource ?xOrg ; # retrieve interaction-pairs
                	emi:hasTarget ?yOrg ;
                        emi:isClassifiedWith ?intxnId .
                ?intxnId rdfs:label ?intxnName .
                ?xOrg emi:inTaxon ?sourceWD ;
			rdfs:label ?sourceName .
		?yOrg emi:inTaxon ?targetWD ;
			rdfs:label ?targetName .
        }

	# metabolite data
	FILTER (?xWD = ?sourceWD || ?xWD = ?targetWD)	# ensure that xWD can be both source/target from the interaction-pairs retrieved before
        {
		SELECT DISTINCT ?xWD ?wd_chem WHERE {
                	?material sosa:hasSample ?extract ;
                        	sosa:isSampleOf ?organe .
                        ?organe emi:inTaxon ?xWD .
                        ?extract sosa:isFeatureOfInterestOf ?lcms .
                        ?lcms sosa:hasResult ?feature_list .
                        ?feature_list emi:hasLCMSFeature ?feature .
                        ?feature emi:hasAnnotation ?sirius_annotation .
                        ?sirius_annotation a emi:StructuralAnnotation ;
                        	emi:hasChemicalStructure ?ik2d .
                        ?ik2d emi:hasSMILES ?smiles ;
                        	emi:isInChIKey2DOf ?ik .
                        ?ik emi:isInChIKeyOf ?wd_chem .
                        VALUES ?wd_chem { wd:Q100138042 }
                }
	}  UNION # union with data from wikidata (lotus)
	{
		SERVICE <https://qlever.dev/api/wikidata> {
			?wd_chem wdt:P235 ?ik ;
   				wdt:P703 ?xWD .
			VALUES ?wd_chem { wd:Q100138042 }
		}
	}
}"""

EMI020_RESTRUCTURED = """PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX emi: <https://w3id.org/emi#>
PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX wgs: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?sourceWD ?sourceName ?intxnName ?targetWD ?targetName ?loc WHERE {
	# interactions whose target produces the compound (computed first, small)
	{ SELECT ?intxn ?xOrg ?yOrg ?targetWD WHERE {
	# metabolite data
        {
		SELECT DISTINCT ?targetWD ?wd_chem WHERE {
			?material sosa:hasSample ?extract ;
                        	sosa:isSampleOf ?organe .
                	?organe emi:inTaxon ?targetWD .	# filter by target wikidata-id
                	?extract sosa:isFeatureOfInterestOf ?lcms .
                	?lcms sosa:hasResult ?feature_list .
                	?feature_list emi:hasLCMSFeature ?feature .
                	?feature emi:hasAnnotation ?sirius_annotation .
                	?sirius_annotation a emi:StructuralAnnotation ;
	                	emi:hasChemicalStructure ?ik2d .
                	?ik2d emi:hasSMILES ?smiles ;
                      		emi:isInChIKey2DOf ?ik .
                	?ik emi:isInChIKeyOf ?wd_chem .
                	VALUES ?wd_chem { wd:Q27107580 } # filter on the wikidata-id of Onpordopicrin
		}
	} UNION	# union with the lotus metabolite data from wikidata
	{
		SERVICE <https://qlever.dev/api/wikidata> {
			?wd_chem wdt:P235 ?ik ;
   				wdt:P703 ?targetWD .
		 	VALUES ?wd_chem { wd:Q27107580 }
		}
	}
		?yOrg emi:inTaxon ?targetWD .
		?intxn emi:hasTarget ?yOrg ;
			emi:hasSource ?xOrg .
	} }
	# then their details
	?intxn emi:isClassifiedWith ?intxnName ;
		prov:atLocation ?loc ;
		wgs:lat ?lats ;
		wgs:long ?longs .
	?xOrg emi:inTaxon ?sourceWD ;
		rdfs:label ?sourceName ;
		sosa:isSampleOf ?sourceSample .
	?yOrg rdfs:label ?targetName ;
		sosa:isSampleOf ?targetSample .
}"""

ORTHODB19_SUBQUERY = """PREFIX up: <http://purl.uniprot.org/core/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX : <http://purl.orthodb.org/>
PREFIX rh: <http://rdf.rhea-db.org/>
PREFIX interpro: <http://www.ebi.ac.uk/interpro/entry/>

SELECT ?og ?equation
    (group_concat (distinct ?clade_name; SEPARATOR="; ") as ?clades)
    (count(distinct ?taxon) as ?cnt_taxons)
    (count(distinct ?gene) as ?cnt_genes)
    (group_concat (distinct ?org_name; SEPARATOR="; ") as ?taxons)
WHERE {
  { SELECT ?og ?gene ?xref ?taxon ?org_name ?clade_name WHERE {
    ?taxon a :Species; up:scientificName ?org_name ;
        rdfs:subClassOf+/up:scientificName ?clade_name.
    ?gene a :Gene ;
        up:organism/a ?taxon.
    ?gene rdfs:seeAlso ?xref.
    ?xref a :Uniprot.
    ?gene :memberOf ?og.
    ?og :xref/:xrefResource interpro:IPR002328 ;
        :ogBuiltAt/up:scientificName "Eukaryota".
    # restrict taxons to several clades
    VALUES ?clade_name { "Stramenopiles" "Alveolata" "Liliopsida" "Coleoptera" }
  } }

    # join via uniprot ?xref
    SERVICE <https://sparql.uniprot.org/sparql/> {
        ?xref a up:Protein ;
            up:annotation/up:catalyticActivity/up:catalyzedReaction ?rh .
    }

    # join via reaction ?rh
    SERVICE <https://sparql.rhea-db.org/sparql> {
        ?rh rdfs:subClassOf rh:Reaction ;
            rh:equation ?equation.
    }

} GROUP BY ?equation ?og ORDER BY ?og ?equation"""


# What is known about single queries beyond the automated checks, keyed by query.
# Collected from the hand-check list of 2026-11-19/20, which this replaces.
#   upstreamFile, upstream: how the query compares with sparql-examples@45e5b53
#   failure:  why the query fails today (short; used in the expected results)
#   open:     what could still be tried
#   checks:   results outside the `verify` runs. method: by-hand (run by the user and
#             recorded on 2026-11-20), probe (one HTTP request from the update session)
#             or computed (count from aggregate queries; not observed). version:
#             catalogue (queryHash pins the exact text), upstream, or variant.
# `build` copies these into each entry; a check is current only while its queryHash
# matches the entry's query.
QUERY_NOTES = OrderedDict([
    ('https://sparql.swisslipids.org/.well-known/sparql-examples/7', OrderedDict([
        ('upstreamFile', 'examples/SwissLipids/7.ttl'),
        ('upstream', (
            'Upstream fixed a typo: `SWISSLIPID:Isomeric_Subspecie` → `SWISSLIPID:Isomeric_Subspecies`;'
            ' its SERVICE URL has no trailing slash.'
        )),
        ('failure', (
            'The catalogue text returns 0 rows because of the class-name typo. The corrected text fails'
            " on SwissLipids' own engine: by hand its stream broke ('Premature end of chunk'), and in "
            'run 11 SwissLipids answered HTTP 400 with an empty message after 682 s.'
        )),
        ('open', (
            'Fix the typo (`QUERY_FIXES`, as upstream) once SwissLipids answers the corrected query, or'
            ' check it with an engine that does the federation itself.'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'variant'),
                ('versionNote', (
                    'the catalogue query with the class name corrected to `SWISSLIPID:Isomeric_Subspecies`, as '
                    'upstream (the UniProt SERVICE URL keeps its trailing slash)'
                )),
                ('upstreamFile', 'examples/SwissLipids/7.ttl'),
                ('outcome', 'error'),
                ('result', (
                    'error -- org.eclipse.rdf4j.query.QueryEvaluationException: '
                    'org.apache.http.ConnectionClosedException: Premature end of chunk coded message body: '
                    'closing chunk expected'
                )),
            ]),
            OrderedDict([
                ('date', '2026-11-20T12:44:54+00:00'),
                ('method', 'http-check'),
                ('run', 11),
                ('version', 'variant'),
                ('versionNote', (
                    'the catalogue query with the class name corrected to `SWISSLIPID:Isomeric_Subspecies`, as '
                    'upstream (the UniProt SERVICE URL keeps its trailing slash)'
                )),
                ('upstreamFile', 'examples/SwissLipids/7.ttl'),
                ('queryHash', '6b23f8ee5cf1cbaf'),
                ('outcome', 'error'),
                ('httpStatus', 400),
                ('durationSeconds', 682.4),
                ('error', 'HTTP 400 with an empty message'),
                ('note', (
                    'The typo fix was in QUERY_FIXES for this run only; it was taken out again because the '
                    'query failed.'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/67_draft_human_metabolome', OrderedDict([
        ('upstreamFile', 'examples/UniProt/67_draft_human_metabolome.ttl'),
        ('upstream', (
            'ChEBI predicates changed (`chebislash:smiles`/`inchikey` → '
            '`chemrof:smiles_string`/`chemrof:inchi_key_string`) and `rh:underlyingChebi/rh:chebi` → '
            '`rh:underlyingChebi`. The catalogue now uses this upstream version '
            '(`UPSTREAM_REPLACEMENTS`).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', 'd978080128b6a20d'),
                ('outcome', 'ok'),
                ('resultCount', 317738),
                ('result', '317,738'),
                ('note', (
                    'Run on the upstream version before it replaced the catalogue text (same query text); '
                    'counted with count_results.py from the downloaded .srj file.'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.rhea-db.org/.well-known/sparql-examples/19_draft_human_metabolome', OrderedDict([
        ('upstreamFile', 'examples/Rhea/19_draft_human_metabolome.ttl'),
        ('upstream', (
            'Same ChEBI changes as UniProt 67 (upstream uses `chemrof:inchikey_string` here but '
            '`chemrof:inchi_key_string` in UniProt 67, so one may be a typo); SERVICE URL without '
            'trailing slash.'
        )),
        ('open', (
            'If not tried yet: the upstream version with `chemrof:inchi_key_string`, the spelling '
            'UniProt 67 uses.'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'upstream'),
                ('upstreamFile', 'examples/Rhea/19_draft_human_metabolome.ttl'),
                ('outcome', 'empty'),
                ('resultCount', 0),
                ('result', 'empty result set'),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/36', OrderedDict([
        ('upstreamFile', 'examples/UniProt/36_orthologous_proteins_via_orthodb.ttl'),
        ('upstream', (
            "Reworked: the OrthoDB group is no longer read from UniProt's `rdfs:seeAlso` "
            'cross-reference (upstream says these can be out of date) but matched inside the OrthoDB '
            'SERVICE. The catalogue now uses this upstream version (`UPSTREAM_REPLACEMENTS`).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', '454a15d6c999601a'),
                ('outcome', 'ok'),
                ('resultCount', 1100),
                ('result', '1100'),
                ('note', 'Run on the upstream version before it replaced the catalogue text (same query text).'),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/117_biosodafrontend_glioblastoma_orthologs_rat', OrderedDict([
        ('upstreamFile', 'examples/UniProt/117_biosodafrontend_glioblastoma_orthologs_rat.ttl'),
        ('upstream', (
            'Restructured: the UniProt disease filter runs in a subquery before the OMA and Bgee '
            'SERVICE calls (OMA and Bgee parts unchanged apart from layout).'
        )),
        ('failure', (
            "OMA rejects UniProt's SERVICE call (HTTP 400, Virtuoso: 'Too many arguments for standard "
            "built-in function'). The upstream version fails on Bgee instead (HTTP 403 to UniProt's "
            'SERVICE call).'
        )),
        ('open', (
            'Run the Bgee part directly on Bgee, or ask the Bgee or UniProt maintainers about the '
            'block. The upstream SERVICE URL with the trailing slash (`https://www.bgee.org/sparql/`) '
            'is blocked too (see UniProt 117 and 118).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'upstream'),
                ('upstreamFile', 'examples/UniProt/117_biosodafrontend_glioblastoma_orthologs_rat.ttl'),
                ('outcome', 'error'),
                ('result', (
                    'error - Error while executing a SERVICE request to <https://www.bgee.org:443/sparql/>: '
                    'SERVICE responded with HTTP status code: 403, Forbidden'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/118_biosodafrontend_rat_brain_human_cancer', OrderedDict([
        ('upstreamFile', 'examples/UniProt/118_biosodafrontend_rat_brain_human_cancer.ttl'),
        ('upstream', (
            'Restructured like UniProt 117 (UniProt part in a subquery) and selects only `?gene '
            '?orthologous_protein2`.'
        )),
        ('failure', (
            "OMA rejects UniProt's SERVICE call (HTTP 400, Virtuoso: 'Too many arguments for standard "
            "built-in function'). The upstream version fails on Bgee instead (HTTP 403 to UniProt's "
            'SERVICE call).'
        )),
        ('open', (
            'Run the Bgee part directly on Bgee, or ask the Bgee or UniProt maintainers about the '
            'block. The upstream SERVICE URL with the trailing slash (`https://www.bgee.org/sparql/`) '
            'is blocked too (see UniProt 117 and 118).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'upstream'),
                ('upstreamFile', 'examples/UniProt/118_biosodafrontend_rat_brain_human_cancer.ttl'),
                ('outcome', 'error'),
                ('result', (
                    'error - Error while executing a SERVICE request to <https://www.bgee.org:443/sparql/>: '
                    'SERVICE responded with HTTP status code: 403, Forbidden'
                )),
            ]),
        ]),
    ])),
    ('https://www.bgee.org/sparql/.well-known/sparql-examples/028-biosodafrontend', OrderedDict([
        ('upstreamFile', 'examples/Bgee/028-biosodafrontend.ttl'),
        ('upstream', (
            'Reworked: `LIMIT 10` on the Bgee genes, `up:transcribedFrom` without `rdfs:seeAlso`, and '
            'no `up:Disease_Annotation` filter any more (it returns all annotation IRIs, not disease '
            'annotation texts).'
        )),
        ('failure', (
            'HTTP 524: Cloudflare in front of Bgee ends requests after ~125 s (also for the upstream '
            'version with `LIMIT 10`).'
        )),
        ('open', 'Run it where the ~125 s limit does not apply, or ask the Bgee maintainers.'),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'upstream'),
                ('upstreamFile', 'examples/Bgee/028-biosodafrontend.ttl'),
                ('outcome', 'error'),
                ('result', 'error - HTTP524'),
            ]),
        ]),
    ])),
    ('https://www.bgee.org/sparql/.well-known/sparql-examples/018', OrderedDict([
        ('upstreamFile', 'examples/Bgee/018.ttl'),
        ('upstream', "Only the SERVICE URL's trailing slash differs."),
        ('failure', 'HTTP 524: Cloudflare in front of Bgee ends requests after ~125 s.'),
        ('open', 'Run it where the ~125 s limit does not apply, or ask the Bgee maintainers.'),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', 'a02e4ecc2a57054d'),
                ('outcome', 'error'),
                ('result', 'error - HTTP 524'),
            ]),
        ]),
    ])),
    ('https://www.bgee.org/sparql/.well-known/sparql-examples/019', OrderedDict([
        ('upstreamFile', 'examples/Bgee/019.ttl'),
        ('upstream', 'Unchanged.'),
        ('failure', 'HTTP 524: Cloudflare in front of Bgee ends requests after ~125 s.'),
        ('open', 'Run it where the ~125 s limit does not apply, or ask the Bgee maintainers.'),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', 'efd2b4024cd402ef'),
                ('outcome', 'error'),
                ('result', 'error - HTTP 524'),
            ]),
        ]),
    ])),
    ('https://biosoda.unil.ch/emi/sparql/.well-known/sparql-examples/006', OrderedDict([
        ('upstreamFile', 'examples/emi/006.ttl'),
        ('upstream', 'Identical apart from the `emi:` PREFIX (already updated).'),
        ('failure', 'HTTP 504: the EMI gateway (nginx) ends requests after 60 s.'),
        ('open', 'Run it where the 60 s limit does not apply, or ask the EarthMetabolome maintainers.'),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', '8fc2ba5e9f1a1c8b'),
                ('outcome', 'error'),
                ('result', 'error - HTTP 504 Gateway Timeout'),
            ]),
        ]),
    ])),
    ('https://purl.org/emi#examples018', OrderedDict([
        ('upstreamFile', 'examples/emi/021.ttl'),
        ('upstream', (
            'Reworked and renumbered upstream. The closest upstream queries are examples/emi/021.ttl '
            '(the same question, for norhyoscyamine, `wd:Q27107545`) and examples/emi/020.ttl '
            '(producers of onopordopicrin, `wd:Q27107580`, and where their interactions were seen).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'variant'),
                ('versionNote', (
                    'upstream examples/emi/021.ttl with asimicin (`wd:Q100138042`, as in the catalogue) instead'
                    ' of norhyoscyamine'
                )),
                ('upstreamFile', 'examples/emi/021.ttl'),
                ('query', EMI018_Q021_ASIMICIN),
                ('outcome', 'error'),
                ('result', 'error - HTTP 504 Gateway Timeout'),
                ('note', 'One result was written for this version and upstream 020 (next check); both timed out.'),
            ]),
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'upstream'),
                ('versionNote', 'as is (a related question)'),
                ('upstreamFile', 'examples/emi/020.ttl'),
                ('outcome', 'error'),
                ('result', 'error - HTTP 504 Gateway Timeout'),
                ('note', 'One result was written for this version and the 021 version above; both timed out.'),
            ]),
            OrderedDict([
                ('date', '2026-11-20T08:57:26+00:00'),
                ('method', 'probe'),
                ('version', 'variant'),
                ('versionNote', 'upstream examples/emi/020.ttl, restructured'),
                ('upstreamFile', 'examples/emi/020.ttl'),
                ('endpoint', 'https://qlever.earthmetabolome.org/api/metrin-kg'),
                ('query', EMI020_RESTRUCTURED),
                ('outcome', 'ok'),
                ('resultCount', 126),
                ('durationSeconds', 2.0),
                ('note', (
                    'The interactions whose target produces the compound are found first, in a subquery, and '
                    'their location and names are added after. As is, 020 joins its 24 producer taxa with all '
                    '8.4 million located interactions and hits the 60 s gateway limit.'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.orthodb.org/.well-known/sparql-examples/19', OrderedDict([
        ('upstreamFile', 'examples/OrthoDB/19.ttl'),
        ('upstream', "Only the SERVICE URL's trailing slash differs."),
        ('failure', (
            "OrthoDB's Virtuoso sends one SERVICE request per binding, in parallel; Rhea answers 503 "
            'Too many concurrent queries. The automated check got no answer within 900 s.'
        )),
        ('open', (
            'Not tried: drop the Rhea equation lookup (return the Rhea reaction IRI), or run the query '
            'from UniProt (QLever) with OrthoDB as a SERVICE, which needs about 2 remote calls but '
            'changes the target.'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', '25f02f2ca4a16ec4'),
                ('outcome', 'error'),
                ('result', (
                    'error - HTTP 503 (bad virtuoso SERVICE handling by sending burst of requests that results '
                    'in HTTP 403)'
                )),
            ]),
            OrderedDict([
                ('date', '2026-11-20T08:57:48+00:00'),
                ('method', 'probe'),
                ('version', 'variant'),
                ('versionNote', 'this query with the OrthoDB part (with the clade filter) in a subquery'),
                ('endpoint', 'https://sparql.orthodb.org/sparql/'),
                ('query', ORTHODB19_SUBQUERY),
                ('outcome', 'error'),
                ('durationSeconds', 64.5),
                ('error', (
                    "HTTP 500: Virtuoso RDFZZ Error DB.DBA.SPARQL_REXEC('https://sparql.rhea-db.org/sparql', "
                    "...) returned Content-Type 'text/html' status 'HTTP/1.1 503' Rhea: Too many concurrent "
                    'queries. Please try again later.'
                )),
                ('note', (
                    'Only the ~1,844 UniProt cross-references in the 4 clades are federated, instead of the '
                    '76,771 genes of the 119 groups. It got through the UniProt SERVICE in about a minute; Rhea'
                    ' then refused the parallel calls. Not retried.'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/49', OrderedDict([
        ('upstreamFile', 'examples/UniProt/49_tissues_where_genes_metabolizing_cholestrol_are_expressed.ttl'),
        ('upstream', 'The SERVICE URL is `https://www.bgee.org/sparql/` (with trailing slash) upstream.'),
        ('failure', (
            "Bgee's Cloudflare bot check answers UniProt's SERVICE call with HTTP 403 (a 'Just a "
            "moment...' page), with or without the trailing slash."
        )),
        ('open', (
            'Run the Bgee part directly on Bgee, or ask the Bgee or UniProt maintainers about the '
            'block. The upstream SERVICE URL with the trailing slash (`https://www.bgee.org/sparql/`) '
            'is blocked too (see UniProt 117 and 118).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', '2ab5e40b819fe1ea'),
                ('outcome', 'error'),
                ('result', 'error - Bgee issued a HTTP 403 error'),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/50', OrderedDict([
        ('upstreamFile', 'examples/UniProt/50_tissues_where_genes_sphingosines_are_expressed.ttl'),
        ('upstream', 'The SERVICE URL is `https://www.bgee.org/sparql/` (with trailing slash) upstream.'),
        ('failure', (
            "Bgee's Cloudflare bot check answers UniProt's SERVICE call with HTTP 403 (a 'Just a "
            "moment...' page), with or without the trailing slash."
        )),
        ('open', (
            'Run the Bgee part directly on Bgee, or ask the Bgee or UniProt maintainers about the '
            'block. The upstream SERVICE URL with the trailing slash (`https://www.bgee.org/sparql/`) '
            'is blocked too (see UniProt 117 and 118).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', 'abcd81b98b76a58f'),
                ('outcome', 'error'),
                ('result', 'error - Bgee issued a HTTP 403 error'),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/109_uniprot_transporter_in_liver', OrderedDict([
        ('upstreamFile', 'examples/UniProt/109_uniprot_transporter_in_liver.ttl'),
        ('upstream', 'The SERVICE URL is `https://www.bgee.org/sparql/` (with trailing slash) upstream.'),
        ('failure', (
            "Bgee's Cloudflare bot check answers UniProt's SERVICE call with HTTP 403 (a 'Just a "
            "moment...' page), with or without the trailing slash."
        )),
        ('open', (
            'Run the Bgee part directly on Bgee, or ask the Bgee or UniProt maintainers about the '
            'block. The upstream SERVICE URL with the trailing slash (`https://www.bgee.org/sparql/`) '
            'is blocked too (see UniProt 117 and 118).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', 'f93c95011b493047'),
                ('outcome', 'error'),
                ('result', 'error - Bgee issued a HTTP 403 error'),
            ]),
        ]),
    ])),
    ('https://purl.org/emi#examples012', OrderedDict([
        ('upstreamFile', 'examples/emi/013.ttl'),
        ('upstream', 'Changed in the upstream SIB catalogue; already replaced with the upstream version.'),
        ('failure', (
            "QLever's Wikidata endpoint (qlever.dev) answers the EMI server's SERVICE call with 429 Too"
            ' Many Requests.'
        )),
        ('open', 'Try again later: the 429 is a rate limit.'),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', 'e7d88b39ff0d01b3'),
                ('outcome', 'error'),
                ('result', 'error - wikidata responds with HTTP 429'),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/90_uniprot_affected_by_metabolic_diseases_using_MeSH', OrderedDict([
        ('upstreamFile', 'examples/UniProt/90_uniprot_affected_by_metabolic_diseases_using_MeSH.ttl'),
        ('upstream', 'Unchanged.'),
        ('failure', (
            'The MeSH endpoint (`https://id.nlm.nih.gov/mesh/sparql`) returns its service description '
            'as text/plain instead of SPARQL results, which UniProt (QLever) cannot use.'
        )),
        ('open', 'Check whether MeSH still offers SPARQL results at that URL.'),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', '8950b09ad6e7285b'),
                ('outcome', 'error'),
                ('result', (
                    "error - MeSH error; endpoint sent 'text/plain;charset=utf-8' not "
                    "'application/sparql-results+json'"
                )),
            ]),
        ]),
    ])),
    ('https://sparql.orthodb.org/.well-known/sparql-examples/20', OrderedDict([
        ('upstreamFile', 'examples/OrthoDB/20.ttl'),
        ('upstream', 'Unchanged.'),
        ('failure', (
            "STRING (`https://sparql.string-db.org/sparql`) answers OrthoDB's SERVICE call with 403 "
            'Forbidden (an HTML page).'
        )),
        ('open', "Check whether STRING's endpoint still accepts SPARQL requests."),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', '324b0fba6393b421'),
                ('outcome', 'error'),
                ('result', 'error - HTTP 403 from STRING db'),
            ]),
        ]),
    ])),
    ('https://sparql.swisslipids.org/.well-known/sparql-examples/26', OrderedDict([
        ('upstreamFile', 'examples/SwissLipids/26.ttl'),
        ('upstream', (
            'The Rhea part binds `?chebi` instead of `?metabolite`, so it joins the SwissLipids part '
            '(the original is a cross product); also `rh:underlyingChebi/rh:chebi` → '
            '`rh:underlyingChebi` and the SERVICE URL without trailing slash. The catalogue now has '
            'this join fix plus DISTINCT and reviewed enzymes only.'
        )),
        ('failure', (
            "SwissLipids' RDF4J engine breaks the result stream part way: the response ends inside the "
            'bindings with invalid JSON (run 8 after 673.9 s; run 10 after 10,720 rows and 356 s), the '
            "same failure as 'Premature end of chunk' for SwissLipids 7 by hand."
        )),
        ('open', (
            'Run it again later, or where the stream does not break. The computed count is its expected'
            ' result: the same computation gave exactly the observed counts of 27 and 34 (run 10).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'computed'),
                ('version', 'catalogue'),
                ('queryHash', '89e373cae51ffd2e'),
                ('outcome', 'ok'),
                ('resultCount', 136755),
                ('note', (
                    'Computed on 2026-11-20 from small aggregate queries, each sent to one endpoint: '
                    'SwissLipids (lipid, ChEBI) pairs, Rhea rows per (reaction, ChEBI) with the fixed ChEBI '
                    'path, and UniProt rows per reaction and (reaction, organism) pairs, reviewed enzymes only.'
                    ' The parts were joined locally. Run 8 broke off before the end, so the count was computed '
                    'instead.'
                )),
            ]),
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'computed'),
                ('version', 'upstream'),
                ('versionNote', 'the join fix only, as upstream (no DISTINCT, all enzymes)'),
                ('upstreamFile', 'examples/SwissLipids/26.ttl'),
                ('outcome', 'ok'),
                ('resultCount', 89319800),
                ('note', (
                    'Computed the same way; too large to download, so the catalogue adds DISTINCT and reviewed '
                    'enzymes only.'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.swisslipids.org/.well-known/sparql-examples/27', OrderedDict([
        ('upstreamFile', 'examples/SwissLipids/27.ttl'),
        ('upstream', (
            'Same join fix as 26 (`?metabolite` → `?chebi`, `rh:underlyingChebi`), SERVICE URL without '
            'trailing slash. The catalogue now has this join fix plus DISTINCT and reviewed enzymes '
            'only.'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'computed'),
                ('version', 'catalogue'),
                ('queryHash', '40bc373c6139da1d'),
                ('outcome', 'ok'),
                ('resultCount', 6400),
                ('note', (
                    'Computed on 2026-11-20 from small aggregate queries, each sent to one endpoint: '
                    'SwissLipids (lipid, ChEBI) pairs, Rhea rows per (reaction, ChEBI) with the fixed ChEBI '
                    'path, and UniProt rows per reaction and (reaction, organism) pairs, reviewed enzymes only,'
                    ' plus isomeric subspecies per lipid. The parts were joined locally. Run 8 broke off before'
                    ' the end, so the count was computed instead. Run 10 then returned exactly this count.'
                )),
            ]),
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'computed'),
                ('version', 'upstream'),
                ('versionNote', 'the join fix only, as upstream (no DISTINCT, all enzymes)'),
                ('upstreamFile', 'examples/SwissLipids/27.ttl'),
                ('outcome', 'ok'),
                ('resultCount', 10548746085),
                ('note', (
                    'Computed the same way; too large to download, so the catalogue adds DISTINCT and reviewed '
                    'enzymes only.'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.swisslipids.org/.well-known/sparql-examples/34', OrderedDict([
        ('upstreamFile', 'examples/SwissLipids/34.ttl'),
        ('upstream', (
            'Same join fix as 26 (`?metabolite` → `?chebi`, `rh:underlyingChebi`), SERVICE URL without '
            'trailing slash. The catalogue now has this join fix only (same as upstream).'
        )),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'computed'),
                ('version', 'catalogue'),
                ('queryHash', 'a68360bcb58003ca'),
                ('outcome', 'ok'),
                ('resultCount', 280679),
                ('note', (
                    'Computed on 2026-11-20 from small aggregate queries, each sent to one endpoint: '
                    'SwissLipids (lipid, ChEBI) pairs, Rhea rows per (reaction, ChEBI) with the fixed ChEBI '
                    'path, and UniProt rows per reaction for human enzymes. The parts were joined locally. Run '
                    '8 broke off before the end, so the count was computed instead. Run 10 then returned '
                    'exactly this count.'
                )),
            ]),
        ]),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/48', OrderedDict([
        ('upstreamFile', 'examples/UniProt/48_glycosylation_sites_and_glycans.ttl'),
        ('upstream', 'Unchanged.'),
        ('checks', []),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/53', OrderedDict([
        ('upstreamFile', 'examples/UniProt/53_mouse_homologs_of_sterol_enzymes_via_omabrowser.ttl'),
        ('upstream', 'Unchanged.'),
        ('checks', []),
    ])),
    ('https://sparql.orthodb.org/.well-known/sparql-examples/17', OrderedDict([
        ('upstreamFile', 'examples/OrthoDB/17.ttl'),
        ('upstream', "Only the SERVICE URL's trailing slash differs."),
        ('checks', []),
    ])),
    ('https://sparql.omabrowser.org/.well-known/sparql-examples/15-rat-TP53-biosodafrontend', OrderedDict([
        ('upstreamFile', 'examples/OMA/15-biosodafrontend-rat-TP53.ttl'),
        ('upstream', "Only the SERVICE URL's trailing slash differs."),
        ('checks', []),
    ])),
    ('https://sparql.uniprot.org/.well-known/sparql-examples/99_uniprot_identifiers_org_translation', OrderedDict([
        ('upstreamFile', 'examples/UniProt/99_uniprot_identifiers_org_translation.ttl'),
        ('upstream', 'Unchanged.'),
        ('failure', (
            "identifiers.org's SPARQL endpoint (`https://sparql.api.identifiers.org`) answers UniProt's"
            ' SERVICE call with HTTP 500.'
        )),
        ('open', 'Check whether identifiers.org still runs its SPARQL endpoint.'),
        ('checks', [
            OrderedDict([
                ('date', '2026-11-20'),
                ('method', 'by-hand'),
                ('version', 'catalogue'),
                ('queryHash', 'a8797b69e07fcf6c'),
                ('outcome', 'error'),
                ('result', 'error - https://sparql.api.identifiers.org issued HTTP 500'),
            ]),
        ]),
    ])),
])

# Queries left out of the updated catalogue (they stay in the original file).
# Queries that use an endpoint in UNAVAILABLE_ENDPOINTS are left out as well.
REMOVED_QUERIES = OrderedDict([
    ("https://purl.org/emi#examples019b", (
        "Removed because it was dropped from the upstream SIB catalogue: at sparql-examples@45e5b53 "
        "examples/emi/ has a single 019 example, the new version of 019a. This version also has a typo "
        "(emi:hasTarget ?parasitoidX where ?parasiteX is meant) and returned 0 rows on the new "
        "EMI endpoint (run 3, 2026-11-19)."
    )),
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
     "011-023 (e.g. names are read with rdfs:label); that rework was only applied to the two "
     "queries replaced with their upstream version (see upstreamReplacements). "
     "Its gateway (nginx) ends requests that run longer than 60 s with HTTP 504 (seen on 2026-11-19)."),
])

# Hints added to a query's manualFixNotes when its check did not succeed. Keyed by
# target endpoint ("*" = any); each hint is used when its regex matches
# "<outcome> <error message>".
FAILURE_HINTS = OrderedDict([
    ("https://qlever.earthmetabolome.org/api/metrin-kg", [
        (r"HTTP 504", "Hint: HTTP 504 after 60 s comes from the endpoint's gateway, so run the "
                      "query by hand or ask the maintainers."),
        (r"^empty", "Hint: the PREFIX namespaces are already updated, but upstream "
                    "sparql-examples@45e5b53 reworked and renumbered the EMI examples "
                    "(examples/emi/); compare with the closest upstream query."),
    ]),
    ("https://www.bgee.org/sparql", [
        (r"HTTP 524", "Hint: HTTP 524 means Cloudflare ended the request after ~125 s; the query "
                      "itself may still be valid."),
    ]),
    ("https://www.bgee.org/sparql/", [
        (r"HTTP 524", "Hint: HTTP 524 means Cloudflare ended the request after ~125 s; the query "
                      "itself may still be valid."),
    ]),
    ("*", [
        (r"\b429\b", "Hint: an endpoint answered 429 Too Many Requests (rate limit); try again "
                     "later by hand."),
    ]),
])

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


def upstream_url(path):
    return f"{UPSTREAM_REPO}/blob/{UPSTREAM_COMMIT_CHECKED[:7]}/{path}"


def add_query_notes(key, entry):
    """Copy QUERY_NOTES into the entry. A check is marked currentQuery only while its
    queryHash matches the entry's query, so a later change to the query retires it."""
    spec = QUERY_NOTES.get(key)
    if not spec:
        return
    if spec.get("upstream"):
        entry["upstreamComparison"] = OrderedDict([
            ("file", spec.get("upstreamFile")),
            ("url", upstream_url(spec["upstreamFile"]) if spec.get("upstreamFile") else None),
            ("note", spec["upstream"]),
        ])
    if spec.get("failure"):
        entry["failureSummary"] = spec["failure"]
    if spec.get("open"):
        entry["openOptions"] = spec["open"]
    h = query_hash(entry)
    checks = []
    for c in spec.get("checks") or []:
        c = OrderedDict(c)
        c["currentQuery"] = c.get("queryHash") == h
        checks.append(c)
    if checks:
        entry["otherChecks"] = checks


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
    src_commit = (original.get("metadata") or {}).get("commit", "?")
    out_data = OrderedDict()
    removed = []
    counts = defaultdict(int)

    for key, entry in data.items():
        new = OrderedDict()
        changes = []

        # 0. queries replaced with their current upstream version
        query, description = entry["query"], entry["description"]
        rep = UPSTREAM_REPLACEMENTS.get(key)
        if rep:
            query, description = rep["query"], rep["description"]
            changes.append(OrderedDict([
                ("field", "queryAndDescription"),
                ("from", f"{short_id(key)} as in the original catalogue (sparql-examples@{src_commit})"),
                ("to", f"{rep['upstreamFile']} at sparql-examples@{UPSTREAM_COMMIT_CHECKED[:7]}"),
                ("upstreamId", rep["upstreamId"]),
                ("reason", rep["reason"]),
            ]))

        # 1. query text: only rewrite IRIs that name a moved endpoint, e.g.
        #    SERVICE <old> -> SERVICE <new>. Data IRIs and PREFIXes are untouched.
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

        # 4b. small fixes for queries that returned no results (QUERY_FIXES)
        fix = QUERY_FIXES.get(key)
        if fix:
            for old, new_text in fix["query"]:
                if query.count(old) != 1:
                    sys.exit(f"QUERY_FIXES for {key}: expected the text once in the query: {old!r}")
                query = query.replace(old, new_text)
            for old, new_text in fix["description"]:
                if description.count(old) != 1:
                    sys.exit(f"QUERY_FIXES for {key}: expected the text once in the description: {old!r}")
                description = description.replace(old, new_text)
            changes.append(OrderedDict([
                ("field", "queryFix"),
                ("replacements", [OrderedDict([("from", o), ("to", n)]) for o, n in fix["query"]]),
                ("descriptionReplacements", [OrderedDict([("from", o), ("to", n)]) for o, n in fix["description"]]),
                ("reason", fix["reason"]),
            ]))

        # 5. leave out queries listed in REMOVED_QUERIES or using an unavailable endpoint
        dead = sorted(ep for ep in set(all_endpoints(OrderedDict(query=query, target=target, federatesWith=fed)))
                      if ep in UNAVAILABLE_ENDPOINTS)
        if key in REMOVED_QUERIES or dead:
            reason = REMOVED_QUERIES.get(key) or (
                "Removed because it uses a dead endpoint: "
                + "; ".join(f"{ep} ({UNAVAILABLE_ENDPOINTS[ep]})" for ep in dead))
            removed.append(OrderedDict([("key", key), ("target", target), ("reason", reason)]
                                       + ([("unavailableEndpoints", dead)] if dead else [])))
            continue

        new["query"] = query
        new["description"] = description
        new["federatesWith"] = fed
        new["target"] = target
        endpoint_changed = any(c["field"] not in ("queryPrefix", "queryAndDescription", "queryFix")
                               for c in changes)
        if rep:
            new["changeStatus"] = "replaced-with-upstream"
        elif fix:
            new["changeStatus"] = "query-fixed"
        else:
            new["changeStatus"] = {(False, False): "unchanged", (True, False): "endpoint-updated",
                                   (False, True): "prefix-updated",
                                   (True, True): "endpoint-and-prefix-updated"}[
                                       (endpoint_changed, prefix_changed)]
        new["changes"] = changes
        new["status"] = "unverified"
        new["manualFixNotes"] = []
        new["verification"] = None
        add_query_notes(key, new)
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
        ("note", "Endpoint URLs and changed PREFIX namespaces were taken from upstream. Query "
                 "logic was not synced, except for the queries in upstreamReplacements, which "
                 "changed in the upstream SIB catalogue and are replaced with their upstream "
                 "version. No newer upstream queries were added."),
    ])
    meta["policy"] = ("Minimal fixes only: endpoint URLs in target, federatesWith and SERVICE "
                      "clauses, and PREFIX namespaces that changed together with an endpoint's "
                      "data (see prefixMap). Queries that no longer work and were reworked "
                      "upstream are replaced with the upstream version (see upstreamReplacements); "
                      "a few queries that returned no results get a small documented fix (see "
                      "queryFixes); "
                      "queries that use an unavailable endpoint or were dropped upstream are left "
                      "out (see removedQueries). Query keys are unchanged so results can be joined "
                      "with earlier experiments, except for replaced queries.")
    meta["endpointMap"] = [OrderedDict([("from", k), ("to", v["to"]), ("reason", v["reason"])])
                           for k, v in ENDPOINT_MAP.items()]
    meta["prefixMap"] = [OrderedDict([("endpoint", ep)] + list(spec.items()))
                         for ep, specs in PREFIX_MAP.items() for spec in specs]
    meta["upstreamReplacements"] = [OrderedDict([("key", k), ("upstreamFile", v["upstreamFile"]),
                                                 ("upstreamId", v["upstreamId"]), ("reason", v["reason"])])
                                    for k, v in UPSTREAM_REPLACEMENTS.items() if k in out_data]
    meta["queryFixes"] = [OrderedDict([("key", k), ("reason", v["reason"])])
                          for k, v in QUERY_FIXES.items() if k in out_data]
    meta["otherChecks"] = (
        "Entries can have otherChecks: results outside the verify runs. method by-hand: run by "
        "the catalogue maintainer from a hand-check list (2026-11-19/20; recorded on "
        "2026-11-20); probe: a single HTTP request sent while updating the catalogue; "
        "computed: a result count computed from small aggregate queries (not observed). "
        "version catalogue means the entry's query text (currentQuery is true while queryHash "
        "matches it); upstream means the file named in upstreamFile at sparql-examples@"
        f"{UPSTREAM_COMMIT_CHECKED[:7]}; variant means the text in query or described in "
        "versionNote. upstreamComparison, failureSummary and openOptions hold what is known "
        "about the query. All of it comes from QUERY_NOTES in the script.")
    missing = [k for k in QUERY_NOTES if k not in out_data]
    if missing:
        print("QUERY_NOTES for queries not in the updated catalogue (ignored): "
              + ", ".join(short_id(k) for k in missing))
    meta["removedQueries"] = removed
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
    if removed:
        print(f"Left out {len(removed)}: " + ", ".join(short_id(r["key"]) for r in removed))
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
    earlier_removed = {r["key"]: r for r in previous["metadata"].get("removedQueries") or []}
    for r in meta.get("removedQueries") or []:
        old = previous["data"].get(r["key"]) or earlier_removed.get(r["key"]) or {}
        for field in ("verification", "previousVerifications"):
            if old.get(field):
                r[field] = old[field]
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
                raise ValueError(f"response ended inside the bindings array after {rows} rows")
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
                # servers that fail while streaming often append their error text here
                raise ValueError(f"invalid JSON inside the bindings array after {rows} rows; "
                                 f"the response ends with: {squash(buf[-600:], 600)}")


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
    """Turn an error body (HTML page, JSON or plain text) into a short readable message."""
    if text.lstrip("﻿ \t\r\n").startswith("<"):  # HTML/XML error page; JSON bodies keep their <IRI>s
        text = re.sub(r"(?is)<(script|style|svg|head)\b.*?</\1>", " ", text)
        text = re.sub(r"data:[^\s\"'>)]+", "", text)  # inline images
        text = re.sub(r"<[^>]+>", " ", text)       # strip HTML tags
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
        if v["outcome"] != "ok":
            text = f"{v['outcome']} {v.get('error') or ''}"
            for pattern, hint in FAILURE_HINTS.get(entry["target"], []) + FAILURE_HINTS.get("*", []):
                if re.search(pattern, text):
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
# expected results
# ---------------------------------------------------------------------------

METHOD_LABELS = {"http-check": "HTTP check", "by-hand": "Run by hand", "probe": "Single HTTP request",
                 "computed": "Computed from aggregate queries"}


def as_check(v):
    """A verification record in the same shape as an otherChecks item."""
    c = OrderedDict([("date", v.get("date")),
                     ("method", "computed" if v.get("method") == "count-by-parts" else "http-check")])
    for k in ("run", "outcome", "resultCount", "httpStatus", "durationSeconds", "responseBytes",
              "countedBy", "error"):
        if v.get(k) is not None:
            c[k] = v[k]
    return c


def checks_of(entry):
    """(current, other): the checks of the entry's current query text, oldest first, and
    the checks of other versions of the query (earlier texts, upstream, variants)."""
    h = query_hash(entry)
    current, other = [], []
    history = list(entry.get("previousVerifications") or [])
    if entry.get("verification"):
        history.append(entry["verification"])
    for v in history:
        if v.get("outcome") == "not-run":
            continue
        c = as_check(v)
        if v.get("queryHash") == h:
            current.append(c)
        else:
            c["version"] = "earlier"
            c["versionNote"] = "an earlier text of this query (before a change listed in `changes`)"
            other.append(c)
    for oc in entry.get("otherChecks") or []:
        c = OrderedDict((k, x) for k, x in oc.items() if k not in ("currentQuery", "queryHash"))
        if oc.get("currentQuery"):
            c.pop("version", None)
            current.append(c)
        else:
            if c.get("version") == "catalogue":  # the query changed after this check
                c["version"] = "earlier"
                c.setdefault("versionNote", "an earlier text of this query")
            if c.get("upstreamFile"):
                c["upstreamUrl"] = upstream_url(c["upstreamFile"])
            other.append(c)
    by_date = lambda c: str(c.get("date") or "")
    return sorted(current, key=by_date), sorted(other, key=by_date)


def where(c):
    date = str(c.get("date") or "")[:10]
    if c["method"] == "http-check":
        return f"HTTP check, run {c.get('run')} ({date})"
    return f"{METHOD_LABELS[c['method']].lower()} ({date})"


def brief(c, limit=200):
    return c.get("result") or squash(c.get("error") or "", limit)


def expected_result(entry):
    """What a complete evaluation of the current query text is expected to give."""
    current, _ = checks_of(entry)
    seen = [c for c in current if c["method"] != "computed"]
    ok = [c for c in seen if c.get("outcome") == "ok"]
    computed = [c for c in current if c["method"] == "computed" and c.get("resultCount") is not None]
    e = OrderedDict()
    if ok:
        c = ok[-1]
        e["outcome"], e["resultCount"], e["basis"] = "ok", c.get("resultCount"), where(c)
        if c.get("durationSeconds") is not None:
            e["durationSeconds"] = c["durationSeconds"]
    elif computed:
        c = computed[-1]
        e["outcome"], e["resultCount"], e["basis"] = "ok", c["resultCount"], where(c)
        e["observed"] = False
        failed = [c for c in seen if c.get("outcome") in ("error", "timeout", "too-large")]
        if failed:
            # the query errors when run, but its result size is known: record both
            e["runError"] = entry.get("failureSummary") or brief(failed[-1])
            e["runErrorBasis"] = "; ".join(where(c) for c in failed)
        if seen:
            e["note"] = (f"Not observed: the latest attempt, {where(seen[-1])}, ended with "
                         f"{seen[-1].get('outcome')}: {brief(seen[-1])}")
    elif seen and seen[-1].get("outcome") in ("empty", "too-large"):
        c = seen[-1]
        e["outcome"] = c["outcome"]
        e["resultCount"] = 0 if c["outcome"] == "empty" else None
        e["basis"] = where(c)
        if entry.get("failureSummary"):
            e["note"] = entry["failureSummary"]
    elif seen:
        e["outcome"], e["resultCount"] = "fails", None
        e["basis"] = "; ".join(where(c) for c in seen)
        e["failure"] = entry.get("failureSummary") or brief(seen[-1])
    else:
        e["outcome"], e["resultCount"], e["basis"] = "unknown", None, "not checked yet"
    return e


def rows(n):
    return f"{n:,} row" + ("" if n == 1 else "s")


def fmt_expected(e):
    if e["outcome"] == "ok":
        text = rows(e["resultCount"]) if e.get("resultCount") is not None else "results"
        if e.get("observed") is False:
            text += " (computed, not observed" + ("; the query errors when run" if e.get("runError") else "") + ")"
        return text
    if e["outcome"] == "empty":
        return "0 rows"
    if e["outcome"] == "fails":
        return f"fails: {e['failure']}"
    return e["outcome"]


EXPECTED_KINDS = ["results (observed)", "results (computed, not observed)", "0 rows", "too large",
                  "fails", "unknown"]


def fmt_expected_kind(e):
    return {"ok": "results (computed, not observed)" if e.get("observed") is False else "results (observed)",
            "empty": "0 rows", "too-large": "too large", "fails": "fails"}.get(e["outcome"], "unknown")


def expected_path(catalogue_path):
    base = catalogue_path[:-5] if catalogue_path.endswith(".json") else catalogue_path
    return base + ".expected-results.json"


def expected(args):
    cat = load_json(args.catalogue)
    data, meta = cat["data"], cat["metadata"]
    out = OrderedDict()
    counts = defaultdict(int)
    for key, entry in data.items():
        current, other = checks_of(entry)
        exp = expected_result(entry)
        counts[exp["outcome"] + (" (computed)" if exp.get("observed") is False else "")] += 1
        item = OrderedDict([("id", short_id(key)), ("target", entry["target"]),
                            ("changeStatus", entry["changeStatus"]), ("queryHash", query_hash(entry)),
                            ("expected", exp), ("checks", current)])
        if other:
            item["otherVersions"] = other
        out[key] = item
    m = OrderedDict()
    m["catalogue"] = os.path.basename(args.catalogue)
    m["catalogueVersion"] = meta.get("catalogueVersion")
    m["generated"] = now_iso()
    m["generatedBy"] = "update_query_catalogue.py expected"
    m["description"] = (
        "The result each query of the updated catalogue is expected to give on its live target endpoint, "
        "with every check behind it: the automated HTTP checks (verify runs), the runs by hand and single "
        "requests made while updating the catalogue, and counts computed from aggregate queries. Counts are "
        "snapshots from November 2026: the endpoints' data change, so later counts can differ.")
    m["expectedRule"] = (
        "expected.outcome is ok (with resultCount) when a check of the current query text returned results; "
        "the latest such check is used. Otherwise, a count computed from aggregate queries is used "
        "(observed: false); if the query errored when it was run, runError says why and runErrorBasis "
        "names those checks. Otherwise, the latest check decides: empty (0 rows) or fails, with the "
        "reason in failure. basis names the check(s) used.")
    m["fields"] = OrderedDict([
        ("queryHash", "sha256(target + NUL + query)[:16] of the catalogue entry; a check applies to the "
                      "query text with this hash"),
        ("checks", "checks of the current query text, oldest first"),
        ("otherVersions", "checks of other versions: earlier texts in the updated catalogue, upstream "
                          "versions (upstreamFile at sparql-examples@" + UPSTREAM_COMMIT_CHECKED[:7]
                          + ") and variants (text in query, or described in versionNote)"),
        ("method", "http-check (run = verify run, see the catalogue's metadata.verificationRuns), "
                   "by-hand (result = what the person wrote down), probe (one HTTP request), computed"),
        ("outcome", "ok, empty, error, timeout or too-large for checks; ok, empty, fails, too-large or "
                    "unknown for expected"),
    ])
    m["summary"] = OrderedDict(sorted(counts.items()))
    m["removedQueries"] = [OrderedDict([("key", r["key"]), ("reason", r["reason"])])
                           for r in meta.get("removedQueries") or []]
    save_json_atomic(OrderedDict([("metadata", m), ("data", out)]), args.output)
    print(f"Wrote {args.output}: " + ", ".join(f"{n} {k}" for k, n in sorted(counts.items())))


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

def md_escape(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def fmt_check(v):
    parts = [f"**{v.get('outcome')}**"]
    if "resultCount" in v:
        parts.append(rows(v["resultCount"]))
    if v.get("httpStatus"):
        parts.append(f"HTTP {v['httpStatus']}")
    if v.get("durationSeconds") is not None:
        parts.append(f"{v['durationSeconds']} s")
    if v.get("countedBy"):
        parts.append(f"counted by {v['countedBy']}")
    run = f"run {v['run']}, " if v.get("run") else ""
    return ", ".join(parts) + f" ({run}{v.get('date')})"


def diff_block(replacements):
    """Markdown diff of exact text replacements (old lines with -, new lines with +)."""
    out = ["```diff"]
    for i, r in enumerate(replacements):
        if i:
            out.append("@@")
        out += ["- " + line for line in r["from"].rstrip("\n").split("\n")]
        out += ["+ " + line for line in r["to"].rstrip("\n").split("\n")]
    out.append("```")
    return out


def fmt_expected_line(exp, with_note=True):
    text = fmt_expected(exp)
    line = f"Expected: {text}" + ("" if text.endswith(".") else ".") + f" Based on: {exp['basis']}."
    if exp.get("runError"):
        line += f" Error when run ({exp['runErrorBasis']}): {exp['runError']}"
        line += "" if line.endswith(".") else "."
    elif with_note and exp.get("note"):
        line += f" {exp['note']}"
    return md_escape(line)


def fmt_other_check(c):
    """One line for an otherChecks item or an earlier-text HTTP check (from checks_of)."""
    date = str(c.get("date") or "")[:10]
    if c["method"] == "http-check":
        head = f"HTTP check, run {c.get('run')} ({date})"
    else:
        head = f"{METHOD_LABELS[c['method']]} ({date})"
    version = c.get("version")
    if version == "upstream":
        what = f"upstream [`{c['upstreamFile']}`]({upstream_url(c['upstreamFile'])})"
        if c.get("versionNote"):
            what += f", {c['versionNote']}"
    elif version in ("variant", "earlier"):
        what = c.get("versionNote") or version
        if c.get("query"):
            what += " (query text below)"
    else:
        what = "this query"
    res = [f"**{c.get('outcome')}**"]
    if c.get("resultCount") is not None:
        res.append(rows(c["resultCount"]))
    if c.get("httpStatus"):
        res.append(f"HTTP {c['httpStatus']}")
    if c.get("durationSeconds") is not None:
        res.append(f"{c['durationSeconds']} s")
    line = f"{head}, {what}: " + ", ".join(res)
    if c.get("result"):
        line += f"; written down: “{c['result']}”"
    elif c.get("error"):
        line += f": {squash(c['error'], 300)}"
    if c.get("note"):
        line += ("" if line.endswith(".") else ".") + f" {c['note']}"
    return md_escape(line)


def query_details(L, key, e, exp, with_key=True):
    """Bullets for one query: expected result, every check, upstream and what to try,
    followed by the text of tried variants."""
    if with_key:
        L.append(f"- Key: <{key}>")
        L.append(f"- Target: `{e['target']}`"
                 + (f" ({e['changeStatus']})" if e["changeStatus"] != "unchanged" else ""))
    L.append(f"- {fmt_expected_line(exp)}")
    h = query_hash(e)
    v = e.get("verification")
    if v:
        L.append(f"- Check: {fmt_check(v)}")
    for pv in e.get("previousVerifications", []):
        later = "" if pv.get("queryHash") == h else " (earlier query text)"
        L.append(f"- Earlier check{later}: {fmt_check(pv)}")
    if e["status"] != "working":
        for n in e["manualFixNotes"]:
            # advice the hand checks and failureSummary already answer
            if (n.startswith("Hint:") and e.get("failureSummary")) or \
                    (n.startswith("Returned 0 results") and e.get("otherChecks")):
                continue
            L.append(f"- {md_escape(n)}")
    texts = []
    for oc in e.get("otherChecks") or []:
        c = OrderedDict(oc)
        if not oc.get("currentQuery") and c.get("version") == "catalogue":
            c["version"], c["versionNote"] = "earlier", "an earlier text of this query"
        label = "Other check" if oc.get("currentQuery") else "Other version"
        L.append(f"- {label}: {fmt_other_check(c)}")
        if c.get("query"):
            texts.append((c.get("versionNote") or "variant", c.get("endpoint") or e["target"], c["query"]))
    up = e.get("upstreamComparison")
    if up:
        link = f" ([`{up['file']}`]({up['url']}))" if up.get("file") else ""
        L.append(f"- Upstream: {md_escape(up['note'])}{link}")
    if e.get("failureSummary") and e["status"] != "working" and exp["outcome"] != "fails" \
            and not exp.get("runError") and exp.get("note") != e["failureSummary"]:
        L.append(f"- Why it fails: {md_escape(e['failureSummary'])}")
    if e.get("openOptions"):
        L.append(f"- Still to try: {md_escape(e['openOptions'])}")
    L.append("")
    for note, endpoint, text in texts:
        L.append(f"Query text of {note} (run on `{endpoint}`):")
        L.append("")
        L.append("```sparql")
        L.extend(text.rstrip("\n").split("\n"))
        L.append("```")
        L.append("")


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
    expected_counts = defaultdict(int)
    exp = {}
    for k, e in data.items():
        status_counts[e["status"]] += 1
        outcome_counts[(e.get("verification") or {}).get("outcome", "unverified")] += 1
        exp[k] = expected_result(e)
        expected_counts[fmt_expected_kind(exp[k])] += 1

    L = []
    L.append(f"# Query catalogue update ({meta['catalogueVersion']})")
    L.append("")
    n_src = src_meta.get("number_of_queries")
    left_out = (f"; {n_src - len(data)} left out, see *Queries removed*" if n_src and n_src != len(data) else "")
    L.append(f"Generated by `update_query_catalogue.py` from `{src['file']}` "
             f"(sparql-examples commit `{src_meta.get('commit')}`, {n_src or len(data)} queries). "
             f"The updated catalogue has {len(data)} queries{left_out}. The original file is unchanged.")
    L.append("")
    L.append(f"- **Policy:** {meta['policy']}")
    L.append(f"- **Upstream consulted:** {meta['upstream']['repository']} "
             f"@ `{meta['upstream']['commitConsultedForEndpointUrls'][:7]}`. "
             f"{meta['upstream']['note']}")
    L.append("- **Live checks:** in each verification run, every selected query is sent once, "
             "with its SERVICE clauses, to its `target` endpoint, one query at a time and "
             "without retries. Status and outcome below come from each query's latest check; "
             "earlier checks are kept. Settings per run are listed under *Verification runs*.")
    L.append("- **Other checks:** the queries that failed were also run by hand (2026-11-19/20, recorded "
             "on 2026-11-20), often in their upstream version, and a few counts were computed from small "
             "aggregate queries. They are listed per query (`otherChecks` in the catalogue).")
    L.append(f"- **Expected results:** `{os.path.basename(expected_path(args.catalogue))}` "
             "(`update_query_catalogue.py expected`) gives the result each query is expected to give, "
             "with every check behind it; see *Expected results* below.")
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
    L.append("| Expected result (all checks) | Queries |")
    L.append("|---|---:|")
    for k, n in sorted(expected_counts.items(), key=lambda kv: EXPECTED_KINDS.index(kv[0])):
        L.append(f"| {k} | {n} |")
    L.append("")
    L.append("## Endpoint changes")
    L.append("")
    L.append("| From | To | Queries affected | Reason |")
    L.append("|---|---|---:|---|")
    for m in meta["endpointMap"]:
        n = sum(1 for e in data.values() if any(c.get("from") == m["from"] for c in e["changes"]))
        L.append(f"| `{m['from']}` | `{m['to']}` | {n} | {md_escape(m['reason'])} |")
    L.append("")
    L.append("### Unavailable endpoints (no replacement; queries using them were removed)")
    L.append("")
    removed = meta.get("removedQueries") or []
    for u in meta["unavailableEndpoints"]:
        gone = [short_id(r["key"]) for r in removed if u["endpoint"] in r.get("unavailableEndpoints", [])]
        left = sum(1 for e in data.values() if u["endpoint"] in all_endpoints(e))
        used = (f"removed: {', '.join(f'`{g}`' for g in gone)}" if gone else "no removed queries")
        used += f"; still used by {left} {'query' if left == 1 else 'queries'}" if left else ""
        L.append(f"- `{u['endpoint']}` ({used}): {u['note']}")
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
                    if any(c["field"] == "queryPrefix" and c.get("from") == m["from"] for c in e["changes"]))
            L.append(f"| `{m['endpoint']}` | `{m['prefix']}:` | `{m['from']}` | `{m['to']}` | {n} | "
                     f"{md_escape(m['reason'])} |")
        L.append("")
    reps = meta.get("upstreamReplacements") or []
    if reps:
        L.append(f"## Queries replaced with their upstream version ({len(reps)})")
        L.append("")
        L.append("These queries changed in the upstream SIB catalogue (sparql-examples). Their key is "
                 "unchanged, but their query text and description now come from upstream, so their "
                 "results are not comparable with earlier experiments.")
        L.append("")
        for r in reps:
            e = data.get(r["key"])
            L.append(f"### `{short_id(r['key'])}`")
            L.append("")
            L.append(f"Upstream [`{r['upstreamFile']}`]({upstream_url(r['upstreamFile'])}) "
                     f"(<{r['upstreamId']}>). {md_escape(r['reason'])}")
            L.append("")
            if e is None:
                continue
            if e["status"] == "needs-manual-fix":
                L.append(f"- {fmt_expected_line(exp[r['key']], with_note=False)} It does not work yet: see "
                         "*Queries that do not work yet*.")
                L.append("")
                continue
            query_details(L, r["key"], e, exp[r["key"]], with_key=False)
    qfixes = meta.get("queryFixes") or []
    if qfixes:
        L.append(f"## Queries with a small fix ({len(qfixes)})")
        L.append("")
        L.append("These queries returned no results, or did not finish. Each got a small, documented change "
                 "(exact text replacements, listed in the entry's `changes` and shown below as a diff) and "
                 "still asks the same question; their results are not directly comparable with earlier "
                 "experiments.")
        L.append("")
        for r in qfixes:
            e = data.get(r["key"])
            L.append(f"### `{short_id(r['key'])}`")
            L.append("")
            L.append(md_escape(r["reason"]))
            L.append("")
            if e is None:
                continue
            for c in e["changes"]:
                if c["field"] == "queryFix":
                    L.extend(diff_block(c["replacements"]))
                    if c.get("descriptionReplacements"):
                        L.append("")
                        L.append("Description: " + "; ".join(f"`{d['from']}` → `{d['to']}`"
                                                             for d in c["descriptionReplacements"]))
            L.append("")
            if e["status"] == "needs-manual-fix":
                L.append(f"- {fmt_expected_line(exp[r['key']], with_note=False)} It does not work yet: see "
                         "*Queries that do not work yet*.")
                L.append("")
                continue
            query_details(L, r["key"], e, exp[r["key"]], with_key=False)
    if removed:
        L.append(f"## Queries removed from the updated catalogue ({len(removed)})")
        L.append("")
        L.append("These queries are left out of the updated catalogue. They are still in the original "
                 "catalogue, and their earlier checks are kept in `metadata.removedQueries`.")
        L.append("")
        for r in removed:
            v = r.get("verification")
            last = f" Last check: {fmt_check(v)}." if v else ""
            L.append(f"- `{short_id(r['key'])}` (target `{r['target']}`): {md_escape(r['reason'])}{last}")
        L.append("")

    manual = [(k, e) for k, e in data.items() if e["status"] == "needs-manual-fix"]
    L.append(f"## Queries that do not work yet ({len(manual)})")
    L.append("")
    if not manual:
        L.append("None.")
    else:
        L.append("These were checked by HTTP request (see *Verification runs*) and by hand, and none gives "
                 "a complete result on today's endpoints. Each lists every check, how the query compares "
                 f"with upstream (sparql-examples@{UPSTREAM_COMMIT_CHECKED[:7]}), other versions that were "
                 "tried and what could still be tried.")
    L.append("")
    for k, e in sorted(manual, key=lambda kv: (kv[1]["target"], short_id(kv[0]))):
        L.append(f"### `{short_id(k)}`")
        L.append("")
        query_details(L, k, e, exp[k])

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
            L.append(f"### `{short_id(k)}`")
            L.append("")
            L.append(f"- Not checked: {md_escape(reason)}")
            query_details(L, k, e, exp[k])
    L.append("")

    L.append(f"## Expected results ({len(data)})")
    L.append("")
    L.append(f"Also in `{os.path.basename(expected_path(args.catalogue))}`, with every check behind each "
             "result. A query is expected to return the row count of the latest check of its current text "
             "that returned results; otherwise a count computed from aggregate queries is used; otherwise the "
             "latest check decides (0 rows, or fails). Counts are snapshots from November 2026 and can change "
             "with the endpoints' data.")
    L.append("")
    L.append("| Query | Target | Expected | Based on |")
    L.append("|---|---|---|---|")
    for k, e in sorted(data.items(), key=lambda kv: (kv[1]["target"], short_id(kv[0]))):
        L.append(f"| `{md_escape(short_id(k))}` | `{e['target']}` | {md_escape(fmt_expected(exp[k]))} | "
                 f"{md_escape(exp[k]['basis'])} |")
    L.append("")

    L.append("## Verification runs")
    L.append("")
    if not runs:
        L.append("None yet.")
        L.append("")
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

    x = sub.add_parser("expected", help="write the expected result of each query, with the checks behind it")
    x.add_argument("-c", "--catalogue", default="sib-swiss-federated-queries.updated-2026-11.json")
    x.add_argument("-o", "--output", help="default: <catalogue>.expected-results.json")
    x.set_defaults(func=expected)

    args = p.parse_args()
    if args.cmd == "expected" and not args.output:
        args.output = expected_path(args.catalogue)
    args.func(args)


if __name__ == "__main__":
    main()
