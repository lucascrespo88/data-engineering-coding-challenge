import xml.dom.minidom
import os
from neo4j import GraphDatabase


def execute_query(cypher_query,payload):
    with driver.session() as session:
         session.run(cypher_query, data=payload)



absolute_path = os.path.dirname(__file__)
relative_path = "data/Q9Y261.xml"
full_path = os.path.join(absolute_path, relative_path)

doc = xml.dom.minidom.parse(full_path)


uri = "neo4j://localhost:7687"
username = "neo4j"
password = "neo4j"
driver = GraphDatabase.driver(uri, auth=(username, password))



### we only have one protein in the sample data and we have 3 accession values, we will hardcode the first to use as protein_id
proteins = []
protein_aux = {}

protein_name = doc.getElementsByTagName("name")[0].firstChild.nodeValue
protein_id = doc.getElementsByTagName("accession")[0].firstChild.nodeValue

protein_aux['protein_id'] = protein_id
protein_aux['protein_name'] = protein_name
proteins.append(protein_aux)


# CLEAR THE DATABASE

cypher_query = """
        MATCH (n)
DETACH DELETE n
    """

execute_query(cypher_query,payload = None)


# QUERY TO CREATE THE MAIN PROTEIN NODE

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Protein {name: row.protein_name, id: row.protein_id})
    """

execute_query(cypher_query,payload = proteins)




fullnames = []
shortnames = []
recomended_names = doc.getElementsByTagName("recommendedName")
for name in recomended_names:
    fullnames_elems = name.getElementsByTagName("fullName")
    for fullname in fullnames_elems:
        fullname_aux = {}
        fullname_aux['protein_id'] = protein_id
        fullname_aux['name_type'] = 'recomended'
        fullname_aux['value'] = fullname.firstChild.nodeValue
        fullnames.append(fullname_aux)

    shortnames_elems = name.getElementsByTagName("shortName")
    for shortname in shortnames_elems:
        name_aux = {}
        name_aux['name_type'] = 'recomended'
        name_aux['full_name'] = fullname_aux['value']
        name_aux['value'] = shortname.firstChild.nodeValue
        shortnames.append(name_aux)

alternative_names = doc.getElementsByTagName("alternativeName")
for name in recomended_names:
    fullnames_elems = name.getElementsByTagName("fullName")
    for fullname in fullnames_elems:
        fullname_aux = {}
        fullname_aux['protein_id'] = protein_id
        fullname_aux['name_type'] = 'alternative'
        fullname_aux['value'] = fullname.firstChild.nodeValue
        fullnames.append(fullname_aux)

    shortnames_elems = name.getElementsByTagName("shortName")
    for shortname in shortnames_elems:
        name_aux = {}
        name_aux['name_type'] = 'alternative'
        name_aux['full_name'] = fullname_aux['value']
        name_aux['value'] = shortname.firstChild.nodeValue
        shortnames.append(name_aux)


# A Protein must have a full name and it can have one or more short names asociated

# Storing all the nodes with the full names
cypher_query = """
        UNWIND $data AS row
        CREATE (p:FullName {name: row.value, name_type: row.name_type})
    """

execute_query(cypher_query,payload =fullnames)

# Asociating the full names with the protein
cypher_query = """
UNWIND $data AS row
MATCH
  (a:Protein),
  (b:FullName)
WHERE a.id = row.protein_id AND (b.name = row.value and b.name_type = row.name_type)
CREATE (a)-[:HAS_FULL_NAME]->(b)
 """

execute_query(cypher_query,payload = fullnames)

# Storing all the nodes with the short names
cypher_query = """
        UNWIND $data AS row
        CREATE (p:ShortName {name: row.value, name_type: row.name_type})
    """

execute_query(cypher_query,payload = shortnames)

# Asociating the short names with the full names

cypher_query = """
UNWIND $data AS row
MATCH
  (a:FullName),
  (b:ShortName)
WHERE (a.name = row.full_name and a.name_type=row.name_type) AND (b.name = row.value and b.name_type = row.name_type)
CREATE (a)-[:HAS_SHORT_NAME]->(b)
 """

execute_query(cypher_query,payload = shortnames)



genes = []
genes_elems = doc.getElementsByTagName("gene")[0]
gene_names= genes_elems.getElementsByTagName("name")
for g in gene_names:
    gene_aux = {}
    gene_aux['protein_id'] = protein_id
    gene_aux['type'] = g.getAttribute("type")
    gene_aux['name'] = g.firstChild.nodeValue
    genes.append(gene_aux)


#Asociating the genes with the protein dividing by status

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Gene {name: row.name})
    """

execute_query(cypher_query,payload = genes)

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Protein),
  (b:Gene)
WHERE a.id = row.protein_id AND b.name = row.name
CREATE (a)-[:FROM_GENE {status:row.type}]->(b)
 """

execute_query(cypher_query,payload = genes)


organisms = []
organism_elems = doc.getElementsByTagName("organism")[0]
organism_aux = {}
organism_aux['protein_id'] = protein_id

organism_names = organism_elems.getElementsByTagName("name")
for o in organism_names:
    if(o.getAttribute("type")) == 'scientific':
        organism_aux['scientific_name'] = o.firstChild.nodeValue
    if(o.getAttribute("type")) == 'common':
        organism_aux['common_name'] = o.firstChild.nodeValue
organisms.append(organism_aux)

#Linking in which Organism the protein is and it lineage

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Organism {common_name: row.common_name, scientific_name:row.scientific_name})
    """

execute_query(cypher_query,payload = organisms)

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Protein),
  (b:Organism)
WHERE a.id = row.protein_id AND b.scientific_name = row.scientific_name
CREATE (a)-[:IN_ORGANISM]->(b)
 """

execute_query(cypher_query,payload = organisms)

#extracting the lineage data from the XML
organism_lineages = []
organism_lineage_elems = organism_elems.getElementsByTagName("lineage")[0].getElementsByTagName("taxon")
for o in organism_lineage_elems:
    organism_lineage_aux = {}
    organism_lineage_aux['protein_id'] = protein_id
    organism_lineage_aux['scientific_name'] = organism_aux['scientific_name']
    organism_lineage_aux['taxonomy'] = o.firstChild.nodeValue
    organism_lineages.append(organism_lineage_aux)

cypher_query = """
        UNWIND $data AS row
        CREATE (p:OrganismLineage {taxonomy: row.taxonomy})
    """

execute_query(cypher_query,payload = organism_lineages)


cypher_query = """
UNWIND $data AS row
MATCH
  (a:Organism),
  (b:OrganismLineage)
WHERE  a.scientific_name = row.scientific_name and b.taxonomy=row.taxonomy
CREATE (a)-[:IN_LINEAGE]->(b)
 """

execute_query(cypher_query,payload = organism_lineages)

references = []
authors = []
databases = []
scopes = []

references_elems = doc.getElementsByTagName("reference")
for r in references_elems:
    reference_aux ={}
    reference_aux['key'] = r.getAttribute("key")
    reference_aux['protein_id'] = protein_id
    citation = r.getElementsByTagName("citation")[0]
    reference_aux['citation_type'] = citation.getAttribute("type")
    reference_aux['citation_name'] = citation.getAttribute("name")
    reference_aux['citation_date'] = citation.getAttribute("date")
    reference_aux['citation_volume'] = citation.getAttribute("volume")
    reference_aux['citation_first'] = citation.getAttribute("first")
    reference_aux['citation_last'] = citation.getAttribute("last")
    reference_aux['citation_title'] = citation.getElementsByTagName("title")[0].firstChild.nodeValue
    references.append(reference_aux)

    author_elems= r.getElementsByTagName("person")
    for a in author_elems:
        author_aux = {}
        author_aux['protein_id'] = protein_id
        author_aux['reference_key'] = reference_aux['key']
        author_aux['name'] = a.getAttribute("name")
        authors.append(author_aux)

    databases_elems= r.getElementsByTagName("dbReference")
    for db in databases_elems:
        database_aux = {}
        database_aux['protein_id'] = protein_id
        database_aux['reference_key'] = reference_aux['key']
        database_aux['type'] = db.getAttribute("type")
        database_aux['id'] = db.getAttribute("id")
        databases.append(database_aux)

    scopes_elems= r.getElementsByTagName("scope")
    for s in scopes_elems:
        scope_aux = {}
        scope_aux['protein_id'] = protein_id
        scope_aux['reference_key'] = reference_aux['key']
        scope_aux['scope'] = s.firstChild.nodeValue
        scopes.append(scope_aux)

#Storing the References in Nodes

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Reference {key: row.key, citation_type:row.citation_type,citation_name:row.citation_name
        , citation_date:row.citation_date, citation_volume:row.citation_volume, citation_first:row.citation_volume, citation_last:row.citation_last})
    """

execute_query(cypher_query,payload = references)

#Linking Reference Nodes with the protein
cypher_query = """
UNWIND $data AS row
MATCH
  (a:Protein),
  (b:Reference)
WHERE a.id = row.protein_id AND b.key = row.key
CREATE (a)-[:HAS_REFERENCE]->(b)
 """

execute_query(cypher_query,payload = references)


#Linking attributes from References : Authors, Scopes and Databases

#AUTHORS

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Author {name: row.name})
    """

execute_query(cypher_query,payload = authors)

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Reference),
  (b:Author)
WHERE a.key = row.reference_key AND b.name = row.name
CREATE (a)-[:HAS_AUTHOR]->(b)
 """

execute_query(cypher_query,payload = authors)


#DATABASES

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Database {id: row.id, type:row.type})
    """

execute_query(cypher_query,payload = databases)

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Reference),
  (b:Database)
WHERE a.key = row.reference_key AND b.id = row.id
CREATE (a)-[:HAS_DATABASE]->(b)
 """

execute_query(cypher_query,payload = databases)

#SCOPES

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Scope {scope: row.scope})
    """

execute_query(cypher_query,payload = scopes)

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Reference),
  (b:Scope)
WHERE a.key = row.reference_key AND b.scope = row.scope
CREATE (a)-[:HAS_SCOPE]->(b)
 """

execute_query(cypher_query,payload = scopes)


#FEATURES

features = []

features_elems = doc.getElementsByTagName("feature")

for f in features_elems:
    feature_aux ={}
    feature_aux['protein_id'] = protein_id
    feature_aux['type'] = f.getAttribute("type")
    feature_aux['description'] = f.getAttribute("description")
    feature_aux['evidence'] = f.getAttribute("evidence")
    location_elems = f.getElementsByTagName("location")[0]
    if len(location_elems.getElementsByTagName("position"))>0:
        feature_aux['position'] = location_elems.getElementsByTagName("position")[0].getAttribute('position')
        feature_aux['begin'] = 0
        feature_aux['end'] = 0
    if len(location_elems.getElementsByTagName("begin"))>0:
        feature_aux['position'] = 0
        feature_aux['begin'] = location_elems.getElementsByTagName("begin")[0].getAttribute('position')
    if len(location_elems.getElementsByTagName("end"))>0:
        feature_aux['end'] = location_elems.getElementsByTagName("end")[0].getAttribute('position')
    features.append(feature_aux)

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Feature {type: row.type, description: row.description, evidence: row.evidence, position: row.position, position_begin: row.begin,  position_end: row.end})
    """

execute_query(cypher_query,payload = features)


##SOME FEATURE HAS A POSITION VALUE AND OTHERS HAVE A POSITION RANGE, SO I DIVIDED THE TWO CASES IN DIFFERENT QUERIES

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Protein),
  (b:Feature)
WHERE a.id = row.protein_id AND b.type = row.type and row.evidence=b.evidence and b.position_begin = row.begin and row.position = 0
CREATE (a)-[:HAS_FEATURE {position_begin:row.begin, position_end:row.end}]->(b)
 """
execute_query(cypher_query,payload = features)

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Protein),
  (b:Feature)
WHERE a.id = row.protein_id AND (b.type = row.type and row.begin = 0 and row.evidence=b.evidence and b.position=row.position)
CREATE (a)-[:HAS_FEATURE {position:row.position}]->(b)
 """
execute_query(cypher_query,payload = features)

## EXTRACTING THE EVIDENCE DATA ASOCIATED TO THE PROTEIN

evidences = []

evidence_elems = doc.getElementsByTagName("evidence")

for e in evidence_elems:
    evidence_aux ={}
    evidence_aux['protein_id'] = protein_id
    evidence_aux['type'] = e.getAttribute("type")
    evidence_aux['key'] = e.getAttribute("key")
    if len( e.getElementsByTagName("source"))>0:
        source = e.getElementsByTagName("source")[0]
        evidence_aux['dbreference_type'] = source.getAttribute("type")
        evidence_aux['dbreference_id'] = source.getAttribute("id")
    else:
        evidence_aux['dbreference_type'] = None
        evidence_aux['dbreference_id'] = None
    evidences.append(evidence_aux)

cypher_query = """
        UNWIND $data AS row
        CREATE (p:Evidence {type: row.type, key: row.key, dbreference_type: row.dbreference_type, dbreference_id: row.dbreference_id})
    """

execute_query(cypher_query,payload = evidences)

cypher_query = """
UNWIND $data AS row
MATCH
  (a:Protein),
  (b:Evidence)
WHERE a.id = row.protein_id AND b.key = row.key
CREATE (a)-[:HAS_EVIDENCE]->(b)
 """

execute_query(cypher_query,payload = evidences)
