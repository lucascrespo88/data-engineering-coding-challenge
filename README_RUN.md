# Data Engineering Coding Challenge , how to run it

## Pre requisites
Having Docker installed in your machine

## Spinning Neo4j db
Run the bash file `./run_local_neo4j_db.sh` located in the `main` directory. The file will spin up a Neo4j db in Docker.

## Creating the virtual environment and install python packages
Run the bash file `./create_env.sh` located in the `main` directory. This will create the virtual environment and install the dependencies to run neo4j in python.

## Running the import file
Run the bash file `./run_import.sh` located in the `main` directory. This read the `Q9Y261.xml` located in the `data` directory, apply the proper transformations to extract the required data from the XML file and create the nodes and relationships in the Neo4j db.

## Go to the Neo4j UI to explore the data
Go to your browser and paste the following link `http://localhost:7474/browser/`
