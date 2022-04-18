# ingestion-scripts
Repository for managing ingestion scripts

## Getting started


After cloning this repository, there are two ways to run it: via Docker or directly on your machine.

### Running directly on host machine
Create a venv in the folder of the repository using:


`python -m venv venv`

To activate the venv, enter the following command:

`venv\Scripts\activate` if you're on Windows

`source venv/bin/activate` on Unix

Afterwards, install all requirements:

`pip install -r requirements.txt`


#### Starting a Faust Worker


Navigate to the src-Folder and type:

`python -m faust -A topic_consumer --debug worker -l info`

(Don't forget to activate your venv beforehand)

### Running with Docker
First, you have to build an image:
`docker build -t ingestion_image --no-cache .`

Then make sure that the docker stack from [oth-pipeline/infrastructure](https://github.com/oth-datapipeline/infrastructure) is running.

Finally, run the `ingestion_image` you previously built, and pass various variables to the container:
`docker run --network=docker_kafka --name=pipeline -e MONGO_INITDB_ROOT_USERNAME=<username> -e MONGO_INITDB_ROOT_PASSWORD=<password> -e BROKER_HOST="broker:29092" -e MONGO_HOST="mongo" ingestion_image`
