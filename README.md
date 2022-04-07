# ingestion-scripts
Repository for managing ingestion scripts

## Getting started

After cloning this repository, create a venv in the folder of the repository using:

`python -m venv venv`

To activate the venv, enter the following command:

`venv\Scripts\activate` if you're on Windows

`source venv/bin/activate` on Unix

Afterwards, install all requirements:

`pip install -r requirements.txt`

## Starting a Faust Worker

Navigate to the src-Folder and type:

`python -m faust -A topic_consumer --debug worker -l info`

(Don't forget to activate your venv beforehand)
