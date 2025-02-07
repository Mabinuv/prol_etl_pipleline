Overview
========

This is a simple project / entry project to proces ETL pipeline using Astronomer.io, Apache airflow, podman container, Postgres and python.
The environment used to run this project is on Apple MAC M1

Only review this an a basic beginner project towards getting a hands on experience with Data engineer 

Requirements
================

1. Deploy apache airflow using astronomer Cli

Installation
brew install astro
podman machine init
podman machine start
 ## is used to change the mode of your Podman machine to rootful, meaning that containers will run as the root user inside the Podman virtual machine (VM) rather than in rootless mode.
podman machine set  --rootful
podman ps
### runs a PostgreSQL 12.6 container
podman run --rm -it postgres:12.6 whoami

Creating Astro Project
astro dev init
astro config set -g container.binary podman 
astro config set -g duplicate volumes false

Run Airflow locally
astro dev start/(restart)

astro dev stop 
podman machine stop 

http://localhost:8080/

