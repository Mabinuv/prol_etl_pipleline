Overview
========

This is a simple project to process ETL pipeline using Astronomer.io, Apache airflow, podman container, Postgres and python.
The environment used to run this project is Apple MAC M1

Requirements
================

1. Installation
```bash
brew install astro
```
2. Deploy Apache airflow using astronomer Cli
```bash
podman machine init
podman machine start
```

### Change the mode of Podman machine to rootful, meaning that containers will run as the root user inside the Podman virtual machine (VM) rather than in rootless mode.
```bash
podman machine set  --rootful 
```

### List all active containers in Podman
```bash
podman ps
```

### runs a PostgreSQL 12.6 container
```bash
podman run --rm -it postgres:12.6 whoami
```

## Creating Astro Project

```bash
astro dev init
astro config set -g container.binary podman 
astro config set -g duplicate volumes false
```

### Start Airflow (Locally)
```bash
astro dev start
```

### Restart Airflow (Locally)
```bash
astro dev restart
```

### Stop Airflow
```bash
astro dev stop 
```
### Stop Podman server
```bash
podman machine stop 
```


