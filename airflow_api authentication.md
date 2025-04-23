
# API Authentication

This guide explains how to configure CORS in Apache Airflow in a Docker environment. It covers how to edit the `airflow.cfg` file, configure CORS (Cross-Origin Resource Sharing) settings, and apply the necessary changes to your Docker-based Airflow instance.

## Prerequisites

- Docker and Docker Compose must be installed on your machine.
- You should have a running Airflow instance inside Docker.
- You should have basic familiarity with Docker and Airflow.

## Steps to Configure Airflow

### 1. Access the Running Docker Container

First, you need to access the running Airflow container. This is typically the Airflow webserver container. Run the following command to list the running containers:

```bash
docker ps
```

Find the container name for the Airflow webserver (e.g., `airflow-airflow-webserver-1`).

Then, use the following command to open a terminal session inside the Airflow webserver container:

```bash
docker exec -it airflow-airflow-webserver-1 /bin/bash
```

You should now be inside the container with a shell prompt.

### 2. Edit the `airflow.cfg` File

The configuration file for Airflow is located at `/opt/airflow/airflow.cfg`. It's often easier to copy the file to your host machine, edit it, and then copy it back.

#### Step 1: Copy the `airflow.cfg` file to your host machine

From your host machine (not inside the container), run:

```bash
docker cp airflow-airflow-webserver-1:/opt/airflow/airflow.cfg ./airflow.cfg
```

#### Step 2: Edit the `airflow.cfg` file

Edit the `airflow.cfg` file using any text editor you prefer (e.g., `nano`, `vim`, or GUI-based editors).

#### Step 3: Apply the necessary changes

Make the necessary changes in the `airflow.cfg` file. For example, configuring CORS settings (see below for details).

#### Step 4: Copy the modified `airflow.cfg` file back to the container

After editing, run the following command from your host machine:

```bash
docker cp ./airflow.cfg airflow-airflow-webserver-1:/opt/airflow/airflow.cfg
```

### 3. Configuring CORS

Airflow uses CORS (Cross-Origin Resource Sharing) to restrict HTTP requests initiated from scripts running in browsers. To configure CORS, you need to modify the settings in the `airflow.cfg` file.

Look for the `[api]` section in the `airflow.cfg` file, and add or modify the following settings:

```ini
[api]
# Allow specific origins (comma-separated list of origins)
access_control_allow_origins = "*"

# Allow specific methods (comma-separated list of methods)
access_control_allow_methods = "GET, POST, PUT, DELETE"

# Allow specific headers (comma-separated list of headers)
access_control_allow_headers = "Content-Type, Authorization"
```

- `access_control_allow_origins`: Set to `*` to allow all origins or specify particular domains that are allowed to access the API.
- `access_control_allow_methods`: Defines the allowed HTTP methods (e.g., GET, POST).
- `access_control_allow_headers`: Specifies the headers that are allowed in requests.

### 4. Enable the Airflow REST API:

Verify that you have the Airflow REST API enabled in your airflow.cfg file:
```ini
enable_experimental_api = True
```
### 5. Restart the Airflow Webserver

Once you've updated the `airflow.cfg` file, restart the Airflow webserver container to apply the changes:

```bash
docker restart airflow-airflow-webserver-1
```

This will reload the Airflow configuration with the updated settings.

By following these steps, you should be able to configure Airflow's settings in your Docker container, including enabling CORS for API requests.
