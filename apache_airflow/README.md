This directory relates to some important notes about `Airflow`. 

In terms of data workflows, the following terms:
1. Automation training, testing, and deploying a machine learning model
2. Ingesting data from different REST APIs
3. Coordinating ETL, and ELT Processes Across an Enterprise Data Lake[1]


Basic Explanation: 

There are two types of executor - those that run tasks locally (inside the scheduler process), and those that run their tasks remotely (usually via a pool of workers). Airflow comes configured with the SequentialExecutor by default, which is a local executor, and the safest option for execution, but we strongly recommend you change this to LocalExecutor for small, single-machine installations, or one of the remote executors for a multi-machine/cloud installation.[2]

- `SequentialExecutor` is not recommended for a production setup, but it should not be an issue for our case.

Python Setup Steps: 

To install apache airflow needs to install Python +3.6. 
`Note`: This tutorial works for Mac/Linux. 

1. Create a new python environment in a directory: 
#Open a terminal change directory to a specific directory  
`python3 -m venv airflow`  
#Activate your airflow  
`source airflow/bin/activate`  
2. Install airflow with pip installation:  
`sudo pip install apache-airflow`  
3. Export Home:  
In the `airflow` location  
This section is critical, so be careful.  
`export AIRFLOW_HOME=$(pwd)`  
4. Database Initialization:  
`airflow db init`
5. Create a User for airflow:  
`airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin`  

References: 
1. [Medium Link](https://medium.com/abn-amro-developer/data-pipeline-orchestration-on-steroids-apache-airflow-tutorial-part-1-87361905db6d)
2. [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/executor/index.html)
