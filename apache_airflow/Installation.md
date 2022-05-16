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


5. These change relate to `airflow.cfg`:  
`[webserver] #Section`    
`x_frame_enabled = False`      
`warn_deployment_exposure = False`    



6. Create a User for airflow:  
`airflow users create \  
    --username admin \  
    --firstname name \  
    --lastname name \  
    --role Admin \  
    --email name@example.info`  
