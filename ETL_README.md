#Introduction
  This is the ETL Application written in Python and intended to provide the 
   -- users, subscriptions & messages data which is extracted from http url(users/messages).
   -- loading it in the mysql database and scheduling this pipeline in daily basis.

#Approach
  1. Python Class will be created and defining the methods for config read, logging, processing the http request(json data)  & transforming it as per usecases.
  2. Then, write this into MySQL databases with these tables(users,subscriptions,messages).
  3. Finally, creating the view based on above tables for final users( Remove PII data).

#How To Use This
  1. Fill in the configurable parameters in the users.properties.
  2. Folder Structure is as follows,
     <RepoName>/source/
       --To keep the python scripts.
     <RepoName>/output/
       --To write the target files.

  3. job_helper.py : This file has 'load_config' method that is used to load the specified config file.
  4. Run pip install -r requirements.txt to install dependencies. we can add more dependencies in this file as well.
  5. users.py : Main python script.
  6. users_airflow_dags.py : airflow script for scheduling.
  7. Run python3 users.py "L3"  (Syntax: python3 <scriptname> <env_variable>)

#Log Details
  1. logging is implemented to capture the pipelines activities.
  2. 'logfiles' folder will be created after running the main script(i.e. users.py). Under that, <scriptname>.log file will be created.
  
*****************
1) Please refer 'users.properties' for mysql db credentials.
   DatabaseName: dataengineering
   TableNames: users, subscriptions, messages 
   ViewNames: users_view,subscriptions_view, messages_view

2) How to schedule this pipeline using Crontab ?
    Step 1: Run this command: crontab -e
    * 8 * * * /usr/bin/python users.py  (it runs daily at 8 AM)
Alternative Approach for Scheduling.
    Airflow can be used to schedule this pipeline. I have created dag script for scheduling. Please refer this script.
    Script Name: users_airflow_dags.py

3) Did you identified any inaccurate/noisy record that somehow could prejudice the data analyses? How to monitor it (SQL query)? Please explain how do you suggest to handle with this noisy data?
I could see that there is an inaccurate data in subscription records. If there is more than one subscription per user, then the previous endDate should be less than the current start date as per SCD2 logic. But, the data is incorrect here.
In order to handle this, SCD2 logic should be implemented. The purpose of an SCD2 is to preserve the history of changes.








