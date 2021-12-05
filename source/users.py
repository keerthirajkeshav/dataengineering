import os
from sys import argv
import logging
import logging.handlers
from sqlalchemy import create_engine
import requests
import json
import pandas as pd
import numpy as np
import job_helper as job_helper

pd.options.mode.chained_assignment = None

script, aws_env = argv
print("Pipeline Path: ", script)
print("The environment is called:", aws_env)


class user_messages:
    def __init__(self):
        # Get the environment, config, and initialize the module
        self.config = job_helper.load_config('users.properties')
        self.pipeline = "user_messages"
        self.read_config()

    def read_config(self):
        self.hostname = self.config[aws_env]['hostname']
        self.port = self.config[aws_env]['port']
        self.username = self.config[aws_env]['username']
        self.username = self.config[aws_env]['username']
        self.password = self.config[aws_env]['password']
        self.dbname = self.config[aws_env]['dbname']

        self.user_url = self.config[aws_env]['user_url']
        self.message_url = self.config[aws_env]['message_url']

        self.user_table = self.config[aws_env]['user_table']
        self.subscription_table = self.config[aws_env]['subscription_table']
        self.message_table = self.config[aws_env]['message_table']

    def create_timed_rotating_log(self):
        print(" **************@create_timed_rotating_log*************")
        current_dir = os.path.dirname(os.path.abspath(__file__))
        logging_dir = current_dir + "/logfiles/"
        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)
        script_name = os.path.basename(__file__)
        logfile_name = os.path.splitext(script_name)[0] + ".log"
        handler = logging.handlers.TimedRotatingFileHandler(logging_dir + logfile_name,
                                                            when="midnight",
                                                            interval=1,
                                                            backupCount=90)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger = logging.getLogger(__name__)
        logger.setLevel(os.environ.get("LOGLEVEL", "DEBUG"))
        logger.addHandler(handler)
        return logger

    def db_connect(self):
        self.logger.info("**********@db_connect**************")
        conn_string = 'mysql+pymysql://' + self.username + ':' + self.password + '@' + self.hostname + ':' + self.port + '/' + self.dbname
        mySqlEngine = create_engine(conn_string)
        db_connection = mySqlEngine.connect()
        return db_connection

    def user_subscription(self):
        try:
            self.logger.info("**********@user_subscription**************")
            user_request = requests.get(self.user_url)
            user_data = json.loads(user_request.content)
            users_normalize_data = pd.json_normalize(user_data)

            subscription_df = users_normalize_data[["id", "subscription"]]
            subscription_data = pd.concat([pd.DataFrame(x) for x in subscription_df['subscription']],
                                          keys=subscription_df['id']).reset_index(level=1, drop=True).reset_index()
            subs_dtypes = {"id": int, "createdAt": np.datetime64, "startDate": np.datetime64, "endDate": np.datetime64,
                           "amount": np.double}
            subscription_data = subscription_data.astype(subs_dtypes)
            subscription_data['createdAt'] = subscription_data['createdAt'].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            subscription_data['createdAt'] = pd.to_datetime(subscription_data['createdAt'])
            subscription_data['startDate'] = subscription_data['startDate'].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            subscription_data['startDate'] = pd.to_datetime(subscription_data['startDate'])
            subscription_data['endDate'] = subscription_data['endDate'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            subscription_data['endDate'] = pd.to_datetime(subscription_data['endDate'])
            self.logger.info(subscription_data.dtypes)
            db_connect = self.db_connect()
            subscription_data.to_sql(con=db_connect, name=self.subscription_table, if_exists='replace', index=False)
            subscription_data.to_csv("subscriptions.csv", index=False)
            self.logger.info("**********subscriptions completed**************")
            user_df = users_normalize_data[
                ["id", "createdAt", "updatedAt", "firstName", "lastName", "address", "city", "country", "zipCode", \
                 "email", "birthDate", "profile.gender", "profile.isSmoking", "profile.profession", "profile.income"]]
            user_dtypes = {"id": int, "createdAt": np.datetime64, "updatedAt": np.datetime64,
                           "birthDate": np.datetime64}
            user_df = user_df.astype(user_dtypes)
            user_df["email"] = user_df['email'].str.split("@").str[1]
            user_df['createdAt'] = user_df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            user_df['createdAt'] = pd.to_datetime(user_df['createdAt'])
            user_df['updatedAt'] = user_df['updatedAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            user_df['updatedAt'] = pd.to_datetime(user_df['updatedAt'])
            user_df['birthDate'] = user_df['birthDate'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            user_df['birthDate'] = pd.to_datetime(user_df['birthDate'])
            self.logger.info(user_df.dtypes)
            user_df.to_sql(con=db_connect, name=self.user_table, if_exists='replace', index=False)
            user_df.to_csv("users.csv", index=False)
            self.logger.info("**********users completed**************")
        except Exception as e:
            self.logger.error(e)
            raise e

    def messages(self):
        try:
            self.logger.info("**********@messages**************")
            messages_url = requests.get(self.message_url)
            messages_content = json.loads(messages_url.content)
            msg_norm = pd.json_normalize(messages_content)
            msg_df = msg_norm[["id", "createdAt", "receiverId", "senderId", "message"]]
            msg_dtypes = {"id": int, "createdAt": np.datetime64, "receiverId": int, "senderId": int}
            msg_df = msg_df.astype(msg_dtypes)
            msg_df['createdAt'] = msg_df['createdAt'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            msg_df['createdAt'] = pd.to_datetime(msg_df['createdAt'])
            self.logger.info(msg_df.dtypes)
            db_connect = self.db_connect()
            msg_df.to_sql(con=db_connect, name=self.message_table, if_exists='replace', index=False)
            msg_df.to_csv("messages.csv", index=False)
            self.logger.info("**********Message completed**************")
        except Exception as e:
            self.logger.error(e)
            raise e

    def execute_user_messages(self):
        try:
            self.logger = self.create_timed_rotating_log()
            self.logger.info("**********@execute_user_messages**********")
            self.user_subscription()
            self.messages()
            self.logger.info("**********Job completed Successfully**********")
        except Exception as e:
            self.logger.error(e)
            raise e


if __name__ == '__main__':
    print("******@name==main*******")
    user = user_messages()
    user.execute_user_messages()