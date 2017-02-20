# The MIT License (MIT)

# Copyright (c) 2013, Groupon, Inc.
# All rights reserved. 

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Neither the name of GROUPON nor the names of its contributors may be
# used to endorse or promote products derived from this software without
# specific prior written permission. 

###########################################################
# Job is a spark app that pulls from the zendesk API and
#      saves to a hive table via spark
###########################################################
# class Zendesk
#   - Generic python class for interacting with the Zendesk API
# class ZendeskSpark
#   - Generic luigi task class for importing data from the Zendesk API to TABLES
#      - subclass and provide table method for use
# Example: The following simple example will
#   1.  extract tickets starting at the latest 'updated_at' time 
#       in our already existing hive table 'tickets_zendesk' since
#       'incremental' =True
#   2.  load these tickets into the 'tickets_zendesk' table 
#   3.  dedupe based on 'id' column and keep the latest 'updated_at' column


# class Tickets(ZendeskSpark):
#   table = 'tickets'
#   primary_key = 'id'
#   timestamp_field = 'updated_at'
#   incremental = True

#   def tickets(self):
#     # get the latest record from the already existing hive table 
#     # since we're doing this incrementally
#     latest = self.convert_to_epoch(self.table_latest())
#     # call the API and get the resulting tickets as json
#     response = client_tickets.incremental_ticket_pull(latest)
#     result = response.json()['results']
#     # make our pandas df
#     df_pandas = pd.DataFrame(result)
#     # return our spark df
#     return spark.createDataFrame(df_pandas)
###########################################################
# Conda ENV required depending on your spark environment. To create a conda env for use with spark:
    # conda create -n py3spark_env --copy -y -q python=3
    # source activate py3spark_env
    # pip install numpy pandas luigi gspread>=0.2.5 oauth2client==1.5.2 slackclient>=0.16 django pandas zen zdesk 
    # conda install pyopenssl
    # source deactivate py3spark_env
    # 
    # cd ~/anaconda3/envs/
    # zip -r py3spark_env.zip py3spark_env
############################################################
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
except:
    print('This is a spark app, please use this on the hadoop cluster...')
import os
import sys
import configparser
import datetime
import time
import requests
import pandas as pd
import luigi
from luigi.contrib.simulate import RunAnywayTarget

LUIGI_CONFIG = 'luigi.cfg'


class ZenDesk:
    '''Generic class that interacts with the Zendesk API'''
    def __init__(self, zendesk_url, zendesk_username, zendesk_token):
        self.zendesk_url = zendesk_url
        self.zendesk_username = zendesk_username
        self.zendesk_token = zendesk_token

    def api_pull(self, endpoint):
        headers = {'Accept': 'application/json'}
        url = self.zendesk_url + endpoint
        response = requests.get(url, 
                                auth=(self.zendesk_username, self.zendesk_token), 
                                headers=headers)
        return response

    def incremental_ticket_pull(self, start_time):
        headers = {'Accept': 'application/json'}
        zendesk_endpoint = '/exports/tickets.json?start_time='
        url = self.zendesk_url + zendesk_endpoint + str(start_time)
        response = requests.get(url, 
                                auth=(self.zendesk_username, self.zendesk_token), 
                                headers=headers)
        return response

    def status_handler(self, response):
        if response.status_code == 429:
            print('Rate limited. Waiting to retry in ' + response.headers.get('retry-after') + ' seconds.')
            time.sleep(float(response.headers.get('retry-after')))
        if 200 <= response.status_code <= 300:
            print('Success.')
        if response.status_code == 422:
            print("Start time is too recent. Try a start_time older than 5 minutes.")
            sys.exit(0)


class ZendeskSpark(luigi.ExternalTask):
    '''Generic class to ETL Zendesk data from API to Hive via Spark
       Subclass this and create a method with the same name as your table which
          return a spark df of your data to Load'''
    hive_db = 'DEFINE YOUR TARGET HIVE DB HERE'
    table = 'DEFINE YOUR TARGET TABLE NAME HERE'
    primary_key = 'DEFINE YOUR UNIQUE PRIMARY KEY HERE'
    timestamp_field = 'DEFINE YOUR TIMESTAMP FIELD HERE IF INCREMENTAL'
    incremental = 'SET AS TRUE IF YOU ARE DOING INCREMENTAL IMPORTS ON YOUR TARGET TABLE'
  
    def table_exists(self):
        spark.sql("USE {0}".format(self.hive_db))
        sql = "SHOW TABLES LIKE '{0}_zendesk'".format(self.table)
        results = spark.sql(sql)
        return not results.rdd.isEmpty()
  
    def table_delete(self, table):
        spark.sql("DROP TABLE IF EXISTS {0}".format(table)).collect()
  
    def table_latest(self):
        sql = """SELECT 
                   MAX( CAST({0} AS TIMESTAMP) ) as {0} 
                 FROM {1}.{2}_zendesk""".format(self.timestamp_field,
                                                self.hive_db,
                                                self.table)
        latest = spark.sql(sql).collect()
        return latest[0][self.timestamp_field]
  
    def convert_to_epoch(self, d):
        return int((d - datetime.datetime(1970, 1, 1)).total_seconds())    
  
    def dictionary_string_to_integer(self, dictionary, key):
        try: 
            dictionary[key] = int(dictionary[key])
        except: 
            dictionary[key] = None
        return dictionary
  
    def read(self):
        self.df = getattr(self, self.table)()
        if self.df.rdd.isEmpty():
            self.output().done()
  
    def write(self):
        spark.sql("USE {0}".format(self.hive_db))
        if self.incremental:
            self.table_delete("{0}_temp_zendesk".format(self.table))
            self.df.write.saveAsTable("{0}_temp_zendesk".format(self.table))
            self.dedupe()
        else:
            self.table_delete("{0}_zendesk".format(self.table))
            self.df.write.saveAsTable("{0}_zendesk".format(self.table))
  
    def dedupe(self):
        spark.sql("USE {0}".format(self.hive_db))
        self.table_delete("{0}_reconcile_zendesk".format(self.table))
        spark.sql("""CREATE TABLE {0}_reconcile_zendesk AS
                     SELECT t1.* FROM
                     (SELECT * FROM {0}_zendesk
                         UNION ALL
                      SELECT * FROM {0}_temp_zendesk) t1
                     JOIN
                         (SELECT {1}, max({2}) {2} FROM
                             (SELECT * FROM {0}_zendesk
                                UNION ALL
                              SELECT * FROM {0}_temp_zendesk) t2
                         GROUP BY {1}) s
                      ON t1.{1} = s.{1} AND t1.{1} = s.{1}""".format(self.table,
                                                                     self.primary_key,
                                                                     self.timestamp_field))
        self.table_delete("{0}_zendesk".format(self.table))    
        spark.sql("CREATE TABLE {0}_zendesk LIKE {0}_reconcile_zendesk".format(self.table))
        spark.sql("INSERT INTO {0}_zendesk SELECT * FROM {0}_reconcile_zendesk".format(self.table))
        self.table_delete("{0}_temp_zendesk".format(self.table))
        self.table_delete("{0}_reconcile_zendesk".format(self.table))
  
    def run(self):
        if (self.incremental) & (not self.table_exists()):
            # if it's an incremental load and the table doesn't exist yet
            # do the same task but non-incrementally
            yield getattr(sys.modules[__name__],self.__class__.__name__)(table=self.table, primary_key=self.primary_key, timestamp_field=self.timestamp_field, incremental=False) #Build the incremental table from scratch if it does not exist
        self.read()
        self.write()
        self.output().done()
  
    def output(self):
        return RunAnywayTarget(self)




if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # get our luigi config & zendesk credentials
    config = configparser.ConfigParser()
    config.read(LUIGI_CONFIG)
    user = config['zendesk']['user']
    password = config['zendesk']['password']
    zen_endpoint = config['zendesk']['endpoint']
    # init our zendesk api client
    client_tickets = ZenDesk(zen_endpoint, user, password)
    # init our spark session
    spark = SparkSession \
          .builder \
          .appName("Luigi on Spark - Zendesk") \
          .config("hive.metastore.warehouse.dir", dir_path) \
          .enableHiveSupport() \
          .getOrCreate()
    luigi.run()
