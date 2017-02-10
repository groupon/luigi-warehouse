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
# Job is a spark app that pulls from a google sheet and
#      saves to a hive table
###########################################################
# Example Usage
      # cd .../anaconda3/envs/
      # export SPARK_HOME=.../spark-2.0.1
      # export PYSPARK_PYTHON="py3spark_env/bin/python"
      # export LUIGI_CONFIG_PATH=.../luigi/luigi.cfg
      # ...spark-2.0.1/bin/spark-submit --master yarn  \
      #                                 --deploy-mode client \
      #                                 --queue public \
      #                                 --archives "py3spark_env.zip#py3spark_env" \
      #                                 gsheet_to_spark.py \
      #                                 "My Google Doc" \
      #                                 "My Sheet" \
      #                                 my_hive_db \
      #                                 my_hive_table
###########################################################
# Conda ENV required depending on your environment. To create a conda env for use with spark:
    # conda create -n py3spark_env --copy -y -q python=3
    # source activate py3spark_env
    # pip install shapely numpy pandas luigi gspread>=0.2.5 oauth2client==1.5.2 slackclient>=0.16
    # conda install pyopenssl
    # source deactivate py3spark_env
    # 
    # cd ~/anaconda3/envs/
    # zip -r py3spark_env.zip py3spark_env
############################################################

# spreadsheet must be shared with a google service account 
# with credentials places in a json auth file 
# your luigi.cfg must point to that json file
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import luigi
import pandas as pd
from oauth2client.client import SignedJwtAssertionCredentials
import gspread

if __name__ == '__main__':
    assert len(sys.argv) == 5, "sheet_name, tab_name, target_db, target_table - required arguments"
    sheet_name = sys.argv[1]
    tab_name = sys.argv[2]
    db = sys.argv[3]
    table = sys.argv[4]
    warehouse_location = "file:/spark-warehouse"

    spark = SparkSession \
        .builder \
        .appName("Spark - Google Sheet") \
        .config("hive.metastore.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()

    sqlContext = SQLContext(spark.sparkContext)

    json_key = json.load(open(luigi.configuration.get_config().get('google_sheets', 'json_auth_file')))
    scope = [luigi.configuration.get_config().get('google_sheets', 'scope')]
    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope)
    gc = gspread.authorize(credentials)

    sheet = gc.open(sheet_name)
    locsheet = sheet.worksheet(tab_name)
    data = locsheet.get_all_values()
    header = locsheet.get_all_values()[0]
    header = [col.strip().lower().replace(' ', '_').replace('/', '_') for col in header]
    data = [l for l in data if l != header]

    df = pd.DataFrame(data, columns=header)
    df_spark = sqlContext.createDataFrame(df)

    sqlContext.sql("USE {0}".format(db))
    df_spark.registerTempTable('{0}_temp'.format(table))
    sqlContext.sql("DROP TABLE IF EXISTS {0}".format(table))
    sqlContext.sql("CREATE TABLE {0} AS SELECT * FROM {0}_temp".format(table))

