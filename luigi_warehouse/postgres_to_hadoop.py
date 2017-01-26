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


########################################################################################################
#
#   This module copies data from our production database (postgres) to hadoop (hive tables) via spark.
#
#   Requirements:
#       * spark v2.0.1 +
#       * postgres driver - tested against postgresql-9.3-1102-jdbc41.jar
#       * you may need to use a python env - see here https://community.hortonworks.com/articles/58418/running-pyspark-with-conda-env.html
#       * luigi.cfg must be in your current directory with the [postgres] credentials   
#
#   class Load
#       * creates the table_pg in the hive_db by importing everything from the postgres table called table
#       * if incremental, gets the latest timestamp_field and pulls all records from the postgres table after that timestamp
#         then saves to hive_db.table_temp_pg.  then creates hive_db.table_reconcile_pg which is deduped based on the primary_key & timestamp_field
#         then recreates hive_db.table_pg as SELECT * FROM hive_db.table_reconcile_pg
#         if the incremental table does not exist, it creates it from scratch
#
##########################################################################################################

import configparser
import luigi
from luigi.contrib.simulate import RunAnywayTarget
try: 
  from pyspark.sql import SparkSession
except:
  print('This is a spark app, please use this on the hadoop cluster...')

LUIGI_CONFIG = 'luigi.cfg'

###############################################################
# Class that creates/loads the table and dedupes if necessary
###############################################################
class Load(luigi.ExternalTask):
  table = luigi.Parameter()
  primary_key = luigi.Parameter(default=None)
  timestamp_field = luigi.Parameter(default=None)
  incremental = luigi.BoolParameter(default=False)
  hive_db = luigi.Parameter(default='default')
  filters = "1 = 1" # default filter that doesn't exclude anything for from_scratch tables

  def table_exists(self):
    # cerebro_metadata.table_metadata 
    spark.sql("USE {0}".format(self.hive_db))
    sql = "SHOW TABLES LIKE '{0}_pg'".format(self.table)
    results = spark.sql(sql)
    return not results.rdd.isEmpty()

  def table_delete(self, table):
      spark.sql("DROP TABLE IF EXISTS {0}".format(table)).collect()

  def table_latest(self):
    sql = """SELECT MAX({0}) as {0} FROM {1}.{2}_pg""".format(self.timestamp_field,self.hive_db,self.table)
    latest = spark.sql(sql)
    return latest.head(1)[0][self.timestamp_field].strftime('%Y-%m-%d %H:%M:%S')

  def read(self):
    jdbcURL = "jdbc:postgresql://{0}:{1}/{2}?user={3}&password={4}&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory".format(
                 config['postgres']['host'],
                 config['postgres']['port'],
                 config['postgres']['database'],
                 config['postgres']['user'],
                 config['postgres']['password'])
    if self.incremental:
      self.filters = "{0} >= TIMESTAMP '{1}'".format(self.timestamp_field,self.table_latest())
    self.df = spark.read.format("jdbc").options(url=jdbcURL,dbtable="(SELECT * FROM {0} WHERE {1}) sub".format(self.table,self.filters)).load()
    if self.df.rdd.isEmpty():
      self.output().done()

  def write(self):
    spark.sql("USE {0}".format(self.hive_db))
    if self.incremental:
      self.table_delete("{0}_temp_pg".format(self.table))
      self.df.write.saveAsTable("{0}_temp_pg".format(self.table))
      self.dedupe()
    else:
      self.table_delete("{0}_pg".format(self.table))
      self.df.write.saveAsTable("{0}_pg".format(self.table))
  
  def dedupe(self):
    spark.sql("USE {0}".format(self.hive_db))
    self.table_delete("{0}_reconcile_pg".format(self.table))
    spark.sql("""CREATE TABLE {0}_reconcile_pg AS
                   SELECT t1.* FROM
                   (SELECT * FROM {0}_pg
                       UNION ALL
                    SELECT * FROM {0}_temp_pg) t1
                   JOIN
                       (SELECT {1}, max({2}) {2} FROM
                           (SELECT * FROM {0}_pg
                              UNION ALL
                            SELECT * FROM {0}_temp_pg) t2
                       GROUP BY {1}) s
                   ON t1.{1} = s.{1} AND t1.{1} = s.{1}""".format(self.table,
                                                                   self.primary_key,
                                                                   self.timestamp_field))
    self.table_delete("{0}_pg".format(self.table))
    spark.sql("CREATE TABLE {0}_pg LIKE {0}_reconcile_pg".format(self.table))
    spark.sql("INSERT INTO {0}_pg SELECT * FROM {0}_reconcile_pg".format(self.table))
    self.table_delete("{0}_temp_pg".format(self.table))
    self.table_delete("{0}_reconcile_pg".format(self.table))

  def run(self):
    if (self.incremental) & (not self.table_exists()):
      yield Load(table=self.table) #Build the incremental table from scratch if it does not exist
    self.read()
    self.write()
    self.output().done()

  def output(self):
    return RunAnywayTarget(self)


###############################################################
# Wrapper to create tables from scratch
###############################################################
class RunFromScratch(luigi.WrapperTask):
  tables = luigi.ListParameter()
  hive_db = luigi.Parameter(default='default')

  def requires(self):
    for table in self.tables:
      yield Load(table=table, hive_db=self.hive_db)


###############################################################
# Wrapper to update incremental tables
###############################################################
class RunIncremental(luigi.WrapperTask):
  tables = luigi.ListParameter()
  hive_db = luigi.Parameter(default='default')
  
  def requires(self):
    for (table,primary_key,timestamp_field) in self.tables:
      yield Load(table=table, primary_key=primary_key, timestamp_field=timestamp_field, incremental=True, hive_db=self.hive_db)


###############################################################
# Wrapper to do everything
###############################################################
class Run(luigi.WrapperTask):
  tables_from_scratch = luigi.ListParameter() #[ 'orders','customers',... ]
  tables_incremental = luigi.ListParameter() #[(table, primarykey, timestamp_field),(),...]
  hive_db = luigi.Parameter(default='default')

  def requires(self):
    return [RunIncremental(tables=tables_incremental, hive_db=hive_db), 
            RunFromScratch(tables=tables_from_scratch, hive_db=hive_db)]




if __name__ == '__main__':
  config = configparser.ConfigParser()
  config.read(LUIGI_CONFIG)
  dir_path = os.path.dirname(os.path.realpath(__file__))
  spark = SparkSession \
        .builder \
        .appName("Luigi on Spark - Postgres") \
        .config("hive.metastore.warehouse.dir",dir_path) \
        .enableHiveSupport() \
        .getOrCreate()
  luigi.run()