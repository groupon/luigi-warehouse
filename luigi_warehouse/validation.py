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


# Data Validation - Mainly Aimed at Redshift - Still Somewhat Experimental
#########################################################################################
# 2 VALIDATION CLASSES
#  - Each has a validate method which returns True if all is good, else returns False and prints an error
#  - You may need to set global vars in your job before using RecordLoss & Aggregate class logic below
#  - Each accepts target(i.e. Redshift), target_schema, target_table and possibly other params
##################
#   - Structure - Checks for 
#      - if the same number of columns in the csv are in the target table
#      = if the columns have the same datatypes 
#         -- uses python_redshift_dtypes to convert 
#   - LoadError - Checks for load errors for the target:schema:table provided since the load_start provided timestamp
##################
# Use the wrapper class RunAnywayTarget if you want to make it easier as we make each validation scheme better
# pass in the taskobj with the following properties
  # - type = ['LoadError', 'Structure']
  # - target = Redshift
  # - table =
  # - schema = 
  # - local_file = local csv file path
  # - load_start = when you started to copy the records from S3
# doing RunAnywayTarget(self).done() will not do validation
# doing RunAnywayTarget(self).validation will do the validation and if successful also say we're done the task
##########################################################################################

import luigi
import os
import random
import json
import os
import time
from time import sleep
import errno
from multiprocessing import Value
import tempfile
import hashlib
import logging
from datetime import datetime,timedelta
import pandas as pd
from . import sources

logger = logging.getLogger('luigi-interface')
NOW = datetime.utcnow()

# conversion from python (incl numpy dtypes to redshift dtypes)
python_redshift_dtypes = {
                   'datetime'   : ['TIMESTAMP', 'TIMESTAMPTZ', 'TIMESTAMP WITHOUT TIME ZONE'],
                   'time'       : ['TIMESTAMP', 'TIMESTAMPTZ'],
                   'date'       : 'DATE',
                   'timedelta'  : 'INTERVAL',
                   'int'        : ['SMALLINT','INTEGER','INT','BIGINT','INT2','INT4','INT8'],
                   'int_'       : ['SMALLINT','INTEGER','INT','BIGINT','INT2','INT4','INT8'],
                   'intc'       : ['SMALLINT','INTEGER','INT','BIGINT','INT2','INT4','INT8'],
                   'intp'       : ['SMALLINT','INTEGER','INT','BIGINT','INT2','INT4','INT8'],
                   'int8'       : ['SMALLINT','INTEGER','INT','BIGINT','INT2','INT4','INT8'],
                   'int16'      : ['SMALLINT','INTEGER','INT','BIGINT','INT2','INT4','INT8'],
                   'int32'      : ['SMALLINT','INTEGER','INT','BIGINT','INT2','INT4','INT8'],
                   'int64'      : ['BIGINT','INT8'],
                   'long'       : ['BIGINT','INT8'],
                   'decimal'    : ['DOUBLE PRECISION','FLOAT','FLOAT8','NUMERIC','DECIMAL','REAL','FLOAT4'],
                   'float'      : ['DOUBLE PRECISION','FLOAT','FLOAT8','NUMERIC','DECIMAL','REAL','FLOAT4'],
                   'float_'     : ['DOUBLE PRECISION','FLOAT','FLOAT8','NUMERIC','DECIMAL','REAL','FLOAT4'],
                   'float16'    : ['DOUBLE PRECISION','FLOAT','FLOAT8','NUMERIC','DECIMAL','REAL','FLOAT4'],
                   'float32'    : ['DOUBLE PRECISION','FLOAT','FLOAT8','NUMERIC','DECIMAL','REAL','FLOAT4'],
                   'float64'    : ['DOUBLE PRECISION','FLOAT','FLOAT8','NUMERIC','DECIMAL','REAL','FLOAT4'],
                   'double'     : ['DOUBLE PRECISION','FLOAT','FLOAT8','NUMERIC','DECIMAL','REAL','FLOAT4'],
                   'bool'       : ['BOOLEAN','BOOL'],
                   'bool_'      : ['BOOLEAN','BOOL'],
                   'str'        : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'object'     : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'json'       : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'tuple'      : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'namedtuple' : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'set'        : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'dict'       : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'list'       : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'unicode'    : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'buffer'     : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'memoryview' : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'bytearray'  : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'bytes'      : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'uuid'       : ['VARCHAR','CHARACTER VARYING','NVARCHAR','TEXT','CHARACTER','NCHAR','BPCHAR'],
                   'None'       : 'NULL',
                   'NaN'        : 'NULL'
                   }
# if redshift datatype is VARCHAR, we should be good no matter what python type it is
redshift_any_dtype = ['VARCHAR','CHARACTER VARYING']


# Wrapper Class To Do the Validation Stuff as a RunAnywayTarget
class RunAnywayTarget(luigi.Target):
  """
  A target used to make a task run everytime it is called and perform Validation.
  Usage:
  Pass `self` as the first argument in your task's `output`:
  .. code-block: python
    def output(self):
      return RunAnywayTarget(self)
  And then mark it as `done` in your task's `run`:
  .. code-block: python
    def run(self):
      # Your task execution
      # ...
      self.output().done() # will then be considered as "existing"
  """

  # Specify the location of the temporary folder storing the state files. Subclass to change this value
  temp_dir = os.path.join(tempfile.gettempdir(), 'luigi-simulate')
  temp_time = 24 * 3600  # seconds

  # Unique value (PID of the first encountered target) to separate temporary files between executions and
  # avoid deletion collision
  unique = Value('i', 0)

  def __init__(self, task_obj):
    self.task_id = task_obj.task_id

    if self.unique.value == 0:
      with self.unique.get_lock():
        if self.unique.value == 0:
          self.unique.value = os.getpid()  # The PID will be unique for every execution of the pipeline

    # Deleting old files > temp_time
    if os.path.isdir(self.temp_dir):
      import shutil
      import time
      limit = time.time() - self.temp_time
      for fn in os.listdir(self.temp_dir):
        path = os.path.join(self.temp_dir, fn)
        if os.path.isdir(path) and os.stat(path).st_mtime < limit:
          shutil.rmtree(path)
          logger.debug('Deleted temporary directory %s', path)
    
    # for ETL validation 
    try:  
      self.type = task_obj.type_
    except:
      self.type = ['LoadError', 'Structure']
    try:
      self.target = task_obj.target
    except:
      self.target = 'Redshift'
    try:
      self.target_schema = task_obj.schema
    except:
      self.target_schema = None
    try:
      self.target_table = task_obj.table
    except:
      self.target_table = None
    try:
      self.df = task_obj.local_file
    except:
      self.df = None
    try:
      self.load_start = task_obj.load_start
    except:
      self.load_start = None
  
  def get_path(self):
    """
    Returns a temporary file path based on a MD5 hash generated with the task's name and its arguments
    """
    md5_hash = hashlib.md5(self.task_id.encode()).hexdigest()
    logger.debug('Hash %s corresponds to task %s', md5_hash, self.task_id)

    return os.path.join(self.temp_dir, str(self.unique.value), md5_hash)

  def exists(self):
    """
    Checks if the file exists
    """
    return os.path.isfile(self.get_path())

  def done(self):
    """
    Creates temporary file to mark the task as `done`
    """
    logger.info('Marking %s as done', self)

    fn = self.get_path()
    os.makedirs(os.path.dirname(fn), exist_ok=True)
    open(fn, 'w').close()

  def validation(self):
    if isinstance(self.type,list):
      result = {}
      for t in self.type:
        result[t] = getattr(self,t)()
    else:
        result = {self.type: get_attr(self,self.type)()}
    
    #if all of the validation is complete, we're done
    if all(list(result.values())):
      self.done()

  def Structure(self):
    return Structure(target=self.target, target_schema=self.target_schema, target_table=self.target_table, df=self.df).validate()

  def LoadError(self):
    return LoadError(target=self.target, target_schema=self.target_schema, target_table=self.target_table, load_start=self.load_start).validate()


class Structure:
  def __init__(self, target='Redshift', target_schema='None', target_table='None', df=None):
    if df:
      self.df = pd.DataFrame.from_csv(df,index_col=None)
    else:
      self.df = df 
    self.target = target
    self.target_schema = target_schema
    self.target_table = target_table

  def datatype_conversion(self, datatype):
    if self.target == 'Redshift':
      return python_redshift_dtypes[datatype]

  def redshift_columns(self):
    redshift = sources.Redshift()
    redshift.cursor.execute("""SET SEARCH_PATH='{0}';
                               SELECT 
                                 "column" 
                               FROM pg_table_def 
                               WHERE tablename = '{1}'""".format(self.target_schema, self.target_table))
    result = redshift.cursor.fetchall()
    return [i[0] for i in result]

  def column_mapping(self):
    rs_columns = self.redshift_columns()
    df_columns = self.df.columns.tolist()
    assert isinstance(rs_columns, list), 'No redshift columns'
    assert isinstance(df_columns, list), 'No local columns'
    return len(df_columns) == len(rs_columns)

  def redshift_column_types(self):
    redshift = sources.Redshift()
    redshift.cursor.execute("""SET SEARCH_PATH='{0}';
                              SELECT 
                                TRIM(
                                     LOWER(
                                           SPLIT_PART("type",'(',1)
                                          ) 
                                     ) 
                              FROM pg_table_def 
                              WHERE tablename = '{1}'""".format(self.target_schema, self.target_table))
    result = redshift.cursor.fetchall()
    return [i[0] for i in result]

  def column_type_mapping(self):
    rs_columns = self.redshift_column_types()
    df_columns = [str(t).lower() for t in self.df.dtypes]
    i = 0
    for col in df_columns:
      if rs_columns[i] in [t.lower() for t in redshift_any_dtype]:
        continue
      elif isinstance(self.datatype_conversion(col), list):
        assert rs_columns[i] in [t.lower() for t in self.datatype_conversion(col)],'Column data type mismatch'
      else:
        assert rs_columns[i] == self.datatype_conversion(col).lower(),'Column data type mismatch'
      i += 1
    return True

  def validate(self):
    if self.column_type_mapping() & self.column_mapping():
      return True
    else:
      print('''\n {0}:{1}.{2} Number of Columns or Column Data Type Mismatch \n'''.format(self.target,self.target_schema,self.target_table))
      return False


class LoadError:
  def __init__(self, target='Redshift', target_schema='None', target_table='None', load_start=None):
    self.target = target
    self.target_schema = target_schema
    self.target_table = target_table
    if load_start:
      self.load_start = load_start
    else:
      self.load_start = (datetime.utcnow()-timedelta(seconds=60*5)).strftime('%Y-%m-%d %H:%M:%S')

  def query_errors(self):
    return """SET SEARCH_PATH='{0}';
              SELECT
                name AS table,
                err_reason,
                starttime,
                filename,
                line_number,
                colname,
                type,
                col_length,
                raw_field_value,
                raw_line
              FROM stl_load_errors sl, stv_tbl_perm sp
              WHERE 
                  sl.tbl = sp.id
                AND name = '{1}'
                AND starttime >= TIMESTAMP '{2}';""".format(self.target_schema, self.target_table, self.load_start)

  def query_errors_abreviated(self):
    redshift = sources.Redshift()
    redshift.cursor.execute("""SET SEARCH_PATH='{0}';
                               SELECT
                                 err_reason,
                                 colname
                               FROM stl_load_errors sl, stv_tbl_perm sp
                               WHERE 
                                   sl.tbl = sp.id
                                 AND name = '{1}'
                                 AND starttime >= TIMESTAMP '{2}'
                               GROUP BY 
                                 err_reason,
                                 colname;""".format(self.target_schema, self.target_table, self.load_start))
    return redshift.cursor.fetchall()

  def validate(self):
    query_errors = self.query_errors_abreviated()
    if query_errors:
      title = '''\n {0}:{1}.{2} Load Errors'''.format(self.target,self.target_schema,self.target_table)
      errors = {i[1]:i[0] for i in query_errors}
      print(title)
      print(errors)
      return False
    elif self.query_copy():
      return True
    else:
      return True

  def query_copy(self):
    # To verify load commits, query the STL_UTILITYTEXT table and 
    # look for the COMMIT record that corresponds with a COPY transaction
    sleep(10)
    sql = '''SELECT 
                  status = 1 AS success
                FROM stl_load_commits l, stl_query q
                WHERE l.query=q.query
                      AND exists
                  (SELECT xid FROM stl_utilitytext WHERE xid=q.xid AND rtrim("text")='COMMIT')
                  AND querytxt LIKE '%{0}%'
                  AND starttime >= TIMESTAMP '{1}'
                GROUP BY status;'''.format('.'.join([self.target_schema,self.target_table]), self.load_start)
    redshift = sources.Redshift()
    redshift.cursor.execute(sql)
    try:
      return redshift.cursor.fetchall()[0][0] # returns True if valid
    # bug in Redshift
    except:
      return True


number_conversion = {1:'one',
                     2:'two',
                     3:'three',
                     4:'four',
                     5:'five'}

class StructureDynamic(Structure):
  '''This class will fix tables for you.
        1.  Check for copy errors
        2.  Handle the copy errors
             - Add column(s) if needed
             - Change dtype(s) if needed
        3.  Get orig table's schema 
        4.  Craft new table's schema with changes from errors
        5.  Make the change and retry the copy and remove duplicate * records
        6.  While there are copy errors
              - handle the errors
              - attempt to fix
              - retry copy
              - remove duplicate * records
  '''

  def __error__(self,error):
    new_column = None
    new_dtype = None
    if not error.find('Invalid digit') == -1:
      old_dtype = error.split('Type:')[1].strip().upper()
      new_dtype = error.split("'")[1].strip()
      if new_dtype.isdigit():
        new_dtype = 'INTEGER'
      elif new_dtype.replace('.','').isdigit():
        new_dtype = 'FLOAT'
      else:
        new_dtype = 'VARCHAR'
    elif not error.find('Unknown boolean format') == -1:
      new_dtype = 'VARCHAR'
    elif not error.find('Extra column') == -1:
      new_column = True
    elif not error.find('String length exceeds'):
      new_dtype = 'VARCHAR(MAX)'
    else:
      raise NotImplementedError
    if new_column:
      return {'new_column': new_column}
    elif new_dtype:
      return {'new_dtype' : new_dtype}

  def __handle__(self):
    self.handler = {}
    # get errors from the load and figure out the changes necessary
    errors = self.__errors__()
    print('Errors:\n {0}'.format(errors))
    if not errors:
      return False
    i = 1
    for col in errors.keys():
      handle = self.__error__(errors[col])
      if 'new_dtype' in handle:
        self.handler[col] = handle['new_dtype'] 
      if 'new_column' in handle:
        self.handler['new_column_'+number_conversion[i]] == 'VARCHAR'
        i += 1
    # self.handler = {'column':'dtype'} for any changes necessary
    return True

  def __errors__(self):
    errors = self.__query_errors_abreviated__() 
    if errors:
      for err in errors:
        if err[1] == '':
          err[1] = str(random.random())
      errors = {i[1]:i[0] for i in errors}
    # make sure there aren't any errors we can't fix right now
      for col in errors.keys():
        assert errors[col] != 'File contains a blank line', 'Your local data file has blank line(s)'
      return errors

  def __current_table__(self):
    redshift = sources.Redshift()
    redshift.cursor.execute("""SET SEARCH_PATH='{0}';
                              SELECT 
                                "column",
                                TRIM(
                                     LOWER(
                                           SPLIT_PART("type",'(',1)
                                          ) 
                                     ) 
                              FROM pg_table_def 
                              WHERE tablename = '{1}'""".format(self.target_schema, self.target_table))
    result = redshift.cursor.fetchall()
    self.current_table = {i[0]:i[1] for i in result}

  def __new_table__(self):
    # TODO - don't change self.current_table values
    self.new_table = self.current_table
    for col in self.handler:
      self.new_table[col] = self.handler[col]
    if self.sortkey:
      for col in self.sortkey:
        self.new_table[col] += ' SORTKEY' 
    if self.distkey:
      for col in self.distkey:
        self.new_table[col] += ' DISTKEY'

  def run(self, add_cols=False, change_dtypes=False, copy=None, load_start=None):
    assert copy
    self.add_cols = add_cols
    self.change_dtypes = change_dtypes
    self.copy = copy
    self.load_start = load_start
    errors = self.__handle__()
    if errors:
      self.__current_table__()
      self.__distkey__()
      self.__sortkey__()
      self.__new_table__()
      self.__change__()

  def __change__(self):
    while self.handler:
      sql = 'BEGIN;'             \
            + self.__path__()    \
            + self.__backup__()  \
            + self.__table__()   \
            + self.__columns__() \
            + ');'               \
            + self.__insert__()  \
            + self.copy          \
            + self.__remove_duplicates__() \
            + self.__permissions__() \
            + self.__drop__()      \
            + 'COMMIT;'
      redshift = sources.Redshift()
      self.load_start = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
      redshift.cursor.execute(sql)
      self.__handle__()
    
    title = '''<!group> \n {0}.{1} Load Errors\n'''.format(self.target_schema,self.target_table)
    title += 'I have modified table schemas successfully'
    result = {  'The original table that failed during the copy'              : '{0}_orig_backup'.format(self.target_table) ,
                          'The new table with schema I created to get the copy to work' :'{0}'.format(self.target_table) ,
                          'The new schema I created'                                    :'\n, '.join(self.__columns__().split(', '))     }
    print(title)
    print(result)

  def __backup__(self):
    return 'ALTER TABLE {0} RENAME TO {0}_orig_backup; CREATE TABLE {0}_backup AS SELECT * FROM {0}_orig_backup;'.format(self.target_table)

  def __unbackup__(self):
    # insert into the backup what's in the new
    # drop new
    # rename backup to new
    raise NotImplementedError

  def __path__(self):
    return "SET search_path='{0}';".format(self.target_schema)

  def __insert__(self):
    if len(self.new_table.keys()) == len(self.current_table.keys()):
      return 'INSERT INTO {0} (SELECT * FROM {0}_backup);'.format(self.target_table)
    else:
      n_cols = len(self.new_table.keys()) - len(self.current_table.keys())
      assert isinstance(n_cols, 'int') 
      assert n_cols > 0
      n_cols = ', NULL' * n_cols
      return 'INSERT INTO {0} (SELECT *'.format(self.target_table) \
             + n_cols \
             + ' FROM {0}_backup);'.format(self.target_table)

  def __table__(self):
    return "CREATE TABLE {0}.{1}(".format(self.target_schema,self.target_table)

  def __columns__(self):
    return ', '.join([col + ' ' + self.new_table[col] for col in self.new_table])

  def __permissions__(self,user='orderup'):
    return 'GRANT SELECT ON TABLE {0} TO {1};'.format(self.target_table,user)

  def __drop__(self):
    return 'DROP TABLE IF EXISTS {0}_backup;'.format(self.target_table)
 
  def __distkey__(self):
    sql = '''SET search_path = '{0}';
             SELECT
               "column" 
             FROM pg_table_def 
             WHERE 
                   tablename = '{1}' 
               AND distkey;
          '''.format(self.target_schema,self.target_table)
    redshift = sources.Redshift()
    redshift.cursor.execute(sql)
    results = redshift.cursor.fetchall()
    self.distkey = [i[0] for i in results]

  def __sortkey__(self):
    sql = '''SET search_path = '{0}';
             SELECT
               "column" 
             FROM pg_table_def 
             WHERE 
                   tablename = '{1}' 
               AND sortkey = 1;
          '''.format(self.target_schema,self.target_table)
    redshift = sources.Redshift()
    redshift.cursor.execute(sql)
    results = redshift.cursor.fetchall()
    self.sortkey = [i[0] for i in results]

  def __primary_key__(self):
    raise NotImplementedError

  def __remove_duplicates__(self):
    return '''CREATE TABLE {0}_temp AS SELECT DISTINCT * FROM {0}; 
              DROP TABLE {0}; 
              ALTER TABLE {0}_temp RENAME TO {0};'''.format(self.target_table)

  def __query_table_errors_abreviated__(self):
    redshift = sources.Redshift()
    redshift.cursor.execute("""SET SEARCH_PATH='{0}';
                               SELECT
                                 err_reason,
                                 colname
                               FROM stl_load_errors sl, stv_tbl_perm sp
                               WHERE 
                                   sl.tbl = sp.id
                                 AND name = '{1}'
                                 AND starttime >= TIMESTAMP '{2}'
                               GROUP BY 
                                 err_reason,
                                 colname;""".format(self.target_schema, self.target_table, self.load_start))
    return redshift.cursor.fetchall()

  def __query_errors_abreviated__(self):
    redshift = sources.Redshift()
    sql = """SET SEARCH_PATH='{0}';
                               SELECT
                                 TRIM(err_reason),
                                 TRIM(colname)
                               FROM stl_load_errors 
                               WHERE 
                                     TRIM(filename) = '{1}'
                                 AND starttime >= TIMESTAMP '{2}'
                               GROUP BY 
                                 err_reason,
                                 colname;""".format(self.target_schema,self.copy.split("'")[1], self.load_start)
    redshift.cursor.execute(sql)
    return redshift.cursor.fetchall()


class OrderedDF:
  def __init__(self, target_cols, df):
    self.df = df
    self.target_cols = target_cols
    self.source_cols = self.df.columns.tolist()
    self.__missing__()
    self.__order__()

  def __missing__(self):
    if len(self.target_cols) == len(self.source_cols):
      return
    elif len(self.target_cols) > len(self.source_cols):
      add_cols = [col for col in self.target_cols if col not in self.source_cols]
      for col in add_cols:
        self.df[col] = None
      self.source_cols = self.df.columns.tolist()
      return
    elif len(self.target_cols) < len(self.source_cols):
      re_cols = [col for col in self.source_cols if col not in self.target_cols]
      for col in re_cols:
        self.df = self.df.drop(col,axis=1)
      self.source_cols = self.df.columns.tolist()
      return

  def __order__(self):
    self.df = self.df[self.target_cols]

