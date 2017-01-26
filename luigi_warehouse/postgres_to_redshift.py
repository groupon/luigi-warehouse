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
#   This module copies data from our production database (postgres) to our reporting database (redshift).
#
#   The PerformFullImport Task consists of 6 Luigi tasks:
#     0.  RemoveLastImport
#
#        -- Deletes the local and S3 files that were created from the previous ExtractPostgresTableToLocalPSV
#
#     1. ExtractPostgresTableToLocalPSV
#
#         -- Extracts Postgres table and stores it locally as a txt.gz, with a | delimiter.
#
#     2. UploadLocalPSVToS3
#
#         -- Uploads that pipe-separated value text file to our S3 bucket (luigi.orderup.com).
#
#     3. GetTableSchema
#
#         -- Generates a CREATE TABLE statement in a SQL file to create the table in a way that conforms
#            to Redshift's contstraints.
#
#     4. CreateTableInRedshift - HAS OPTION TO SPECIFY THE SCHEMA FOR OTHER MODULES TO USE THIS TASK
#
#         -- DROP IF EXISTS table; CREATE TABLE based on the SQL generated in the GetTableSchema task.
#
#     5. CopyTableFromS3ToRedshift
#
#         -- Now that we have our table created, we run the COPY command to move the data from
#            S3 into Redshift.
#
#     5. CreateTempSchema
#
#         -- Creates a temporary schema to be used during import
#
#     5. SwapTempSchema
#
#         -- Swaps out the temporary schema with newly imported data for the old schema
#
#   The PerformIncrementalImport Task consists of the same workflow as above except that incremental data is
#        is loaded to already existing tables
#       - Depends on primary key = id and a field called updated_at
#       - globals import_from_scratch_tables and import_incremental_tabls define which table goes through each workflow
#       - If this task structure fails make sure that the tables already exist in the incremental schema
# 
#
#   This module can be run in parallel using the --workers flag
#     - The key to parallelizing luigi workflows is using external tasks
#     - Note using too many N in --workers N will overwhelm the scheduler
#          - Symptoms include: scheduler timeouts, luigi not running all dependencies, effect other jobs
#          - Some of these symptoms can be avoided if you tweak luigi.cfg (example [core]rpc-connect-timeout)
#          - Tested successfully with up to 100 workers
#
########################################################################################################

import os
import glob
import datetime
import logging
import time
import luigi
from luigi.contrib.simulate import RunAnywayTarget
from . import sources

run_at = datetime.datetime.now().strftime('%m_%d_%Y_T%H_%M_%S')

##############################################
# Remove last imported files from previous run
##############################################
class RemoveLastImport(luigi.ExternalTask):
    table = luigi.Parameter()
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    timestamp = run_at

    def run(self):
        table, timestamp = self.table, self.timestamp
        incremental_schema = luigi.configuration.get_config().get('redshift', 'incremental_schema_name')

        # remove all files on local box, S3 before doing the import.
        for prevfile in glob.glob('data/%(table)s_dbdump_*.txt.gz' % locals()):
            os.remove(prevfile)
        for prevfile in glob.glob('data/%(incremental_schema)s.%(table)s_dbdump_*.txt.gz' % locals()):
            os.remove(prevfile)
        for prevfile in glob.glob('data/%(table)s_create_*.sql' % locals()):
            os.remove(prevfile)

        client = luigi.s3.S3Client()

        # checks only for the existence of the exact file which will be written by this import
        if client.exists(self.s3_bucket + '/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals()):
            client.remove(self.s3_bucket + '/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals())
        print("table still exists in S3?", client.exists(self.s3_bucket + '/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals()))
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

##########################################################################
# Extract each table locally and upload each to S3 using multipart uploads
##########################################################################
class ExtractPostgresTableToLocalPSV(luigi.ExternalTask):
    host_resources = {'postgres': 1, 'memory': 0.4}
    resources = {'postgres': 1}
    table = luigi.Parameter()
    timestamp = run_at

    # ensures files to be written by this import do not already exist
    def requires(self):
        return [RemoveLastImport(self.table)]

    def output(self):
        table, timestamp = self.table, self.timestamp
        ## Temporarily store the table locally as a gzipped text file.
        return luigi.LocalTarget('data/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(), format=luigi.format.Gzip)

    def run(self):
        ## Connect to the postgres database. See modules/sources.py for class.
        ## The included luigi 'postgres' module didn't really have the right methods. Same idea here though.

        ## Use the pg COPY command to write the local file.
        with self.output().open('w') as out_file:
            copy_sql = "copy %s TO STDOUT (FORMAT csv, DELIMITER '|', HEADER 0)" % self.table
            sources.get_pg_source().cursor.copy_expert(copy_sql, out_file)

class UploadLocalPSVToS3(luigi.ExternalTask):
    host_resources = {'s3': 1}
    resources = {'s3': 1}
    table = luigi.Parameter()
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    timestamp = run_at

    def requires(self):
        ## Cannot run until our table has been successfully extracted and saved locally.
        return [ExtractPostgresTableToLocalPSV(self.table)]

    def input(self):
        table, timestamp = self.table, self.timestamp
        return luigi.LocalTarget('data/%(table)s_dbdump_%(timestamp)s.text.gz' % locals())

    def output(self):
        table, timestamp = self.table, self.timestamp
        return luigi.s3.S3Target(self.s3_bucket + '/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(),
                                 format=luigi.format.Gzip)

    def run(self):
        table, timestamp = self.table, self.timestamp
        client = luigi.s3.S3Client()
        client.put_multipart('data/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(),
                             self.s3_bucket + '/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(),
                             part_size=67108864)

###############################################################
# For each table do below clasess reverse sequentailly below
# Create table and copy to table redshift will be in parallel
# Wrapper to import everything listed
###############################################################
class CreateTempSchema(luigi.ExternalTask):
    host_resources = {'redshift': 1}
    resources = {'redshift': 1}
    timestamp = run_at

    def output(self):
        timestamp = self.timestamp
        ## This will be a .SQL file that we'll use to create our table in Redshift.
        return luigi.LocalTarget('data/temp_schema_%(timestamp)s.sql' % locals())

    def run(self):
        import_schema_name = luigi.configuration.get_config().get('redshift', 'import_schema_name')
        rs_user = luigi.configuration.get_config().get('redshift', 'user')

        redshift_source = sources.Redshift()
        create_schema_sql = """DROP SCHEMA IF EXISTS %(import_schema_name)s CASCADE;
                               CREATE SCHEMA %(import_schema_name)s;
                               SET search_path TO %(import_schema_name)s;
                               GRANT ALL ON SCHEMA %(import_schema_name)s TO %(rs_user)s;
                               GRANT USAGE ON SCHEMA %(import_schema_name)s TO %(rs_user)s;
                               GRANT SELECT ON ALL TABLES IN SCHEMA %(import_schema_name)s TO %(rs_user)s;
                               COMMENT ON SCHEMA %(import_schema_name)s IS 'temporary refresh schema';""" % locals()
        redshift_source.cursor.execute(create_schema_sql)

        with self.output().open('w') as out_file:
            out_file.write("temp schema created at %s" % self.timestamp)

class GetTableSchema(luigi.ExternalTask):
    host_resources = {'postgres': 1}
    resources = {'postgres': 1}
    table = luigi.Parameter()
    bypassTempSchema = luigi.Parameter(default=False)
    timestamp = run_at

    # ensures files to be written by this import do not already exist
    def requires(self):
        return [RemoveLastImport(self.table)]

    def output(self):
        table, timestamp = self.table, self.timestamp
        bypass_marker = "bypassTempSchema" if self.bypassTempSchema else ""
        ## This will be a .SQL file that we'll use to create our table in Redshift.
        return luigi.LocalTarget('data/%(table)s_create_%(timestamp)s%(bypass_marker)s.sql' % locals())

    def export_postgres_table_schema(self):

        schema_sql = """SELECT  column_name
                              , udt_name
                              , character_maximum_length
                              , numeric_precision
                              , numeric_scale
                        FROM information_schema.columns
                        WHERE    table_schema = 'public'
                             AND table_name = '%s'""" % self.table

        return sources.get_pg_source().query(schema_sql)

    def identify_primary_keys(self):
        table = self.table
        ## Go figure out what this table is indexed on.
        sql = """SELECT 
                   a.attname, 
                   format_type(a.atttypid, a.atttypmod) AS data_type
                 FROM pg_index i
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                 WHERE   i.indrelid = '%s'::regclass
                     AND i.indisprimary""" % self.table

        result = sources.get_pg_source().query(sql)
        #keep only the first col of each row
        primary_keys = [row[0] for row in result]
        return (primary_keys)

    def generate_create_sql(self, columns):
        table = self.table
        temp_schema_name = luigi.configuration.get_config().get('redshift', 'import_schema_name')
        actual_schema_name = luigi.configuration.get_config().get('redshift', 'actual_schema_name')
        schema_name = actual_schema_name if self.bypassTempSchema else temp_schema_name

        acceptable_redshift_data_types = ('int2', 'int', 'int4', 'bigint', 'int8', 'decimal', 'numeric', 'real', 'float4', 'double precision', 'float8', 'float', 'boolean', 'bool', 'char', 'character', 'nchar', 'bpchar', 'varchar', 'character varying', 'nvarchar', 'text', 'date', 'timestamp', 'timestamp with time zone', 'timestamp without time zone')
        create_sql = "CREATE table %(schema_name)s.%(table)s(" % locals()

        for attributes in columns:
            column_name = attributes[0]
            data_type = attributes[1]
            char_max = '' if attributes[2] is None else attributes[2]
            num_precision = '' if attributes[3] is None else attributes[3]
            num_scale = '' if attributes[4] is None else attributes[4]

            if data_type == 'varchar':
                data_type = 'text' if char_max == 255 else 'varchar(%(char_max)s)' % locals()

            elif data_type == 'numeric':
                if num_precision == '':
                    data_type = 'numeric'
                else:
                    data_type = 'numeric(%(num_precision)s, %(num_scale)s)' % locals()

            elif data_type not in acceptable_redshift_data_types:
                data_type = 'varchar(65535)'

            create_sql += '"%(column_name)s" %(data_type)s, ' % locals()

        create_sql = create_sql[:-2]
        create_sql += ")"

        # if the table has any primary keys, use the first as sortkey and distkey
        # otherwise, distribute + sort on the first column
        primkeys = self.identify_primary_keys()
        sortkey = primkeys[0] if len(primkeys) > 0 else columns[0][0]
        distkey = primkeys[0] if len(primkeys) > 0 else columns[0][0]

        create_sql += " DISTKEY(%(distkey)s) SORTKEY(%(sortkey)s)" % locals()

        return create_sql

    def run(self):
        ## First, we'll export the columns from the information_schema.columns table in our PG database.
        columns = self.export_postgres_table_schema()
        ## Next, we'll generate some SQL that conforms to Redshift's standards.
        create_sql = self.generate_create_sql(columns)
        ## Finally, we'll output the SQL and store it locally.
        with self.output().open('w') as out_file:
            out_file.write(create_sql)

class CreateTableInRedshift(luigi.ExternalTask):
    host_resources = {'redshift': 1}
    resources = {'redshift': 1}
    table = luigi.Parameter()
    bypassTempSchema = luigi.BoolParameter(default=False)
    timestamp = run_at

    def input(self):
        table, timestamp = self.table, self.timestamp
        bypass_marker = "bypassTempSchema" if self.bypassTempSchema else ""
        return luigi.LocalTarget('data/%(table)s_create_%(timestamp)s%(bypass_marker)s.sql' % locals())

    def requires(self):
        yield GetTableSchema(self.table, self.bypassTempSchema)
        if not self.bypassTempSchema:
            yield CreateTempSchema()

    def run(self):
        table = self.table
        temp_schema_name = luigi.configuration.get_config().get('redshift', 'import_schema_name')
        actual_schema_name = luigi.configuration.get_config().get('redshift', 'actual_schema_name')
        schema_name = actual_schema_name if self.bypassTempSchema else temp_schema_name
        looker_user = luigi.configuration.get_config().get('redshift', 'looker_user')

        drop_sql = "DROP TABLE IF EXISTS %(schema_name)s.%(table)s " % locals()
        create_sql = self.input().open("r").read()
        permissions_sql = "GRANT select on %(schema_name)s.%(table)s to %(looker_user)s;" % locals()
        full_sql = "BEGIN; " + drop_sql + "; " + create_sql +    "; " + permissions_sql + " COMMIT;"

        redshift_source = sources.Redshift()
        redshift_source.cursor.execute(full_sql)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

class CopyTableFromS3ToRedshift(luigi.ExternalTask):
    host_resources = {'redshift': 1}
    resources = {'redshift': 1}
    table = luigi.Parameter()
    bypassTempSchema = luigi.BoolParameter(default=False)
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    timestamp = run_at

    def requires(self):
        return UploadLocalPSVToS3(self.table), CreateTableInRedshift(self.table, self.bypassTempSchema)

    def run(self):
        redshift = sources.Redshift()

        table = self.table
        timestamp = self.timestamp
        temp_schema_name = luigi.configuration.get_config().get('redshift', 'import_schema_name')
        actual_schema_name = luigi.configuration.get_config().get('redshift', 'actual_schema_name')
        schema_name = actual_schema_name if self.bypassTempSchema else temp_schema_name

        s3_path = "'" + self.s3_bucket + "/%(table)s_dbdump_%(timestamp)s.txt.gz" % locals() + "'"
        aws_credentials = "'aws_access_key_id=" + redshift.aws_access_key_id + ";" + "aws_secret_access_key=" + redshift.aws_secret_access_key + "'"
        options = """CSV DELIMITER '|' IGNOREHEADER 0 
                     ACCEPTINVCHARS TRUNCATECOLUMNS GZIP 
                     TRIMBLANKS BLANKSASNULL EMPTYASNULL 
                     DATEFORMAT 'auto' ACCEPTANYDATE 
                     REGION '{0}' COMPUPDATE ON MAXERROR 1;""".format(redshift.s3_region)

        sql = 'COPY %(schema_name)s.%(table)s FROM %(s3_path)s CREDENTIALS %(aws_credentials)s %(options)s' % locals()
        redshift.cursor.execute(sql)

        # remove the s3 file now that we're done with it
        try:
            client = luigi.s3.S3Client()
            if client.exists(self.s3_bucket + '/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals()):
                client.remove(self.s3_bucket + '/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals())
        except:
            logging.error("Unable to remove %(table)s_dbdump_%(timestamp)s.txt.gz from S3; remove this file manually" % locals())

        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

class ImportTableFromPostgresToRedshift(luigi.WrapperTask):
    table = luigi.Parameter()
    bypassTempSchema = luigi.BoolParameter(default=False)

    def requires(self):
        yield CopyTableFromS3ToRedshift(self.table, self.bypassTempSchema)

class ImportAllTablesFromPostgresToRedshift(luigi.WrapperTask):
    import_from_scratch_tables = luigi.ListParameter()

    def requires(self):
        for table in self.import_from_scratch_tables:
            yield ImportTableFromPostgresToRedshift(table)

class SwapTempSchema(luigi.ExternalTask):
    import_from_scratch_tables = luigi.ListParameter()

    def requires(self):
        return [ImportAllTablesFromPostgresToRedshift(self.import_from_scratch_tables)]

    def run(self):
        import_schema_name = luigi.configuration.get_config().get('redshift', 'import_schema_name')
        actual_schema_name = luigi.configuration.get_config().get('redshift', 'actual_schema_name')
        backup_schema_name = luigi.configuration.get_config().get('redshift', 'backup_schema_name')
        rs_user = luigi.configuration.get_config().get('redshift', 'user')
        looker_user = luigi.configuration.get_config().get('redshift', 'looker_user')

        redshift_source = sources.Redshift()
        swap_schema_sql = """BEGIN;
                             CREATE SCHEMA IF NOT EXISTS %(actual_schema_name)s;
                             DROP SCHEMA IF EXISTS %(backup_schema_name)s CASCADE;
                             ALTER SCHEMA %(actual_schema_name)s RENAME TO %(backup_schema_name)s;
                             ALTER SCHEMA %(import_schema_name)s RENAME TO %(actual_schema_name)s;
                             GRANT ALL ON SCHEMA %(actual_schema_name)s TO %(rs_user)s;
                             GRANT USAGE ON SCHEMA %(actual_schema_name)s TO %(rs_user)s;
                             GRANT SELECT ON ALL TABLES IN SCHEMA %(actual_schema_name)s TO %(rs_user)s;
                             GRANT ALL ON SCHEMA %(actual_schema_name)s TO %(looker_user)s;
                             GRANT USAGE ON SCHEMA %(actual_schema_name)s TO %(looker_user)s;
                             GRANT SELECT ON ALL TABLES IN SCHEMA %(actual_schema_name)s TO %(looker_user)s;
                             COMMENT ON SCHEMA %(actual_schema_name)s IS 'analytics data schema';
                             COMMIT;""" % locals()
        # print(swap_schema_sql)
        redshift_source.cursor.execute(swap_schema_sql)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

class PerformFullImport(luigi.WrapperTask):
    import_from_scratch_tables = luigi.ListParameter()

    def requires(self):
        return[SwapTempSchema(self.import_from_scratch_tables)]

#################################################################
# Below is the same relative workflow as above except
# It performs an incremental import to the already existing table
# after checking that that table exists already with some data
#################################################################
class IncrementalTableExistence(luigi.ExternalTask):
    host_resources = {'redshift': 1}
    resources = {'redshift': 1}
    table = luigi.Parameter()
    incremental_schema = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'incremental_schema_name'))

    def table_exists(self):
        redshift = sources.Redshift()
        cursor = redshift.cursor
        cursor.execute("""
                          SET SEARCH_PATH = '{0}';
                          SELECT tablename
                          FROM pg_table_def
                          WHERE schemaname = '{1}' GROUP BY 1;""".format(self.incremental_schema, self.incremental_schema))
        tables = cursor.fetchall()
        return [table for (table,) in tables if self.table in table] == [self.table] or [table for (table,) in tables if self.table == table] == [self.table]

    def run(self):
        if self.table_exists():
            self.output().done()
        else:
            print('WARNING - Incremental Import will fail on table {0}.{1}'.format(self.incremental_schema, self.table))

    def output(self):
        return RunAnywayTarget(self)

class ExtractPostgresTableToLocalPSVIncrementally(luigi.ExternalTask):
    host_resources = {'postgres': 1}
    resources = {'postgres': 1}
    table = luigi.Parameter()
    incremental_schema = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'incremental_schema_name'))
    timestampfield = luigi.Parameter(default='updated_at')
    actual_schema_name = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'actual_schema_name'))
    timestamp = run_at
    extradaysback = luigi.IntParameter(default=0)

    def requires(self):
        return [IncrementalTableExistence(self.table, self.incremental_schema), RemoveLastImport(self.table)]

    def get_last_import_time(self):
        incremental_schema = self.incremental_schema
        table, timestampfield = self.table, self.timestampfield
        time_format = '%Y-%m-%d %H:%M:%S'

        try:
            redshift = sources.Redshift()
            hoursback = 1 + 24*self.extradaysback
            redshift.cursor.execute('SELECT MAX(dateadd(hour,-%(hoursback)i,%(timestampfield)s::timestamp)) FROM %(incremental_schema)s.%(table)s;' % locals())
            result = redshift.cursor.fetchall()[0][0]
            lastimport = result.strftime(time_format)
            return lastimport
        except:
            logging.error("Unable to obtain previous import time for table %s" % self.table)
            start_time = (datetime.datetime.now()-datetime.timedelta(days=7))
            return start_time.strftime(time_format)

    def output(self):
        table, timestampfield = self.table, self.timestampfield
        timestamp, incremental_schema = self.timestamp, self.incremental_schema
        ## Temporarily store the table locally as a gzipped text file.
        return luigi.LocalTarget('data/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(), format=luigi.format.Gzip)
        # return luigi.LocalTarget('data/%(table)s_dbdump_%(timestamp)s.txt.gz' % locals())

    def run(self):
        ## Connect to the postgres database. See modules/sources.py for class.
        ## The included luigi 'postgres' module didn't really have the right methods. Same idea here though.
        table, timestampfield = self.table, self.timestampfield
        incremental_schema, actual_schema_name = self.incremental_schema, self.actual_schema_name
        lastimport = self.get_last_import_time()
        # import pdb;pdb.set_trace()

        ## Use the pg COPY command to write the local file.
        with self.output().open('w') as out_file:
            copy_sql = "copy (SELECT * FROM %(actual_schema_name)s.%(table)s WHERE %(timestampfield)s > '%(lastimport)s') TO STDOUT (FORMAT csv, DELIMITER '|', HEADER 0)" % locals()
            sources.get_pg_source().cursor.copy_expert(copy_sql, out_file)

class UploadLocalPSVToS3Incrementally(luigi.ExternalTask):
    host_resources = {'s3': 1}
    resources = {'s3': 1}
    table = luigi.Parameter()
    incremental_schema = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'incremental_schema_name'))
    timestampfield = luigi.Parameter(default='updated_at')
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    timestamp = run_at

    def requires(self):
        ## Cannot run until our table has been successfully extracted and saved locally.
        return [ExtractPostgresTableToLocalPSVIncrementally(self.table, self.incremental_schema, self.timestampfield)]

    def input(self):
        table, timestamp, incremental_schema = self.table, self.timestamp, self.incremental_schema
        return luigi.LocalTarget('data/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.text.gz' % locals())

    def output(self):
        table, timestamp, incremental_schema = self.table, self.timestamp, self.incremental_schema
        return luigi.s3.S3Target(
            self.s3_bucket + '/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(),
            format=luigi.format.Gzip)

    def run(self):
        table, timestamp, incremental_schema = self.table, self.timestamp, self.incremental_schema
        client = luigi.s3.S3Client()
        client.put_multipart('data/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(),
                             self.s3_bucket + '/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.txt.gz' % locals(),
                             part_size=67108864)

class CopyTableFromS3ToRedshiftIncrementally(luigi.ExternalTask):
    host_resources = {'redshift': 1}
    resources = {'redshift': 1}
    table = luigi.Parameter()
    incremental_schema = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'incremental_schema_name'))
    timestampfield = luigi.Parameter(default='updated_at')
    primarykey = luigi.Parameter(default='id')
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    timestamp = run_at

    def requires(self):
        return [UploadLocalPSVToS3Incrementally(self.table, self.incremental_schema, self.timestampfield)]

    def run(self):
        redshift = sources.Redshift()
        table, timestampfield = self.table, self.timestampfield
        timestamp, primarykey, incremental_schema = self.timestamp, self.primarykey, self.incremental_schema

        #construct clause to filter rows where ALL primary keys match
        primary_keys = primarykey.split(',')
        match_statements = ["%s.%s.%s=tmp.%s" % (incremental_schema, table, pk, pk) for pk in primary_keys]
        dupe_match_clause = "(" + " AND ".join(match_statements) + ")"

        s3_path = "'" + self.s3_bucket + "/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.txt.gz" % locals() + "'"
        aws_credentials = "'aws_access_key_id=" + redshift.aws_access_key_id + ";" + "aws_secret_access_key=" + redshift.aws_secret_access_key + "'"
        options = """CSV DELIMITER '|' IGNOREHEADER 0 
                     ACCEPTINVCHARS TRUNCATECOLUMNS 
                     GZIP TRIMBLANKS BLANKSASNULL EMPTYASNULL 
                     DATEFORMAT 'auto' ACCEPTANYDATE 
                     REGION '{0}' COMPUPDATE ON MAXERROR 1;""".format(redshift.s3_region)

        sql = """BEGIN;
                 DROP TABLE IF EXISTS %(incremental_schema)s.%(table)s_tmp;
                 CREATE TABLE %(incremental_schema)s.%(table)s_tmp AS (SELECT * FROM %(incremental_schema)s.%(table)s WHERE FALSE);
                 COPY %(incremental_schema)s.%(table)s_tmp FROM %(s3_path)s CREDENTIALS %(aws_credentials)s %(options)s;
                 DELETE FROM %(incremental_schema)s.%(table)s USING %(incremental_schema)s.%(table)s_tmp tmp WHERE %(dupe_match_clause)s;
                 INSERT INTO %(incremental_schema)s.%(table)s (SELECT * FROM %(incremental_schema)s.%(table)s_tmp);
                 DROP TABLE %(incremental_schema)s.%(table)s_tmp;
                 COMMIT;
                  """ % locals()
        redshift.cursor.execute(sql)

        # remove the file now that we're done with it
        try:
            client = luigi.s3.S3Client()
            if client.exists(self.s3_bucket + '/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.txt.gz' % locals()):
                client.remove(self.s3_bucket + '/%(incremental_schema)s.%(table)s_dbdump_%(timestamp)s.txt.gz' % locals())
        except:
            logging.error("Unable to remove %(table)s_dbdump_%(timestamp)s.txt.gz from S3; remove this file manually" % locals())

        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

class PerformIncrementalImport(luigi.WrapperTask):
    import_incremental_tables = luigi.ListParameter()
    incremental_schema = luigi.configuration.get_config().get('redshift', 'incremental_schema_name')

    def requires(self):
        for (table, timestampfield, primarykey) in self.import_incremental_tables:
            yield CopyTableFromS3ToRedshiftIncrementally(table, self.incremental_schema, timestampfield, primarykey)

#################################################################
# copies whole incremental tables to public schema
#################################################################
class CopyIncrementalTablesToPublic(luigi.Task):
    host_resources = {'redshift': 1}
    resources = {'redshift': 1}
    import_incremental_tables = luigi.ListParameter()

    def requires(self):
        return []

    def run(self):
        actual_schema_name = luigi.configuration.get_config().get('redshift', 'actual_schema_name')
        incremental_schema_name = luigi.configuration.get_config().get('redshift', 'incremental_schema_name')
        redshift = sources.Redshift()
        for (table, timestamp, primarykey) in self.import_incremental_tables:
            try:
                distkey = primarykey.split(',')[0]
                redshift.cursor.execute('''
                                           BEGIN;
                                           DROP TABLE IF EXISTS %(actual_schema_name)s.%(table)s;
                                           CREATE TABLE %(actual_schema_name)s.%(table)s DISTKEY(%(distkey)s) SORTKEY(%(primarykey)s) AS SELECT * FROM %(incremental_schema_name)s.%(table)s;
                                           GRANT SELECT ON TABLE %(actual_schema_name)s.%(table)s TO looker;
                                           COMMIT;''' % locals())
            except Exception as e:
                logging.error("Error copying table %s from incremental to public: " % table, e)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

#################################################################
# class to validate load success
#################################################################
class Validation(luigi.ExternalTask):
    table = luigi.Parameter()
    timestampfield = luigi.Parameter(default='created_at')
    rs_schema = luigi.Parameter(default='public')
    host_resources = {'postgres': 1, 'redshift':1}
    resources = {'postgres': 1, 'redshift':1}

    def get_latest(self):
        redshift = sources.Redshift()
        sql_cmd = """SELECT {0}
                     FROM {1}.{2}
                     ORDER BY {0} DESC
                     LIMIT 1""".format(self.timestampfield, self.rs_schema, self.table)
        cursor = redshift.cursor

        cursor.execute(sql_cmd)
        newest = cursor.fetchall()[0][0]
        return newest > datetime.datetime.utcnow() - datetime.timedelta(days=1)

    def get_avg_stats(self):
        sql_cmd = """SELECT AVG(id) FROM {0}""".format(self.table)
        tokyo = sources.get_pg_source().query(sql_cmd)[0][0]

        redshift = sources.Redshift()
        cursor = redshift.cursor
        sql_cmd = """SELECT AVG(id) FROM {0}.{1}""".format(self.rs_schema, self.table)
        cursor.execute(sql_cmd)
        rs = cursor.fetchall()[0][0]
        # ensure pg has same or 3% more at max records in the table
        return ((tokyo >= rs) & (tokyo <= rs * 1.05))

    def get_counts(self):
        sql_cmd = """SELECT COUNT(*) FROM {0}""".format(self.table)
        tokyo = sources.get_pg_source().query(sql_cmd)[0][0]

        redshift = sources.Redshift()
        cursor = redshift.cursor
        sql_cmd = """SELECT COUNT(*) FROM {0}.{1}""".format(self.rs_schema, self.table)
        cursor.execute(sql_cmd)
        rs = cursor.fetchall()[0][0]
        # ensure pg has same or 3% more at max records in the table
        return ((tokyo >= rs) & (tokyo <= rs * 1.05))

    def verify_copy(self):
        # To verify load commits, query the STL_UTILITYTEXT table and
        # look for the COMMIT record that corresponds with a COPY transaction
        sql = '''select 
                     l.query,
                     rtrim(l.filename),
                     q.xid
                 from stl_load_commits l, stl_query q
                 where l.query=q.query
                 and exists
                 (select xid from stl_utilitytext where xid=q.xid and rtrim("text")='COMMIT');'''

    def run(self):
        if self.get_latest() & self.get_avg_stats() & self.get_counts():
            self.output().done()

    def output(self):
        return RunAnywayTarget(self)

#################################################################
# Wrapper to do everything
#################################################################
class Run(luigi.WrapperTask):
    import_from_scratch_tables = luigi.ListParameter() #[ 'orders','customers',... ]
    import_incremental_tables = luigi.ListParameter() #[(table, timestamp_field, primarykey),(),...]

    def requires(self):
        return [PerformIncrementalImport(self.import_incremental_tables),
                PerformFullImport(self.import_from_scratch_tables)]

    # copy all tables from 'incremental' schema to 'public'
    def run(self):
        CopyIncrementalTablesToPublic(self.import_incremental_tables).run()
        for (table, timestampfield, primarykey) in self.import_incremental_tables:
            Validation(rs_schema='public', table=table, timestampfield=timestampfield).run()



if __name__ == '__main__':
    luigi.run()
