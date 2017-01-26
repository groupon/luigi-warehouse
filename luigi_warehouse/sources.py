# The MIT License (MIT)

# Copyright (c) 2013, Groupon, Inc.
# All rights reserved. 
# Copyright (c) 2015 Chris Hall - salesforce-reporting

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


##########################################################################
# sources.py - classes that represent connections to data 
##########################################################################
import os
import sys
import time
import datetime
import calendar
import json
import csv
import re
import urllib
from multiprocessing import Pool, Process
from xml.dom import minidom
from html import escape
import luigi
from luigi.s3 import S3Client
from django.utils.encoding import smart_str
import numpy as np
import requests
import pandas as pd

##############################################################################
# Database APIs
    # - each class has two exposed methods
    #     1.    query     - executes the provided sql and returns [result],[cols]
    #     2.    execute - executes the provided sql
    # - Redshift has the option to copy (also see RedshiftSchemaGenerator below)
##############################################################################

class Postgres:
    def __init__(self,cfg_name='postgres'):
        import psycopg2
        self.host = luigi.configuration.get_config().get(cfg_name, 'host')
        self.database = luigi.configuration.get_config().get(cfg_name, 'database')
        self.user = luigi.configuration.get_config().get(cfg_name, 'user')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.port = luigi.configuration.get_config().get(cfg_name, 'port')
        self.connection = self.__connect__()
        self.cursor = self.connection.cursor()

    def __connect__(self):
        connection = psycopg2.connect(host=self.host,
                                      port=self.port,
                                      database=self.database,
                                      user=self.user,
                                      password=self.password)
        connection.set_client_encoding('utf-8')
        connection.autocommit = True
        return connection

    def query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall(), [row[0] for row in self.cursor.description]

    def execute(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall(), [row[0] for row in self.cursor.description]


class Redshift(Postgres):
    def __init__(self, cfg_name='redshift'):
        import psycopg2
        self.host = luigi.configuration.get_config().get(cfg_name, 'host')
        self.database = luigi.configuration.get_config().get(cfg_name, 'database')
        self.user = luigi.configuration.get_config().get(cfg_name, 'user')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.port = luigi.configuration.get_config().get(cfg_name, 'port')
        self.aws_access_key_id = luigi.configuration.get_config().get(cfg_name, 'aws_access_key_id')
        self.aws_secret_access_key = luigi.configuration.get_config().get(cfg_name, 'aws_secret_access_key')
        self.s3_region = luigi.configuration.get_config().get(cfg_name, 's3_region')
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def copy(self, schema, table, s3_file, options, dedupe=True, post_copy_sql=None):
        self.copy_args = {'schema' : schema,
                          'table'    : table,
                          's3_file': s3_file,
                          'options': options}
        sql = 'BEGIN; ' + self.__copy__()
        if dedupe:
            sql += self.__dedupe__()
        if post_copy_sql:
            sql += post_copy_sql
        sql += ' COMMIT;'
        self.cursor.execute(sql)

    def __copy__(self):
        return '''
                  COPY {0}.{1} FROM '{2}'
                  CREDENTIALS 'aws_access_key_id={3};aws_secret_access_key={4}'
                  {5}; '''.format(self.copy_args['schema'],
                                  self.copy_args['table'],
                                  self.copy_args['s3_file'],
                                  self.aws_access_key_id,
                                  self.aws_secret_access_key,
                                  self.copy_args['options'])

    def __dedupe__(self):
        return ''' CREATE TABLE {0}.{1}_temp AS SELECT DISTINCT * FROM {0}.{1};
                   DROP TABLE {0}.{1};
                   ALTER TABLE {0}.{1}_temp RENAME TO {1}; '''.format(self.copy_args['schema'],
                                                                      self.copy_args['table'])


class Impala:
    def __init__(self,cfg_name='impala'):
        import impala
        self.host = luigi.configuration.get_config().get(cfg_name, 'host')
        self.port = luigi.configuration.get_config().get(cfg_name, 'port')
        self.auth_mechanism = luigi.configuration.get_config().get(cfg_name,'auth_mechanism')
        self.user = luigi.configuration.get_config().get(cfg_name, 'user')
        self.connection = self.__connect__()
        self.cursor = conn.cursor()

    def __connect__(self):
        return impala.dbapi.connect(host=self.host, port=self.port,auth_mechanism=self.auth_mechanism, user=self.user)

    def query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall(),[desc[0] for desc in self.cursor.description]

    def execute(self, sql):
        self.cursor.execute(sql)


class Teradata:
    def __init__(self,cfg_name='teradata'):
        import teradata
        self.appName = luigi.configuration.get_config().get(cfg_name, 'appName')
        self.appVersion = luigi.configuration.get_config().get(cfg_name, 'appVersion')
        self.method = luigi.configuration.get_config().get(cfg_name, 'method')
        self.system = luigi.configuration.get_config().get(cfg_name, 'system')
        self.username = luigi.configuration.get_config().get(cfg_name, 'username')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.udaExec = teradata.UdaExec (appName=self.appName, version=self.appVersion,logConsole=False)
        self.connection = self.__connect__()

    def __connect__(self):
        return self.udaExec.connect(method=self.method,
                                    system=self.system,
                                    username=self.username,
                                    password=self.password)

    def query(self, sql):
        self.udaExec.config["my_query"] = sql
        cursor = self.connection.execute("${my_query}")
        return cursor.fetchall(), [row[0] for row in cursor.description]

    def execute(self, sql):
        self.udaExec.config["my_query"] = sql
        self.connection.execute("${my_query}")


class MySQL:
    def __init__(self, cfg_name='mysql'):
        import pymysql.cursors
        import pymysql
        self.host = luigi.configuration.get_config().get(cfg_name, 'host')
        self.database = luigi.configuration.get_config().get(cfg_name, 'database')
        self.user = luigi.configuration.get_config().get(cfg_name, 'user')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.port = int(luigi.configuration.get_config().get(cfg_name, 'port'))
        self.connection = self.__connect__()
        self.cursor = self.connection.cursor()

    def __connect__(self):
        connection = pymysql.connect(host=self.host,
                                     port=self.port,
                                     database=self.database,
                                     user=self.user,
                                     password=self.password,
                                     autocommit=True,
                                     charset='utf8mb4')
        return connection

    def query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall(),[desc[0] for desc in self.cursor.description]

    def execute(self,sql):
        self.cursor.execute(sql)


class MySQLTunnel(MySQL):
    def __init__(self,cfg_name='mysql_tunnel'):
        import paramiko
        import pymysql.cursors
        import pymysql
        self.host = luigi.configuration.get_config().get(cfg_name, 'host')
        self.database = luigi.configuration.get_config().get(cfg_name, 'database')
        self.user = luigi.configuration.get_config().get(cfg_name, 'user')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.port = int(luigi.configuration.get_config().get(cfg_name, 'port'))
        self.tunnel_dest_host = luigi.configuration.get_config().get(cfg_name, 'tunnel_dest_host')
        self.tunnel_dest_user = luigi.configuration.get_config().get(cfg_name, 'user')
        self.tunnel_source_host = luigi.configuration.get_config().get(cfg_name, 'tunnel_source_host ')
        self.tunnel_source_port = luigi.configuration.get_config().get(cfg_name, 'tunnel_source_port')
        self.channel = self.__tunnel__()
        self.connection = self.__connect__()
        self.cursor = self.connection.cursor()

    def __tunnel__(self):
        tunnel = paramiko.SSHClient()
        tunnel.load_system_host_keys()
        tunnel.connect(self.tunnel_dest_host,username=self.tunnel_dest_user)
        transport = tunnel.get_transport()
        return transport.open_channel('direct-tcpip',
                                      (self.host, self.port),
                                      (self.tunnel_source_host,self.tunnel_source_port))

    def __connect__(self):
        c = pymysql.connect(database=self.database,
                            user=self.user,
                            password=self.password,
                            defer_connect=True)
        c.connect(self.channel)
        return c


global singleton_pg_source
singleton_pg_source = None

def get_pg_source():
    global singleton_pg_source
    if singleton_pg_source is None:
        singleton_pg_source = Postgres()
    return singleton_pg_source

#################################################################
# RedshiftSchema Generator
#     - given a pandas dataframe and some options
#         create a redshift table with an equivalent redshift schema
#################################################################

class RedshiftSchemaGenerator:
    def __init__(self, df, schema, table, primary_key=None, distkey=None, sortkey=None, user=None, overwrite=False, all_varchar=False):
        import psycopg2
        self.type_conversion = {'datetime'     : 'TIMESTAMP',
                                'time'         : 'TIMESTAMP',
                                'date'         : 'DATE',
                                'timedelta'    : 'INTERVAL',
                                'int'          : 'INTEGER',
                                'int_'         : 'INTEGER',
                                'intc'         : 'INTEGER',
                                'intp'         : 'INTEGER',
                                'int8'         : 'INTEGER',
                                'int16'        : 'INTEGER',
                                'int32'        : 'INTEGER',
                                'int64'        : 'BIGINT',
                                'long'         : 'BIGINT',
                                'decimal'      : 'FLOAT',
                                'float'        : 'FLOAT',
                                'float_'       : 'FLOAT',
                                'float16'      : 'FLOAT',
                                'float32'      : 'FLOAT',
                                'float64'      : 'FLOAT',
                                'double'       : 'FLOAT',
                                'bool'         : 'BOOLEAN',
                                'bool_'        : 'BOOLEAN',
                                'str'          : 'VARCHAR',
                                'object'       : 'VARCHAR',
                                'json'         : 'VARCHAR',
                                'tuple'        : 'VARCHAR',
                                'namedtuple'   : 'VARCHAR',
                                'set'          : 'VARCHAR',
                                'dict'         : 'VARCHAR',
                                'list'         : 'VARCHAR',
                                'unicode'      : 'VARCHAR',
                                'buffer'       : 'VARCHAR',
                                'memoryview'   : 'VARCHAR',
                                'bytearray'    : 'VARCHAR',
                                'bytes'        : 'VARCHAR',
                                'uuid'         : 'VARCHAR',
                                'None'         : 'NULL',
                                'NaN'          : 'NULL'}
        self.df = df
        self.columns = df.columns
        self.dtypes = df.dtypes
        self.schema = schema
        self.table = table
        self.primary_key = primary_key
        self.distkey = distkey
        self.sortkey = sortkey
        self.user = user
        self.overwrite = overwrite
        self.all_varchar = all_varchar
        self.sql = self._generate()

    def execute(self):
        redshift = RedshiftSource()
        print(self.sql)
        redshift.cursor.execute('BEGIN;' + self.sql + 'COMMIT;')

    def _generate(self):
        sql = self._schema() + self._path()        
        if self.overwrite:
            sql += self._overwrite()
        sql += self._table() + self._columns() 
        if self.primary_key:
            sql += self._primary_key() + ');'
        else:
            sql = sql[:-1] + ');'
        if self.user:
            sql += self._permissions(user=self.user)
        return sql

    def _primary_key(self):
        return 'primary key({0})'.format(self.primary_key)

    def _distkey(self):
        return ' DISTKEY'

    def _sortkey(self):
        return ' SORTKEY'

    def _columns(self):
        if self.all_varchar:
            return ''.join(''.join(map(str,    ['"' + a + '" VARCHAR, ' for a in self.columns])).rsplit(',', 1))
        else:
            sql_cols = []
            for col in self.columns:
                col_name = col.lower().replace(' ','_')
                col_type = self.type_conversion[str(self.dtypes[col])]
                if col_type == 'VARCHAR':
                    try:
                        length = self.df[col].map(len).max()
                        if length <= 65535:
                            col_type == 'VARCHAR({0})'.format(length)
                        else:
                            col_type == 'VARCHAR(MAX)'
                    except:
                        col_type == 'VARCHAR(MAX)'
                if col == self.distkey:
                    col_type += self._distkey()
                if col == self.sortkey:
                    col_type += self._sortkey()
                sql_cols += ['{0} {1},'.format(col_name,col_type)]
            return ' '.join(sql_cols)

    def _schema(self):
        return '''CREATE SCHEMA IF NOT EXISTS {0};'''.format(self.schema)

    def _table(self):
        return '''CREATE TABLE IF NOT EXISTS {0}('''.format(self.table)

    def _path(self):
        return '''SET search_path='{0}';'''.format(self.schema)

    def _overwrite(self):
        return '''DROP TABLE IF EXISTS {0};'''.format(self.table)

    def _permissions(self, user):
        return '''GRANT ALL ON SCHEMA {0} TO {1};
                  GRANT SELECT ON TABLE {0}.{2} TO {1};'''.format(self.schema,user,self.table)

###############################################################################
# VariableS3Client
#     - for using the luigi S3Client with different luigi.cfg name spaces
#     - i.e. buckets in different regions or different S3 bucket/user permissions
###############################################################################

class VariableS3Client(S3Client):

    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 s3_cfg='s3'):
            # only import boto when needed to allow top-lvl s3 module import
        import boto
        import boto.s3.connection
        from boto.s3.key import Key

        options = self._get_s3_config(s3_cfg=s3_cfg)
        # Removing key args would break backwards compability
        role_arn = options.get('aws_role_arn')
        role_session_name = options.get('aws_role_session_name')

        aws_session_token = None

        if role_arn and role_session_name:
            from boto import sts

            sts_client = sts.STSConnection()
            assumed_role = sts_client.assume_role(role_arn, role_session_name)
            aws_secret_access_key = assumed_role.credentials.secret_key
            aws_access_key_id = assumed_role.credentials.access_key
            aws_session_token = assumed_role.credentials.session_token

        else:
            if not aws_access_key_id:
                aws_access_key_id = options.get('aws_access_key_id')

            if not aws_secret_access_key:
                aws_secret_access_key = options.get('aws_secret_access_key')

        for key in ['aws_access_key_id', 'aws_secret_access_key', 'aws_role_session_name', 'aws_role_arn']:
            if key in options:
                options.pop(key)

        self.s3 = boto.s3.connection.S3Connection(aws_access_key_id,
                                                  aws_secret_access_key,
                                                  security_token=aws_session_token,
                                                  **options)
        self.Key = Key

    def _get_s3_config(self, key=None, s3_cfg='s3'):
        try:
            config = dict(luigi.configuration.get_config().items(s3_cfg))
        except NoSectionError:
            return {}
        # So what ports etc can be read without us having to specify all dtypes
        for k, v in luigi.six.iteritems(config):
            try:
                config[k] = int(v)
            except ValueError:
                pass
        if key:
            return config.get(key)
        return config


##########################################################################################
# # Web APIs
#    - each class represents a different data source
#    - each class exposes self.client if authentication is needed prior to making api calls
#    - each class may have additional helper methods to pull data
##########################################################################################

class Splunk:
    def __init__(self, cfg_name='splunk'):
        self.baseurl = luigi.configuration.get_config().get(cfg_name,'baseurl')
        self.username = luigi.configuration.get_config().get(cfg_name,'username')
        self.password = luigi.configuration.get_config().get(cfg_name,'password')
        self.session_key = self.__login__()

    def __login__(self):
        server_content = requests.post(baseurl + '/services/auth/login',
                                       data={'username':self.username, 'password':self.password}, 
                                       verify=False)
        return minidom.parseString(server_content.content.decode()).getElementsByTagName('sessionKey')[0].childNodes[0].nodeValue

    def search_query(self, query):
        if query.find('search ') == -1:
            query = 'search ' + query
        self.query = query
        self.__submit__()
        self.__status__()
        self.__fetch__()
        return self.results

    def __submit__(self):
        job = requests.post(self.baseurl + '/services/search/jobs',
                            headers = {'Authorization': 'Splunk %s' % self.session_key},
                            verify=False,
                            data={'search': self.query})
        self.sid = minidom.parseString(job.content.decode()).getElementsByTagName('sid')[0].childNodes[0].nodeValue

    def __status__(self):
        services_search_status_str = '/services/search/jobs/%s/' % self.sid
        isNotDone = True
        while isNotDone:
            print("time.sleeping")
            time.sleep(10)
            searchStatus = requests.get(self.baseurl + services_search_status_str,
                                        headers = {'Authorization': 'Splunk %s' % self.session_key},
                                        verify=False).content.decode()
            isDone = re.compile('isDone">1')
            isDoneStatus = isDone.search(searchStatus)
            if isDoneStatus:
                isNotDone = False
            print("============>search status:    %s    <============" % isDoneStatus)

    def __fetch__(self):
        services_search_results_str = '/services/search/jobs/%s/results?output_mode=json&count=0' % self.sid
        self.results = requests.get(self.baseurl + services_search_results_str,
                                    headers = {'Authorization': 'Splunk %s' % self.session_key},
                                    verify=False).json()


class Adwords:
    def __init__(self,cfg_name='googleads'):
        import googleads
        self.config = luigi.configuration.get_config().get(cfg_name, 'yaml')
        self.client = googleads.adwords.AdWordsClient.LoadFromStorage(self.config)

    def set_client_customer_id(self, client_customer_id):
        self.client.client_customer_id = client_customer_id

    def get_report_downloader(self, version='v201605'):
        return self.client.GetReportDownloader(version=version)


class GoogleSheet:
    def __init__(self, cfg_name='google_drive'):
        import gspread
        from oauth2client.client import SignedJwtAssertionCredentials
        credentials_json = json.loads(luigi.configuration.get_config().get(cfg_name, 'credentials_json'))
        client_email = credentials_json["client_email"]
        private_key = credentials_json["private_key"]
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = SignedJwtAssertionCredentials(client_email, private_key.encode(), scope)
        self.client = gspread.authorize(credentials)

    def workbook(self,name):
        return self.client.open(name)


class Slack:
    def __init__(self,cfg_name='slack'):
        import slackclient
        token = luigi.configuration.get_config().get(cfg_name, 'api_token')
        self.client = slackclient.SlackClient(token)

    def slack(self, room, message):
        self.client.rtm_connect()
        return self.client.rtm_send_message(room, message)

    def slack_attachments(self, room, title, message, color='danger'):
        attachments = {'color': color}
        fields = []
        for label, msg in message.items():
            fields.append({'title': label, 'value': msg, 'short': False})
        attachments['fields'] = fields
        attachments = json.dumps([attachments])
        return slack.api_call('chat.postMessage',
                              text=title,
                              attachments=attachments,
                              channel=room,
                              as_user=True,
                              parse='full')


class Braintree:
    def __init__(self,cfg_name='braintree'):
        import braintree
        self.merchant_id = luigi.configuration.get_config().get(cfg_name, 'merchant_id')
        self.public_key = luigi.configuration.get_config().get(cfg_name, 'public_key')
        self.private_key = luigi.configuration.get_config().get(cfg_name, 'private_key')
        braintree.Configuration.configure(braintree.Environment.Production,
                                          merchant_id=self.merchant_id, 
                                          public_key=self.public_key,
                                          private_key=self.private_key)


class Five9:
    def __init__(self,cfg_name='five9'):
        import suds
        import base64
        self.username = luigi.configuration.get_config().get(cfg_name, 'username')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.email = luigi.configuration.get_config().get(cfg_name,'email')
        self.base64string = base64.encodestring(b'Basic ' + self.username.encode() + b':' + self.password.encode())
        self.authenticationHeader = {"Authorization":self.base64string}
        self.wsdl = "https://api.five9.com/wsadmin/AdminWebService?wsdl&user={0}".format(self.email)
        self.client = suds.client.Client(url=self.wsdl, 
                                         password=self.password,
                                         username=self.username,
                                         headers=self.authenticationHeader)


class Twilio:
    def __init__(self, cfg_name='twilio'):
        import twilio
        self.account_id = luigi.configuration.get_config().get(cfg_name, 'account_id')
        self.auth_token = luigi.configuration.get_config().get(cfg_name, 'auth_token')
        self.client = twilio.rest.TwilioRestClient(self.account_id, self.auth_token)


class LiveChat:
    def __init__(self,cfg_name='livechat'):
        self.livechat_url = luigi.configuration.get_config().get(cfg_name, 'livechat_url')
        self.livechat_username = luigi.configuration.get_config().get(cfg_name, 'livechat_username')
        self.livechat_pw = luigi.configuration.get_config().get(cfg_name, 'livechat_pw')
        self.livechat_token = luigi.configuration.get_config().get(cfg_name, 'livechat_token')

    def __request__(self, url):
        headers = {'Accept': 'application/json', 'X-API-Version':'2'}
        return requests.get(url, auth=(self.livechat_username, self.livechat_token), headers=headers)

    def chats(self, date_from, page=1):
        url = self.livechat_url + 'chats?' + 'date_from=' + date_from +'&page=' + str(page)
        return self.__request__(url)

    def agents(self):
        url = self.livechat_url + 'agents?'
        return self.__request__(url)

    def groups(self):
        url = self.livechat_url + 'groups?'
        return self.__request(url)


class Zendesk:
    def __init__(self, cfg_name='zendesk', parameters=None):
        import zdesk
        self.user = luigi.configuration.get_config().get(cfg_name, 'user')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.endpoint = luigi.configuration.get_config().get(cfg_name, 'endpoint')
        self.url = luigi.configuration.get_config().get(cfg_name, 'url')
        self.user_client = zdesk.Zendesk(self.url,self.user,self.password)

    def incremental_tickets(self, start_time):
        headers = {'Accept': 'application/json'}
        zendesk_endpoint = '/exports/tickets.json?start_time='
        url = self.zendesk_url + zendesk_endpoint + str(start_time)
        response = requests.get(url, auth=(self.zendesk_username, self.zendesk_token), headers=headers)
        return response

    def status_handler(self, response):
        if response.status_code==429:
            print('Rate limited. Waiting to retry in ' + response.headers.get('retry-after') + ' seconds.')
            time.sleep(float(response.headers.get('retry-after')))
        if 200 <= response.status_code <= 300:
            print('Success.')
        if response.status_code==422:
            print("Start time is too recent. Try a start_time older than 5 minutes.")
            sys.exit(0)


class ShiftPlanning:
    def __init__(self, cfg_name='shiftplanning', parameters=None):
        self.key = luigi.configuration.get_config().get(cfg_name, 'key')
        self.username = luigi.configuration.get_config().get(cfg_name, 'username')
        self.password = luigi.configuration.get_config().get(cfg_name, 'password')
        self.api_endpoint = "https://www.shiftplanning.com/api/"
        self.output_type = "json"
        self.request = None
        self.token = None
        self.response = None
        self.response_data = None
        self.callback = None
        self.login_params = {"module":"staff.login",
                            "method":"GET",
                            "username":self.username,
                            "password":self.password}

        self.login_reader = urllib.request.urlopen(self.api_endpoint, 
                                                   urllib.parse.urlencode([('data',json.dumps({'key':self.key,'request':self.login_params}))]).encode())
        response = json.loads(self.login_reader.read().decode())
        self.token = response['token']
        self.parameters = parameters
        if not self.parameters is None:
            self.reader = urllib.request.urlopen(self.api_endpoint, 
                                                 urllib.parse.urlencode([('data',json.dumps({'token':self.token,'request':self.parameters}))]).encode())

    def perform_request(self, params):
        reader = urllib.request.urlopen(self.api_endpoint, 
                                        urllib.parse.urlencode([('data',json.dumps({'key':self.key,'request':params}))]).encode())

        if reader.code != 200:
            raise Exception(internal_errors['2'])
        response = reader.read()
        if response == "":
            return (None, "No JSON object received from server.")
        response = json.loads(response)

        if response.has_key('error'):
            return {'error': response['error']}
        else:
            self.response_data = response['data']
            self.response = response
            if self.callback:
                self.callback()
        if params['module'] == 'staff.login':
            if response.has_key('token'):
                self.token = response['token']


class Kochava:
    def __init__(self,cfg_name='kochava'):
        self.username = luigi.configuration.get_config().get(cfg_name, 'username')
        self.api_token = luigi.configuration.get_config().get(cfg_name, 'api_token')
        self.base_url = 'http://control.kochava.com/v1/'

    def apps(self):
        call = self.base_url + 'cpi/get_apps?account={0}&api_key={1}'.format(self.username,self.api_token)
        response = requests.get(call)
        return response.json() 

    def clicks(self, start_date, end_date, kochava_app_id, network, page):
        #     If report length>10,000 records, specific pages must be queried to receive complete data set.
        call = self.base_url + 'cpi/get_clicks?start_date={0}&end_date={1}&app={2}&api_key={3}&network={4}&format=json&page={5}'.format(start_date, end_date, kochava_app_id, self.api_token, network, page)
        response = requests.get(call)
        return response.json()

    def installs(self, start, end, kochava_app_id, campaign_id, rtn_device_id_type = 'android_id,android_md5,android_sha1,imei,imei_md5,imei_sha1,udid,udid_md5,udid_sha1,idfa,idfv,Kochava_device_id,odin,mac,adid'):
        call = self.base_url + 'cpi/get_installs?start_date={0}&end_date={1}&timezone=utc&rtn_device_id_type={2}&campaign_id={3}&kochava_app_id={4}&api_key={5}'.format(start, end, rtn_device_id_type, campaign_id, kochava_app_id, self.api_token)
        response = requests.get(call)
        return response.json()

    def daily_campaign_summary_after(self, kochava_app_id, start_date):
        call = self.base_url + 'cpi/get_campaign_summary?start_date={0}&kochava_app_id={1}&api_key={2}'.format(start_date, kochava_app_id, self.api_token)
        response = requests.get(call)
        return response.json()


class OnboardIQ:
    def __init__(self,cfg_name='onboardiq'):
        private_API_token = luigi.configuration.get_config().get(cfg_name, 'private_API_token')
        public_API_token = luigi.configuration.get_config().get(cfg_name, 'public_API_token')
        self.base_uri = 'https://www.onboardiq.com/api'
        self.headers = {'X-ACCESS-TOKEN':private_API_token}
        self.authenticate = requests.get(self.base_uri, headers=self.headers)

    def get(self, method, data_key):
        pagination = '?page='
        next_page = 1
        data = []
        while next_page is not None:
            print(len(data))
            if next_page == 1:
                json_response = requests.get(self.base_uri + method, headers=self.headers).json()
                if 'error' in json_response:
                    if 'name' in json_response['error'] and json_response['error']['name'] == 'exceeded_rate':
                        time.sleep(60)
                        continue
                    else:
                        break
                data += json_response[data_key]
                try:
                    next_page = json_response['pagination']['next']
                except:
                    next_page = None
            else:
                response = requests.get(self.base_uri + method + pagination + str(next_page), headers=self.headers)
                if 'error' in response.json():
                    if 'name' in response.json()['error'] and response.json()['error']['name'] == 'exceeded_rate':
                        time.sleep(60)
                        continue
                    else:
                        break
                if 'This page does not exist!' in response.text:
                    next_page = None
                else:
                    json_response = response.json()
                    try:
                        json_data = json_response
                        json_data.pop('pagination')
                    except:
                        json_data = json_response
                    data += json_data[data_key]
                    try:
                        next_page = response.json()['pagination']['next']
                    except:
                        next_page = None
        return data #list of dicts

    def applicants(self):
        method = '/v2/applicants'
        return self.get(method, 'applicants')

    def funnels(self):
        method = '/v2/funnels'
        return self.get(method, 'funnels')

    def stages(self, funnel_id):
        method = '/v2/funnels/{0}/stages'.format(funnel_id)
        return self.get(method, 'stages')

    def slots(self, stage_id):
        #available slots only
        method = '/v2/stages/{0}/available_slots'.format(stage_id)
        return self.get(method, 'slots')

    def labels(self, stage_id):
        method = '/v2/stages/{0}/labels'.format(stage_id)
        return self.get(method, 'labels')

    def all_stages(self):
        #funnels data also has stages data
        method = '/v2/funnels'
        funnels = self.funnels()
        stages_per_funnel = [funnel['stages'] for funnel in funnels]
        stages = [val for sublist in stages_per_funnel for val in sublist]
        return stages

    def all_slots(self):
        stages = self.all_stages()
        stage_ids = set([stage['id'] for stage in stages])
        data = []
        for id in stage_ids:
            data += self.slots(id)
        return data

    def all_labels(self):
        stages = self.all_stages()
        stage_ids = set([stage['id'] for stage in stages])
        data = []
        for id in stage_ids:
            data += self.labels(id)
        return data


class Checkr:
    def __init__(self,cfg_name='checkr'):
        self.api_key = luigi.configuration.get_config().get(cfg_name,'api_key')
        self.api_base = 'https://api.checkr.com/v1/'
        self.api_version = None
        self.pagination_limit = 100

    def request(self, endpoint, method='get', params=None):
        if method == 'get':
            return requests.get(self.api_base + endpoint, auth=(self.api_key,''), params=params)
        elif method == 'put':
            raise NotImplementedError
        else: 
            raise NotImplementedError

    def get_paginated(self, endpoint, params=None):
        result = []
        result_page = self.request(endpoint + '?per_page={0}'.format(self.pagination_limit), params=params)
        assert result_page.status_code == 200, 'Bad API request: {0}'.format(result_page.status_code)
        assert result_page.json()['object'] == 'list', 'API did not return a list of candidates...'
        result += result_page.json()['data']
        next_page = result_page.json()['next_href']
        print('Going to pull {0} candidates'.format(result_page.json()['count']))
        while next_page:
            result_page = requests.get(next_page,    auth=(self.api_key,''))
            assert result_page.status_code == 200, 'Bad API request: {0}'.format(result_page.status_code)
            assert result_page.json()['object'] == 'list', 'API did not return a list of candidates...'
            result += result_page.json()['data']
            next_page = result_page.json()['next_href']
        return result

    def candidate(self, id):
        return self.request('candidates/{0}'.format(id))

    def candidates(self):
        return self.get_paginated('candidates')

    def invitation(self, id):
        return self.request('invitations/{0}'.format(id))

    def invitations(self, status=None):
        params = {}
        if status:
            params['status'] = status
        return self.get_paginated('invitations', params)

    def document(self, id):
        return self.request('documents/{0}'.format(id))

    def verification(self, report_id, verification_id):
        return self.request('reports/{0}/verifications/{1}'.format(report_id,verification_id))

    def verifications(self, report_id):
        return self.get_paginated('reports/{0}/verifications'.format(report_id))

    def report(self, id):
        return self.request('reports/' + id).json()

    def reports(self, ids):
        result = []
        for id in ids:
            result.append(self.report(id))
        return result

    def adverse_items(self, report_id):
        return self.request('reports/{0}/adverse_items'.format(report_id)).json()['data']

    def adverse_action(self, id):
        return self.request('adverse_actions/{0}'.format(id)).json()

    def adverse_actions(self, created_after=None, created_before=None, status=None):
        params = {}
        if created_after:
            params['created_after'] = created_after
        if created_before:
            params['created_before'] = created_before
        if status:
            params['status'] = status
        return self.get_paginated('adverse_actions', params=params)

    def geo(self, id):
        return self.request('geos/{0}'.format(id)).json()

    def geos(self,ids):
        result = []
        for id in ids:
            result.append(self.geo(id))
        return result

    def ssn_trace(self, id):
        return self.request('ssn_traces/{0}'.format(id))

    def sex_offender(self, id):
        return self.request('sex_offender_searches/{0}'.format(id))

    def global_watchlist(self, id):
        return self.request('global_watchlist_searches/{0}'.format(id))

    def national_criminal(self, id):
        return self.request('national_criminal_searches/{0}'.format(id))

    def county_criminal(self, id):
        return self.request('county_criminal_searches/{0}'.format(id))

    def state_criminal(self, id):
        return self.request('state_criminal_searches/{0}'.format(id))

    def motor_vehicle(self, id):
        return self.request('motor_vehicle_reports/{0}'.format(id)).json()

    def motor_vehicle_reports(self, ids):
        result = []
        for id in ids:
            result.append(self.motor_vehicle(id))
        return result


class AppBoy:
    def __init__(self,cfg_name='appboy'):
        self.group_id = luigi.configuration.get_config().get(cfg_name, 'group_id')
        self.base_url = 'https://api.appboy.com/'
        self.headers = {'X-ACCESS-TOKEN':self.group_id}
        self.client = requests.get(self.base_url, headers=self.headers)

    def get_segments(self):
        # api returns in groups of 100 so if < 100 no next page
        call = requests.get(self.base_url + 'segments/list?app_group_id=' + self.group_id)
        if call.status_code == 200:
            response = call.json()
            if response['message'] == 'success':
                segments = response['segments']
        if len(segments) < 100:
            return segments

        last_page = False
        i = 2
        while not last_page:
            call = requests.get(self.base_url + 'segments/list?app_group_id=' + self.group_id + '&page={0}'.format(str(i)))
            if segments.status_code == 200:
                response = call.json()
                if response['message'] == 'success':
                    segments_temp = response['segments']
                    segments += segments_temp
            if len(segments_temp) < 100:
                last_page = True
            i += 1
        return segments


class SalesforceAnalytics:
    def __init__(self, username=None, password=None, security_token=None, sandbox=False, api_version='v29.0'):
        self.username = username
        self.password = password
        self.security_token = security_token
        self.sandbox = sandbox
        self.api_version = api_version
        self.login_details = self.login(self.username, self.password, self.security_token)
        self.token = self.login_details['oauth']
        self.instance = self.login_details['instance']
        self.headers = {'Authorization': 'OAuth {}'.format(self.token)}
        self.base_url = 'https://{}/services/data/v31.0/analytics'.format(self.instance)

    @staticmethod
    def element_from_xml_string(xml_string, element):
        xml_as_dom = xml.dom.minidom.parseString(xml_string)
        elements_by_name = xml_as_dom.getElementsByTagName(element)
        element_value = None

        if len(elements_by_name) > 0:
            element_value = elements_by_name[0].toxml().replace('<' + element + '>', '').replace(
                '</' + element + '>', '')

        return element_value

    @staticmethod
    def _get_login_url(is_sandbox, api_version):
        if is_sandbox:
            return 'https://{}.salesforce.com/services/Soap/u/{}'.format('test', api_version)
        else:
            return 'https://{}.salesforce.com/services/Soap/u/{}'.format('login', api_version)

    def login(self, username, password, security_token):
        username = escape(username)
        password = escape(password)

        url = self._get_login_url(self.sandbox, self.api_version)

        request_body = """<?xml version="1.0" encoding="utf-8" ?>
        <env:Envelope
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
            <env:Body>
                <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                    <n1:username>{username}</n1:username>
                    <n1:password>{password}{token}</n1:password>
                </n1:login>
            </env:Body>
        </env:Envelope>""".format(
            username=username, password=password, token=security_token)

        request_headers = {
            'content-type': 'text/xml',
            'charset': 'UTF-8',
            'SOAPAction': 'login'
        }

        response = requests.post(url, request_body, headers=request_headers)

        if response.status_code != 200:
            exception_code = self.element_from_xml_string(response.content, 'sf:exceptionCode')
            exception_msg = self.element_from_xml_string(response.content, 'sf:exceptionMessage')

            raise SalesforceAuthenticationFailure(exception_code, exception_msg)

        oauth_token = self.element_from_xml_string(response.content, 'sessionId')
        server_url = self.element_from_xml_string(response.content, 'serverUrl')

        instance = (server_url.replace('http://', '')
                     .replace('https://', '')
                     .split('/')[0]
                     .replace('-api', ''))

        return {'oauth': oauth_token, 'instance': instance}

    def _get_metadata(self, url):
        return requests.get(url + '/describe', headers=self.headers).json()

    def _get_report_filtered(self, url, filters):
        metadata_url = url.split('?')[0]
        metadata = self._get_metadata(metadata_url)
        for report_filter in filters:
            metadata["reportMetadata"]["reportFilters"].append(report_filter)

        return requests.post(url, headers=self.headers, json=metadata).json()

    def _get_report_all(self, url):
        return requests.post(url, headers=self.headers).json()

    def get_report(self, report_id, filters=None, details=True):
        """
        Return the full JSON content of a Salesforce report, with or without filters.
        Parameters
        ----------
        report_id: string
            Salesforce Id of target report
        filters: dict {field: filter}, optional
        details: boolean, default True
            Whether or not detail rows are included in report output
        Returns
        -------
        report: JSON
        """
        details = 'true' if details else 'false'
        url = '{}/reports/{}?includeDetails={}'.format(self.base_url, report_id, details)

        if filters:
            return self._get_report_filtered(url, filters)
        else:
            return self._get_report_all(url)

    def get_dashboard(self, dashboard_id):
        url = '{}/dashboards/{}/'.format(self.base_url, dashboard_id)
        return requests.get(url, headers=self.headers).json()


class SalesforceAuthenticationFailure(Exception):

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return "{}: {}.".format(self.code, self.msg)


class SalesforceReportParser:
    """
    Parser with generic functionality for all Report Types (Tabular, Summary, Matrix)
    Parameters
    ----------
    report: dict, return value of Connection.get_report()
    """
    def __init__(self, report):
        self.data = report
        self.type = self.data["reportMetadata"]["reportFormat"]
        self.has_details = self.data["hasDetailRows"]

    def get_grand_total(self):
        return self.data["factMap"]["T!T"]["aggregates"][0]["value"]

    @staticmethod
    def _flatten_record(record):
        return [field["label"] for field in record]

    def _get_field_labels(self):
        columns = self.data["reportMetadata"]["detailColumns"]
        column_details = self.data["reportExtendedMetadata"]["detailColumnInfo"]
        return {key: column_details[value]["label"] for key, value in enumerate(columns)}

    def records(self):
        """
        Return a list of all records included in the report. If detail rows are not included
        in the report a ValueError is returned instead.
        Returns
        -------
        records: list
        """
        if not self.has_details:
            raise ValueError('Report does not include details so cannot access individual records')

        records = []
        fact_map = self.data["factMap"]

        for group in fact_map.values():
            rows = group["rows"]
            group_records = (self._flatten_record(row["dataCells"]) for row in rows)

            for record in group_records:
                records.append(record)

        return records

    def records_dict(self):
        """
        Return a list of dictionaries for all records in the report in {field: value} format. If detail rows
        are not included in the report a ValueError is returned instead.
        Returns
        -------
        records: list of dictionaries in {field: value, field: value...} format
        """
        if not self.has_details:
            raise ValueError('Report does not include details so cannot access individual records')

        records = []
        fact_map = self.data["factMap"]
        field_labels = self._get_field_labels()

        for group in fact_map.values():
            rows = group["rows"]
            group_records = (self._flatten_record(row["dataCells"]) for row in rows)

            for record in group_records:
                labelled_record = {field_labels[key]: value for key, value in enumerate(record)}
                records.append(labelled_record)

        return records


class Salesforce:
    def __init__(self,cfg_name='salesforce'):
        from simple_salesforce import Salesforce
        username = luigi.configuration.get_config().get(cfg_name,'username')
        password = luigi.configuration.get_config().get(cfg_name,'password')
        security_token = luigi.configuration.get_config().get(cfg_name,'security_token')
        host = luigi.configuration.get_config().get(cfg_name,'host')
        self.client_soql = Salesforce(username=username,password=password,security_token=security_token)
        self.client_analytics = SalesforceAnalytics(username=username,password=password,security_token=security_token)
        self.operators = {'equals':                     '=',
                                            'not equals':             '!=',
                                            'less than':                '<',
                                            'less or equal':        '<=',
                                            'greater than':         '>',
                                            'greater or equal': '>=',
                                            'like':                         'like',
                                            'in':                             'in',
                                            'not in':                     'not in'
                                         }

    def query(self, soql):
        '''https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql.htm'''
        return self.soql_client.query_all(soql)

    def query_results(self, soql):
        records = self.query(soql)['records']
        cols_cleaning = self.__soql_columns__(soql)
        df = pd.DataFrame()
        # Fan out the dict into a table
        for col in cols_cleaning:
            records_clean = []
            hierarchy = col.split('.')
            for record in records:
                for child in hierarchy:
                    try:    
                        record = record.get(child)
                    except:
                        record = ''
                records_clean += [record]
            df[col] = records_clean
        df.columns = [col.replace('.','_') for col in df.columns]
        return df

    def available_objects(self):
        obj = self.soql_client.describe()
        obj = obj['sobjects']
        result = []
        for o in obj:
            result += [o['name']]
        return result

    def object_fields(self, object_name):
        attr = self.soql_client.__getattr__(object_name).describe()
        return attr

    def object_query_fields(self, object_name):
        attr = self.object_fields(object_name)
        assert attr['queryable'], 'object is not queryable'
        assert not attr['deprecatedAndHidden'], 'object is deprecated or hidden'
        fields = attr['fields']
        return [field['name'] for field in fields]

    def get_report_id(self, name):
        result = self.soql_client.query("SELECT Id FROM Report WHERE Name = '{0}'".format(name.replace("'", r"\'")))
        result_id = result['records'][0]['Id']
        return result_id

    def get_report(self, id, filters=None, details='false'):
        if filters:
            assert isinstance(filters,list),"filters needs to be a list of hashes: [{'value':'test','column':'my_sf_column','operator':'equals'}...]"
            return self.analytics_client.get_report(id,filters=filters,details=details)
        return self.analytics_client.get_report(id,details=details)
 
    def get_report_results(self, id):
        report = self.get_report(id)
        parser = SalesforceReportParser(report)
        assert report['allData'], 'Code is not pulling all data for the report'
        assert len(parser.records()) > 0, 'No results'
        return parser.records()

    def get_report_columns(self, id):
        report = self.get_report(id)
        cols = report['reportMetadata']['detailColumns']
        assert len(cols) > 0, 'Report has no columns'
        return cols
    
    def __soql_columns__(self, soql):
        return [a.strip() for a in soql.split('SELECT')[1].split('FROM')[0].split(',')]

    def __clean_columns__(self,cols):
        # [(col,length_of_inheritence) for col in columns]
        #                                (column name minus table name,    (number children in name heirarchy - number of children in heirarcy without __c))
        cols_cleaning = [('.'.join(col.split('.')[1:]), len(col.split('.')[1:]) - (len(col.split('.')[1:]) - '.'.join(col.split('.')[1:]).count('__c'))) for col in cols]
        # custom inheritence vs absolute, __c -> __r 
        cols_cleaning = [col.replace('__c','__r',l) for col,l in cols_cleaning]
        # rename last child __r
        cols_cleaning = ['.'.join(col.split('.')[:-1] + [col.split('.')[-1].replace('__r','__c')]) for col in cols_cleaning]
        return cols_cleaning

    def __report_filters__(self, report):
        raw = report['reportMetadata']['reportFilters']
        result = ''
        for f in raw:
            col = f['column']
            col = self.__clean_columns__([col])[0]
            operator = self.operators[f['operator'].lower().strip()]
            value = f['value']
            result += ' AND ' + col + ' ' + operator + ' ' + "'" + value + "'" + ' '
        # get rid of first AND
        result = result.replace('AND','',1)
        # TO DO - only supports string filter values
        return result

    def report_to_soql(self, id, aggregates=False):
        # TODO report aggregates and filters and group by and include joins
        if aggregates:
            raise NotImplementedError
        report = self.get_report(id)
        cols = list(report['reportExtendedMetadata']['detailColumnInfo'].keys())
        assert len(set([col.split('.')[0] for col in cols])) == 1, 'Columns must all come from same soql table -> Cannot convert completely'
        table = list(set([col.split('.')[0] for col in cols]))[0]
        cols_cleaning = self.__clean_columns__(cols)
        filters = self.__report_filters__(report)
        soql = 'SELECT ' + ','.join(cols_cleaning) + ' FROM ' + table + ' WHERE ' + filters
        return soql

