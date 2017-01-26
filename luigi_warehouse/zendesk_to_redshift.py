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

import json
import csv
import os
import itertools as it
from datetime import datetime, timedelta
import time
from string import Template
import luigi
from luigi.contrib.simulate import RunAnywayTarget
from django.utils.encoding import smart_str
import pandas as pd
from . import sources


# helper function to grab the latest record timestamp from the tickets or ticket_events table
def get_last_import_time(table='zendesk_tickets'):
    try:
        timestamp_col = 'updated_at' if (table == 'zendesk_tickets') else 'created_at'
        redshift = sources.Redshift()
        redshift.cursor.execute('SELECT MAX({0}) FROM zendesk.{1};'.format(timestamp_col,table))
        updated_at = redshift.cursor.fetchall()[0][0]
        return convert_to_epoch(updated_at)
    except:
        updated_at = (datetime.now() - timedelta(days=7))
        return convert_to_epoch(updated_at)

def convert_to_epoch(d):
    return int((d - datetime(1970,1,1)).total_seconds())


class RemoveLastImport(luigi.ExternalTask):
    host_resources = {'s3': 1}
    resources = {'s3': 1}
    table = luigi.Parameter()
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))

    def run(self):
      #remove all files on local EC2 box, S3 before doing the import
        if os.path.isfile('data/{0}.csv'.format(self.table)):
            os.remove('data/{0}.csv'.format(self.table))
        client = luigi.s3.S3Client()
        if client.exists(self.s3_bucket + '/{0}.csv'.format(self.table)):
            client.remove(self.s3_bucket + '/{0}.csv'.format(self.table))
        self.done = ((not client.exists(self.s3_bucket + '/{0}.csv'.format(self.table))) & (not os.path.isfile('data/{0}.csv'.format(self.table))))
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


class ExtractZendeskTicketsToCSV(luigi.ExternalTask):
    host_resources = {'api': 1}
    table = 'zendesktickets5'
    start_time = get_last_import_time(table)
    client = sources.Zendesk()

    def requires(self):
        return [RemoveLastImport(self.table)]

    def columns(self):
        return [      'agent_wait_time_in_minutes'
                     ,'agent_wait_time_in_minutes_within_business_hours'
                     ,'assigned_at'
                     ,'assigned_stations'
                     ,'assignee_external_id'
                     ,'assignee_id'
                     ,'assignee_name'
                     ,'assignee_stations'
                     ,'brand_name'
                     ,'created_at'
                     ,'current_tags'
                     ,'domain'
                     ,'due_date'
                     ,'first_reply_time_in_minutes'
                     ,'first_reply_time_in_minutes_within_business_hours'
                     ,'first_resolution_time_in_minutes'
                     ,'first_resolution_time_in_minutes_within_business_hours'
                     ,'full_resolution_time_in_minutes'
                     ,'full_resolution_time_in_minutes_within_business_hours'
                     ,'generated_timestamp'
                     ,'group_id'
                     ,'group_name'
                     ,'group_stations'
                     ,'id'
                     ,'initially_assigned_at'
                     ,'on_hold_time_in_minutes'
                     ,'on_hold_time_in_minutes_within_business_hours'
                     ,'organization_name'
                     ,'priority'
                     ,'reopens'
                     ,'replies'
                     ,'req_email'
                     ,'req_external_id'
                     ,'req_id'
                     ,'req_name'
                     ,'requester_wait_time_in_minutes'
                     ,'requester_wait_time_in_minutes_within_business_hours'
                     ,'resolution_time'
                     ,'satisfaction_score'
                     ,'solved_at'
                     ,'status'
                     ,'subject'
                     ,'submitter_name'
                     ,'ticket_type'
                     ,'updated_at'
                     ,'url'
                     ,'via']

    def transformed_keys(self):
        return ['agent_wait_time_in_minutes',
                'agent_wait_time_in_minutes_within_business_hours',
                'assigned_at',
                'assigned_stations',
                'first_reply_time_in_minutes',
                'first_reply_time_in_minutes_within_business_hours',
                'first_resolution_time_in_minutes',
                'first_resolution_time_in_minutes_within_business_hours',
                'full_resolution_time_in_minutes',
                'full_resolution_time_in_minutes_within_business_hours',
                'group_stations',
                'on_hold_time_in_minutes',
                'on_hold_time_in_minutes_within_business_hours',
                'reopens',
                'replies',
                'req_id',
                'requester_wait_time_in_minutes',
                'resolution_time',]

    def dictionary_string_to_integer(self, dictionary, key):
        try: dictionary[key] = int(dictionary[key])
        except: dictionary[key] = None
        return dictionary

    def run(self):
        try:
            response = self.client.incremental_tickets(self.start_time)
            if response.status_code == 429:
                self.client.status_handler(response)
                response = self.client.incremental_tickets(self.start_time)
            else:
                self.client.status_handler(response)
            clientresult = response.json()
            tickets = clientresult['results']
            header = clientresult['field_headers']
            header['assigned_stations']='assigned_stations'
            transformed_tickets = []
            while bool(clientresult['end_time']): # != '':
                for ticket in tickets:
                    for k in self.transformed_keys():
                        ticket_temp = self.dictionary_string_to_integer(ticket, k)
                    for key in sorted(ticket_temp):
                        if isinstance(ticket_temp[key],str):
                            ticket_temp[key] = ticket_temp[key].replace('"','').replace("'",'')
                    transformed_tickets = transformed_tickets + [ticket_temp]
                response = self.client.incremental_tickets(clientresult['end_time'])
                if response.status_code == 429:
                    try: self.client.status_handler(response)
                    except: print('Zendesk ticket extraction error: rate limited & status_handler failed'.format(e))
                    response = self.client.incremental_tickets(clientresult['end_time'])
                elif response.status_code == 422:
                    break
                clientresult = response.json()
                tickets = clientresult['results']
            df = pd.DataFrame(transformed_tickets)
            df[self.columns()].to_csv('data/{0}.csv'.format(self.table),sep=',',na_rep=None,header=False,index=False)
            time.sleep(120)
            self.output().done()
        except Exception as e:
            print('Zendesk ticket extraction error: {0}'.format(e))
            raise

    def output(self):
        return RunAnywayTarget(self)


class ExtractZendeskUsersToCSV(luigi.ExternalTask):
    host_resources = {'api': 1}
    table = 'zendesk_users'
    client = sources.Zendesk().user_client

    def requires(self):
        return [RemoveLastImport(self.table)]

    def columns(self):
        return ['active', 
                'alias', 
                'chat_only', 
                'created_at', 
                'custom_role_id', 
                'details', 
                'email', 
                'external_id', 
                'id', 
                'last_login_at', 
                'locale', 
                'locale_id', 
                'moderator', 
                'name', 
                'notes', 
                'only_private_comments', 
                'organization_id', 
                'phone', 
                'photo', 
                'restricted_agent', 
                'role', 
                'shared', 
                'shared_agent', 
                'signature', 
                'suspended', 
                'tags', 
                'ticket_restriction', 
                'time_zone', 
                'two_factor_auth_enabled', 
                'updated_at', 
                'url', 
                'user_fields', 
                'verified']

    def run(self):
        try:
            users = self.client.users_list(get_all_pages=True)['users']
            df = pd.DataFrame(users)[self.columns()].to_csv('data/{0}.csv'.format(self.table),sep=',',na_rep=None,header=False,index=False)
            self.output().done()
        except Exception as e:
            print('Zendesk ticket extraction error: {0}'.format(e))
            raise

    def output(self):
        return RunAnywayTarget(self)


class ExtractZendeskOrganizationsToCSV(luigi.ExternalTask):
    host_resources = {'api': 1}
    table = 'zendesk_organizations'
    start_time = 0 
    now = int(convert_to_epoch(datetime.now()))
    client = sources.Zendesk().user_client

    def requires(self):
        return RemoveLastImport('zendesk_organizations')

    def run(self):
        try:
            result = self.client.incremental_organizations_list(start_time=self.start_time) #can't do get_all_pages=True because of API limit
        except:
            time.sleep(60)
            result = self.client.incremental_organizations_list(start_time=self.start_time) #can't do get_all_pages=True because of API limit
        orgs = result['organizations']
        all_headers = [list(i.keys()) for i in orgs]
        header = set([item for sublist in all_headers for item in sublist])
        while bool(result['end_time'] < self.now):
            try:
                result = self.client.incremental_organizations_list(start_time = int(result['end_time']))
                orgs += result['organizations']
            except:
                print('API LIMIT REACHED...')
                time.sleep(60)
            if result['count'] < 1000:
                break
        orgs = pd.DataFrame(orgs).fillna('')
        for index in orgs.index:
            #clean details
            orgs.loc[index,'details'] = orgs.at[index,'details'].replace(',','').replace('//','')
            #clean timestamps
            orgs.loc[index,'created_at'] = orgs.at[index,'created_at'].replace('T',' ').replace('Z','')
            orgs.loc[index,'deleted_at'] = orgs.at[index,'deleted_at'].replace('T',' ').replace('Z','')
            orgs.loc[index,'updated_at'] = orgs.at[index,'updated_at'].replace('T',' ').replace('Z','')
        orgs.to_csv('data/{0}.csv'.format(self.table), index=False, header=False)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


class ExtractZendeskTicketEventsToCSV(luigi.ExternalTask):
    host_resources = {'api': 1}
    table = 'zendesk_ticket_events'
    start_time = get_last_import_time(table = 'zendesk_ticket_events') 
    now = int(convert_to_epoch(datetime.now()))
    client = sources.Zendesk().user_client

    def requires(self):
        return RemoveLastImport('zendesk_ticket_events')

    def columns(self):
        return ['child_events'
              ,'created_at'
              ,'id'
              ,'merged_ticket_ids'
              ,'system'
              ,'ticket_id'
              ,'timestamp'
              ,'updater_id'
              ,'via'
              ,'tags'
              ,'removed_tags'
              ,'added_tags'
              ,'priority'
              ,'status'
              ,'comment_public']

    def run(self):
        print('{0}\nStart_time = {1}'.format(datetime.now(), self.start_time))
        clientresult = self.client.incremental_ticket_events_list(start_time = self.start_time) #can't do get_all_pages=True because of API limit
        ticket_events = clientresult['ticket_events']
        all_headers = [list(i.keys()) for i in clientresult['ticket_events']]
        header = set([item for sublist in all_headers for item in sublist])
        while bool(clientresult['end_time'] < self.now - 2 * 60 * 60): #get results ending less than 2 hours before now
            try:
                clientresult = self.client.incremental_ticket_events_list(start_time = clientresult['end_time'])
                ticket_events += clientresult['ticket_events']
            except:
                print('API LIMIT REACHED...')
                time.sleep(60)

        ticket_events = pd.DataFrame(ticket_events)
        for index in ticket_events.index:
            try:
                ticket_events.loc[index,'tags'] = str([i for i in ticket_events.at[index,'child_events'] if 'tags' in i.keys()][0]['tags'])
            except:
                ticket_events.loc[index,'tags'] = None
            try:
                ticket_events.loc[index,'removed_tags'] = str([i for i in ticket_events.at[index,'child_events'] if 'tags' in i.keys()][0]['removed_tags'])
            except:
                ticket_events.loc[index,'removed_tags'] = None
            try:
                ticket_events.loc[index,'added_tags'] = str([i for i in ticket_events.at[index,'child_events'] if 'tags' in i.keys()][0]['added_tags'])
            except:
                ticket_events.loc[index,'added_tags'] = None
            try:
                ticket_events.loc[index,'priority'] = [i for i in ticket_events.at[index,'child_events'] if 'priority' in i.keys()][0]['priority']
            except:
                ticket_events.loc[index,'priority'] = None
            try:
                ticket_events.loc[index,'status'] = [i for i in ticket_events.at[index,'child_events'] if 'status' in i.keys()][0]['status']
            except:
                ticket_events.loc[index,'status'] = None
            try:
                children = [i for i in ticket_events.at[index,'child_events'] if 'comment_public' in i.keys()]
                ticket_events.loc[index,'comment_public'] = ''.join([str(child['comment_public']) for child in children])
            except:
                ticket_events.loc[index,'comment_public'] = None

            ticket_events.loc[index,'created_at'] = ticket_events.at[index,'created_at'].replace('T',' ').replace('Z','')#datetime.strftime(ticket_events.at[index,'created_at'], '%Y-%m-%d %H:%M:%H')

        ticket_events = ticket_events[self.columns()]

        ticket_events.to_csv('data/{0}.csv'.format(self.table), index=False, header=False)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


class UploadLocalCSVToS3(luigi.ExternalTask):
    host_resources = {'s3': 1}
    resources = {'s3': 1}
    table = luigi.Parameter()
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))

    def requires(self):
        if (self.table == 'zendesk_tickets'):
            return [ExtractZendeskTicketsToCSV()]
        elif (self.table == 'zendesk_users'):
            return [ExtractZendeskUsersToCSV()]
        elif (self.table == 'zendesk_ticket_events'):
            return[ExtractZendeskTicketEventsToCSV()]
        elif (self.table == 'zendesk_organizations'):
            return [ExtractZendeskOrganizationsToCSV()]

    def input(self):
        return luigi.LocalTarget('data/{0}.csv'.format(self.table))

    def output(self):
        return luigi.s3.S3Target(
            self.s3_bucket + '/{0}.csv'.format(self.table))

    def run(self):
        client = luigi.s3.S3Client()
        client.put('data/{0}.csv'.format(self.table),
                   self.s3_bucket + '/{0}.csv'.format(self.table) )


class CopyTableFromS3ToRedshift(luigi.ExternalTask):
    host_resources = {'redshift': 1}
    resources = {'redshift': 1}
    table = luigi.Parameter()
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))

    def requires(self):
        return UploadLocalCSVToS3(self.table)

    def run(self):
        redshift = sources.Redshift()
        import_schema_name = luigi.configuration.get_config().get('redshift', 'zendesk_schema_name')

        s3_path = "'" + self.s3_bucket + "/{0}.csv".format(self.table) + "'"
        aws_credentials = "'aws_access_key_id=" + redshift.aws_access_key_id + ";" + "aws_secret_access_key=" + redshift.aws_secret_access_key + "'"
        options = """CSV DELIMITER ',' ACCEPTINVCHARS TRUNCATECOLUMNS 
                     TRIMBLANKS BLANKSASNULL EMPTYASNULL 
                     DATEFORMAT 'auto' ACCEPTANYDATE 
                     REGION '{0}' COMPUPDATE ON MAXERROR 1;""".format(redshift.s3_region)

        sql = Template("""
                          BEGIN;
                          COPY ${import_schema_name}.${current_table} FROM ${s3_path} CREDENTIALS ${aws_credentials} ${options}
                          CREATE TEMP TABLE ${current_table}_dupe_row_list AS
                          SELECT t.$primary_key FROM zendesk.$current_table t WHERE t.$primary_key IS NOT NULL GROUP BY t.$primary_key  HAVING COUNT(t.$primary_key)>1;
                          CREATE TEMP TABLE ${current_table}_duped_rows AS
                            SELECT $current_table.*,
                                   ROW_NUMBER() OVER (PARTITION BY $current_table.$primary_key ORDER BY $updated_at_col DESC) AS rn
                             FROM zendesk.$current_table
                             JOIN ${current_table}_dupe_row_list on $current_table.$primary_key = ${current_table}_dupe_row_list.$primary_key;
                          CREATE TEMP TABLE ${current_table}_rows_to_keep AS
                            SELECT * from ${current_table}_duped_rows where rn = 1;
                          ALTER TABLE ${current_table}_rows_to_keep
                            DROP COLUMN rn;
                          DELETE FROM zendesk.$current_table USING ${current_table}_dupe_row_list l WHERE l.$primary_key=$current_table.$primary_key;
                          INSERT INTO zendesk.$current_table SELECT * FROM ${current_table}_rows_to_keep;
                          DROP TABLE ${current_table}_dupe_row_list;
                          DROP TABLE ${current_table}_duped_rows;
                          DROP TABLE ${current_table}_rows_to_keep;
                          COMMIT;""")

        primary_key = "ticket_id" if (self.table == "zendesktickets4") else "id"
        updated_at_col = "timestamp_at" if (self.table == 'zendesk_ticket_events_luigi') else "updated_at"

        sql = sql.substitute(current_table=self.table,
                             primary_key=primary_key,
                             updated_at_col=updated_at_col,
                             import_schema_name=import_schema_name,
                             s3_path=s3_path,
                             aws_credentials=aws_credentials,
                             options=options)

        redshift.cursor.execute(sql)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


class ImportZendeskTablesToRedshift(luigi.WrapperTask):
    def relevant_tables(self):
        tables = """zendesk_tickets
                    zendesk_users
                    zendesk_organizations
                    zendesk_ticket_events"""
        return [table.strip() for table in tables.splitlines()]

    def requires(self):
        for table in self.relevant_tables():
            yield CopyTableFromS3ToRedshift(table)


class Run(luigi.WrapperTask):
    def requires(self):
        yield ImportZendeskTablesToRedshift()


if __name__ == '__main__':
    luigi.run()

########################################################
# SCHEMAS
########################################################
# CREATE TABLE zendesk.zendesk_users (
# active varchar,
# alias varchar,
# chat_only varchar,
# created_at VARCHAR,
# custom_role_id varchar,
# details varchar,
# email varchar,
# external_id varchar,
# id varchar,
# last_login_at VARCHAR,
# locale varchar,
# locale_id varchar,
# moderator varchar,
# name varchar,
# notes varchar,
# only_private_comments varchar,
# organization_id VARCHAR,
# phone varchar,
# photo varchar,
# restricted_agent varchar,
# role varchar,
# shared varchar,
# shared_agent varchar,
# signature varchar,
# suspended varchar,
# tags varchar,
# ticket_restriction varchar,
# time_zone varchar,
# two_factor_auth_enabled varchar,
# updated_at VARCHAR,
# url varchar,
# user_fields varchar,
# verified varchar)

# CREATE TABLE zendesk.zendesk_tickets (
# agent_wait_time_in_minutes varchar
# ,agent_wait_time_in_minutes_within_business_hours varchar
# ,assigned_at varchar
# ,assigned_stations varchar
# ,assignee_external_id varchar
# ,assignee_id varchar
# ,assignee_name varchar
# ,assignee_stations varchar
# ,brand_name varchar
# ,created_at varchar
# ,current_tags varchar
# ,domain varchar
# ,due_date varchar
# ,first_reply_time_in_minutes varchar
# ,first_reply_time_in_minutes_within_business_hours varchar
# ,first_resolution_time_in_minutes varchar
# ,first_resolution_time_in_minutes_within_business_hours varchar
# ,full_resolution_time_in_minutes varchar
# ,full_resolution_time_in_minutes_within_business_hours varchar
# ,generated_timestamp varchar
# ,group_id varchar
# ,group_name varchar
# ,group_stations varchar
# ,id varchar
# ,initially_assigned_at varchar
# ,on_hold_time_in_minutes varchar
# ,on_hold_time_in_minutes_within_business_hours varchar
# ,organization_name varchar
# ,priority varchar
# ,reopens varchar
# ,replies varchar
# ,req_email varchar
# ,req_external_id varchar
# ,req_id varchar
# ,req_name varchar
# ,requester_wait_time_in_minutes varchar
# ,requester_wait_time_in_minutes_within_business_hours varchar
# ,resolution_time varchar
# ,satisfaction_score varchar
# ,solved_at varchar
# ,status varchar
# ,subject varchar
# ,submitter_name varchar
# ,ticket_type varchar
# ,updated_at varchar
# ,url varchar
# ,via varchar
# )

# CREATE TABLE zendesk.zendesk_organizations(
# created_at             TIMESTAMP,
# deleted_at             TIMESTAMP,
# details                VARCHAR(max),
# domain_names           VARCHAR,
# external_id            VARCHAR,
# group_id               VARCHAR,
# id                     BIGINT distkey sortkey,
# name                   VARCHAR,
# notes                  VARCHAR(max),
# organization_fields    VARCHAR(max),
# shared_comments        BOOLEAN,
# shared_tickets         BOOLEAN,
# tags                   VARCHAR(max),
# updated_at             TIMESTAMP,
# url                    VARCHAR,
# primary key(id))