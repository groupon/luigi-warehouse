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


#########################################################################################
#### typeform_to_redshift.py
# INPUTS
### form_id                            - typeform id
### account_key                    - typeform account key
### schema                             - target redshift schema
### table                                - target redshift table
### field_ids (OPTIONAL) - "['field1','field2','field3']"
#########################################################################################
import os
import sys
import urllib.request
import csv
import json
import string
from ast import literal_eval
import luigi
from luigi.contrib.simulate import RunAnywayTarget
from luigi.util import inherits
import pandas as pd
import numpy as np
from . import sources


class Run(luigi.WrapperTask):
    form_id = luigi.Parameter()
    account_key = luigi.Parameter()
    schema = luigi.Parameter()
    table = luigi.Parameter()
    # comma separated list of field_ids
    # "['field1','field2','field3']"
    field_ids = luigi.Parameter(default=None)
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))

    def requires(self):
        # ensure if there are field_ids specified there are atleast 2
        if not self.field_ids:
            yield self.clone(ImportTypeFormToRedshift)
        elif len(literal_eval(self.field_ids)) < 2:
            print('The number of field ids must be at least 2')
        else:
            yield self.clone(ImportTypeFormToRedshift)

@inherits(Run)
class RemoveLastImport(luigi.ExternalTask):
    resources = {'s3': 1}
    host_resources = {'s3': 1}

    def run(self):
        if os.path.isfile('data/{0}.csv'.format(self.table)):
            os.remove('data/{0}.csv'.format(self.table))
        client = luigi.s3.S3Client()
        if client.exists(self.s3_bucket + '/{0}.csv'.format(self.table)):
            client.remove(self.s3_bucket + '/{0}.csv'.format(self.table))

        if (not os.path.isfile('data/{0}.csv'.format(self.table)) and not client.exists(self.s3_bucket + '/{0}.csv'.format(self.table))):
            self.output().done()

    def output(self):
        return RunAnywayTarget(self)

@inherits(Run)
class ExtractTypeForm(luigi.ExternalTask):
    resources = {'api': 1}
    host_resources = {'api': 1}

    def requires(self):
        return self.clone(RemoveLastImport)

    def clean_string(self, string):
        return ''.join(i for i in string if ord(i) < 128)

    def run(self):
        form_id = self.form_id
        typeform_account_key = self.account_key
        url = 'https://api.typeform.com/v1/form/{0}?key={1}'.format(form_id, typeform_account_key)
        response = urllib.request.urlopen(url)
        response_text = response.read()
        number_of_responses = json.loads(response_text.decode())['stats']['responses']['total']
        response_obj = json.loads(response_text.decode())
        df = pd.DataFrame([record['answers'] for record in response_obj['responses']])
        #     for pagination
        for i in range(1000, number_of_responses, 1000):
            url = 'https://api.typeform.com/v1/form/{0}?key={1}&offset={2}'.format(form_id, typeform_account_key, i)
            response = urllib.request.urlopen(url)
            response_text = response.read()
            response_obj = json.loads(response_text.decode())
            temp_df = pd.DataFrame([record['answers'] for record in response_obj['responses']])
            df = pd.concat([temp_df, df])
        # drop any rows with no responses
        df = df.dropna(how='all')
        if self.field_ids:
            field_ids = literal_eval(self.field_ids)
            df = df[field_ids]
        # set column names to be the questions cleaned strings
        questions = {i['id']:i['question'].lower().replace(' ','_').replace('?','').replace(':','').replace("'",'').replace('"','').replace(')','').replace('(','').replace('!','').replace('.','').replace('/','').replace('\\','') for i in response_obj['questions']}
        cols = []
        for col in df.columns:
            cols += [self.clean_string(questions[col])]
        df.columns = cols
        df.to_csv("data/{0}.csv".format(self.table), index=False, header=True)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

@inherits(Run)
class UploadLocalCSVToS3(luigi.ExternalTask):
    resources = {'s3': 1}
    host_resources = {'s3': 1}

    def requires(self):
        return self.clone(ExtractTypeForm)

    def input(self):
        return luigi.LocalTarget('data/{0}.csv'.format(self.table))

    def output(self):
        return luigi.s3.S3Target(
            self.s3_bucket + '/{0}.csv'.format(self.table))

    def run(self):
        client = luigi.s3.S3Client()
        client.put('data/{0}.csv'.format(self.table),
                   self.s3_bucket + '/{0}.csv'.format(self.table))

@inherits(Run)
class ImportTypeFormToRedshift(luigi.ExternalTask):
    resources = {'redshift': 1}
    host_resources = {'redshift': 1}

    def requires(self):
        return self.clone(UploadLocalCSVToS3)

    def run(self):
        df = pd.DataFrame.from_csv('data/{0}.csv'.format(self.table), index_col=None)
        generator = sources.RedshiftSchemaGenerator(df, self.schema, self.table, overwrite=True, all_varchar=False)
        generator.execute()
        
        redshift = sources.Redshift()
        options = """CSV DELIMITER ',' ACCEPTINVCHARS 
                     IGNOREHEADER 1 TRUNCATECOLUMNS 
                     TRIMBLANKS BLANKSASNULL EMPTYASNULL 
                     DATEFORMAT 'auto' ACCEPTANYDATE 
                     REGION '{0}' COMPUPDATE ON MAXERROR 1;""".format(redshift.s3_region)
        s3_path = "'" + self.s3_bucket + "/{0}.csv".format(table) + "'"
        redshift.copy(self.schema, self.table, s3_path, options)
        RemoveLastImport(form_id=self.form_id, account_key=self.account_key, table=self.table, field_ids=self.field_ids).run()

        self.output().done()

    def output(self):
        return RunAnywayTarget(self)



if __name__ == '__main__':
    luigi.run()
