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
# Assumptions
# - Google sheet has been shared with your service account
# - Google Sheet - 1st row = header
# - Complete copy/replace to redshift
# - Uses custom validation.RunAnywayTarget
#########################################################################################

import json
import os
import datetime
import luigi
from luigi.util import inherits
import gspread
#oauth2client must be version 1.5.2
from oauth2client.client import SignedJwtAssertionCredentials
import pandas as pd
from . import sources
from .validation import RunAnywayTarget

# spreadsheet must be shared with: the service account you create
JSON_KEY = json.load(open(luigi.configuration.get_config().get('google_sheets', 'json_auth_file')))
SCOPE = [luigi.configuration.get_config().get('google_sheets', 'SCOPE')]

global records_before
records_before = 0 # since we're doing a complete copy replace
global records_local
global records_after


class Run(luigi.ExternalTask):
    gsheet = luigi.Parameter()
    sheet_name = luigi.Parameter()
    table = luigi.Parameter()
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    schema = luigi.Parameter(default='googlesheets')
    '''Wrapper to do the whole pipeline and
       then remove all of the files created'''

    def requires(self):
        return self.clone(CopyToRedshift)

    def run(self):
        self.filename = self.table + '.csv'
        os.remove("data/" + self.filename)
        client = luigi.s3.S3Client()
        # checks only for the existence of the exact file which will be written by this import
        if client.exists(self.s3_bucket + '/' + self.filename):
            client.remove(self.s3_bucket + '/' + self.filename)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


@inherits(Run)
class Data(luigi.ExternalTask):
    host_resources = {'api':1}
    '''Task to pull data from your googlesheet
         and save it to a CSV'''

    def run(self):
        self.filename = self.table + '.csv'
        credentials = SignedJwtAssertionCredentials(JSON_KEY['client_email'],
                                                    JSON_KEY['private_key'].encode(),
                                                    SCOPE)
        gc = gspread.authorize(credentials)
        sheet = gc.open(self.gsheet)
        locsheet = sheet.worksheet(self.sheet_name)
        data = locsheet.get_all_values()
        # list of lists, first value of each list is column header
        header = locsheet.get_all_values()[0]
        data = [l for l in data if l != header]
        data = pd.DataFrame(data, columns=header).to_csv("data/" + self.filename,
                                                         index=False,
                                                         header=True)
        global records_local
        records_local = len(data)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


@inherits(Run)
class UploadToS3(luigi.ExternalTask):
    resources = {'s3':1}
    host_resources = {'s3': 1}
    '''Task to upload the CSV to S3'''

    def requires(self):
        return self.clone(Data)

    def run(self):
        self.filename = self.table + '.csv'
        client = luigi.s3.S3Client()
        client.put('data/' + self.filename,
                   self.s3_bucket + '/' + self.filename)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


@inherits(Run)
class CopyToRedshift(luigi.ExternalTask):
    resources = {'redshift':1}
    host_resources = {'redshift':1}
    '''Task that copies the CSV from S3 to Redshift table.
         Performs full copy/replace of the table'''

    def requires(self):
        return self.clone(UploadToS3)

    def run(self):
        df = pd.DataFrame.from_csv("data/" + self.filename,
                                   index_col=None)
        generator = sources.RedshiftSchemaGenerator(df,
                                                    self.schema,
                                                    self.table,
                                                    overwrite=True,
                                                    all_varchar=False)
        generator.execute()

        s3_path = "'" + self.s3_bucket + "/" + self.local_file.split('/')[-1] + "'"
        options = """CSV DELIMITER ',' IGNOREHEADER 1
                     TRUNCATECOLUMNS TRIMBLANKS BLANKSASNULL EMPTYASNULL
                     ACCEPTINVCHARS DATEFORMAT 'auto' ACCEPTANYDATE
                     REGION '{0}' COMPUPDATE ON MAXERROR 0;""".format(redshift.s3_region)
        self.load_start = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        sources.Redshift().copy(self.schema, self.table, s3_path, options)

        self.output().validation()

    def output(self):
        self.local_file = 'data/{0}.csv'.format(self.table)
        return RunAnywayTarget(self)


if __name__ == '__main__':
    luigi.run() 
