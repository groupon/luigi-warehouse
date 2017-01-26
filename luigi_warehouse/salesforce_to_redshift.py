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


############################################################################################
# SOQL to Redshift
#  - requires target schema, table, and soql to run - does complete copy/replace to redshift
# 
# Report to Redshift
#  - requires target schema, table, and report name to run - does complete copy/replace
#  - TODO - analytics API limits the results to 2000 - implement filtering and iteration
############################################################################################

import os
import luigi
from luigi.util import inherits
import pandas as pd
from . import sources
from .validation import RunAnywayTarget


class SOQLtoRedshift(luigi.WrapperTask):
    schema = luigi.Parameter()
    table = luigi.Parameter()
    soql = luigi.Parameter()

    def requires(self):
        yield CopytoRedshift(schema=schema, table=table, soql=soql)


class ReporttoRedshift(luigi.WrapperTask):
    schema = luigi.Parameter()
    table = luigi.Parameter()
    report = luigi.Parameter()

    def requires(self):
        yield CopytoRedshift(schema=schema, table=table, report=report)


class CopyToRedshift(luigi.ExternalTask):
    schema = luigi.Parameter()
    table = luigi.Parameter()
    report = luigi.Parameter(default=None)
    soql = luigi.Parameter(default=None)
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    resources = {'redshift':1}
    host_resources = {'redshift':1}

    def requires(self):
        return self.clone(UploadLocalCSVToS3)

    def run(self):
        redshift = sources.RedshiftSource()
        schema, table, filename = SCHEMA, self.table, self.local_file.split('/')[-1]

        sources.RedshiftSchemaGenerator(df=pd.DataFrame.from_csv(self.local_file, index_col=None),
                                        schema=schema,
                                        table=table,
                                        user=luigi.configuration.get_config().get('redshift', 'rs_user'),
                                        overwrite=True).execute()

        redshift.copy(schema=schema,
                      table=table,
                      s3_file=self.s3_bucket + "/" + filename,
                      options="""  CSV DELIMITER ',' IGNOREHEADER 1 
                                   ACCEPTINVCHARS TRUNCATECOLUMNS 
                                   TRIMBLANKS BLANKSASNULL EMPTYASNULL 
                                   DATEFORMAT 'auto' ACCEPTANYDATE 
                                   REGION '{0}' COMPUPDATE ON MAXERROR 1;""".format(redshift.s3_region))

        os.remove(self.local_file)
        client = luigi.s3.S3Client()
        if client.exists(self.s3_bucket + '/' + filename):
            client.remove(self.s3_bucket + '/' + filename)

        self.output().done()

    def output(self):
        self.local_file = 'data/{0}.csv'.format(self.table)
        return RunAnywayTarget(self)


@inherits(CopyToRedshift)
class UploadLocalCSVToS3(luigi.ExternalTask):
    resources = {'s3':1}
    host_resources = {'s3': 1}

    def requires(self):
        if self.soql:
            return self.clone(ExtractFromSOQL)
        elif self.report:
            return self.clone(ExtractFromReport)

    def run(self):
        client = luigi.s3.S3Client()
        client.put('data/' + self.table + '.csv',
                   self.s3_bucket + '/' + self.table + '.csv')
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


@inherits(CopyToRedshift)
# Extracts a maximum of 2000 records (API limit)
class ExtractFromReport(luigi.ExternalTask):
    host_resources = {'api':1}

    def run(self):
        sf = sources.Salesforce()
        data = pd.DataFrame(sf.get_report_results(sf.get_report_id(self.report)),
                            columns=[col.replace('.', '_') for col in sf.get_report_columns(sf.get_report_id(self.report))])
        data.to_csv('data/' + self.table + '.csv', index=False, header=True)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


@inherits(CopyToRedshift)
class ExtractFromSOQL(luigi.ExternalTask):
    host_resources = {'api':1}

    def run(self):
        sf = sources.Salesforce()
        data = sf.query_results(self.soql)
        data.to_csv('data/' + self.table + '.csv', index=False, header=True)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)



if __name__ == '__main__':
    luigi.run()
