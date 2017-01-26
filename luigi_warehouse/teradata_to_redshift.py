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
# This Job copies data from a teradata table to a redshift table incrementally
# If the target redshift table does not exist, it creates one
# # Arguments
#     table = target redshift table
#     schema = target redshift schema
#     sql = the sql to pull the data from the teradata table(s)
############################################################################################
import luigi
from luigi.contrib.simulate import RunAnywayTarget
from luigi.util import inherits, requires
import os
from datetime import datetime, timedelta
import pandas as pd
from . import sources


class Run(luigi.WrapperTask):
    table = luigi.Parameter()
    schema = luigi.Parameter()
    sql = luigi.Parameter()
    s3_bucket = luigi.Parameter(default=luigi.configuration.get_config().get('redshift', 'bucket'))
    fileTimestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')

    def requires(self):
        yield self.clone(CopyToRedshift)


@inherits(Run)
class Data(luigi.ExternalTask):
    resources = {'teradata': 1}
    host_resources = {'teradata': 1}

    def run(self):
        teradata_client = sources.Teradata()
        results,cols = teradata_client.query(self.sql)
        with self.output().open('w') as out_file:
            pd.DataFrame(results, columns=cols).to_csv(out_file, index=False, header=False)

    def output(self):
        return luigi.LocalTarget("data/{0}_{1}.csv".format(self.table, self.fileTimestamp))


@inherits(Run)
class UploadToS3(luigi.Task):
    resources = {'s3':1}
    host_resources = {'s3':1}

    def requires(self):
        return self.clone(Data)

    def input(self):
        return luigi.LocalTarget("data/{0}_{1}.csv".format(self.table, self.fileTimestamp))

    def output(self):
        return luigi.s3.S3Target(self.s3_bucket + "/{0}_{1}.csv".format(self.table, self.fileTimestamp))

    def run(self):
        client = luigi.s3.S3Client()
        client.put("data/{0}_{1}.csv".format(self.table, self.fileTimestamp), self.s3_bucket + "/{0}_{1}.csv".format(self.table, self.fileTimestamp))
        os.remove("data/{0}_{1}.csv".format(self.table, self.fileTimestamp))


@inherits(Run)
class CopyToRedshift(luigi.ExternalTask):
    resources = {'redshift': 1}
    host_resources = {'redshift': 1}

    def requires(self):
        return self.clone(UploadToS3)

    def run(self):
        redshift = sources.Redshift()
        s3_path = "'" + self.s3_bucket + "/{0}_{1}.csv".format(self.table, self.fileTimestamp) + "'"
        options = """CSV DELIMITER ',' IGNOREHEADER 0 
                     ACCEPTINVCHARS TRUNCATECOLUMNS 
                     TRIMBLANKS BLANKSASNULL EMPTYASNULL 
                     DATEFORMAT 'auto' ACCEPTANYDATE 
                     REGION '{0}' COMPUPDATE ON MAXERROR 1""".format(redshift.s3_region)
        redshift.copy(self.schema, self.table, s3_file, options, dedupe=True)
        try:
            client = luigi.s3.S3Client()
            if client.exists(self.s3_bucket + "/" + s3_filename):
                client.remove(self.s3_bucket + "/" + s3_filename)
        except:
            logging.error("Unable to remove " + self.s3_bucket + '/' + s3_filename + " from S3; remove this file manually")
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)


if __name__ == '__main__':
    luigi.run() 
