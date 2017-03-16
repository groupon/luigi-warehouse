# Luigi-Warehouse 
#### A boilerplate implementation of [Luigi](http://luigi.readthedocs.org/en/stable/index.html) at Groupon
![pic](https://i.kinja-img.com/gawker-media/image/upload/s--u1D3aUdn--/p5onht9mlgdkj0yvnjao.jpg)

* [Luigi](http://luigi.readthedocs.org/en/stable/index.html) is a Python package that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization, handling failures, command line integration, and much more
 
* Luigi-Warehouse adds 
 * example workflows (i.e. replicating postgresql tables to redshift) 
 * more data sources
 * variable data sources that do not rely on default luigi behavior/configs (i.e. `VariableS3Client`)


## Install / Setup

* Install [python3](https://www.continuum.io/downloads) - This repo has been tested against python 3.4+

###### Simple

`python setup.py install`

###### Developers - if you're wanting to modify/use the workflows with your custom logic
* Clone this repo 
* `pip3 install -r requirements.txt` if you want full functionality of all data sources

###### Post-Install
* `mkdir your-path-to/data`
* Put your credentials and settings in `luigi.cfg`.  `luigi.cfg-example` shows some possible options. You can also `$ export LUIGI_CONFIG_PATH=/path/to/your/luigi.cfg && python...`
* You're ready to replicate or move data around...


## Getting Started

* Some example workflows are included.  Assumptions, Args & Comments are in the File

| File                      | Description                                                                               | Main Class(es)                                   |
| ------------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------ |
| gsheet_to_redshift.py     | replicates all data from a google sheet to a redshift table (full copy/replace)           | Run                                              |
| gsheet_to_hadoop.py     | replicates all data from a google sheet to a hadoop hive table via spark (full copy/replace)           | __main__                                              |
| postgres_to_redshift.py   | replicates postgres tables to redshift (incrementally or full copy/replace)               | Run              - PerformIncrementalImport PerformFullImport |
| postgres_to_hadoop.py     | spark app that replicates postgres tables to hadoop(hive) (incrementally or copy/replace) | Run              - RunIncremental RunFromScratch |
| salesforce_to_redshift.py | replicates a salesforce report or SOQL to a redshift table(full copy/replace)             | SOQLtoRedshift ReporttoRedshift                  |
| teradata_to_redshift.py   | replicates given teradata SQL to redshift table (incrementally or full copy/replace)      | Run                                              |
| typeform_to_redshift.py   | replicates all data from typeform responses to a redshift table (full copy/replace)       | Run                                              |
| zendesk_to_redshift.py    | extracts users,orgs,tickets,ticket_events from zendesk to redshift (partially incremental)| Run                                              |
| zendesk_to_hadoop.py    | generic class to extract from zendesk API and load to hadoop hive via spark (incrementally or full copy/replace) | ZendeskSpark                                              |


* Example to start the luigi scheduler daemon
```
$ ./start_luigi_server.bash
```

* Example to run a workflow with multiple workers in parallel
```
$ LUIGI_CONFIG_PATH=/path/to/your/luigi.cfg && python3 luigi_warehouse/postgres_to_redshift.py Run --params here --workers 50
```


## Data Sources 

### Dependent python packages required & API reference

##### Luigi - [Spotify/Luigi](https://github.com/spotify/luigi/tree/7db1ceadc4d3f8ef62eccffa1a6412bc747f8132/luigi/contrib)

##### Postgres / Redshift - [psycopg2](http://initd.org/psycopg/docs/install.html)
 
##### MySQL - [pymysql](https://github.com/PyMySQL/PyMySQL)

##### Adwords - [googleads](https://github.com/googleads/googleads-python-lib) : [API Reference](https://developers.google.com/adwords/api/docs/guides/start) 

##### Googlesheets - [gspread](https://github.com/burnash/gspread) : [API Reference](https://developers.google.com/google-apps/spreadsheets/)

##### Slack - [slackclient](https://github.com/slackapi/python-slackclient) : [API Reference](https://api.slack.com/)

##### Five9 - [suds](https://bitbucket.org/jurko/suds) : [API Reference](https://github.com/Five9DeveloperProgram)

##### Twilio - [twilio](https://github.com/webficient/twilio) : [API Reference](https://www.twilio.com/docs/api/rest)

##### Livechat - [API Reference](https://developers.livechatinc.com/rest-api/) 

##### Zendesk - [zdesk](https://github.com/fprimex/zdesk) : [API Reference](https://developer.zendesk.com/rest_api/docs/core/introduction)

##### Shiftplanning - [API Reference](https://www.shiftplanning.com/api/)

##### Kochava - [API Reference](https://support.kochava.com/analytics-reports-api)

##### Teradata - [teradata](https://developer.teradata.com/tools/reference/teradata-python-module)

 * requires some configuring to install. We typically have to do

 ```
$ mv ~/.odbc.ini ~/.odbc.ini.orig 
$ cp /opt/teradata/client/15.10/odbc_64/odbcinst.ini ~/.odbcinst.ini 
$ cp /opt/teradata/client/15.10/odbc_64/odbc.ini ~/.odbc.ini
 ```

##### OnboardIQ - [API Reference](https://www.onboardiq.com/docs/apiv2)

##### AppBoy - [API Reference](https://www.appboy.com/documentation/REST_API/)

##### Salesforce - [simple-salesforce](https://github.com/simple-salesforce/simple-salesforce) : [API Reference](https://developer.salesforce.com/page/Salesforce_APIs) 

* Props to [cghall](https://github.com/cghall/salesforce-reporting) for the capability to query salesforce reports directly using the analytics API

* Also available are `SalesforceBulk` and `SalesforceBulkJob` classes which use the Salesforce bulk API

##### Braintree - [braintree](https://github.com/braintree/braintree_python) : [API Reference](https://developers.braintreepayments.com/)

##### Typeform - [API Reference](https://www.typeform.com/help/data-api/)

##### Checkr - [API Reference](https://docs.checkr.com/)

##### AWS - [boto](https://github.com/boto/boto) : [boto3](https://github.com/boto/boto3)


## Notifications

* We currently use slack or email for job status notifications which can easily be added

* [luigi-slack](https://github.com/bonzanini/luigi-slack)

```python
from luigi_slack import SlackBot, notify
slack_channel = 'luigi-status-messages'
...
...
...

if __name__ == '__main__':
  slack_channel = 'luigi-status-messages'
  slacker = SlackBot(token=luigi.configuration.get_config().get('slackbots', 'BOWSER_SLACK_API_KEY'),
                   channels=[slack_channel])
  with notify(slacker):
    luigi.run() 
```

* [AWS SES](https://aws.amazon.com/ses/)

```python
import boto3

class Email:
  def __init__(self, region, aws_access_key_id, aws_secret_access_key):
    self.client = boto3.client('ses',region_name=region,aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

  def send(self, from_, to_list, subject, body):
    return self.client.send_email(Source=from_,
                                  Destination={'ToAddresses': to_list},
                                  Message={'Subject':
                                                     {'Data': subject},
                                           'Body':
                                                     {'Text':
                                                             {'Data': body},
                                                      'Html':
                                                              {'Data':' '}
                                                      }
                                            }
                                   )
```


## Data Validation 

* Targeted towards ensuring successful replication of data to Redshift (see `modules/validation.py`)

##### Structure

* if the same number of columns in the csv are in the target table
* if the columns have the same datatypes in the same order (`VARCHAR` is acceptable for any python datatype)
  * uses python_redshift_dtypes to convert 

##### LoadError

* Checks for load errors for the target:schema:table provided since the load_start provided timestamp

##### RunAnywayTarget

* Use the wrapper class RunAnywayTarget if you want to make it easier as we make each validation scheme better
* pass in the `taskobj` with the following attributes
  *  `type` = ['LoadError', 'Structure']
  *  `target` = Redshift
  *  `table` =
  *  `schema` = 
  *  `local_file` = local csv file path
  *  `load_start` = when you started to copy the records from S3

* doing `RunAnywayTarget(self).done()` will not do validation
* doing `RunAnywayTarget(self).validation()` will do the validation and if successful also say we're done the task


##### OrderedDF
 
* Takes the following args
1. `target_cols` : a list of columns ordered for how you want your dataframe to be structured
2. `df`          : your dataframe you want restructured

* example: I my dataframe to have columns in this order `['one','two','three','four','five','six']`
```python
>>> from validation import OrderedDF
>>> import pandas as pd
>>> test = [[None,'',1,7,8],[None,'',2,5,6]]
>>> test = pd.DataFrame(test,columns=['one','two','four','five','three'])
>>> test
    one two  four  five  three
0  None         1     7      8
1  None         2     5      6
>>> result = OrderedDF(['one','two','three','four','five','six'],t)
>>> result.df
    one two  three  four  five   six
0  None          8     1     7  None
1  None          6     2     5  None
```

##### StructureDynamic

* This class will fix tables for you

1.  Check for copy errors
2.  Handle the copy errors
  * Add column(s) if needed
  * Change dtype(s) if needed
3.  Get orig table's schema 
4.  Craft new table's schema with changes from errors
5.  Make the change and retry the copy and remove duplicate * records
6.  While there are copy errors
  * handle the errors
  * attempt to fix
  * retry copy
  * remove duplicate * records
              
* To run use 
```python
StructureDynamic(target_schema=  ,# redshift schema your table is in
                 target_table=    # your table
                 )
                 .run(
                      add_cols=  ,# True or False for if you want columns added in attempting to fix
                      change_dtypes=  ,# True or False if you want column data types changed in attempting to fix
                      copy=           ,# copy command you attempted
                      load_start=      # when you started the copy command, '%Y-%m-%d %H:%M:$S
                      )
```
 
* Example usage:
  * sql prep: create the table
```sql
CREATE TABLE public.test(id INT, col VARCHAR);
INSERT INTO test VALUES (1,'2');
INSERT INTO test VALUES (2, 'two');
```
  * test.csv: create the csv you want to attempt to copy
```
1,2
two,2
3,4
5,6
ab,test
```
  * we attempt to copy normally but we get load errors because one of the columns isn't right
```sql
COPY public.test FROM 's3://luigi-godata/test.csv' 
CREDENTIALS 'aws_access_key_id=XXXX;aws_secret_access_key=XXXX'
CSV DELIMITER ',' COMPUPDATE ON MAXERROR 0;
```
  * we run ValidationDynamic
```python
from validation import StructureDynamic
copy = '''COPY public.test FROM 's3://luigi-godata/test.csv' 
          CREDENTIALS 'aws_access_key_id=XXXX;aws_secret_access_key=XXXX'
          CSV DELIMITER ',' COMPUPDATE ON MAXERROR 0;'''
StructureDynamic(target_schema='public',target_table='test').run(add_cols=True,change_dtypes=True,copy=copy,load_start='2016-10-6 10:15:00')
```
  * our table is fixed and called `public.test`
  * our original table is kept as `public.test_orig_backup`
  * stdout lists the stl_load_errors
  * the changes made to the table's ddl is printed to stdout
  