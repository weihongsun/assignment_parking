from selenium import webdriver
from bs4 import BeautifulSoup
import time
from google.cloud import bigquery
from pythonbq import pythonbq
from google.oauth2 import service_account

##Client to bundle configuration
credentials = service_account.Credentials.from_service_account_file('e:\Assignment-91ae2beabce8.json')   
project_id='assignment-11111'
client = bigquery.Client(credentials= credentials,project=project_id)  
dataset_ref = client.dataset('Assignment')    

##create table in BigQuery
table_ref = dataset_ref.table('park1')
#schema = [
#   bigquery.SchemaField('Date','STRING', mode='REQUIRED'),
#   bigquery.SchemaField('Time','STRING', mode='REQUIRED'),
#   bigquery.SchemaField('A_Parking','STRING', mode='REQUIRED'),
#   bigquery.SchemaField('B_Parking','STRING', mode='REQUIRED'),
#   bigquery.SchemaField('CD_Parking','STRING', mode='REQUIRED'),
#]
#table = bigquery.Table(table_ref, schema=schema)
#table = client.create_table(table)    
table = client.get_table(table_ref)    

##scrape data from website and insert into BIgQuery table
urls='https://www.laguardiaairport.com/to-from-airport/parking'
for i in range(0,10):        ## for loop for 10 hours by step of half an hour 
    driver=webdriver.Chrome()          ##invoke Chrome
    driver.implicitly_wait(10)         ## wait for 10 sec
    driver.get(urls)
    time.sleep(15)                    ## wait for 15 sec for analog 
    soup=BeautifulSoup(driver.page_source,'html.parser')      ## parse webpage 
    T=soup.find_all('div',{"class":"terminal-percentage"})    ## find out tag on key words
    T1=T[0].text.split()    ## pick up information of parking lot A
    T2=T[1].text.split()    ## B
    T3=T[2].text.split()    ## C/D
    A=T1[0]                ## ready input variables
    B=T2[0]
    C=T3[0]
    Date=time.strftime('%Y-%m-%d',time.localtime(time.time()))   ##generate date and time
    Time=time.strftime('%H:%M',time.localtime(time.time()))
    rows_to_insert = [(Date, Time,A,B,C)]           ## insert into park1 table in BigQuery
    errors = client.insert_rows(table, rows_to_insert)
    assert errors == []              ##required bu API
    driver.quit()          ##close Chrome web invoked
    time.sleep(1800)      ## 1800sec for half an hour

## query the table in BigQuery 
myProject=pythonbq(
    bq_key_path='e:\Assignment-91ae2beabce8.json',
    project_id='assignment-11111'
)
## query code
SQL_CODE="""
SELECT *
FROM Assignment.park1
"""
output=myProject.query(sql=SQL_CODE)  ## output the results
output
