# -*- coding: utf-8 -*-
import psycopg2
import pymssql
import os
import uuid
import csv
import boto3
from botocore.client import Config

def handler(event, context):
    ret = {
        "statusCode":200,
        "body": ""
    }

    #get input value
    dbhost = event.get("dbhost")
    dbname = event.get("dbname")
    dbuser = event.get("dbuser")
    dbpassword = event.get("dbpassword")
    dbquery = event.get("dbquery")
    dbtype = event.get("dbtype","redshift")

    
    return ret


# connect to database and fetch data
def getdatafromdb(dbname,dbhost,dbuser,dbpassword,dbquery):
    print('connecting to database...')
    conn = psycopg2.connect(dbname=dbname, host=dbhost, port= '5439', user= dbuser, password= dbpassword)
    print('connection successful')

    #execute query
    curr = conn.cursor()
    curr.execute(dbquery)
    recs = curr.fetchall()
    
    # close connection and cursor
    curr.close()
    conn.close()
    return recs    

