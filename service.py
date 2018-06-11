# -*- coding: utf-8 -*-
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor

def handler(event, context):
    ret = {
        "statusCode":200,
        "body": ""
    }

    #get input value
    dbhost = "ivy-dev.chwrf3ujkohq.ap-southeast-1.redshift.amazonaws.com"
    dbname = "dev_png"
    dbuser = "admin"
    dbpassword = "Ivy12345"
    dbtype = event.get("dbtype","redshift")

    #Open Connection
    print('connecting to database...')
    conn = psycopg2.connect(dbname=dbname, host=dbhost, port= '5439', user= dbuser, password= dbpassword)
    print('connection successful')

    #Stock Validation Process--- Need to check org_id
    dbquery = "select * from Config_Application where config_type='SALES_ORDER' AND config_key='ORDER_ON_PARENT_SKU'"  
    ConfigApp_data = getdatafromdb(conn,dbquery)



    #Scheme Process

    #Discount Details

    #Tax Details

    #Other Changes

    conn.close()
    return ret


# connect to database and fetch data
def getdatafromdb(conn,dbquery):    

    #execute query
    curr = conn.cursor(cursor_factory=RealDictCursor)
    curr.execute(dbquery)
    df = pd.DataFrame(curr.fetchall())
    #df.columns = resoverall.keys()

    #Another way of fetching data
    #df = pd.read_sql(dbquery, con=conn)
    
    # close cursor
    curr.close()    
    return df    

