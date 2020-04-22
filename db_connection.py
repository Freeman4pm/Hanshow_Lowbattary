# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 12:39:51 2020

@author: hanshow
"""

from sqlalchemy import create_engine
import chardet
import pymysql
import json
from datetime import date


time_stamp = date.today().strftime("%y%m%d")
def get_encoding(file):
    # 二进制方式读取，获取字节数据，检测类型
    with open(file, 'rb') as f:
        return chardet.detect(f.read())['encoding']

def data_to_db(database_host, database_port, username, password, 
               database_name, table_name, df):
    db_data = 'mysql+mysqldb://' + username + ':' + password + '@' + database_host + ':'+ database_port + '/' \
           + database_name + '?charset=utf8'
    engine = create_engine(db_data)
    
    df.to_sql(table_name, engine, if_exists='append', index=False)
    
    engine.dispose()

def create_new_db(database_host, database_port, username, password, 
                  database_name, table_name, data_type = 'esl'):
    conn = pymysql.connect(host=database_host, port=int(database_port), 
                           user=username, passwd=password, db=database_name)
    cursor = conn.cursor()
    exist = cursor.execute("show tables like '%s'" % table_name)
    if exist:
        cursor.execute("drop table %s" % table_name)
        print("The table already exists! Old table has been dropped.")
    cursor.execute("CREATE TABLE IF NOT EXISTS " + table_name + ' ' + "(LIKE dm____struct_{}_all)".format(data_type))
    conn.commit()
    cursor.close()
    conn.close()  
    return exist
def drop_duplicates_db(database_host, database_port, username, password, 
                  database_name, table_name):
    conn = pymysql.connect(host=database_host, port=int(database_port), 
                           user=username, passwd=password, db=database_name)
    cursor = conn.cursor()
    sql = "delete from {} where auto_id not in \
             (select id from \
              (SELECT min(auto_id) as id, eslid From {} \
               group by eslid) t ) ".format(table_name, table_name)
    dropped = cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    return dropped

def drop_duplicates_db_2(database_host, database_port, username, password, 
                  database_name, table_name_s1, table_name_s3):
    conn = pymysql.connect(host=database_host, port=int(database_port), 
                           user=username, passwd=password, db=database_name)
    cursor = conn.cursor()
    sql = "SELECT REPLACE(GROUP_CONCAT(COLUMN_NAME), 'auto_id,', '')\
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' \
    AND TABLE_SCHEMA = '{}'".format(table_name_s1, database_name)
    cursor.execute(sql)
    column_list = cursor.fetchall()
    sql = "INSERT INTO {} ({}) SELECT {} FROM {} th where not exists\
                   (select 1 from {} where esl_id = th.esl_id\
                    and esl_create_date>th.esl_create_date) ".format(table_name_s3,column_list[0][0], column_list[0][0], table_name_s1, table_name_s1)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def db_table_list(database_host, database_port, username, password, 
                  database_name, tag, data_type = 'esl'):
    conn = pymysql.connect(host=database_host, port=int(database_port), 
                           user=username, passwd=password, db=database_name)
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_NAME \
                   FROM INFORMATION_SCHEMA.TABLES\
                   WHERE TABLE_TYPE = 'BASE TABLE' \
                   AND TABLE_SCHEMA = '{}' AND TABLE_NAME LIKE 's0%{}%{}%_{}'".format(database_name,tag, data_type, time_stamp))
    table_list = cursor.fetchall()
    cursor.close()
    conn.close()
    return table_list

def merge_tables(database_host, database_port, username, password, 
                  database_name, table_list, s3_table):
    conn = pymysql.connect(host=database_host, port=int(database_port), 
                           user=username, passwd=password, db=database_name)
    cursor = conn.cursor()
    sql = "SELECT REPLACE(GROUP_CONCAT(COLUMN_NAME), 'auto_id,', '')\
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' \
    AND TABLE_SCHEMA = '{}'".format(s3_table, database_name)
    cursor.execute(sql)
    column_list = cursor.fetchall()
    for table_name in table_list:
        sql = "INSERT INTO {} ({}) SELECT {} FROM {}".format(s3_table, column_list[0][0], column_list[0][0], table_name)
        cursor.execute(sql)
        conn.commit()
    cursor.close()
    conn.close()
        
if __name__ == "__main__":
    json_encoder = get_encoding("setting.json")
    with open("setting.json",'r', encoding = json_encoder) as load_f:
        str1 = load_f.read().replace("\\","\\\\")
        settings = json.loads(str1)
    database_host = settings['conn']['ip']
    database_port = settings['conn']['port']
    username = settings['conn']['uname']
    password = settings['conn']['pwd']
    database_name = settings['conn']['db']
    try:
        conn = pymysql.connect(host=database_host, port=int(database_port), 
                           user=username, passwd=password, db=database_name)
        print("Successfully connect to the database")
        conn.close()
    except:
        print("Failed to connect to the database")