# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 12:38:00 2020

@author: hanshow
"""

import chardet
import os
from datetime import date
import pandas as pd
import db_connection
import json 

def get_encoding(file):
    # 二进制方式读取，获取字节数据，检测类型
    with open(file, 'rb') as f:
        return chardet.detect(f.read())['encoding']
###############################Import setting file#############################
json_encoder = get_encoding("setting.json")
with open("setting.json",'r', encoding = json_encoder) as load_f:
    str1 = load_f.read().replace("\\","\\\\")
    settings = json.loads(str1)
    
try:   
    path = settings['scandata']['himpath']
except:
    print("no data to scan")
database_host = settings['conn']['ip']
database_port = settings['conn']['port']
username = settings['conn']['uname']
password = settings['conn']['pwd']
database_name = settings['conn']['db']
try:
    merge_tags = settings['mergedata']['fromtag']
    merge_table = settings['mergedata']['totag']
except:
    print("no data to merge")




###############################SCANDATA#######################################
file_list = os.listdir(path)
time_stamp = date.today().strftime("%y%m%d")
print("================Scan data==================")
column_names = ['eslid','eslcode','outdate','hardwareversion']
for file_name in file_list:
    df = pd.DataFrame()
    tag = file_name.split('-')[2]
    full_path = os.path.join(path,file_name)
    print("converting codec {}".format(full_path))
    encoding = get_encoding(full_path)
    if encoding[:3] != 'UTF' and encoding[:3] != 'utf':
        df = pd.read_csv(full_path,encoding = 'GBK', header = None, names = column_names)
    else:
        df = pd.read_csv(full_path,encoding = encoding, header = None, names = column_names)
    df['outdate'] = df['outdate'].replace('	null', '	1900-01-01 00:00:00.00')
    df['outdate'] = pd.to_datetime(df['outdate'], format = "%Y-%m-%d %H:%M:%S")

    s0_table_name = 's0___' + tag + '_' + 'him' + '_' + time_stamp
    s3_table_name = 's3___' + tag + '_' + 'him' + '_' + time_stamp
    db_connection.create_new_db(database_host, database_port, username, password, 
                  database_name, s0_table_name, data_type = 'him')

    db_connection.data_to_db(database_host, database_port, username, password, 
                   database_name, s0_table_name, df) 
    print("Data reading complete {}".format(full_path)) 
    
    
    db_connection.create_new_db(database_host, database_port, username, password, 
                  database_name, s3_table_name, data_type = 'him')
    db_connection.data_to_db(database_host, database_port, username, password, 
               database_name, s3_table_name, df) 
#
    dropped = db_connection.drop_duplicates_db(database_host, database_port, username, password, 
                  database_name, s3_table_name)
#    db_connection.drop_duplicates_db_2(database_host, database_port, username, password, 
#                  database_name, s0_table_name, s3_table_name)
    print("Duplicated eslids dropped for {}".format(tag))
###########################MERGEDATA###########################################
print("================Merge data==================")
s2_table_name = 's2___' + merge_table + '_' + 'him' + '_' + time_stamp + '_' + 'merged'
s3_table_name = 's3___' + merge_table + '_' + 'him' + '_' + time_stamp + '_' + 'merged'

table_list = []
for tag in merge_tags:
    table_tuple = db_connection.db_table_list(database_host, database_port, 
                                              username, password, database_name, tag, data_type = 'him')
    for row in table_tuple:
        table_list.append(row[0])
table_list = list(set(table_list))
db_connection.create_new_db(database_host, database_port, username, password, 
                  database_name, s3_table_name, data_type = 'him')
db_connection.merge_tables(database_host, database_port, username, password, 
                           database_name, table_list, s3_table_name)
#db_connection.create_new_db(database_host, database_port, username, password, 
#                  database_name, s3_table_name, data_type = 'him')
#db_connection.drop_duplicates_db_2(database_host, database_port, username, password, 
#                  database_name, s2_table_name, s3_table_name)
print('Data Merge Completed')
dropped = db_connection.drop_duplicates_db(database_host, database_port, username, password, 
                  database_name, s3_table_name)
print("{} Duplicated eslids dropped".format(dropped))

######################数据去重并将s3表输入数据库###################################
#df_l1 = df.shape[0]
##df.drop_duplicates('esl_id', 'first', inplace = True)
##df_l2 = df.shape[0]
##print("{} Duplicated eslids dropped".format(df_l1-df_l2))
#
#######################保存合并后的表以及提取出的eslid###############################
#if df_l1 != 0:
#    df.to_csv('esl_data.csv',mode = 'a', encoding = 'GBK', index = False)
#    df['esl_id'].to_csv('esl_id.csv',mode = 'a', encoding = 'GBK', index = False)
print("Finished")