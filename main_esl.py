# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 20:42:56 2020

@author: hanshow
"""

import chardet
import os
from datetime import date
import datetime
import db_connection
import csv
import json 

def get_encoding(file):
    # 二进制方式读取，获取字节数据，检测类型
    with open(file, 'rb') as f:
        return chardet.detect(f.read())['encoding']
###############################Import setting file#############################
json_encoder = get_encoding("setting.json")
if json_encoder[:3] != 'utf' and 'UTF':
    json_encoder = "GBK"
with open("setting.json",'r', encoding = json_encoder) as load_f:
    str1 = load_f.read().replace("\\","\\\\")
    settings = json.loads(str1)

try:   
    path = settings['scandata']['eslpath']
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
g = os.walk(path)
time_stamp = date.today().strftime("%y%m%d")
print("================Scan data==================")
for path,dir_list,file_list in g:  
    if len(file_list) == 0:
        continue
    for i, n in enumerate(file_list):
        if n[-3:] != 'csv':
            file_list.pop(i)
    print("-----------------------------------")
    print("Reading csv data in {}".format(path))
    tag = [x.split('-')[2] for x in file_list]
    tag = list(set(tag))
    if len(tag) > 1:
        raise Exception('CustomerError: The folder ' + path + ' contains more than one customer data') 
    tag = tag[0]
    data = []
    esl_id = []
    heading = []
    for file_name in file_list:
        full_path = os.path.join(path,file_name)
        print("Reading csv file {}".format(full_path))
        encoding = get_encoding(full_path)
        if encoding[:3] != 'UTF' and encoding[:3] != 'utf':
            encoding = 'GBK'
        with open(full_path, 'r', encoding = encoding) as f:
            f_csv = csv.DictReader(f)
            for i, row in enumerate(f_csv):
                esl_id.append(row['esl_id'])
                row['tag_customer'] = tag
                if i == 0:
                    heading = list(row.keys())
                if row['esl_firmware'] == '':
                    row['esl_firmware'] = None
                if row['firmware_resolution_x'] == '':
                    row['firmware_resolution_x'] = None
                if row['firmware_resolution_y'] == '':
                    row['firmware_resolution_y'] = None
                row['esl_create_date'] = datetime.datetime.strptime(row['esl_create_date'], '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                row['tag_create_date'] = datetime.datetime.strptime(row['tag_create_date'], '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                row['esl_battery_date'] = datetime.datetime.strptime(row['esl_battery_date'], '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                data.append(list(row.values()))

    s0_table_name = 's0___' + tag + '_' + 'esl' + '_' + time_stamp
    s3_table_name = 's3___' + tag + '_' + 'esl' + '_' + time_stamp
    db_connection.create_new_db(database_host, database_port, username, password, 
                  database_name, s0_table_name)
    print("Data reading complete for folder " + path) 
    db_connection.data_to_db(database_host, database_port, username, password, 
                   database_name, s0_table_name, heading, data) 
    print("Data uploading complete for folder " + path) 
    with open('esl_id/esl_id_{}_{}.csv'.format(tag, time_stamp), 'w', newline='') as csvfile:
        for esl in esl_id:
            csvfile.write(esl)
            csvfile.write('\n')
    print("ESL-ids for {} have been exported".format(tag))
#    df.drop_duplicates("esl_id", inplace=True)
#    df['esl_id'].to_csv('esl_id/esl_id_{}_{}.csv'.format(tag, time_stamp),mode = 'w', encoding = 'GBK', index = False)
#
    db_connection.create_new_db(database_host, database_port, username, password, 
                  database_name, s3_table_name)
    db_connection.data_to_db(database_host, database_port, username, password, 
                   database_name, s3_table_name, heading, data)     
#    db_connection.data_to_db(database_host, database_port, username, password, 
#               database_name, 's3___'+tag+'_'+time_stamp, df) 
#
    dropped = db_connection.drop_duplicates_db(database_host, database_port, username, password, 
                  database_name, s3_table_name, 'esl_create_date', 'esl_id')
#    db_connection.drop_duplicates_db_2(database_host, database_port, username, password, 
#                  database_name, s0_table_name, s3_table_name)
    print("{} Duplicated eslids dropped for {}".format(dropped, tag))
#    df.drop(df.index, inplace=True)
###########################MERGEDATA###########################################
print("================Merge data==================")
table_list = []
s2_table_name = 's2___' + merge_table + '_' + 'esl' + '_' + time_stamp + '_' + 'merged'
s3_table_name = 's3___' + merge_table + '_' + 'esl' + '_' + time_stamp + '_' + 'merged'
for tag in merge_tags:
    table_tuple = db_connection.db_table_list(database_host, database_port, 
                                              username, password, database_name, tag)
    for row in table_tuple:
        table_list.append(row[0])
table_list = list(set(table_list))
print("Merging table with tags {}".format(merge_tags))
db_connection.create_new_db(database_host, database_port, username, password, 
                  database_name, s2_table_name)
db_connection.merge_tables(database_host, database_port, username, password, 
                           database_name, table_list, s2_table_name)
db_connection.create_new_db(database_host, database_port, username, password, 
                  database_name, s3_table_name)
db_connection.merge_tables(database_host, database_port, username, password, 
                           database_name, table_list, s3_table_name)
print('Data Merge Completed')
dropped = db_connection.drop_duplicates_db(database_host, database_port, username, password, 
                  database_name, s3_table_name, 'esl_create_date', 'esl_id')
print('{} Duplicated eslids dropped'.format(dropped))
#dropped = db_connection.drop_duplicates_db(database_host, database_port, username, password, 
#                  database_name, s3_table_name)
#print("{} Duplicated eslids dropped".format(dropped))

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

    

    

