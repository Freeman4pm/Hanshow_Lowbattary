# Hanshow_Lowbattary
## 数据库上传使用说明
### 文件说明：
1. main_esl.py: 用于向数据库上传客户经理收集的esl信息。
2. main_him.py: 用于向数据库上传him信息。
3. db_connection.py: 直接执行用于检测是否可以正常连接数据库。
### 配置文件setting.json说明

### 使用步骤：
0. (optional)执行db_connection.py 测试数据库是否能正常连接
1. 将esl信息的csv文件按客户名分成多个文件夹，放入esl文件夹内，单个文件名要求：【客户经理】-【国家】-【客户名】-【门店号】.csv。**注意同一个文件夹内只能放同一个客户的文件，否则程序将自动报错。**
2. 执行main_esl.py将文件上传数据库，并根据mergedata的配置将多张表进行合并。程序将自动每个csv文件中的eslid导出至esl_id文件夹中，用于生成him文件。
3. 得到him文件后将him文件以与esl文件相同的文件名和文件夹结构放入him文件夹中。
4. 执行main_him.py将him文件上传至数据库，并根据mergedata的配置将多张表进行合并。
