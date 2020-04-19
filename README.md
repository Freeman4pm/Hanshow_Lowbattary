# Hanshow_Lowbattary
## 数据库上传使用说明
### 文件说明：
1. main_esl.py: 用于向数据库上传客户经理收集的esl信息。
2. main_him.py: 用于向数据库上传him信息。
3. db_connection.py: 直接执行用于检测是否可以正常连接数据库。
### 配置文件setting.json说明：
1. "conn":{"db":【数据库名】,"uname":【数据库登录用户名】,"pwd":【数据库登录密码】,"port":【数据库端口】,"ip":【数据库ip地址】}
2. "scandata":{"eslpath":【esl文件所在根目录名】, "himpath":【him文件所在根目录名】} **如果将eslpath和himpath置空则会跳过上传数据库的步骤**
3. "mergedata":{"fromtag":【需要合并的数据表名称列表】,"totag":【合并后的数据表名】} **如果将fromtag置空则会跳过合并数据表的步骤**
### 使用步骤：
0. (optional)执行db_connection.py 测试数据库是否能正常连接
1. 将esl信息的csv文件按客户名分成多个文件夹，放入esl文件夹内（例如：将来自华润的csv文件统一放入“esl/华润/”路径下）。单个文件名格式要求：【客户经理】-【国家】-【客户名】-【门店号】.csv（例如：张三-中国-华润-101）。**注意同一个文件夹内只能放同一个客户的文件，否则程序将自动报错。**
2. 执行main_esl.py将文件上传数据库，并根据mergedata的配置将多张表进行合并。程序将自动csv文件中的eslid按客户名分开导出至esl_id文件夹中，将him文件提交him数据库管理员用于生成him文件。
3. 得到him文件后将him文件按照【客户经理】-【国家】-【客户名】.csv的格式命名并放入him文件夹中。
4. 执行main_him.py将him文件上传至数据库，并根据mergedata的配置将多张表进行合并。

## 低电报告生成
*待开发中。*
