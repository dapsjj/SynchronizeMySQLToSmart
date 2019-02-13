#!/usr/bin/env python3

import pymysql
import datetime
import time
import logging
import os

conn = None  # 连接
cur = None  # 游标


def write_log():
    '''
    写log
    :return: 返回logger对象
    '''
    # 获取logger实例，如果参数为空则返回root logger
    logger = logging.getLogger()
    now_date = datetime.datetime.now().strftime('%Y%m%d')
    log_file = now_date+".log"# 文件日志
    if not os.path.exists("log"):#python文件同级别创建log文件夹
        os.makedirs("log")
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)s line:%(lineno)s %(message)s')
    file_handler = logging.FileHandler("log" + os.sep + log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter) # 可以通过setFormatter指定输出格式
    # 为logger添加的日志处理器，可以自定义日志处理器让其输出到其他地方
    logger.addHandler(file_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    return logger


def getConn(mysql_server, mysql_user, mysql_password, mysql_database):
    '''
    声明数据库连接对象
    '''
    global conn
    global cur
    try:
        conn = pymysql.connect(mysql_server, mysql_user, mysql_password, mysql_database)
        cur = conn.cursor()
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method getConn() error!")
        raise ex


def closeConn():
    '''
    关闭数据库连接对象
    '''
    global conn
    global cur
    try:
        if conn.open:
            cur.close()
            conn.close()
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method closeConn() error!")
        raise ex
    finally:
        if conn.open:
            cur.close()
            conn.close()


def read_txtConfig_file():
    '''
    读配置文件
    '''
    txt_config_list = []
    try:
        with open('config', 'r', encoding='utf-8') as txtConfig:
            lines = txtConfig.readlines()
            for line in lines:
                line = line.strip()
                if not line: #如果line是空
                    continue
                else:
                    row_list = line.split(" ")
                    txt_config_list.append(row_list)
            return txt_config_list
    except Exception as ex:
        logger.error("Call method read_txtConfig_file() error!")
        raise ex


def read_mysql_select(file_name):
    '''
    :param file_name:select文件名
    :return:查询语句
    '''
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            sql_data = f.read()
            return sql_data
    except Exception as ex:
        logger.error("Call method read_mysql_select() error!")
        raise ex


def mysql_data_wtite_to_linux(config_list):
    '''
    :param config_list:配置文件列表
    '''
    try:
        if config_list:
            for config in config_list:
                list_write_to_linux = get_data_from_mysql_table(config)
                save_txt_to_disk(config,list_write_to_linux)
    except Exception as ex:
        logger.error("Call method mysql_data_wtite_to_linux() error!")
        raise ex


def save_txt_to_disk(para_config,para_list):
    '''
    :param para_config:一行配置文件
    :param para_list:一个表的list
    '''
    try:
        file_path = para_config[10]
        file_name = para_config[5]
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        with open(file_path + file_name + "_" + date_common, "w", encoding="utf-8") as fo:
            fo.write('\n'.join([' '.join(i) for i in para_list]))
    except Exception as ex:
        logger.error("Call method save_txt_to_disk() error!")
        raise ex


def get_data_from_mysql_table(para_list):
    '''
    :param para_list:一行配置文件
    :return:
    '''
    try:
        mysql_server = para_list[0]
        mysql_user = para_list[1]
        mysql_password = para_list[2]
        mysql_database = para_list[3]
        select_sql_file = para_list[4]
        select_statement = read_mysql_select(select_sql_file)
        getConn(mysql_server, mysql_user, mysql_password, mysql_database)
        sql = select_statement
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            list_select = [list(row) for row in rows]
            strList = []
            for row in list_select:
                myList = [str(item) for item in row]
                strList.append(myList)
            closeConn()
            return strList
        else:
            return ""
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_data_from_mysql_table() error!")
        logger.error("Exception:" + str(ex))
        raise ex


if __name__=="__main__":
    logger = write_log()  # 获取日志对象
    time_start = datetime.datetime.now()
    start = time.time()
    logger.info("Program start,now time is:"+str(time_start))
    current_date = datetime.datetime.now().strftime("%Y-%m-%d") #系统当前日期
    date_common = datetime.datetime.now().strftime("%Y%m%d")
    mysql_linux_config_list = read_txtConfig_file() #读文本配置文件
    mysql_data_wtite_to_linux(mysql_linux_config_list) #写文件到Linux
    time_end = datetime.datetime.now()
    end = time.time()
    logger.info("Program end,now time is:"+str(time_end))
    logger.info("Program run : %f seconds" % (end - start))



