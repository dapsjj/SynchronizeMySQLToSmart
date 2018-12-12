# -*- coding: UTF-8 -*-

import pymysql
import datetime
import time
import paramiko
import logging
import os
import configparser
import decimal #不加打包成exe会出错


conn = None  # 连接
cur = None  # 游标



def read_dateConfig_file_set_database():
    '''
    读dateConfig.ini,设置数据库信息
    '''
    if os.path.exists(os.path.join(os.path.dirname(__file__), "dateConfig.ini")):
        try:
            conf = configparser.ConfigParser()
            conf.read(os.path.join(os.path.dirname(__file__), "dateConfig.ini"), encoding="utf-8-sig")
            mysql_server = conf.get("mysql_server", "mysql_server")
            mysql_user = conf.get("mysql_user", "mysql_user")
            mysql_password = conf.get("mysql_password", "mysql_password")
            mysql_database = conf.get("mysql_database", "mysql_database")
            linux_hostname = conf.get("linux_hostname", "linux_hostname")
            linux_port = conf.get("linux_port", "linux_port")
            linux_username = conf.get("linux_username", "linux_username")
            linux_password = conf.get("linux_password", "linux_password")
            linux_save_path = conf.get("linux_save_path", "linux_save_path")
            return mysql_server, mysql_user, mysql_password, mysql_database, linux_hostname, linux_port, linux_username, linux_password, linux_save_path
        except Exception as ex:
            logger.error("Content in dateConfig.ini about database has error.")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("DateConfig.ini doesn't exist!")


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


def getConn():
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


def get_data_from_t_m_categorytab():
    '''
    从t_m_categorytab获取数据
    '''
    try:
        sql = " select " \
            " tabid, "  \
            " case when length(tabname)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tabname,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end  as tabname, " \
            " categoryId, " \
            " case when length(creator)=0 then '_' else ifnull(creator,'_') end  as creator, " \
            " ifnull(DATE_FORMAT(createtime,'%Y%m%d%H%i%s'),'19000101000000') as createtime, " \
            " case when length(modifier)=0 then '_' else ifnull(modifier,'_') end  as modifier, " \
            " ifnull(DATE_FORMAT(modifytime,'%Y%m%d%H%i%s'),'19000101000000') as modifytime " \
            " from newminicollege.t_m_categorytab "
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            # for row in rows:
            #     list_categorytab.append(list(row))
            list_categorytab = [list(row) for row in rows]
            strList = []
            for row in list_categorytab:
                myList = [str(item) for item in row]
                strList.append(myList)
            return strList
        else:
            return ""
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_data_from_t_m_categorytab() error!")
        logger.error("Exception:" + str(ex))
        raise ex


def get_data_from_t_m_category():
    '''
    从t_m_category获取数据
    '''
    try:
        sql = " select " \
            " categoryId, " \
            " companyId, " \
            " ifnull(parentId,0) as parentId, " \
            " case when length(name)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as name, " \
            " ifnull(status,0) as status, " \
            " type, " \
            " ifnull(orderNum,0) as orderNum, " \
            " ifnull(sizeflag,0) as sizeflag, " \
            " case when length(creator)=0 then '_' else ifnull(creator,'_') end as creator, " \
            " ifnull(DATE_FORMAT(createtime,'%Y%m%d%H%i%s'),'19000101000000') as createtime, " \
            " case when length(modifier)=0 then '_' else ifnull(modifier,'_') end as modifier, " \
            " ifnull(DATE_FORMAT(modifytime,'%Y%m%d%H%i%s'),'19000101000000') as modifytime " \
            " from newminicollege.t_m_category "
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            # for row in rows:
            #     list_categorytab.append(list(row))
            list_category = [list(row) for row in rows]
            strList = []
            for row in list_category:
                myList = [str(item) for item in row]
                strList.append(myList)
            return strList
        else:
            return ""
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_data_from_t_m_category() error!")
        logger.error("Exception:" + str(ex))
        raise ex


def get_data_from_t_m_course():
    '''
    从t_m_course获取数据
    '''
    try:
        sql = " select " \
            " companyId, " \
            " courseId, " \
            " case when length(courseName)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(courseName,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as courseName, " \
            " ifnull(courseType,0) as courseType, " \
            " case when length(courseSee)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(courseSee,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as courseSee, " \
            " ifnull(status,0) as status, " \
            " case when length(creator)=0 then '_' else ifnull(creator,'_') end as creator, " \
            " ifnull(DATE_FORMAT(createtime,'%Y%m%d%H%i%s'),'19000101000000') as createtime, " \
            " case when length(modifier)=0 then '_' else ifnull(modifier,'_') end as modifier, " \
            " ifnull(DATE_FORMAT(modifytime,'%Y%m%d%H%i%s'),'19000101000000') as modifytime " \
            " from newminicollege.t_m_course "
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            # for row in rows:
            #     list_categorytab.append(list(row))
            list_course = [list(row) for row in rows]
            strList = []
            for row in list_course:
                myList = [str(item) for item in row]
                strList.append(myList)
            return strList
        else:
            return ""
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_data_from_t_m_course() error!")
        logger.error("Exception:" + str(ex))
        raise ex



def get_data_from_t_m_subcourse():
    '''
    从t_m_subcourse获取数据
    '''
    try:
        sql = " select " \
            " case when length(managmentId)=0 then '_' else ifnull(managmentId,'_') end as managmentId, " \
            " companyId, " \
            " courseId, " \
            " subCourseId, " \
            " case when length(subCourseComment)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(subCourseComment,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as subCourseComment, " \
            " case when length(subCourseContent)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(subCourseContent,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as subCourseContent, " \
            " case when length(subCourseName)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(subCourseName,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as subCourseName, " \
            " case when length(subCourseImg)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(subCourseImg,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as subCourseImg, " \
            " case when length(playtime)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(playtime,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as playtime, " \
            " ifnull(status,0) as status, " \
            " unpublished, " \
            " ipflag, " \
            " ifnull(dogaflag,0) as dogaflag, " \
            " ifnull(commendflag,0) as commendflag, " \
            " ifnull(carouselflag,0) as carouselflag, " \
            " showstatus, " \
            " case when length(tag)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tag,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_')  end as tag, " \
            " case when length(creator)=0 then '_' else ifnull(creator,'_') end as creator, " \
            " ifnull(DATE_FORMAT(createtime,'%Y%m%d%H%i%s'),'19000101000000') as createtime, " \
            " case when length(modifier)=0 then '_' else ifnull(modifier,'_') end as modifier, " \
            " ifnull(DATE_FORMAT(modifytime,'%Y%m%d%H%i%s'),'19000101000000') as modifytime " \
            " from newminicollege.t_m_subcourse "
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            # for row in rows:
            #     list_categorytab.append(list(row))
            list_subcourse = [list(row) for row in rows]
            strList = []
            for row in list_subcourse:
                myList = [str(item) for item in row]
                strList.append(myList)
            return strList
        else:
            return ""
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_data_from_t_m_subcourse() error!")
        logger.error("Exception:" + str(ex))
        raise ex


def get_data_from_t_u_joincourse():
    '''
    从t_u_joincourse获取数据
    '''
    try:
        sql = " select " \
            " companyId, " \
            " case when length(joinCourseId)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(joinCourseId,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as joinCourseId, " \
            " joinSubCourseId, " \
            " case when length(userCd)=0 then '_' else ifnull(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(userCd,'<br>','_'),char(13), '_'),char(10), '_'),char(9), '_'),'　', '_'),' ', '_'),'_') end as userCd, " \
            " ifnull(joinCnt,0) as joinCnt, " \
            " ifnull(learnTime,0) as learnTime, " \
            " ifnull(status,0) as status, " \
            " case when length(url)=0 then '_' else ifnull(url,'_') end as url, " \
            " case when length(creator)=0 then '_' else ifnull(creator,'_') end as creator, " \
            " ifnull(DATE_FORMAT(createtime,'%Y%m%d%H%i%s'),'19000101000000') as createtime, " \
            " case when length(modifier)=0 then '_' else ifnull(modifier,'_') end as modifier, " \
            " ifnull(DATE_FORMAT(modifytime,'%Y%m%d%H%i%s'),'19000101000000') as modifytime " \
            " from newminicollege.t_u_joincourse " \
            " where DATE_FORMAT(createtime,'%Y%m%d') = DATE_FORMAT(date_sub(curdate(),interval 1 day),'%Y%m%d') " \
            " or DATE_FORMAT(modifytime,'%Y%m%d') = DATE_FORMAT(date_sub(curdate(),interval 1 day),'%Y%m%d') "
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            # for row in rows:
            #     list_categorytab.append(list(row))
            list_joincourse = [list(row) for row in rows]
            strList = []
            for row in list_joincourse:
                myList = [str(item) for item in row]
                strList.append(myList)
            return strList
        else:
            return ""
    except pymysql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_data_from_t_u_joincourse() error!")
        logger.error("Exception:" + str(ex))
        raise ex


def save_txt_to_disk(fileName,para_list):
    '''
    :param fileName: 要保存的文本文件名
    :param para_list: 要保存的list
    '''
    with open(fileName, "w", encoding="utf-8") as fo:
        # para_list = [i[:-1] + [""] if i[-1] == 'None' else i for i in para_list]
        # for row in para_list:
        #     list1 = [(item + " ") for item in row]
        #     fo.writelines(list1)
        #     fo.write('\n')
        fo.write('\n'.join([' '.join(i) for i in para_list]))


def sftp_upload_file(server_path, local_path_list):
    '''
    :param server_path: linux保存文本文件的路径
    :param local_path_list: 要上传到linux的文本文件的名称list
    :return:
    '''
    try:
        t = paramiko.Transport((linux_hostname, int(linux_port)))
        t.connect(username=linux_username, password=linux_password)
        sftp = paramiko.SFTPClient.from_transport(t)
        if not os.path.exists(server_path):
            logger.error("Path "+server_path+" not exists!")
            # return
        for file_path in local_path_list:
            linux_file = server_path + file_path
            sftp.put(file_path, linux_file)
        t.close()
    except Exception as ex:
        logger.error("Call method sftp_upload_file() error!")
        logger.error("Exception:" + str(ex))
        raise ex


if __name__=="__main__":
    logger = write_log()  # 获取日志对象
    time_start = datetime.datetime.now()
    start = time.clock()
    logger.info("Program start,now time is:"+str(time_start))
    mysql_server, mysql_user, mysql_password, mysql_database, linux_hostname, linux_port, linux_username, linux_password, linux_save_path = read_dateConfig_file_set_database()#读取配置文件中的数据库信息
    getConn()#数据库连接对象
    current_date = datetime.datetime.now().strftime("%Y-%m-%d") #系统当前日期
    date_common = datetime.datetime.now().strftime("%Y%m%d")
    categorytab_file = r"categorytab_" + date_common #设置文件名categorytab_+当前日期
    category_file = r"category_" + date_common #设置文件名category_+当前日期
    course_file = r"course_" + date_common #设置文件名course_+当前日期
    subcourse_file = r"subcourse_" + date_common #设置文件名subcourse_+当前日期
    joincourse_file = r"joincourse_" + date_common #设置文件名joincourse_+当前日期
    list_categorytab = get_data_from_t_m_categorytab() #从mysql获取t_m_categorytab数据
    save_txt_to_disk(categorytab_file,list_categorytab) #保存数据到程序所在的文件夹
    list_category = get_data_from_t_m_category() #从mysql获取t_m_category数据
    save_txt_to_disk(category_file, list_category) #保存数据到程序所在的文件夹
    list_course = get_data_from_t_m_course() #从mysql获取t_m_course数据
    save_txt_to_disk(course_file,list_course) #保存数据到程序所在的文件夹
    list_subcourse = get_data_from_t_m_subcourse() #从mysql获取t_m_subcourse数据
    save_txt_to_disk(subcourse_file, list_subcourse) #保存数据到程序所在的文件夹
    list_joincourse = get_data_from_t_u_joincourse() #从mysql获取t_u_joincourse数据
    save_txt_to_disk(joincourse_file, list_joincourse) #保存数据到程序所在的文件夹
    local_file_list = [categorytab_file, category_file, course_file, subcourse_file, joincourse_file]
    sftp_upload_file(linux_save_path,local_file_list) #从程序所在文件夹上传文本文件到linix中
    closeConn()
    time_end = datetime.datetime.now()
    end = time.clock()
    logger.info("Program end,now time is:"+str(time_end))
    logger.info("Program run : %f seconds" % (end - start))



