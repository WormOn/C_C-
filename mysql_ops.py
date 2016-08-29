#/usr/bin/env python
#coding=utf-8

import os
import logging
import sys

import mysql.connector

__name__ = 'mysql_ops'
stdout_stream = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(threadName)s %(asctime)s '\
        '%(name)-15s %(levelname)-8s: %(message)s')
stdout_stream.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(stdout_stream)
logger.setLevel(logging.DEBUG)

db_user = os.environ.get('DB_USER', 'root')
db_pass = os.environ.get('DB_PASS', 'root')
db_host = os.environ.get('DB_HOST', '192.168.2.22')
db_port = os.environ.get('DB_PORT', '3366')
db_name = os.environ.get('DB_NAME', 'icloud')

#数据库连接
def connect_mysql():
    try:
        db = mysql.connector.connect(
            user=db_user, password=db_pass,
            host=db_host, port=db_port, database=db_name
        )                                                                                                                                            
        return db                                                                                                                                    
    except Exception as e:                                                                                                                           
        logger.error(e)                                                                                                                              
        raise 
#数据库的写入操作
def hander_mysql(db, cur, sql):                                                                                                                      
    logger.info('sql = %s'%sql)                                                                                                                      
    try:                                                                                                                                             
        cur.execute(sql)                                                                                                                             
        db.commit()                                                                                                                                  
    except Exception as e:                                                                                                                           
        logger.error(e)                                                                                                                              
        raise 
#数据库的读取操作
def select_mysql(db, cur, sql):                                                                                                                      
    logger.info('sql = %s'%sql)                                                                                                                      
    try:                                                                                                                                             
        cur.execute(sql)                                                                                                                             
        data = cur.fetchall()                                                                                                                        
        return data                                                                                                                                  
    except Exception as e:                                                                                                                           
        logger.error(str(e))                                                                                                                         
        raise  
#关闭数据库
def close_mysql(db, cur):                                                                                                                            
    cur.close()                                                                                                                                      
    db.close()    
