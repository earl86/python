#!/usr/bin/python
# -*- coding: utf-8 -*- 
#Author：earl86
#version 1.0
#requie >=python3.4
#how to use：python sqlonline-py3.py -f xxx.sql -s dbserver.list

'''
cat dbserver.list(servicename与dbname必须以,分隔)
192.168.1.100,databasename
192.168.1.100,databasename

cat xxx.sql(每条sql必须以;结尾)
drop table if exists test;
CREATE TABLE test (
  a int(10) NOT NULL COMMENT 'ID',
  b int(10) NOT NULL DEFAULT '0' COMMENT 'xxx',
  c int(10) NOT NULL DEFAULT '0' COMMENT 'xxx',
  d int(10) NOT NULL DEFAULT '0' COMMENT 'xxx',
  e int(11) NOT NULL DEFAULT '0',
  f int(11) NOT NULL DEFAULT '0',
  g int(10) NOT NULL DEFAULT '0',
  h int(10) NOT NULL DEFAULT '0',
  i tinyint(4) NOT NULL DEFAULT '0' COMMENT '1:测试1; 0:测试2',
  PRIMARY KEY (a),
  KEY idx_b (b),
  KEY idx_c (c),
  KEY idx_d (d),
  KEY idx_e (e)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
insert into test values(1,2,3,4,5,6,7,8,9);
insert into test values(2,2,3,4,5,6,7,8,9);
insert into test values(3,2,3,4,5,6,7,8,9);
drop index idx_e on test;
create index idx_h on test(h);
'''

import sys
import argparse
import getpass
import logging
import random
import asyncio
from aiomysql import create_pool

parser = argparse.ArgumentParser()
parser.add_argument("-f", action="store", dest='sqlfile', help="input the sql file dir and file name", required=True)
parser.add_argument("-s", action="store", dest='dbserverlistfile', help="input the serverlist file dir and file name", required=True)
args = parser.parse_args()

DBUSERNAME='root'

def readallsql():
    with open(args.sqlfile, 'r') as f:
        sqlall = f.read()
    f.close()
    return sqlall

def readdbservicelist():
    dbservicelist = []
    with open(args.dbserverlistfile, 'r') as f:
        f = f.readlines()
        for line in f:
            if len(line.strip()) != line.strip().count('\n'):
                dbservicelist.append(line.strip('\n'))
    return dbservicelist


# 实际的sql访问处理函数，通过aiomysql实现异步非阻塞请求
async def query_do_something(dbservicename, dbname, sql):
    async with create_pool(host=dbservicename, port=3306, db=dbname, user=DBUSERNAME, password=dbpassword) as pool:
        async with pool.get() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(sql)
                    conn.commit()
                    print("successs!=====" + dbservicename)
                    print("***********************************************************")
                except Exception as e:
                    conn.rollback()
                    print(e)
                    print("sqlerror!=====" + dbservicename)
                    print("***********************************************************")
                    break
                cur.close()
                conn.close()

# 生成sql访问队列, 队列的每个元素包含要对某个表进行访问的函数及参数
def gen_tasks(dbservicelist, sql):
    tasks = []
    for dblist in dbservicelist:
        dbargs=dblist.split(',')
        dbservicename = dbargs[0]
        dbname = dbargs[1]
        tasks.append(
            (query_do_something, dbservicename, dbname, sql)
        )
    random.shuffle(tasks)
    return tasks


# 按批量运行sql访问请求队列
def run_tasks(tasks):
    loop = asyncio.get_event_loop()
    do_tasks = tasks
    try:
        for i in range(0, len(tasks)):
            l_tasks = tasks[i]
            do_tasks[i] = asyncio.ensure_future(l_tasks[0](*l_tasks[1:]))
        loop.run_until_complete(asyncio.gather(*do_tasks))
    except Exception as e:
        logging.warning(e)
        loop.close()


def readlinesql(dbservicelist):
    sql = ''
    with open(args.sqlfile, 'r') as f:
        f = f.readlines()
        for line in f:
            sql = sql + line.strip('\n').rstrip() + ' '
            if line.strip('\n').rstrip().endswith(';'):
                print('++++++++++++execsql++++++++++++' + sql)
                tasks = gen_tasks(dbservicelist, sql)
                run_tasks(tasks)
                sql = ''


def main():
    print('==============================Check DBServerargs=======================================')
    dbservicelist = readdbservicelist()
    print(dbservicelist)
    content = input("check dbserverargs input(y or n):")
    if (content != "y"):
        sys.exit()

    print('==============================Check Sql============================================')
    sqlall = ''
    sqlall = readallsql()
    print(sqlall)
    content = input("check sql input(y or n):")
    if (content != "y"):
        sys.exit()

    print('===============================Input Password=======================================')
    global dbpassword
    dbpassword = getpass.getpass('Enter password: ')
    readlinesql(dbservicelist)

if __name__=='__main__':
    main()
