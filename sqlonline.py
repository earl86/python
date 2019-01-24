#!/usr/bin/python
#coding=utf-8

import os
import string
import sys
import codecs
import MySQLdb
import argparse
import getpass
from multiprocessing import Pool, freeze_support
import itertools

parser = argparse.ArgumentParser()
parser.add_argument("-f", action="store", dest='sqlfile', help="input the sql file dir and file name", required=True)
parser.add_argument("-s", action="store", dest='dbserverlistfile', help="input the serverlist file dir and file name", required=True)
parser.add_argument("-d", action="store", dest='databasename', help="input the database name", required=True)
args = parser.parse_args()


reload(sys)
sys.setdefaultencoding('utf8')


def readserverlist():
    dbserverlist = []
    with open(args.dbserverlistfile, 'r') as f:
        f = f.readlines()
        for line in f:
            dbserverlist.append(line.strip('\n'))
    return dbserverlist


def readallsql():
    with open(args.sqlfile, 'r') as f:
        sqlall = f.read()
    f.closed
    return sqlall
    
def readlinesql(dbserverlist):
    sql = ''
    with open(args.sqlfile, 'r') as f:
        f = f.readlines()
        for line in f:
            sql = sql + line.strip('\n').rstrip() + ' '
            if line.strip('\n').rstrip().endswith(';'):
                 print '++++++++++++execsql++++++++++++' + sql
                 try:
                     pool = Pool()
                     pool.map(func_start,itertools.izip(dbserverlist, itertools.repeat(sql)))
                     pool.close()
                     pool.join()
                 except Exception, e:
                     print e
                 sql =''


def execsql(dbserver,sql):
    try:
        conn = MySQLdb.connect(host=dbserver,port=3306, user='dba', passwd=password, db=dbname, charset="utf8")
    except Exception, e:
        print "error++++++"+dbserver
        print e
        os._exit()
    cursor = conn.cursor()
    try:
          #cursor.execute("SET NAMES utf8")
        cursor.execute(sql)
        conn.commit()
        print "succeess!-----" + dbserver
    except Exception, e:
        print "sqlerror!=====" + dbserver
        print e
    cursor.close()
    conn.close()

def func_start(dbserver_sql):
    return execsql(*dbserver_sql)



if __name__ == '__main__':
    freeze_support()
    print '==============================Check DBName========================================='
    dbname = args.databasename
    print dbname
    content = raw_input("check dbname input(y or n):")
    if (content != "y"):
        sys.exit()

    print '==============================Check DBServer======================================='
    dbserverlist = readserverlist()
    print dbserverlist
    content = raw_input("check dbserver input(y or n):")
    if (content != "y"):
        sys.exit()

    print '==============================Check Sql============================================'
    sqlall = ''
    sqlall = readallsql()
    print sqlall
    content = raw_input("check sql input(y or n):")
    if (content != "y"):
        sys.exit()

    print '===============================Input Password======================================='
    password = getpass.getpass('Enter password: ')

    readlinesql(dbserverlist)
