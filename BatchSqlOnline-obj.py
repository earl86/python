#coding=utf-8
#encoding:utf8

from threading import Thread 
from Queue import Queue
import MySQLdb
import os
import time
import random
import sys
import argparse
import getpass
import codecs

reload(sys)
sys.setdefaultencoding('utf-8')
type = sys.getfilesystemencoding()


que =Queue(maxsize=100)
dbusername='dba'

parser = argparse.ArgumentParser()
parser.add_argument("-f", action="store", dest='sqlfile', help="input the sql file dir and file name", required=True)
parser.add_argument("-s", action="store", dest='dbserverlistfile', help="input the serverlist file dir and file name", required=True)
args = parser.parse_args()


class Reader(Thread):
    def __init__(self,sqlfilename,sqlqueue):
        self.__sqlfilename=sqlfilename
        self.__sqlqueue=sqlqueue
        super(Reader,self).__init__()

    def readlinesql(self):
        sql = ''
        with open(self.__sqlfilename, 'r') as f:
            f = f.readlines()
            for line in f:
                if line[0:3] == codecs.BOM_UTF8:
                    line=line.rstrip('\r\n').lstrip('\xef\xbb\xbf')
                sql = sql + line.strip('\n').rstrip() + ' '
                if line.strip('\n').rstrip().endswith(';'):
                    #print('++++++++++++execsql++++++++++++' + sql)
                    self.__sqlqueue.put(sql)
                    sql = ''
            print '读取sql文件完毕\n'.decode('utf-8').encode(type)
    
    def readallsql(self):
        with open(self.__sqlfilename, 'r') as f:
            allsql = f.read()
            if allsql[0:3] == codecs.BOM_UTF8:    
                allsql=allsql.rstrip('\r\n').lstrip('\xef\xbb\xbf')
            f.close()
            return allsql    
    
class Runner(Thread):
    def __init__(self,dbservicename,dbname,dbusername,dbpassword,sql):
        self.__dbservicename= dbservicename
        self.__dbname= dbname
        self.__dbusername=dbusername
        self.__dbpassword=dbpassword
        self.__sql=sql
        super(Runner,self).__init__()
        
    def exesql(self):
        try:
            conn = MySQLdb.connect(host=self.__dbservicename, port=3306, db=self.__dbname, user=self.__dbusername, passwd=self.__dbpassword, charset="utf8")
        except Exception, e:
            sys.stdout.write("error++++++" + self.__dbservicename +" " + self.__dbname + "\n" +str(e)+ "\n")
            try:
                os._exit(0)
            except Exception, e:
                sys.stdout.write(str(e)+ "\n")
        try:
            cursor = conn.cursor()
            cursor._defer_warnings = True
            cursor.execute(self.__sql)
            conn.commit()
            sys.stdout.write("succeess!-----" + self.__dbservicename +" " + self.__dbname + "\n" )
        except Exception, e:
            conn.rollback()
            sys.stdout.write("sqlerror!=====" + self.__dbservicename +" "  + self.__dbname + "\n" +str(e)+ "\n")
        cursor.close()
        conn.close()
    
    def run(self):
        #sleeptime=random.randint(0, 3)
        #time.sleep(sleeptime)
        self.exesql()
        
        



class Creater(Thread):
    def __init__(self,dbserverlistfile,sqlqueue):
        self.__dbserverlistfile=dbserverlistfile
        self.__sqlqueue=sqlqueue
        super(Creater,self).__init__()
        
    def readdbservicelist(self):
        dbservicelist = []
        with open(self.__dbserverlistfile, 'r') as f:
            f = f.readlines()
            for line in f:
                if line[0:3] == codecs.BOM_UTF8:    
                    line=line.rstrip('\r\n').lstrip('\xef\xbb\xbf')
                if len(line.strip()) != line.strip().count('\n'):
                    dbservicelist.append(line.strip('\n'))
        return dbservicelist        

    def gen_tasks(self,dbservicelist, sql):
        threads=[]
        i=0
        for dbservice in dbservicelist:
            dbargs = dbservice.split(',')
            dbservicename = dbargs[0]
            dbname = dbargs[1]
            threads.append(Runner(dbservicename,dbname,dbusername,dbpassword,sql))
            threads[i].setDaemon(True)
            threads[i].start()
            i = i+1

        for t in threads:
            t.join()

    def run(self):
        while True:
            if self.__sqlqueue.empty():
                print 'sql上线执行完毕\n'.decode('utf-8').encode(type)
                break
            else :
                sql=self.__sqlqueue.get()
                print('++++++++++++execsql++++++++++++' + sql.decode('utf-8').encode(type))
                self.gen_tasks(self.readdbservicelist(), sql)
                time.sleep(2)
        try:
            sys.exit(0)
        except  Exception, e:
            print e
            

def main():
    reader1=Reader(args.sqlfile,que)
    creater1 = Creater(args.dbserverlistfile,que)

    print('==============================Check DBServerargs=======================================')
    dbservicelist = creater1.readdbservicelist()
    for dbservice in dbservicelist:
        print(dbservice)
    content = raw_input("check dbserverargs input(y or n):")
    if (content != 'y'):
        sys.exit()

    print('==============================Check Sql============================================')
    allsql = reader1.readallsql()
    print(allsql.decode('utf-8').encode(type))
    content = raw_input("check sql input(y or n):")
    if (content != "y"):
        sys.exit()

    print('===============================Input Password=======================================')
    global dbpassword
    dbpassword = getpass.getpass('Enter password: ')
    reader1.readlinesql()
    creater1.start()


if __name__=='__main__':
    main()


