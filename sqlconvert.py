#!/usr/bin/python
#coding=utf-8
#Author earl86
#version 1.0
#the script for 'delete from xxxtable;' and 'update xxxtable set yyycol='',xxxcol='';'
#mysqlbinlog -v --base64-output=decode-rows --start-position=6908 --stop-position=8900 mysql-bin.000001 >mysql-bin.000001.sql
#how to use python sqlconvert.py -f mysql-bin.000001.sql -s columnsize -t update/delete
#need to modify the value of colunmlist
#update sql need customed by yourself


import os
import string
import sys
import argparse

reload(sys)
sys.setdefaultencoding('utf8')

parser = argparse.ArgumentParser()
parser.add_argument("-f", action="store", dest='sqlfile', help="input the sql file dir and file name", required=True)
parser.add_argument("-s", action="store", dest='colnum', help="input the Number of columns", required=True)
parser.add_argument("-t", action="store", dest='sqltype', help="input the convert sql type", required=True)
args = parser.parse_args()



def updatereadlines():
    result = list()
    with open(args.sqlfile, 'r') as f:
        f = f.readlines()
        for line in f:
            if line.startswith('### '):
                if line.startswith('### UPDATE '):
                    #print result
                    convertupdatesql(result)
                    result = []
                result.append(line.strip('\n').replace('### ',' '))
        #print result
        convertupdatesql(result)

def updatereadline():
    result = list()
    f = open(args.sqlfile,'r')
    for line in open(args.sqlfile):  
        line = f.readline()  
        if line.startswith('### '):
            if line.startswith('### UPDATE '):
                #print result
                convertupdatesql(result)
                result = []
            result.append(line.strip('\n').replace('### ',' '))  
    #print result
    convertupdatesql(result)
    f.close()

def convertupdatesql(result):
     if (len(result) > 0):
         sql = result[0] + result[int(args.colnum)+2] + result[8].replace('@7','local_backup_status') + result[1] + result[2].replace('@1','id') + ';' 
         #print sql
         open('result-update.sql', 'a').write(sql+'\n')  

def deletereadlines(colunmlist):
    result = list()
    with open(args.sqlfile, 'r') as f:
        f = f.readlines()
        for line in f:
            if line.startswith('### '):
                if line.startswith('### DELETE '):
                    #print result
                    convertdeletesql(result,colunmlist)
                    result = []
                result.append(line.strip('\n').replace('### ',' '))
        #print result
        convertdeletesql(result,colunmlist)

def deletereadline(colunmlist):
    result = list()
    f = open(args.sqlfile,'r')
    for line in open(args.sqlfile):
        line = f.readline()
        if line.startswith('### '):
            if line.startswith('### DELETE '):
                #print result
                convertdeletesql(result,colunmlist)
                result = []
            result.append(line.strip('\n').replace('### ',' '))
    #print result
    convertdeletesql(result,colunmlist)
    f.close()

def convertdeletesql(result,colunmlist):
     if (len(result) > 0):
         sql =  result[0].replace('DELETE FROM','INSERT INTO') + ' SET'
         for i in range(2,int(args.colnum)+2):
             if (i == int(args.colnum)+1):
                 sql = sql + result[i].replace('@' + str(i-1),colunmlist[i-2]) + ';'
             else:
                 sql = sql + result[i].replace('@' + str(i-1),colunmlist[i-2]) + ','
         #print sql
         open('result-delete.sql', 'a').write(sql+'\n')


    
if __name__ == '__main__':
    colunmlist =('id','user_id','item_id','count')
    if ('delete' == args.sqltype):
        try:
            os.remove('result-delete.sql')
        except:
            pass 
        deletereadline(colunmlist)
    elif ('update' == args.sqltype): 
        try:
            os.remove('result-update.sql')
        except:
            pass
        updatereadline()