#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Python 2.7 scirpt

import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import subprocess
import re
import csv
import getpass

#build connection with mysql
def connectMysql(usr,pw,h,db):
    dbConnect = mysql.connector.connect(user=usr,password=pw,host=h,database=db)
    return dbConnect

#close connection
def closeConnect(dbConnect):
    dbConnect.close()

#create table function
#arguments: connection,table name,columns,data type for every column
def createTable(dbConnect,name,columnList,typeList):
    cursor = dbConnect.cursor()
    #drop the table if it already exists
    cursor.execute("DROP TABLE IF EXISTS %s" %name)
    #create table sql query
    sqlCreateor = "CREATE TABLE " + name + "("
    for e in range(0,columnList.__len__()-1):
        sqlCreateor+= columnList[e]+" "+typeList[e]+","
    sqlCreateor+= columnList[-1]+" "+typeList[-1]+")"
    cursor.execute(sqlCreateor)

#load file function
def loadFile(dbConnect,file,table,fieldsT,enclosed,linesT,emptyline):
    cursor = dbConnect.cursor()
    #sqlLoader is a string for storing the sql query
    sqlLoader = "LOAD DATA LOCAL INFILE "
    sqlLoader+= ("'%s' " %file)
    sqlLoader+= ("INTO TABLE %s " %table)
    if enclosed== "'":
        sqlLoader+= ("""FIELDS TERMINATED BY '%s' ENCLOSED BY "'" """ % fieldsT)
    elif enclosed=='"':
        sqlLoader+= ("""FIELDS TERMINATED BY '%s' ENCLOSED BY '"' """ % fieldsT)
    else :
        sqlLoader+= ("""FIELDS TERMINATED BY '%s' ENCLOSED BY '"' """ % fieldsT)
    if emptyline>0: #file contains empty lines
        sqlLoader+= ("LINES TERMINATED BY '%s' " % (linesT+linesT))
    else:
        sqlLoader+= ("LINES TERMINATED BY '%s' " % linesT)
    sqlLoader+="IGNORE 1 LINES"
    cursor.execute(sqlLoader)

#use linux command to detect the lines terminater
def linesTDetector(filename):
    file_output = subprocess.check_output(['file', filename])
    return file_output.find('CRLF')

#detect the columns in the file
def columnsDetector(filename):
    #get the first line of the file
    with open(filename, 'r') as f:
        first_line = f.readline()
    line = re.split(';|/|:|,|\t',first_line)
    columns = [x.strip("'").strip('"') for x in line]  #a list of columns
    columns[-1]=columns[-1].strip('"\r\n').strip("'\r\n")
    if columns[-1]==0:
        columns.pop(1)
    return columns

#detect the data types of columns
def dataSample(filename,delimiter,columnList):
    emptyline = 0
    with open(filename, 'r') as f:
        lines = f.readlines()
    line = re.split(delimiter,lines[1])  #take the second line as a sample of data types
    datalist = [x.strip("'").strip('"') for x in line]
    datalist[-1]=datalist[-1].strip('"\r\n').strip("'\r\n")
    if len(datalist)<len(columnList):  #if the file contains empty lines, take the thrid line instead
        emptyline = 1
        line = re.split(delimiter,lines[2])
        datalist = [x.strip("'").strip('"') for x in line]
        datalist[-1]=datalist[-1].strip('"\r\n').strip("'\r\n")
    #if len(datalist)>len(columnList):
    #    k = len(datalist)-len(columnList)
    #    datalist=[t for t in datalist[:-k]]
    return datalist,emptyline

#detect the column delimiter and enclose character
def dialectFinder(filename):
    dlist = []
    with open(filename, 'r') as f:
        first_line = f.readline()
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(first_line)
    delimiter = dialect.delimiter
    dlist.append(delimiter)

    line = first_line.strip("\r\n")
    if line.startswith('"') and line.endswith('"'):
        quotechar='"'
    elif line.startswith("'") and line.endswith("'"):
        quotechar="'"
    else:
        quotechar=0
    dlist.append(quotechar)

    if dialect.escapechar >0:
        dlist.append(dialect.escapechar)
    return dlist


def getType(value):
    tests = [("INT", int),("FLOAT", float),
             ("TIME", lambda value: datetime.strptime(value,"%H:%M:%S")),
             ("DATE", lambda value: datetime.strptime(value, "%Y-%m-%d")),
             ("DATETIME", lambda value: datetime.strptime(value, "%Y-%m-%d %H:%M:%S"))]
    for typ, test in tests:
        try:
            test(value)
            return typ
        except ValueError:
            continue
    if len(value) < 500:
        return "TEXT"
    else:
        return "BLOB"



def main():
    print "Welcome to MantaRay!"
    user = raw_input('Enter MySQL username: ')
    password= getpass.getpass('Enter password: ')  #relax. I wouldn't tell anyone ;)
    host = raw_input('Enter hostname: ')
    db = raw_input('Enter database name: ')
    #connect database
    while(1):
        try:
            cnx = connectMysql(user,password,host,db)
            break
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
                user = raw_input('Enter MySQL username: ')
                password= getpass.getpass('Enter password: ')
                host = raw_input('Enter hostname: ')
            elif err.errno ==errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                db = raw_input('Enter database name: ')
            else:
                print "Can not connect database"
                print(err)
                user = raw_input('Enter MySQL username: ')
                password= getpass.getpass('Enter password: ')
                host = raw_input('Enter hostname: ')
                db = raw_input('Enter database name: ')


    while(1):

        #input file directory
        while(1):
            filename = raw_input("Enter your file directory: ")
            try:
                file_output = subprocess.check_output(['ls', filename])
                break
            except subprocess.CalledProcessError as err:
                print "File does not exist"


        #column name in first line
        columnList = columnsDetector(filename)
        print "Columns detected: "
        for c in columnList:
            print c,
        print ""


        #dialect detect
        #with CRLF?
        crlf = linesTDetector(filename)
        if crlf<=0:
            linesT = '\n'
        else:
            linesT = '\r\n'
        dialectList = dialectFinder(filename)

        #detect data types and empty lines in file
        datalist,emptyline = dataSample(filename,dialectList[0],columnList)
        #print datalist
        typeList = [getType(data) for data in datalist]
        print "Data types detected:"
        for ty in typeList:
            print ty,
        print ""


        #name the table
        table = raw_input("Please enter table name: ")
        try:
            createTable(cnx,table,columnList,typeList)
            cnx.commit()
            print "MantaRay is ready to load your file."
        except mysql.connector.Error as e:
            print "Can not create table. Column name might contain invalid characters. "
            continue


        #load file
        try:
            loadFile(cnx,filename,table,dialectList[0],dialectList[1],linesT,emptyline)
            cnx.commit()
            print "🍻  Your file is already loaded. Hooray!"
        except mysql.connector.Error as e:
            print "Oops...Can not load the file :("
            #print(e)
            continue

        load_another = raw_input("Load another file?(y/n) ")
        if load_another.lower()=="y" or load_another.lower()=="yes":
            continue
        elif load_another.lower()=="n" or load_another.lower()=="no":
            print "Bye!"
            return
        else:
            print "That's a yes."

    closeConnect(cnx)


if __name__ == '__main__':
    main()
