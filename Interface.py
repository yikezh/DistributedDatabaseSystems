#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import StringIO
import os
import sys

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute('DROP TABLE IF EXISTS ' + ratingstablename)
    sqlQuery = "CREATE TABLE " + ratingstablename + "(UserID integer, MovieID integer, Rating double precision);"
    cur.execute(sqlQuery)
    file = open(ratingsfilepath, "r")
    originData = file.readlines()
    separator = "::"
    outputData = ""
    for item in originData :
        #print item
        handl = item.find(separator)
        userID = item[:handl]
        item = item[handl + 2:]
        handl = item.find(separator)
        movieID = item[:handl]
        item = item[handl + 2:]
        handl = item.find(separator)
        rating = item[:handl]
        outputData += userID + "\t" + movieID + "\t" + rating + "\n"
    f = StringIO.StringIO(outputData)
    cur.copy_from(f, ratingstablename, columns = ('UserID', 'MovieID', 'Rating'))
    cur.close()
    file.close()
    pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0 or (not str(numberofpartitions).isdigit()) :
        print "wrong number of partitions"
        return

    cur = openconnection.cursor()
    sep = 5.0 / numberofpartitions

    for i in range(0, numberofpartitions) :
        tableName = "range_part" + str(i)
        cur.execute('DROP TABLE IF EXISTS ' + tableName)
        sqlQuery = "CREATE TABLE " + tableName + "(UserID integer, MovieID integer, Rating double precision);"
        cur.execute(sqlQuery)
        min = i * sep
        max = (i + 1) * sep
        outputData = ""
        if i == 0:
            sqlQuery = "SELECT * FROM " + ratingstablename + " WHERE Rating >= 0 and Rating <= " + str(max) + ";"
            cur.execute(sqlQuery)
            res = cur.fetchall();
        else:
            if i == numberofpartitions - 1 :
                max = 5
            sqlQuery = "SELECT * FROM " + ratingstablename + " WHERE Rating <= " + str(max) + " and Rating > " + str(
                min) + ";"
            cur.execute(sqlQuery)
            res = cur.fetchall();
        for item in res :
            userID = str(item[0])
            movieID = str(item[1])
            rating = str(item[2])
            outputData += userID + "\t" + movieID + "\t" + rating + "\n"
        f = StringIO.StringIO(outputData)
        cur.copy_from(f, tableName, columns=('UserID', 'MovieID', 'Rating'))
    cur.close()
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0 or (not str(numberofpartitions).isdigit()) :
        print "wrong number of partitions"
        return
    cur = openconnection.cursor()
    for i in range(0, numberofpartitions):
        tableName = "rrobin_part" + str(i)
        cur.execute('DROP TABLE IF EXISTS ' + tableName)
        sqlQuery = "CREATE TABLE " + tableName + "(UserID integer, MovieID integer, Rating double precision);"
        cur.execute(sqlQuery)
    cur.execute("SELECT * FROM " + ratingstablename)
    items = cur.fetchall()

    for i in range(0, numberofpartitions) :
        #print i;
        flag = 0;
        outputData = ""
        tableName = "rrobin_part" + str(i)
        for item in items :
            #print item
            if flag == numberofpartitions :
                flag = 0;
            if flag == i :
                userID = str(item[0])
                movieID = str(item[1])
                rating = str(item[2])
                outputData += userID + "\t" + movieID + "\t" + rating + "\n"
            flag += 1
        #print outputData
        f = StringIO.StringIO(outputData)
        cur.copy_from(f, tableName, columns=('UserID', 'MovieID', 'Rating'))
            #cur.execute("INSERT INTO " + tableName + " VALUES " + str(item) + ";")
    cur.close()
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'rrobin_part%'")
    partTables = cur.fetchall()
    size = []
    for table in partTables :
        tableName = table[0]
        cur.execute("SELECT COUNT(*) FROM " + tableName)
        #print tableName
        size.append(cur.fetchone()[0])
    index = size.index(min(size))
    outputData = ""
    outputData += str(userid) + "\t" + str(itemid) + "\t" + str(rating) + "\n"
    f = StringIO.StringIO(outputData)
    cur.copy_from(f, partTables[index][0], columns=('UserID', 'MovieID', 'Rating'))
    cur.close
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection) :
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(*) tablename FROM pg_tables WHERE tablename LIKE 'range_part%'")
    count = cur.fetchone()[0]
    sep = 5.0 / (count)
    #print 1
    for i in range(0, count) :
        #print 2
        min = i * sep
        max = (i + 1) * sep
        outputData = ""
        if i == 0 :
            if rating >= 0 and rating <= max :
                tableName = "" + str(i)
                outputData += str(userid) + "\t" + str(itemid) + "\t" + str(rating) + "\n"
                f = StringIO.StringIO(outputData)
                cur.copy_from(f, tableName, columns=('UserID', 'MovieID', 'Rating'))
                cur.close()
                return
        else :
            if rating > min and rating <= max :
                tableName = "range_part" + str(i)
                outputData += str(userid) + "\t" + str(itemid) + "\t" + str(rating) + "\n"
                f = StringIO.StringIO(outputData)
                cur.copy_from(f, tableName, columns=('UserID', 'MovieID', 'Rating'))
                cur.close()
                return
    cur.close()
    pass

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(*) tablename FROM pg_tables WHERE tablename LIKE 'range_part%'")
    count1 = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) tablename FROM pg_tables WHERE tablename LIKE 'rrobin_part%'")
    count2 = cur.fetchone()[0]
    #print count
    if count1 <= 0 :
        print "no range fragment"
    else :
        for i in range(0, count1) :
            tableName = "range_part" + str(i)
            cur.execute("DROP TABLE " + tableName)
    if count2 <= 0 :
        print "no round robin fragment"
    else :
        for i in range(0, count2) :
            tableName = "rrobin_part" + str(i)
            cur.execute("DROP TABLE " + tableName)
    cur.execute("DROP TABLE ratings")
    cur.close()
    sys.exit(1)
    pass



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    #cur = openconnection.cursor()
    #cur.copy_to(sys.stdout, 'ratings', sep = "|")
    #cur.close()
    #cur = openconnection.cursor()
    #cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'range_part%' OR tablename LIKE 'roundrobin_part%'")
    #count = cur.fetchall()
    #print count
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            #loadratings('ratings', 'ratings.dat', con)
            #rangepartition('ratings', 5, con)
            #deletepartitions(con)
            #roundrobinpartition('ratings', 5, con)
            #rangeinsert('ratings', 1, 3, 5, con)
            #roundrobininsert('ratings', 1, 4, 5, con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to

            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
