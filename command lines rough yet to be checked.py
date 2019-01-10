import getopt
import sys
import mysql.connector
import time
import threading
import random, string


options, remainder = getopt.getopt(sys.argv[1:], 'a:v', 'server','ip','port','threads','Insertrows','performinsert','performselect','performupdate')

for opt, arg in options:
    if opt in ('-s', '--server'):
        server = arg
    elif opt in ('-a', '--ip'):
        ip = arg
    elif opt in ('-p', '--port'):
        prt = arg
    elif opt in ('-t', '--threads'):
        th = arg
    elif opt in ('-i', '--insertrows'):
        rows = arg
    elif opt in ('-s', '--select'):
        select = arg
    elif opt in ('u', '--update'):
        update =arg


def testing(): 
    
#establishing connection to db

    mydb = mysql.connector.connect(
      host=ip,
        port=prt,
      user="root",
      passwd="toor",
      )
    mycursor = mydb.cursor()

    #to view the databases
    mycursor.execute("SHOW DATABASES")
    
    for x in mycursor:
      print(x)

#to select the database
    mycursor.execute("use testdb")

#to capture the start time
    start = time.time()
    letters = string.ascii_lowercase
    tname = ''.join(random.choice(letters) for i in range(4))
    print(tname)

#to create a table
    sql = ("""CREATE TABLE %s (`col1` INT ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH, RANGE),
               `col2` DOUBLE ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH),
               `col3` TEXT ENCRYPTED FOR(STORE),`col4` VARCHAR(20),`col5` VARCHAR(20));"""%tname)
    mycursor.execute(sql)

#inserting rows in batches to the table(repeating until 20k records from here)
    for k in range(1,rows):
        sql = "INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (%s, %s,%s,%s,%s);"
        val = (tname,k,k*3,k*8,"'JURONG"+str(k) +"'", "'NTU-"+str(k)+"'")
        m = sql%val
        mycursor.execute(m)
        

    mydb.commit() 
    
    sql = "SELECT col1,col2*col2,col3 FROM %s where col1<%s;"
    Val = (tname,select)


    #selecting the records from table
    mycursor.execute(sql,val)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)
 
  
    #dropping tables
    mycursor.execute("DROP TABLE %s;"%tname)
    
    mydb.commit()

#end time of all the run queries
    end = time.time()

    a = end - start
#total time taken to execute all the queries
    print("total time taken:"+str(a))
    
    #closing connection
    mycursor.close()
    mydb.close()
    

threads = []

for i in range(th):
    print (i)
    t = threading.Thread(target=testing)
    threads.append(t)
    t.start()
    t.join()
