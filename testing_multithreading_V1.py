import mysql.connector
import time
import threading
import random, string


def testing(): 
    
#establishing connection to db

    mydb = mysql.connector.connect(
      host="155.69.149.158",
        port=6000,
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
    for k in range(1,500):
        sql = "INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (%s, %s,%s,%s,%s);"
        val = (tname,k,k*3,k*8,"'JURONG"+str(k) +"'", "'NTU-"+str(k)+"'")
        m = sql%val
        mycursor.execute(m)
        

    mydb.commit() 


    #selecting the records from table
    mycursor.execute("SELECT col1,col2*col2,col3 FROM %s where col1<100;"%tname)
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

for i in range(3):
    print (i)
    t = threading.Thread(target=testing)
    threads.append(t)
    t.start()
    t.join()

