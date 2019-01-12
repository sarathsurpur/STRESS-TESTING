from datetime import datetime
import mysql.connector
import threading
import random, string
import traceback
import sys

def testing(): 
    connection = 0
    for i in range(3):
        mydb = mysql.connector.connect(host="155.69.149.158",port=6000,user="root",passwd="toor")
        mycursor = mydb.cursor()

        #to select the database
        mycursor.execute("use testdb")


        try:
            #generating random table names
            letters = string.ascii_lowercase
            tname = ''.join(random.choice(letters) for i in range(4))

            #to capture the start time
            c_start = time.time()
            #creating the table
            sql = ("""CREATE TABLE %s (`col1` INT ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH, RANGE),
                   `col2` DOUBLE ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH),
                   `col3` TEXT ENCRYPTED FOR(STORE),`col4` VARCHAR(20),`col5` VARCHAR(20));"""%tname)
            mycursor.execute(sql)
            c_end = time.time()
            create_time = c_end - c_start
            print("Total time taken to create: "+str(create_time)) 

            #inserting records
            I_start = time.time()
            for k in range(1,200):
                sql = "INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (%s, %s,%s,%s,%s);"
                val = (tname,k,k*3,k*8,"'JURONG"+str(k) +"'", "'NTU-"+str(k)+"'")
                m = sql%val
                mycursor.execute(m)    
            mydb.commit()
            I_end = time.time()
            insert_time = I_end - I_start
            print("Total time taken to insert: "+str(insert_time)) 

            #selecting records
            s_start = time.time()
            mycursor.execute("SELECT col1,col2*col2,col3 FROM %s where col1<100;"%tname)
            myresult = mycursor.fetchall()
            for x in myresult:
                print(x)
            s_end = time.time()
            select_time = s_end - s_start
            print("Total time taken to select: "+str(select_time))
            mycursor.execute("DROP TABLE %s;"%tname)

         #closing connection
            mycursor.close()
            mydb.close()
            
            connection += 1

        except Exception as exc:
            sttime = datetime.now().strftime('%Y%m%d_%H:%M:%S - ')
            error = "ERROR! {}".format(exc)
            log = 'testlog.txt'
            with open(log, 'a') as logfile:
                logfile.write(sttime + error + '\n')
            raise
    

threads = []

for i in range(2):
    print ("thread"+str(i))
    t = threading.Thread(target=testing)
    threads.append(t)
    t.start()
    t.join()
