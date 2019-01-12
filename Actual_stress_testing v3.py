import getopt
import sys
import mysql.connector
import time
import threading
import random, string
from datetime import datetime

try:
    options, remainder = getopt.getopt(sys.argv[1:], 'a:p:t:i:s:u:',['ip','port','threads','performinsert','performselect','performupdate'])
except getopt.GetoptError:
    print('Incorrect options.')
    sys.exit()    

ip=''
prt=''
th=''
rows=''
select=''
update=''
for opt, arg in options:
    if opt in ('-a', '--ip'):
        ip = arg
        #print(ip)
    elif opt in ('-p', '--port'):
        prt = arg
        
    elif opt in ('-t','--thread'):
        th = arg
    elif opt in ('-i','--performinsert'):
        rows = arg
    elif opt in ('-s','--performselect'):
        select = arg
    elif opt in ('-u', '--performupdate'):
        update = arg
    else:
        print("Unknown arg")
        sys.exit()

def testing():
    print ("number of threads:"+str(update))
    print("host"+ip)
    print("port"+prt) 
    print("thread"+th)
    print("insert"+rows)
    print("select"+select)
    try:
        for i in range(3):
            mydb = mysql.connector.connect(host=ip,port=int(prt),user="root",passwd="toor")
            mycursor = mydb.cursor()
            #to view the databases
            mycursor.execute("SHOW DATABASES")
            for x in mycursor:
               print(x)
            #to select the database
            mycursor.execute("use testdb")

        #to capture the start time
            c_start = time.time()
        #randomly generating the letters as table name    
            letters = string.ascii_lowercase
            tname = ''.join(random.choice(letters) for i in range(4))   
            print("Table name: "+tname)

        #to create a table
            sql = ("""CREATE TABLE %s (`col1` INT ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH, RANGE),
                    `col2` DOUBLE ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH),
                    `col3` TEXT ENCRYPTED FOR(STORE),`col4` VARCHAR(20),`col5` VARCHAR(20));"""%tname)
            mycursor.execute(sql)

            c_end = time.time()
            create_time = c_end - c_start
            print("Total time taken to create: "+str(create_time))

             #inserting records
            I_start = time.time()
            for k in range(1,int(rows)):
                sql = "INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (%s, %s,%s,%s,%s);"
                val = (tname,k,k*3,k*8,"'JURONG"+str(k) +"'", "'NTU-"+str(k)+"'")
                m = sql%val
                mycursor.execute(m)
            mydb.commit()
            I_end = time.time()
            insert_time = I_end - I_start
            print("Total time taken to insert: "+str(insert_time))     

            #selecting the records from table
            s_start = time.time()   
            sql = "SELECT col1,col2*col2,col3 FROM %s where col1<%s;"%(tname,int(select))
            mycursor.execute(sql)
            myresult = mycursor.fetchall()
            for x in myresult:
                print(x)
            s_end = time.time()
            select_time = s_end - s_start
            print("Total time taken to select: "+str(select_time))
            mycursor.execute("DROP TABLE %s;"%tname)
            mydb.commit()
            mycursor.close()
            mydb.close()
    except Exception as exc:
        sttime = datetime.now().strftime('%Y%m%d_%H:%M:%S - ')
        error = "ERROR! {}".format(exc)
        log = 'testlog.txt'
        with open(log, 'a') as logfile:
            logfile.write(sttime + error + '\n')
        raise
            

threads = []

for i in range(int(th)):
    print (i)
    t = threading.Thread(target=testing)
    threads.append(t)
    t.start()
    t.join()

    
