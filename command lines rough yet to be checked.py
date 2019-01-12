import getopt
import sys
import mysql.connector
import time
import threading
import random, string


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
    mydb = mysql.connector.connect(
      host=ip,
        port=int(prt),
      user="root",
      passwd="toor"
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
    print("table name"+tname)

#to create a table
    sql = ("""CREATE TABLE %s (`col1` INT ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH, RANGE),
               `col2` DOUBLE ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH),
               `col3` TEXT ENCRYPTED FOR(STORE),`col4` VARCHAR(20),`col5` VARCHAR(20));"""%tname)
    mycursor.execute(sql)

#inserting rows in batches to the table(repeating until 20k records from here)
    for k in range(1,int(rows)):
        sql = "INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (%s, %s,%s,%s,%s);"
        val = (tname,k,k*3,k*8,"'JURONG"+str(k) +"'", "'NTU-"+str(k)+"'")
        m = sql%val
        mycursor.execute(m)
        

    mydb.commit() 
    
    sql = "SELECT col1,col2*col2,col3 FROM %s where col1<%s;"%(tname,int(select))


    #selecting the records from table
    mycursor.execute(sql)
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

for i in range(int(th)):
    print (i)
    t = threading.Thread(target=testing)
    threads.append(t)
    t.start()
    t.join()

    
