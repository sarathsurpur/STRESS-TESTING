import mysql.connector
import threading
import random, string

def testing(): 
    
    mydb = mysql.connector.connect(host="155.69.149.158",port=6000,user="root",passwd="toor")
    mycursor = mydb.cursor()
    letters = string.ascii_lowercase
    tname = ''.join(random.choice(letters) for i in range(4))
    print(tname)
    sql = ("""CREATE TABLE %s (`col1` INT ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH, RANGE),
               `col2` DOUBLE ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH),
               `col3` TEXT ENCRYPTED FOR(STORE),`col4` VARCHAR(20),`col5` VARCHAR(20));"""%tname)
    mycursor.execute(sql)
    mydb = mysql.connector.connect(host="155.69.149.158",port=6000,user="root",passwd="toor")
    mycursor = mydb.cursor()
    for k in range(1,500):
        sql = "INSERT INTO %s (col1, col2, col3, col4, col5) VALUES (%s, %s,%s,%s,%s);"
        val = (tname,k,k*3,k*8,"'JURONG"+str(k) +"'", "'NTU-"+str(k)+"'")
        m = sql%val
        mycursor.execute(m)
        
    mydb.commit()
    mydb = mysql.connector.connect(host="155.69.149.158",port=6000,user="root",passwd="toor")
    mycursor = mydb.cursor()
    mycursor.execute("SELECT col1,col2*col2,col3 FROM %s where col1<50;"%tname)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)
    mycursor.execute("DROP TABLE %s;"%tname)

threads = []

for i in range(3):
    print (i)
    t = threading.Thread(target=testing)
    threads.append(t)
    t.start()
    t.join()
