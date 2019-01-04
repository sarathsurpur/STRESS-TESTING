import mysql.connector
import time

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

#to create a table
mycursor.execute("CREATE TABLE `demo14`(`col1` INT ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH),`col2` DOUBLE ENCRYPTED FOR(MULTIPLICATION, ADDITION, SEARCH),`col3` TEXT ENCRYPTED FOR(STORE),`col4` VARCHAR(20),`col5` VARCHAR(20));")

#establishing connection to db

mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
#to select the database
mycursor.execute("use testdb")

mycursor = mydb.cursor()
#decrypt the columns
mycursor.execute("PRISMADB DECRYPT demo14.col1;")
mycursor.execute("PRISMADB DECRYPT demo14.col2;")
mycursor.execute("PRISMADB DECRYPT demo14.col3;")
mycursor.execute("PRISMADB DECRYPT demo14.col4;")
mycursor.execute("PRISMADB DECRYPT demo14.col5;")

mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
mycursor = mydb.cursor()
mycursor.execute("use testdb")

#encrypt the columns
mycursor.execute("PRISMADB ENCRYPT demo14.col1 for (MULTIPLICATION, ADDITION, SEARCH, RANGE);")
mycursor.execute("PRISMADB ENCRYPT demo14.col2 for (MULTIPLICATION, ADDITION, SEARCH);")
mycursor.execute("PRISMADB ENCRYPT demo14.col3 for (STORE, SEARCH);")
mycursor.execute("PRISMADB ENCRYPT demo14.col5 for (STORE, SEARCH);")


mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
mycursor = mydb.cursor()
mycursor.execute("use testdb")

#inserting rows in batches to the table(repeating until 20k records from here)
for i in range(1,2000):
    sql = "INSERT INTO demo14 (col1, col2, col3, col4, col5) VALUES (%s, %s,%s,%s,%s);"
    val = [(i,i*3,i*8,"JURONG"+str(i), "NTU-"+str(i)),((i+10),(i+11),(i+12),"SG-"+str(i), "AG-"+str(i)),((i+20),(i+21),(i+22),"LK-"+str(i), "PL-"+str(i))]
    mycursor.executemany(sql, val)
    
mydb.commit() 

mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
mycursor = mydb.cursor()
mycursor.execute("use testdb")

#decrypt the columns
mycursor.execute("PRISMADB DECRYPT demo14.col1;")
mycursor.execute("PRISMADB DECRYPT demo14.col2;")
mycursor.execute("PRISMADB DECRYPT demo14.col3;")
mycursor.execute("PRISMADB DECRYPT demo14.col4;")
mycursor.execute("PRISMADB DECRYPT demo14.col5;")

mydb.commit() 

mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
mycursor = mydb.cursor()
mycursor.execute("use testdb")

#encrypt the columns
mycursor.execute("PRISMADB ENCRYPT demo14.col1 for (MULTIPLICATION, ADDITION, SEARCH, RANGE);")
mycursor.execute("PRISMADB ENCRYPT demo14.col2 for (MULTIPLICATION, ADDITION, SEARCH);")
mycursor.execute("PRISMADB ENCRYPT demo14.col3 for (STORE, SEARCH);")
mycursor.execute("PRISMADB ENCRYPT demo14.col5 for (STORE, SEARCH);")

mydb.commit() 

mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
mycursor = mydb.cursor()
mycursor.execute("use testdb")

#selecting the records from table

mycursor.execute("SELECT col1,col2*col2,col3 FROM demo14 where col1<5")
myresult = mycursor.fetchall()
for x in myresult:
  print(x)
  
mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
mycursor = mydb.cursor()
mycursor.execute("use testdb")  
  
#scenario-2 for selecting records  
mycursor.execute("SELECT col1,col2*col2,col3 FROM demo14 where col1>100")
myresult = mycursor.fetchall()
for x in myresult:
  print(x)

 #updating few records in col2
  
  mydb = mysql.connector.connect(
  host="155.69.149.158",
    port=6000,
  user="root",
  passwd="toor",
  )
mycursor = mydb.cursor()
mycursor.execute("use testdb")

mycursor = mydb.cursor()

for i in range(100):
    a = i*3
    b = i*5
    sql = "UPDATE demo14 SET col2 = %s WHERE col2 = %s"
    inputs = (b, a)
    mycursor.execute(sql, inputs)

mydb.commit()

#end time of all the run queries
end = time.time()

a = end - start
#total time taken to execute all the queries
print("total time taken:"+str(a))


