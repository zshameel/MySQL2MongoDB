import mysql.connector
import pymongo
import datetime

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

begin_time = datetime.datetime.now()
print(f"{bcolors.HEADER}Script started at: {begin_time} {bcolors.ENDC}")

delete_existing_documents = True;
mysql_host="localhost"
mysql_database="mydatabase"
mysql_schema = "myschhema"
mysql_user="root"
mysql_password=""

mongodb_host = "mongodb://localhost:27017/"
mongodb_dbname = "mymongodb"

print(f"{bcolors.HEADER}Initializing database connections...{bcolors.ENDC}")
print("")

#MySQL connection
print(f"{bcolors.HEADER}Connecting to MySQL server...{bcolors.ENDC}")
mysqldb = mysql.connector.connect(
    host=mysql_host,
    database=mysql_database,
    user=mysql_user,
    password=mysql_password
)
print(f"{bcolors.HEADER}Connection to MySQL Server succeeded.{bcolors.ENDC}")

#MongoDB connection
print(f"{bcolors.HEADER}Connecting to MongoDB server...{bcolors.ENDC}")
myclient = pymongo.MongoClient(mongodb_host)
mydb = myclient[mongodb_dbname]
print(f"{bcolors.HEADER}Connection to MongoDB Server succeeded.{bcolors.ENDC}")

print(f"{bcolors.HEADER}Database connections initialized successfully.{bcolors.ENDC}")

#Start migration
print(f"{bcolors.HEADER}Migration started...{bcolors.ENDC}")
dblist = myclient.list_database_names()
if mongodb_dbname in dblist:
    print(f"{bcolors.OKBLUE}The database exists.{bcolors.ENDC}")
else:
    print(f"{bcolors.WARNING}The database does not exist, it is being created.{bcolors.ENDC}")

#Function migrate_table 
def migrate_table(db, col_name):
    mycursor = db.cursor(dictionary=True)
    mycursor.execute("SELECT * FROM " + col_name + ";")
    myresult = mycursor.fetchall()

    mycol = mydb[col_name]
    
    if delete_existing_documents:
        #delete all documents in the collection
        mycol.delete_many({})

    #insert the documents
    if len(myresult) > 0:
        x = mycol.insert_many(myresult)
        return len(x.inserted_ids)
    else:
        return 0

#Iterate through the list of tables in the schema
table_list_cursor = mysqldb.cursor()
table_list_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name LIMIT 15;", (mysql_schema,))
tables = table_list_cursor.fetchall()

total_count = len(tables)
success_count = 0
fail_count = 0

for table in tables:
    try:
        print(f"{bcolors.OKCYAN}Processing table: {table[0]}...{bcolors.ENDC}")
        inserted_count = migrate_table(mysqldb, table[0])
        print(f"{bcolors.OKGREEN}Processing table: {table[0]} completed. {inserted_count} documents inserted.{bcolors.ENDC}")
        success_count += 1
    except Exception as e:
        print(f"{bcolors.FAIL} {e} {bcolors.ENDC}")
        fail_count += 1
        
print("")
print("Migration completed.")
print(f"{bcolors.OKGREEN}{success_count} of {total_count} tables migrated successfully.{bcolors.ENDC}")
if fail_count > 0:
    print(f"{bcolors.FAIL}Migration of {fail_count} tables failed. See errors above.{bcolors.ENDC}")

end_time = datetime.datetime.now()
print(f"{bcolors.HEADER}Script completed at: {end_time} {bcolors.ENDC}")
print(f"{bcolors.HEADER}Total execution time: {end_time-begin_time} {bcolors.ENDC}")
