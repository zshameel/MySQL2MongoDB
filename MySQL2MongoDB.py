import mysql.connector
import pymongo

delete_existing_documents = True;
mysql_host="localhost"
mysql_database="mydatabase"
mysql_schema = "myschema"
mysql_user="myuser"
mysql_password="********"

mongodb_host = "mongodb://localhost:27017/"
mongodb_dbname = "mymongodb"

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
table_list_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name;", (mysql_schema,))
tables = table_list_cursor.fetchall()

total_count = len(tables)
success_count = 0
fail_count = 0

for table in tables:
    try:
        print(f"{bcolors.OKCYAN}Processing table: " + table[0] + "..." + f"{bcolors.ENDC}")
        inserted_count = migrate_table(mysqldb, table[0])
        print(f"{bcolors.OKGREEN}Processing table: " + table[0] + " completed. " + str(inserted_count) + " documents inserted." + f"{bcolors.ENDC}")
        success_count += 1
    except Exception as e:
        print(f"{bcolors.FAIL}"+ str(e) + f"{bcolors.ENDC}")
        fail_count += 1
        
print("")
print("Migration completed.")
print(f"{bcolors.OKGREEN}" + str(success_count) + " of " + str(total_count) + " tables migrated successfully." + f"{bcolors.ENDC}")
if fail_count > 0:
    print(f"{bcolors.FAIL}Migration of " + str(fail_count) + " tables failed. See errors above." + f"{bcolors.ENDC}")
