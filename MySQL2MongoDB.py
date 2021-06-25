# Author: Shameel Ahmed (https://github.com/zshameel)
# Date Created: 17-Jun-2021
# Date Last Modified: 25-Jun-20201
# Python version: 3.4+

import mysql.connector
import pymongo
import datetime
import enum

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

class MsgType(enum.Enum):
    HEADER = 1
    OKBLUE = 2
    OKCYAN = 3
    OKGREEN = 4
    WARNING = 5
    FAIL = 6
    ENDC = 7
    BOLD = 8
    UNDERLINE = 9

#Pretty Print Function
def prettyprint(msg_text, msg_type):
    if msg_type == MsgType.HEADER:
        print(f"{bcolors.HEADER}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.OKBLUE:
        print(f"{bcolors.OKBLUE}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.OKCYAN:
        print(f"{bcolors.OKCYAN}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.OKGREEN:
        print(f"{bcolors.OKGREEN}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.WARNING:
        print(f"{bcolors.WARNING}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.FAIL:
        print(f"{bcolors.FAIL}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.BOLD:
        print(f"{bcolors.BOLD}{msg_text}{bcolors.ENDC}")
    elif msg_type == MsgType.UNDERLINE:
        print(f"{bcolors.UNDERLINE}{msg_text}{bcolors.ENDC}")

#Function migrate_table 
def migrate_table(db, table_name):
    #TODO: Sanitize table name to conform to MongoDB Collection naming restrictions
    #For example, the $ sign is allowed in MySQL table names but not in MongoDB Collection names
    mycursor = db.cursor(dictionary=True)
    mycursor.execute("SELECT * FROM " + table_name + ";")
    myresult = mycursor.fetchall()

    mycol = mydb[table_name]
    
    if delete_existing_documents:
        #delete all documents in the collection
        mycol.delete_many({})

    #insert the documents
    if len(myresult) > 0:
        x = mycol.insert_many(myresult)
        return len(x.inserted_ids)
    else:
        return 0

begin_time = datetime.datetime.now()
abort = False
prettyprint(f"Script started at: {begin_time}", MsgType.HEADER)

delete_existing_documents = False;
mysql_host="localhost"
mysql_database="mydatabase"
mysql_schema = "myschhema"
mysql_user="root"
mysql_password=""

mongodb_host = "mongodb://localhost:27017/"
mongodb_dbname = "mymongodb"

if (delete_existing_documents):
    confirm_delete = input("Delete existing documents from collections (y)es/(n)o/(a)bort?")
    if confirm_delete.lower() == "a":
        abort = True
    elif confirm_delete.lower() == "n":
        delete_existing_documents = False
    else:
        #Confirm again
        confirm_delete = input("Are you sure (y)es/(n)?")
        if confirm_delete.lower() == "y":
            delete_existing_documents = True
        else:
            abort = True

if abort:
    prettyprint("Script aborted by user", MsgType.FAIL)
else:
    if (delete_existing_documents):
        prettyprint("Existing documents will be deleted from collections", MsgType.FAIL)
    else:
        prettyprint("Existing documents will not be deleted from collections", MsgType.OKGREEN)
        
    #MySQL connection
    prettyprint("Connecting to MySQL server...", MsgType.HEADER)
    mysqldb = mysql.connector.connect(
        host=mysql_host,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password
    )
    prettyprint("Connection to MySQL Server succeeded.", MsgType.OKGREEN)

    #MongoDB connection
    prettyprint("Connecting to MongoDB server...", MsgType.HEADER)
    myclient = pymongo.MongoClient(mongodb_host)
    mydb = myclient[mongodb_dbname]
    prettyprint("Connection to MongoDB Server succeeded.", MsgType.OKGREEN)

    #Start migration
    prettyprint("Migration started...", MsgType.HEADER)

    dblist = myclient.list_database_names()
    if mongodb_dbname in dblist:
        prettyprint("The database exists.", MsgType.OKBLUE)
    else:
        prettyprint("The database does not exist, it is being created.", MsgType.WARNING)

    #Iterate through the list of tables in the schema
    table_list_cursor = mysqldb.cursor()
    table_list_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name LIMIT 15;", (mysql_schema,))
    tables = table_list_cursor.fetchall()

    total_count = len(tables)
    success_count = 0
    fail_count = 0

    for table in tables:
        try:
            prettyprint(f"Processing table: {table[0]}...", MsgType.OKCYAN)
            inserted_count = migrate_table(mysqldb, table[0])
            success_count += 1
            prettyprint(f"Processing table: {table[0]} completed. {inserted_count} documents inserted.", MsgType.OKGREEN)
        except Exception as e:
            fail_count += 1
            prettyprint(f"{e}", MsgType.FAIL)

    prettyprint("Migration completed.", MsgType.HEADER)
    prettyprint(f"{success_count} of {total_count} tables migrated successfully.", MsgType.OKGREEN)
    if fail_count > 0:
        prettyprint(f"Migration of {fail_count} tables failed. See errors above.", MsgType.FAIL)
    
end_time = datetime.datetime.now()
prettyprint(f"Script completed at: {end_time}", MsgType.HEADER)
prettyprint(f"Total execution time: {end_time-begin_time}", MsgType.HEADER)
