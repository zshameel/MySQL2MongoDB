# MySQL2MongoDB
A Python script to migrate MySQL table data to MongoDB collections

## How to use this script
1. Install MySQL and MoggoDB modules using the following commands from a Python shell:

    `pip install mysql-connector-python`
    `pip install pymongo`


2. Assign values to the following variables in the script:

    `delete_existing_documents = True; #if set to true, deletes all documents from the collection before inserting new ones.`

    `mysql_host="localhost"`

    `mysql_database="mydatabase"`

    `mysql_schema = "myschema"`

    `mysql_user="myuser"`

    `mysql_password="********"`

    `mongodb_host = "mongodb://localhost:27017/"`

    `mongodb_dbname = "mymongodb"`


3. Execute the script

For more information, please check this blog out: https://thedeveloperspace.com/migrate-mysql-table-data-to-mongodb-collections-using-python/
