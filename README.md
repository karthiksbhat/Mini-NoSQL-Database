##README
This is the repository for our DBMS course project, where we are consturcting a NoSQL database from scratch. We will be constructing a **query language**, as well as a structure for the database. We will be coding primarily in *Python*.

###Description
####Query language
The query language is designed to work similar to SQL. We will be generating queries similar to SQL's queries like-
1. Insert
2. Select
3. Modify
4. Delete
5. Drop(?)

Currently, it does not have the capacity to handle complex queries.

####Backend
The data received from the user, in the form of queries, will be handled by the backend. The data will be stored as key-value pairs, and stored in permanent storage in the form of json objects.