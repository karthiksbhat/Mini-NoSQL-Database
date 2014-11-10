##README
This is the repository for our DBMS course project, where we are consturcting a NoSQL database from scratch. We will be constructing a **query language**, as well as a structure for the database. We will be coding primarily in *Python*.

###Description
####Query language
The query language is designed to work similar to SQL. We will be generating queries similar to SQL's queries like-
1. Insert
	This is a command to insert an entry to a collection.
	Syntax
		insert in collection: <collection-name> primary_keys:<value>(s) compressed: true|false <attributes>: <values>
2. Modify
	This is a command to modify an entered entry to the collection.
	Syntax
		modify collection: <collection-name> <attribute>: <value>(s) NEWVALUES <attribute>: <new-value>(s)
3. Delete
	This is a command to delete an entry in the collection.
	Syntax
		delete from collection: <collection-name> <attribute>: <value>(s)
4. Display
	This is a command to display all elements in a collection.
	Syntax
		display collection: <collection-name> <attribute>: <value> <attribute>: <value>(*)
	Suppose an attribute is not mentioned.
	Syntax
		display collection: <collection-name>
5. Describe
	This is a command to describe the collection specified.
	Syntax
		describe collection: <collection-name>
6. Number
	This is a query to display the number of elements in the specified collection.
	Syntax
		number collection: <collection-name>
7. Drop
	This is a query to destroy a collection.
	Syntax
		drop collection: <collection-name>

Currently, it does not have the capacity to handle complex queries.

####Middleware
To be updated.
####Backend
The data received from the user, in the form of queries, will be handled by the backend. The data will be stored as key-value pairs, and stored in permanent storage in the form of json objects.