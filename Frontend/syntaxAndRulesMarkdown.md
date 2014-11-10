Thank you for using this &lt;unnamed&gt; NoSQL database v0.1, with the &lt;unnamed&gt; Language. We define here the rules of the language. We have tried to make it as user friendly as possible, and if you think something needs to be changed, please feel free to access our source code at http://github.com/karthiksbhat/Mini-NoSQL-Database and contribute to it.

#Syntax
1. Insert
	This is a command to insert an entry to a collection.
	Syntax
	insert in collection: &lt;collection-name&gt; primary_keys:&lt;value&gt;(s) compressed: true|false &lt;attributes&gt;: &lt;values&gt;

2. Modify
	This is a command to modify an entered entry to the collection.
	Syntax
		modify collection: &lt;collection-name&gt; &lt;attribute&gt;: &lt;value&gt; &lt;attribute&gt;: &lt;value&gt;(*) NEWVALUES &lt;attribute&gt;: &lt;new-value&gt; &lt;attribute&gt;: &lt;new-value&gt;(*)

3. Delete
	This is a command to delete an entry in the collection.
	Syntax
		delete from collection: &lt;collection-name&gt; &lt;attribute&gt;: &lt;value&gt; &lt;attribute&gt;: &lt;value&gt;(*)

4. Display
	This is a command to display all elements in a collection.
	Syntax
		display collection: &lt;collection-name&gt; &lt;attribute&gt;: &lt;value&gt; &lt;attribute&gt;: &lt;value&gt;(*)

	Suppose an attribute is not mentioned.
	Syntax
		display collection: &lt;collection-name&gt;

5. Describe
	This is a command to describe the collection specified.
	Syntax
		describe collection: &lt;collection-name&gt;

6. Number
	This is a query to display the number of elements in the specified collection.
	Syntax
		number collection: &lt;collection-name&gt;

7. Drop
	This is a query to destroy a collection.
	Syntax
		drop collection: &lt;collection-name&gt;