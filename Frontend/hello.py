from Tkinter import *
import ttk
import os
import tkMessageBox

#Function to process the query
def process_query(*args):
	value = query.get()
	
	#Displays obtained result in a pop-up box.
	output=os.popen('python queryLanguage.py "'+value+'"').read()
	tkMessageBox.showinfo("answer",output)
    
root = Tk()
root.title("Welcome to noPandaDB")

#GUI Configuration
mainframe = ttk.Frame(root, padding="100 100 100 100")
mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
mainframe.columnconfigure(0, weight=1)
mainframe.rowconfigure(0, weight=1)

query = StringVar()
meters = StringVar()

#text area to accept input
query_entry = ttk.Entry(mainframe, width=100, textvariable=query)
query_entry.grid(column=2, row=12, sticky=(W, E))

ttk.Label(mainframe, textvariable=meters).grid(column=2, row=3, sticky=(W, E))

#Defining contents of the GUI display
ttk.Label(mainframe, text="QUERY LANGUAGE SYNTAX").grid(column=2, row=1)
ttk.Label(mainframe, text="").grid(column=2, row=2, sticky=N)
ttk.Label(mainframe, text="Create Query - insert (in) collection: <collection-name> primary_keys:<value>,<value>(s) compressed: true|false <attributes>: <values>").grid(column=2, row=3, sticky=W)
ttk.Label(mainframe, text="Insert Query - insert (in) collection: <collection-name> <attributes>: <values>").grid(column=2, row=4, sticky=W)
ttk.Label(mainframe, text="Modify Query - modify (the) collection: <collection-name> <attribute>: <value>(s) NEWVALUES <attribute>: <new-value>(s)").grid(column=2, row=5, sticky=W)
ttk.Label(mainframe, text="Delete Query - delete (from) collection: <collection-name> <attribute>: <value>(s)").grid(column=2, row=6, sticky=W)
ttk.Label(mainframe, text="Display Query - display (the) collection: <collection-name> <attribute>: <value>(s)").grid(column=2, row=7, sticky=W)
ttk.Label(mainframe, text="Describe Query - describe (the) collection: <collection-name>").grid(column=2, row=8, sticky=W)
ttk.Label(mainframe, text="Number Query - number (of) collection: <collection-name>").grid(column=2, row=9, sticky=W)
ttk.Label(mainframe, text="Drop Query - drop collection: <collection-name>").grid(column=2, row=10, sticky=W)
ttk.Button(mainframe, text="Process the Query", command=process_query).grid(column=3, row=12, sticky=W)
ttk.Label(mainframe, text="").grid(column=2, row=11, sticky=N)
ttk.Label(mainframe, text="Enter Query here").grid(column=1, row=12, sticky=N)

root.mainloop()