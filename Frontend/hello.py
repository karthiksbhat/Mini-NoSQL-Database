from Tkinter import *
import ttk
import os
import tkMessageBox
def process_query(*args):
	value = query.get()
	# print value
	output=os.popen('python queryLanguage.py "'+value+'"').read()
	tkMessageBox.showinfo("answer",output)

	#file = open("input_file.txt", "w")
	#file.write(value)
	#file.close()
    
root = Tk()
root.title("Welcome to NoSQL database")

mainframe = ttk.Frame(root, padding="100 100 100 100")
mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
mainframe.columnconfigure(0, weight=1)
mainframe.rowconfigure(0, weight=1)

query = StringVar()
meters = StringVar()

query_entry = ttk.Entry(mainframe, width=100, textvariable=query)
query_entry.grid(column=2, row=2, sticky=(W, E))

ttk.Label(mainframe, textvariable=meters).grid(column=2, row=3, sticky=(W, E))
ttk.Label(mainframe, text="The syntax rules are going to be here.").grid(column=2, row=1)
ttk.Button(mainframe, text="Process the Query", command=process_query).grid(column=3, row=3, sticky=W)

ttk.Label(mainframe, text="Enter Query here").grid(column=1, row=2, sticky=N)

##for child in mainframe.winfo_children(): child.grid_configure(padx=5, pady=5)

##query_entry.focus()
##root.bind('<Return>', process_query)

root.mainloop()