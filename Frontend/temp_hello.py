from Tkinter import *
# import ttk
import os
import queryLanguage

def process_query(*args):
	# output=queryLanguage.send_query(value)
	# print output
	var.set("yo")
	
	#file = open("input_file.txt", "w")
	#file.write(value)
	#file.close()
    
root = Tk()
root.title("Welcome to NoSQL database")
var = StringVar()

l=Label(root,textvariable=var)
l.pack()

process_query()

'''
mainframe = ttk.Frame(root, padding="200 200 200 200")
mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
mainframe.columnconfigure(0, weight=1)
mainframe.rowconfigure(0, weight=1)

query = StringVar()
meters = StringVar()
var=StringVar()

query_entry = ttk.Entry(mainframe, width=100, textvariable=query)
query_entry.grid(column=2, row=2, sticky=(W, E))

ttk.Label(mainframe, textvariable=meters).grid(column=2, row=3, sticky=(W, E))
ttk.Label(mainframe, text="The syntax rules are going to be here.").grid(column=2, row=1)
ttk.Button(mainframe, text="Process the Query", command=process_query).grid(column=3, row=3, sticky=W)
ttk.Label(mainframe, text="Enter Query here").grid(column=1, row=2, sticky=N)
ttk.Label(mainframe,text="output").grid(column=1, row=4, sticky=N)
'''


#ttk.Label(mainframe,textvariable=output).grid(column=2, row=6,stick=N)
#var.set(output)

#ttk.Label(mainframe,text="Output").grid(column=1, row=4, sticky=N)
#ttk.Label(mainframe,text=var).grid(column=2, row=6, sticky=N)

##for child in mainframe.winfo_children(): child.grid_configure(padx=5, pady=5)

##query_entry.focus()
##root.bind('<Return>', process_query)

root.mainloop()
root.update_idletasks()