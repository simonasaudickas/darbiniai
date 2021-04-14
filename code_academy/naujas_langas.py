from tkinter import *

langas=Tk()
vardas= Label(langas, text="Vardas")
laukelis1= Entry(langas)
pavarde=Label(langas, text="Pavarde")
laukelis2= Entry(langas)
pazymeti=Checkbutton(langas, text="Pazymekite varnele")

vardas.grid(row=0, column=0, sticky=E)
laukelis1.grid(row=0, column=1)
pavarde.grid(row=2, column=0, sticky=E)
laukelis2.grid(row=2, column=1)
pazymeti.grid(row=3, columnspan=2)


langas.mainloop()




