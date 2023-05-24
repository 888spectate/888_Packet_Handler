from datetime import date, datetime
from tkinter import Tk, Button, StringVar, OptionMenu, Label, W, E, Entry, Canvas, Scrollbar, Frame, HORIZONTAL, Toplevel
from tkinter.ttk import Progressbar
from tkcalendar import DateEntry
import threading
import os
import paramiko
import sys
import shutil
import supplier_folders
import supplier_functions
import time

# Variables need for holding events, servers and widget values
known_host_servers, known_host_optionMenu, events = [], [], []
date_labels, dates = {}, {}
username, password = "", ""
date_counter, date_labels_y_pos, event_counter = 0, 0, 0

# Options to load the the supplier dropdowns
# Not sure if the the first index of of the child lists are needed now
    # supplier_options[0][0] - points to the lowercase lsports
supplier_options = [
  ["lsport", "LSports"],
  ["sportscast", "SportsCast"],
  ["sportradar", "SportRadar"],
  ["swish", "Swish"],
  ["METRIC", "Metric Gaming"],
  ["PA", "Press Association"],
  ["PAGH", "Dogs"],
  ["SSOL", "Sporting Solutions"],
  ["SSOLIN", "Sporting Solutions InPlay"],
  ["BGIN", "BetGenius"]
]

# Tests that a user can login to any jumpbox
# It will look through each jumpbox and test what can be login to based on the known hosts file
# If a user can login to at least one jumpbox, it will load the suppliers and jumpboxes we can grab files from.
def login_to_server():
    
    # Grab the username and password
    # successful_login will increment if a jumpbox has been login to.
    global username
    global password
    username = username_input.get()
    password = password_input.get()
    successful_login = 0

    # Verify username and password has been supplied. Otherwise throw the Label
    if username == "":
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("400x50")
        Label(newWindow, text="Username cannot be blank.",font=("Arial", 20)).pack()
        return
    if password == "":
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("400x50")
        Label(newWindow, text="Password cannot be blank.",font=("Arial", 20)).pack()
        return

    # While we are testing login we remove the current input and disabled the buttons
    username_input.delete(0, "end")
    username_input["state"] = "disabled"
    password_input.delete(0, "end")
    password_input["state"] = "disabled"
    login_button["state"] = "disabled"

    # This code will search each line of the known hosts file to look for any server with jump. in its name
    # Once it finds it, it will add the address into the known_host_servers list
    with open(os.path.expanduser('~/.ssh/known_hosts'), 'r') as reader:
        while (line := reader.readline()):
            if line.partition(' ')[0].partition(',')[0][0].isalpha() and "jump." in line:
              if line.partition(' ')[0].partition(',')[0] in known_host_servers:
                continue
              else:
                known_host_servers.append(line.partition(' ')[0].partition(',')[0])
    
    # Open up the Paramiko SSH Client
    client = paramiko.SSHClient()

    # This try, except, else code will attempt to the the known_hosts file so we can test login to all
    # jump. addresses.
    try:
        client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
    except:
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("600x85")
        Label(newWindow, text="Known Hosts file could not be read",font=("Arial", 20)).pack()
    else:
        print('Known host files has been loaded')

    # This loop will test all jumpboxes found previouly and attempt to connect to them
    # If we get a successful connection, we add the jumpboxes address to the known_host_optionMenu list.
    # Our dropdown widget will load any successful jumpbox connection so we can choose which server we need to
    # grab packets from.
    for index, host in enumerate(known_host_servers):
        # Filter out jump., spectate and .com so we will get either PROD, MIRAGE OR PRODUSA in the dropdown
        server = host[13:]
        server = server[:-4].upper()
        try:
            client.connect(hostname=host, username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
        except paramiko.ssh_exception.BadHostKeyException:
            newWindow = Toplevel(root)
            newWindow.title("888 Packet Handler")
            newWindow.geometry("750x50")
            Label(newWindow, text="The host key given by the SSH server did not match what we were expecting.",font=("Arial", 20)).pack()
            username_input["state"] = "normal"
            password_input["state"] = "normal"
            login_button["state"] = "normal"
            return
        except paramiko.ssh_exception.AuthenticationException:
            newWindow = Toplevel(root)
            newWindow.title("888 Packet Handler")
            newWindow.geometry("750x50")
            Label(newWindow, text="Authentication failed for some reason. Maybe Password?",font=("Arial", 20)).pack()
            username_input["state"] = "normal"
            password_input["state"] = "normal"
            login_button["state"] = "normal"
            return
        else:
            client.close()
            successful_login += 1
            known_host_optionMenu.append(server)
    
    # If we get any successful login, we disable the login fields and enable the widgets for the other
    # part of the handler.
    # If we get no logins successful, we renable the username, passwords and login widgets
    if successful_login != 0:
        username_input["state"] = "disabled"
        password_input["state"] = "disabled"
        login_button["state"] = "disabled"
        event_id_input["state"] = "normal"
        feed_event_id_input["state"] = "normal"
        add_date_button["state"] = "normal"
        delete_date_button["state"] = "disabled"
        add_event_details["state"] = "normal"
        start_gathering_packets_details["state"] = "disabled"

        global chosen_options_value
        global chosen_server_value

        #Loading Supplier Dropdown
        global options
        chosen_options_value = StringVar(root)
        chosen_options_value.set(supplier_options[1][1])
        options = OptionMenu(root, chosen_options_value, supplier_options[0][1], supplier_options[1][1], supplier_options[2][1], supplier_options[3][1], supplier_options[4][1], supplier_options[5][1], supplier_options[6][1], supplier_options[7][1], supplier_options[8][1], supplier_options[9][1], command=date_disabler)
        options.place(x=485, y=13)

        # Loading Server Dropdown
        global server_options
        chosen_server_value = StringVar(root)
        chosen_server_value.set(known_host_optionMenu[0])
        server_options = OptionMenu(root, chosen_server_value, *known_host_optionMenu)
        server_options.place(x=720, y=13)
    else:
        username_input["state"] = "normal"
        password_input["state"] = "normal"
        login_button["state"] = "normal"

# Allows us to add multiple date wigets for an event.
def add_date_function():
    global date_counter
    global date_labels_y_pos

    # Sets our minimum and current date as a range of packets we have available
    minimum_date = datetime.strptime('2022-10-01', '%Y-%m-%d').date()
    maximum_date = date.today()

    # These lines create our dynamic labels and date entry widgets and updates the position so they dont load
    # ontop of each others
    date_labels["date_label{0}".format(date_counter)] = Label(date_frame, text="Date #" + str(date_counter + 1))
    date_labels["date_label{0}".format(date_counter)].grid(column=0, row=date_labels_y_pos, sticky=W, padx=(120,10), pady=5)
    dates["date_{0}".format(date_counter)] = DateEntry(date_frame, mindate = minimum_date, maxdate = maximum_date, values=date_counter, year=date.today().year, state="readonly", date_pattern="yyyy-mm-dd")
    dates["date_{0}".format(date_counter)].grid(column=1, row=date_labels_y_pos, sticky=E, pady=5)
    date_labels_y_pos += 1
    date_counter += 1

    # Everytime a date is added into the frame the scrollable bar will become active and the canvas will be 
    # updated and the delete date button will be set to active
    date_canvas.update_idletasks()
    date_canvas.configure(scrollregion=date_canvas.bbox('all'), yscrollcommand=date_canvas_scroll_y.set)
    date_canvas.yview_moveto(1)
    if delete_date_button["state"] == "disabled":
        delete_date_button["state"] = "normal"

# Allows us to delete multiple date wigets for an event.
def delete_date_function():
    global date_counter
    global date_labels_y_pos
    
    # Delete the widgets and the entries made in the lists.
    date_label_str="date_label{0}".format(date_counter - 1)
    dates_str="date_{0}".format(date_counter - 1)
    date_labels["date_label{0}".format(date_counter - 1)].destroy()
    dates["date_{0}".format(date_counter - 1)].destroy()
    del date_labels[date_label_str]
    del dates[dates_str]
    date_labels_y_pos -= 1
    date_counter -= 1

    # Everytime a date is remeoved from the frame the scrollable bar will update if dates exceed the current view
    date_canvas.update_idletasks()
    date_canvas.configure(scrollregion=date_canvas.bbox('all'), yscrollcommand=date_canvas_scroll_y.set)
    if delete_date_button["state"] == "normal" and date_counter == 0:
        delete_date_button["state"] = "disabled"

# When the supplier dropdown is clicked and a new one is selected, we remove all current dates entered.
def date_disabler(supplier):
    for i in range(date_counter):
        delete_date_function()
    add_date_button["state"] = "normal"
    delete_date_button["state"] = "disabled"

# Once all information for an event is entered this function will verify the input and add it to the 
# events list
def add_event_details_function():
    global event_counter
    global date_counter
    global chosen_options_value
    global chosen_server_value

    # Grab all the inputs needed.
    supplier = chosen_options_value.get()
    servername = chosen_server_value.get()
    eventid = event_id_input.get()
    feedeventid = feed_event_id_input.get()
    servername = chosen_server_value.get()

    # Verify we have an event id and feed event id and that their not empty
    if not eventid:
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("400x50")
        Label(newWindow, text="Please include an Event ID", font=("Arial", 20)).pack()
        return

    if not feedeventid:
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("500x50")
        Label(newWindow, text="Please include a Feed Event ID", font=("Arial", 20)).pack()
        return

    # Next two loops will convert the string value of the server to the correct index value
    # Lsports is index 0 in list. Same process applied to known_hosts_optionmenu
    for index, value in enumerate(supplier_options):
        if supplier == value[1]:
            supplier = index
            break
    
    for index, value in enumerate(known_host_optionMenu):
        if servername == value:
            servername = known_host_servers[index]
            break
    
    # All suppliers currently need dates to find the files.
    # If we dont have any we stop and show a new Window with the error.
    # Otherwise we loop through all the dates we entered and extract the date into the following string
    # yyyy-mm-dd

    # Once the dates are verified, we add all the event details into the events list.
    if not dates:
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("500x50")
        Label(newWindow, text="Dates are needed to grab packets", font=("Arial", 20)).pack()
        return
    else:
        temp_dates = []
        for index, value in dates.items():
            temp_dates.append(str(value.get_date()))
        events.append([supplier, servername, eventid, feedeventid, temp_dates])

    # Delete the event, deed event id and currents dates values entered
    event_id_input.delete(0, "end")
    feed_event_id_input.delete(0, "end")
    for i in range(date_counter):
        delete_date_function()

    # If the event is verified, let the user know it is added.
    if events:
        start_gathering_packets_details["state"] = "normal"
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("620x50")
        Label(newWindow, text=f"Event {eventid} has been successfully added", font=("Arial", 20)).pack()

# This function will start to gather packets based on all event(s) added.
def start_gathering_packets_details_functions():
    # Get count of events, disabled the active wigets needed for input, start a timer.
    total_event_count = len(events)
    options["state"] = "disabled"
    event_id_input["state"] = "disabled"
    feed_event_id_input["state"] = "disabled"
    add_date_button["state"] = "disabled"
    delete_date_button["state"] = "disabled"
    add_event_details["state"] = "disabled"
    start_gathering_packets_details["state"] = "disabled"
    server_options["state"] = "disabled"
    start = time.time()
    # Loop through the events, we first create a folder for the current event, then we go to the supplier_folders
    # file and using the supplier index, specifiy which folders are needed for the event
    for index, value in enumerate(events):
        supplier, hostname, eventID, feedEventID, eventDates, currentEvent = value[0], value[1], value[2], value[3], value[4], index + 1
        event_folder = create_folders(eventID)
        chosen_directories = supplier_folders.choose_supplier_directories(supplier, hostname, username, password)
        # Depending on the supplier a different function will run.
        if supplier == 0:
            supplier_functions.lsports(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar)
        elif supplier == 1:
            supplier_functions.sportscast(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar)
        elif supplier == 2:
            supplier_functions.sportsradar(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar)
        elif supplier == 3:
            supplier_functions.swish(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar)
        elif supplier == 4:
            supplier_functions.metric(hostname, username, password, feedEventID, eventDates, event_folder, chosen_directories, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar)
        elif 5 <= supplier <= 9:
            supplier_functions.other_suppliers(hostname, username, password, feedEventID, eventDates, event_folder, chosen_directories, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar)
        # After gathering the files for the current event, zip the folder for more space saved.
        zip_event_folder(event_folder)

    # Once all events are grabbed, clear the events list, renable the widgets that were disabled, give us 
    # total time taken to grab all the event(s) 
    events.clear()

    options["state"] = "normal"
    event_id_input["state"] = "normal"
    feed_event_id_input["state"] = "normal"
    chosen_options_value.set(supplier_options[1][1])
    add_date_button["state"] = "normal"
    delete_date_button["state"] = "disabled"
    add_event_details["state"] = "normal"
    start_gathering_packets_details["state"] = "disabled"
    server_options["state"] = "normal"
    progress_label_string.set("")
    supplier_functions.reset_progress(progress_bar)
    duration = time.strftime("%H:%M:%S", time.gmtime(time.time() - start))
    # Popup to let people know all packets have been gathered
    newWindow = Toplevel(root)
    newWindow.title("888 Packet Handler")
    newWindow.geometry("600x110")
    Label(newWindow, text =f"Event(s) have been gathered.\nTotal Time (H:M:S): {duration}\nThanks for using the 888 Packet Handler.",font=("Arial", 20)).pack()

# This will create the folder for the current event.
def create_folders(event):
    # Some of this code is only needed so when the exe is built with pyinstaller, if creates the directory
    # where the exe is running
    if getattr(sys, 'frozen', False):
        event_folder = os.path.dirname(os.path.realpath(sys.executable)) + "/Packets_for_Event_" + event
    else:
        event_folder = os.path.abspath(os.path.dirname(__file__)) + "/Packets_for_Event_" + event

    # If the event folder already exists we delete it and create the folder again if needed
    # (If we dont delete this issues can occur when grabbing packets)
    if os.path.isdir(event_folder):
      shutil.rmtree(event_folder)
      os.makedirs(event_folder)
    else:
      os.makedirs(event_folder)
    return event_folder

# Zip the event folder and remove the originally directory created before.
def zip_event_folder(event_folder):
    shutil.make_archive(os.path.join(event_folder), 'zip', os.path.join(event_folder))
    shutil.rmtree(event_folder)

# All wigets needed for the handler
root = Tk()
root.title("888 Packet Handler (v12)")
root.geometry("1000x490")
  
username_label = Label(root, text="Username")
username_label.place(x=30,y=30)
username_input = Entry(root, width=30)
username_input.place(x=120, y=30)

password_label = Label(root, text="Password")
password_label.place(x=30,y=60)
password_input = Entry(root, width=30, show="*")
password_input.place(x=120, y=60)

login_button = Button(root, text="Login", command=login_to_server)
login_button.place(x=180,y=90)

supplier_label = Label(root, text="Supplier: ")
supplier_label.place(x=400,y=18)

server_label = Label(root, text="Server: ")
server_label.place(x=670,y=18)

event_id_label = Label(root, text="Event ID")
event_id_label.place(x=400, y=50)
event_id_input = Entry(root, width=50)
event_id_input["state"] = "disabled"
event_id_input.place(x=490, y=50)

feed_event_id_label = Label(root, text="Feed Event ID")
feed_event_id_label.place(x=400, y=80)
feed_event_id_input = Entry(root, width=50)
feed_event_id_input["state"] = "disabled"
feed_event_id_input.place(x=490,y=80)

add_date_button = Button(root, text="Add Date Field", command=add_date_function)
add_date_button["state"] = "disabled"
add_date_button.place(x=400,y=110)

delete_date_button = Button(root, text="Delete Date Field", command=delete_date_function)
delete_date_button["state"] = "disabled"
delete_date_button.place(x=510,y=110)

add_event_details = Button(root, text="Add Event", command=add_event_details_function)
add_event_details["state"] = "disabled"
add_event_details.place(x=630,y=110)

start_gathering_packets_details = Button(root, text="Start Packet Gathering", command=lambda:threading.Thread(target=start_gathering_packets_details_functions).start())
start_gathering_packets_details["state"] = "disabled"
start_gathering_packets_details.place(x=720, y=110)

#Scrollable Date Sections
date_canvas = Canvas(root, width=450, height = 200)
date_canvas_scroll_y = Scrollbar(root, orient="vertical", command=date_canvas.yview)
date_frame = Frame(date_canvas)

# put the frame in the canvas
date_canvas.create_window(0, 0, anchor='nw', window=date_frame)
# make sure everything is displayed before configuring the scrollregion
date_canvas.update_idletasks()
date_canvas.configure(scrollregion=date_canvas.bbox('all'), yscrollcommand=date_canvas_scroll_y.set)            
date_canvas.pack(fill='both', side='left')
date_canvas_scroll_y.pack(fill='y', side='right')
date_canvas_scroll_y.place(in_=date_canvas, relx=1.0, relheight=1.0, bordermode="outside")
date_canvas.place(x=420, y=150)

# Event Progress String
progress_label_string = StringVar()
progress_label_string.set("")

# Event Progress Label
progress_label = Label(root, textvariable=progress_label_string)
progress_label.place(x=400, y=420)

# Event Progress Bar
progress_bar = Progressbar(root, orient=HORIZONTAL, length=900, mode='determinate')
progress_bar.place(x=50, y=450)
root.mainloop()