from datetime import date, datetime
from pprint import pformat
from tkinter import Tk, Button, StringVar, OptionMenu, Label, W, E, Entry, Canvas, Scrollbar, Frame, HORIZONTAL, Toplevel
from tkinter.ttk import Progressbar
from tkcalendar import DateEntry
from bs4 import BeautifulSoup
import json
import re
import tarfile
import threading
import os
import paramiko
import sys
import shutil
import supplier_folders
import time

known_host_servers, known_host_optionMenu, events = [], [], []
date_labels, dates = {}, {}
username, password = "", ""
date_counter, date_labels_y_pos, event_counter, total_progress_value= 0, 0, 0, 0

supplier_options = [
  ["lsport", "LSports"],
  ["sportradar", "SportRadar"],
  ["METRIC", "Metric Gaming"],
  ["PA", "Press Association"], # Todays date maybe (5420 files at 07:57 - 5:57 minutes)
  ["PAGH", "Dogs"], # Todays date maybe (70 files at 8:05 - 7 seconds)
  ["BR", "BetRadar"],  
  ["BRIN", "BetRadar Inplay"],
  ["SSOL", "Sporting Solutions"],
  ["SSOLIN", "Sporting Solutions InPlay"],
  ["BGIN", "BetGenius"]
]

def login_to_server():
    global username
    global password
    username = username_input.get()
    password = password_input.get()
    successful_login = 0

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


    username_input.delete(0, "end")
    username_input["state"] = "disabled"
    password_input.delete(0, "end")
    password_input["state"] = "disabled"
    login_button["state"] = "disabled"

    with open(os.path.expanduser('~/.ssh/known_hosts'), 'r') as reader:
        while (line := reader.readline()):
            if line.partition(' ')[0].partition(',')[0][0].isalpha() and "jump." in line:
              if line.partition(' ')[0].partition(',')[0] in known_host_servers:
                print(f"{line.partition(' ')[0].partition(',')[0]} already in known_host_servers array")
              else:
                known_host_servers.append(line.partition(' ')[0].partition(',')[0])
                print(f"Adding {line.partition(' ')[0].partition(',')[0]} to known_host_servers array")
    
    client = paramiko.SSHClient()

    try:
        client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
    except:
        print('File could not be read')
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("600x85")
        Label(newWindow, text="Known Hosts file could not be read",font=("Arial", 20)).pack()
    else:
        print('Known host files has been loaded')

    for index, host in enumerate(known_host_servers):
        server = host[13:]
        server = server[:-4].upper()
        try:
            client.connect(hostname=host, username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
        except paramiko.ssh_exception.BadHostKeyException:
            print('The host key given by the SSH server did not match what we were expecting.')
            newWindow = Toplevel(root)
            newWindow.title("888 Packet Handler")
            newWindow.geometry("750x50")
            Label(newWindow, text="The host key given by the SSH server did not match what we were expecting.",font=("Arial", 20)).pack()
            username_input["state"] = "normal"
            password_input["state"] = "normal"
            login_button["state"] = "normal"
            return
        except paramiko.ssh_exception.AuthenticationException:
            print('Authentication failed for some reason')
            newWindow = Toplevel(root)
            newWindow.title("888 Packet Handler")
            newWindow.geometry("750x50")
            Label(newWindow, text="Authentication failed for some reason. Maybe Password?",font=("Arial", 20)).pack()
            username_input["state"] = "normal"
            password_input["state"] = "normal"
            login_button["state"] = "normal"
            return
        else:
            print('Connection to server has been successful')
            client.close()
            successful_login += 1
            known_host_optionMenu.append(server)
            print(known_host_optionMenu[index], known_host_servers[index])

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
        chosen_options_value.set(supplier_options[2][1])
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

def add_date_function():
    global date_counter
    global date_labels_y_pos

    supplier = chosen_options_value.get()
    for index, value in enumerate(supplier_options):
        if supplier == value[1]:
            supplier = index
            break
    
    supplier_dates = supplier_folders.choose_supplier_dates(supplier)
    minimum_date = supplier_dates[0]
    maximum_date = supplier_dates[1]

    date_labels["date_label{0}".format(date_counter)] = Label(date_frame, text="Date #" + str(date_counter + 1))
    date_labels["date_label{0}".format(date_counter)].grid(column=0, row=date_labels_y_pos, sticky=W, padx=(120,10), pady=5)
    dates["date_{0}".format(date_counter)] = DateEntry(date_frame, mindate = minimum_date, maxdate = maximum_date, values=date_counter, year=2022, state="readonly", date_pattern="yyyy-mm-dd")
    dates["date_{0}".format(date_counter)].grid(column=1, row=date_labels_y_pos, sticky=E, pady=5)
    date_labels_y_pos += 1
    date_counter += 1

    # Everytime a date is added into the frame the scrollable bar will become active
    date_canvas.update_idletasks()
    date_canvas.configure(scrollregion=date_canvas.bbox('all'), yscrollcommand=date_canvas_scroll_y.set)
    date_canvas.yview_moveto(1)
    if delete_date_button["state"] == "disabled":
        delete_date_button["state"] = "normal"

def delete_date_function():
    global date_counter
    global date_labels_y_pos
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

def date_disabler(supplier):
    for index, value in enumerate(supplier_options):
        if supplier == value[1]:
            supplier = index
            break
    if supplier == 1:
        add_date_button["state"] = "disabled"
        delete_date_button["state"] = "disabled"
        for i in range(date_counter):
            delete_date_function()
    else:
        for i in range(date_counter):
            delete_date_function()
        add_date_button["state"] = "normal"
        delete_date_button["state"] = "disabled"

def add_event_details_function():
    global event_counter
    global date_counter
    global chosen_options_value
    global chosen_server_value

    supplier = chosen_options_value.get()
    servername = chosen_server_value.get()
    eventid = event_id_input.get()
    feedeventid = feed_event_id_input.get()
    servername = chosen_server_value.get()

    if not eventid:
        print("Please include an Event ID")
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

    for index, value in enumerate(supplier_options):
        if supplier == value[1]:
            print(f"{supplier} is at index {index}")
            supplier = index
            break
    
    for index, value in enumerate(known_host_optionMenu):
        if servername == value:
            print(f"{servername} is at index {index}")
            servername = known_host_servers[index]
            break
    
    if not dates and supplier == 1:
        print("No Dates needed")
        events.append([supplier, servername, eventid, feedeventid])
    elif not dates and supplier != 1:
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("500x50")
        Label(newWindow, text="Dates are needed for this supplier", font=("Arial", 20)).pack()
        print("Dates are needed for this supplier")
        return
    else:
        temp_dates = []
        for index, value in dates.items():
            temp_dates.append(str(value.get_date()))
        events.append([supplier, servername, eventid, feedeventid, temp_dates])

    event_id_input.delete(0, "end")
    feed_event_id_input.delete(0, "end")
    for i in range(date_counter):
        delete_date_function()

    if events:
        start_gathering_packets_details["state"] = "normal"
        newWindow = Toplevel(root)
        newWindow.title("888 Packet Handler")
        newWindow.geometry("620x50")
        Label(newWindow, text=f"Event {eventid} has been successfully added", font=("Arial", 20)).pack()

def start_gathering_packets_details_functions():
    global total_progress_value
    options["state"] = "disabled"
    event_id_input["state"] = "disabled"
    feed_event_id_input["state"] = "disabled"
    add_date_button["state"] = "disabled"
    delete_date_button["state"] = "disabled"
    add_event_details["state"] = "disabled"
    start_gathering_packets_details["state"] = "disabled"
    server_options["state"] = "disabled"
    start = time.time()
    for index, value in enumerate(events):
        current_event_count = index + 1
        event_folder = create_folders(value[2])
        chosen_directories = supplier_folders.choose_supplier_directories(value[0])
        # Downloading files and pulling files with feed event id
        if value[0] == 0:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                folder_one_verify = f"/mnt/feeds_data/fi_lsports_connector/{value[3]}"
                folder_two_verify = f"/mnt/feeds_data/fi_lsports_connector/{year}/{month}/{day}/{value[3]}"
                ssh_client=paramiko.SSHClient()
                ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                ftp_client=ssh_client.open_sftp()
                try:
                    print("Try first folder structure")
                    for index, filename in enumerate(ftp_client.listdir(folder_one_verify)):
                        ftp_client.get(f"{folder_one_verify}/{filename}", os.path.join(event_folder, filename))
                        label_message(f"Downloading files from {folder_one_verify}. Progress - {round((index + 1) / len(ftp_client.listdir(folder_one_verify)) * 100, 2)}%", 260 , 420, round((index + 1) / len(ftp_client.listdir(folder_one_verify)) * 100, 2))
                except FileNotFoundError:
                    print("First folder structure failed - Trying second structure.")
                    try:
                        for index, filename in enumerate(ftp_client.listdir(folder_two_verify)):
                            ftp_client.get(f"{folder_two_verify}/{filename}", os.path.join(event_folder, filename))
                            label_message(f"Downloading files from {folder_two_verify}. Progress - {round((index + 1) / len(ftp_client.listdir(folder_two_verify)) * 100, 2)}%", 260 , 420, round((index + 1) / len(ftp_client.listdir(folder_two_verify)) * 100, 2))
                    except FileNotFoundError:
                        print("Folder does not exist - please double check the event")
                    
                if len(value[3]) > 7:
                    value[3] = value[3][0:7]

                for number, i in enumerate(chosen_directories):
                    for index, filename in enumerate(ftp_client.listdir(i)):
                        ftp_client.get(f"{i}/{filename}", os.path.join(event_folder, filename))
                        opened_file = open(os.path.join(event_folder, filename), "r")
                        read_opened_file = opened_file.read()
                        pattern = re.search(str('"FixtureId":' + " " + value[3]), str(read_opened_file))
                        if pattern != None:
                            read_opened_file = opened_file.close()
                            os.rename(os.path.join(event_folder, filename), os.path.join(event_folder, "1_" + filename))
                        else:
                            read_opened_file = opened_file.close()
                            os.remove(os.path.join(event_folder, filename))
                        label_message(f"Checking files from {i} for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(ftp_client.listdir(i)) * 100, 2)}%", 200 , 420, round((index + 1) / len(ftp_client.listdir(i)) * 100, 2))
        elif value[0] == 1:
            feed_event_id_folder = f"sr_match{value[3][-8:]}"
            ssh_client=paramiko.SSHClient()
            ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
            ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
            ftp_client=ssh_client.open_sftp()
            for i in chosen_directories:
                try:
                    for index, filename in enumerate(ftp_client.listdir(f"{i}/{feed_event_id_folder}")):
                        ftp_client.get(f"{i}/{feed_event_id_folder}/{filename}", os.path.join(event_folder, filename))
                        label_message(f"Downloading files from {i}/{feed_event_id_folder}. Progress - {round((index + 1) / len(ftp_client.listdir(f'{i}/{feed_event_id_folder}')) * 100, 2)}%", 230 , 420, round((index + 1) / len(ftp_client.listdir(f"{i}/{feed_event_id_folder}")) * 100, 2))
                    break    
                except IOError:
                    print("Remote Folder not in this directory")
        elif value[0] == 2:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    if str(format) == str(date.today()):
                        for index, filename in enumerate(ftp_client.listdir(f"{i}/{year}/{month}/{day}")):
                            ftp_client.get(f"{i}/{year}/{month}/{day}/{filename}", os.path.join(event_folder, filename))
                            label_message(f"Downloading files from {i}/{year}/{month}/{day}. Progress - {round((index + 1) / len(ftp_client.listdir(f'{i}/{year}/{month}/{day}')) * 100, 2)}%", 230 , 420, round((index + 1) / len(ftp_client.listdir(f"{i}/{year}/{month}/{day}")) * 100, 2))
                    else:
                        ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 16.65 / len(value[4]), 2) / len(events)), current_event_count))
                        ftp_client.close()
                        tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                        # This list will be used to store any files that have been found with the pattern needed.
                        files = []
                        for index, member in enumerate(tar.getmembers()):
                            f = tar.extractfile(member)
                            if f is not None:
                                pattern = re.search(str(value[3]), str(f.read()))
                                if pattern != None:
                                    files.append(member)
                            label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))

                        tar.extractall(path = event_folder, members=files)
                        tar.close()

                        if os.path.exists(f"{event_folder}/{day}"):
                            files_list = os.listdir(f"{event_folder}/{day}")
                            for files in files_list:
                                shutil.move(os.path.join(f"{event_folder}/{day}", files), os.path.join(event_folder, files))
                            shutil.rmtree(f"{event_folder}/{day}")
                            os.remove(f"{event_folder}/{day}.tgz")
                        else:
                            os.remove(f"{event_folder}/{day}.tgz")
        elif value[0] == 3:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                print(year, month, day)
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    if str(format) == str(date.today()):
                        for index, filename in enumerate(ftp_client.listdir(f"{i}/{year}/{month}/{day}")):
                            ftp_client.get(f"{i}/{year}/{month}/{day}/{filename}", os.path.join(event_folder, filename))
                            label_message(f"Downloading files from {i}/{year}/{month}/{day}. Progress - {round((index + 1) / len(ftp_client.listdir(f'{i}/{year}/{month}/{day}')) * 100, 2)}%", 230 , 420, round((index + 1) / len(ftp_client.listdir(f"{i}/{year}/{month}/{day}")) * 100, 2))
                    else:     
                        ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 33.3 / len(value[4]), 2) / len(events)), current_event_count))
                        ftp_client.close()

                        tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                        # This list will be used to store any files that have been found with the pattern needed.
                        files = []
                        for index, member in enumerate(tar.getmembers()):
                            f = tar.extractfile(member)
                            if f is not None:
                                pattern = re.search(str(value[3]), str(f.read()))
                                if pattern != None:
                                    files.append(member)
                            label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))
                        tar.extractall(path = event_folder, members=files)
                        tar.close()

                        if os.path.exists(f"{event_folder}/{day}"):
                            files_list = os.listdir(f"{event_folder}/{day}")
                            for files in files_list:
                                shutil.move(os.path.join(f"{event_folder}/{day}", files), os.path.join(event_folder, files))
                            shutil.rmtree(f"{event_folder}/{day}")
                            os.remove(f"{event_folder}/{day}.tgz")
                        else:
                            os.remove(f"{event_folder}/{day}.tgz")
        elif value[0] == 4:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    if str(format) == str(date.today()):
                        non_event_files = []
                        for index, filename in enumerate(ftp_client.listdir(f"{i}/{year}/{month}/{day}")):
                            ftp_client.get(f"{i}/{year}/{month}/{day}/{filename}", os.path.join(event_folder, filename))
                            label_message(f"Downloading files from {i}/{year}/{month}/{day}. Progress - {round((index + 1) / len(ftp_client.listdir(f'{i}/{year}/{month}/{day}')) * 100, 2)}%", 230 , 420, round((index + 1) / len(ftp_client.listdir(f"{i}/{year}/{month}/{day}")) * 100, 2))
                        for num, i in enumerate(os.listdir(event_folder)):
                            if ".xml" in str(i):
                                    f = open(f"{event_folder}/{i}")
                                    soup = BeautifulSoup(f.read(), 'xml')
                                    meeting_tag, race_tag = soup.find_all('Meeting'), soup.find_all('Race')
                                    raceNumber_match = False
                                    for k in meeting_tag:
                                        if k["meetingId"] == value[3][:6]:
                                            raceNumberId = value[3][6:8]
                                            if int(raceNumberId) >= 10:
                                                raceNumberId = value[3][6:8]
                                            else:
                                                raceNumberId = value[3][7:8]
                                            for j in race_tag:
                                                if j["raceNumber"] == raceNumberId:
                                                    raceNumber_match = True
                                                    break
                                    f.close()
                                    if raceNumber_match == True:
                                        continue
                                    else:
                                        non_event_files.append(str(i))
                            label_message(f"Checking files from {event_folder} for the Feed Event ID {value[3]}. Progress - {round((num + 1) / len(os.listdir(event_folder)) * 100, 2)}%", 200 , 420, round((num + 1) / len(os.listdir(event_folder)) * 100, 2))
                        for files in non_event_files:
                            print(f"REMOVING {files}")
                            os.remove(os.path.join(event_folder, str(files)))                    
                    else:
                        ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 50 / len(value[4]), 2) / len(events)), current_event_count))
                        ftp_client.close()

                        tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                        # This list will be used to store any files that have been found with the pattern needed.
                        files = []
                        for index, member in enumerate(tar.getmembers()):
                            f = tar.extractfile(member)
                            if f is not None:
                                if ".xml" in str(member):
                                    soup = BeautifulSoup(f.read(), 'xml')
                                    meeting_tag, race_tag = soup.find_all('Meeting'), soup.find_all('Race')
                                    raceNumber_match = False
                                    for i in meeting_tag:
                                        if i["meetingId"] == value[3][:6]:
                                            raceNumberId = value[3][6:8]
                                            if int(raceNumberId) >= 10:
                                                raceNumberId = value[3][6:8]
                                            else:
                                                raceNumberId = value[3][7:8]
                                            for j in race_tag:
                                                if j["raceNumber"] == raceNumberId:
                                                    raceNumber_match = True
                                                    break

                                    if raceNumber_match == True:
                                        files.append(member)
                                label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))
                        tar.extractall(path = event_folder, members=files)
                        tar.close()

                        if os.path.exists(f"{event_folder}/{day}"):
                            files_list = os.listdir(f"{event_folder}/{day}")
                            for files in files_list:
                                shutil.move(os.path.join(f"{event_folder}/{day}", files), os.path.join(event_folder, files))
                            shutil.rmtree(f"{event_folder}/{day}")
                            os.remove(f"{event_folder}/{day}.tgz")
                        else:
                            os.remove(f"{event_folder}/{day}.tgz")                   
        elif value[0] == 5:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 50 / len(value[4]), 2) / len(events)), current_event_count))
                    ftp_client.close()

                    tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                    # This list will be used to store any files that have been found with the pattern needed.
                    files = []
                    for index, member in enumerate(tar.getmembers()):
                        f = tar.extractfile(member)
                        if f is not None:
                            if ".xml" in str(member):
                                soup = BeautifulSoup(f.read(), 'xml')
                                match_tag = soup.find_all('Match')
                                BetradarMatchID_match = False
                                for i in match_tag:
                                    if i["BetradarMatchID"] == value[3]:
                                        BetradarMatchID_match = True
                                        break
                                if BetradarMatchID_match == True:
                                    files.append(member)
                            label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))
                    tar.extractall(path = event_folder, members=files)
                    tar.close()

                    if os.path.exists(f"{event_folder}/{day}"):
                        files_list = os.listdir(f"{event_folder}/{day}")
                        for files in files_list:
                            shutil.move(os.path.join(f"{event_folder}/{day}", files), os.path.join(event_folder, files))
                        shutil.rmtree(f"{event_folder}/{day}")
                        os.remove(f"{event_folder}/{day}.tgz")
                    else:
                        os.remove(f"{event_folder}/{day}.tgz")
        elif value[0] == 6:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 50 / len(value[4]), 2) / len(events)), current_event_count))
                    ftp_client.close()

                    tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                    # This list will be used to store any files that have been found with the pattern needed.
                    files = []
                    for index, member in enumerate(tar.getmembers()):
                        f = tar.extractfile(member)
                        if f is not None:
                            if ".xml" in str(member):
                                soup = BeautifulSoup(f.read(), 'xml')
                                match_tag = soup.find_all('Match')
                                BetradarMatchID_match = False
                                for i in match_tag:
                                    if i["matchid"] == value[3]:
                                        BetradarMatchID_match = True
                                        break
                                if BetradarMatchID_match == True:
                                    files.append(member)
                            label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))
                    tar.extractall(path = event_folder, members=files)
                    tar.close()

                    if os.path.exists(f"{event_folder}/{day}"):
                        files_list = os.listdir(f"{event_folder}/{day}")
                        for files in files_list:
                            shutil.move(os.path.join(f"{event_folder}/{day}", files), os.path.join(event_folder, files))
                        shutil.rmtree(f"{event_folder}/{day}")
                        os.remove(f"{event_folder}/{day}.tgz")
                    else:
                        os.remove(f"{event_folder}/{day}.tgz")
        elif value[0] == 7:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 50 / len(value[4]), 2) / len(events)), current_event_count))
                    ftp_client.close()

                    tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                    # This list will be used to store any files that have been found with the pattern needed.
                    files = []
                    for index, member in enumerate(tar.getmembers()):
                        f = tar.extractfile(member)
                        if f is not None:
                            if ".json" in str(member):
                                if value[3] in str(member):
                                    print("MATCHING JSON FILE")
                                    files.append(member)
                            label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))
                    tar.extractall(path = event_folder, members=files)
                    tar.close()

                    for path, subdirs, files in os.walk(f"{event_folder}"):
                        for name in files:
                            shutil.move(os.path.join(path, name), os.path.join(event_folder, name))
                    shutil.rmtree(f"{event_folder}/{day}")
                    os.remove(f"{event_folder}/{day}.tgz")
        elif value[0] == 8:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 50 / len(value[4]), 2) / len(events)), current_event_count))
                    ftp_client.close()

                    tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                    # This list will be used to store any files that have been found with the pattern needed.
                    files = []
                    for index, member in enumerate(tar.getmembers()):
                        f = tar.extractfile(member)
                        if f is not None:
                            if ".json" in str(member):
                                if value[3] in str(member):
                                    print("MATCHING JSON FILE")
                                    files.append(member)
                            label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))
                    tar.extractall(path = event_folder, members=files)
                    tar.close()

                    for path, subdirs, files in os.walk(f"{event_folder}"):
                        for name in files:
                            shutil.move(os.path.join(path, name), os.path.join(event_folder, name))
                    shutil.rmtree(f"{event_folder}/{day}")
                    os.remove(f"{event_folder}/{day}.tgz")
        elif value[0] == 9:
            for index, format in enumerate(value[4]):
                year = format[0:4]
                month = format[5:7]
                day = format[8:10]
                for i in chosen_directories:
                    ssh_client=paramiko.SSHClient()
                    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
                    ssh_client.connect(hostname=value[1], username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
                    ftp_client=ssh_client.open_sftp()
                    if str(format) == str(date.today()):
                        try:
                            for index, filename in enumerate(ftp_client.listdir(f"{i}/{year}/{month}/{day}/{value[3]}")):
                                ftp_client.get(f"{i}/{year}/{month}/{day}/{str(value[3])}/{filename}", os.path.join(event_folder, filename))
                                label_message(f"Downloading files from {i}/{year}/{month}/{day}/{str(value[3])}. Progress - {round((index + 1) / len(ftp_client.listdir(f'{i}/{year}/{month}/{day}/{str(value[3])}')) * 100, 2)}%", 230 , 420, round((index + 1) / len(ftp_client.listdir(f"{i}/{year}/{month}/{day}/{str(value[3])}")) * 100, 2))
                        except:
                            print("Folder dosnt exist")
                    else:
                        ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(x, y, f"Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", 230, 420, round(x / y * 100, 2), (round(x / y * 16.65 / len(value[4]), 2) / len(events)), current_event_count))
                        ftp_client.close()
                        
                        if datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d').date() <= datetime.strptime('2022-07-20', '%Y-%m-%d').date():
                            tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                            # This list will be used to store any files that have been found with the pattern needed.
                            files = []
                            for index, member in enumerate(tar.getmembers()):
                                f = tar.extractfile(member)
                                if f is not None:
                                    pattern = re.search(str(value[3]), str(f.read()))
                                    if pattern != None:
                                        files.append(member)
                                label_message(f"Searching zip folder {day}.tgz for the Feed Event ID {value[3]}. Progress - {round((index + 1) / len(tar.getmembers()) * 100, 2)}%", 310 , 420, round((index + 1) / len(tar.getmembers()) * 100, 2))
                            tar.extractall(path = event_folder, members=files)
                            tar.close()

                            if os.path.exists(f"{event_folder}/{day}"):
                                files_list = os.listdir(f"{event_folder}/{day}")
                                for files in files_list:
                                    shutil.move(os.path.join(f"{event_folder}/{day}", files), os.path.join(event_folder, files))
                                shutil.rmtree(f"{event_folder}/{day}")
                                os.remove(f"{event_folder}/{day}.tgz")
                            else:
                                os.remove(f"{event_folder}/{day}.tgz")

                        elif datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d').date() == datetime.strptime('2022-07-21', '%Y-%m-%d').date():
                            print("OLD and NEW Format")
                            continue
                            
                        elif datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d').date() >= datetime.strptime('2022-07-22', '%Y-%m-%d').date():
                            with tarfile.open(f"{event_folder}/{day}.tgz", "r:gz") as tar:
                                files = [
                                    tarinfo for tarinfo in tar.getmembers()
                                    if f"/{value[3]}/" in tarinfo.name
                                ]
                                tar.extractall(path = event_folder, members=files)

                            if os.path.exists(f"{event_folder}/{day}/{value[3]}/"):
                                files_list = os.listdir(f"{event_folder}/{day}/{value[3]}/")
                                for files in files_list:
                                    shutil.move(os.path.join(f"{event_folder}/{day}/{value[3]}/", files), os.path.join(f"{event_folder}", files))
                                shutil.rmtree(f"{event_folder}/{day}/")
                                os.remove(f"{event_folder}/{day}.tgz")
                            else:
                                os.remove(f"{event_folder}/{day}.tgz")
        
        # Verifying JSON and XML Files with the feed event id is matched to a tag
        if value[0] == 2:
            non_event_files = []
            for index, i in enumerate(os.listdir(event_folder)):
                if ".json" in str(i):
                    f = open(f"{event_folder}/{i}")
                    content = json.loads(f.read())
                    feed_id_match = False
                    if content["eventId"] == value[3]:
                        feed_id_match = True
                    if feed_id_match == True:
                        print("KEEPING JSON FILE")
                        f.close()
                    else:
                        print("REMOVING JSON FILE")
                        f.close()
                        non_event_files.append(str(i))
                label_message(f"Verifying files pulled have Feed Event ID {value[3]}. Progress - {round((index + 1) / len(os.listdir(event_folder)) * 100, 2)}%", 320 , 420, round((index + 1) / len(os.listdir(event_folder)) * 100, 2))
            for files in non_event_files:
                print(f"REMOVING {files}")
                os.remove(os.path.join(event_folder, str(files)))
        if value[0] == 3:
            non_event_files = []
            for index, i in enumerate(os.listdir(event_folder)):
                if ".xml" in str(i):
                    f = open(f"{event_folder}/{i}")
                    soup = BeautifulSoup(f.read(), 'xml')
                    race_tag = soup.find_all('Race')
                    raceNumber_match = False
                    for j in race_tag:
                        if j["id"] == value[3]:
                            print("PA FOUND FILE WOOOO")
                            raceNumber_match = True
                            break

                    if raceNumber_match == True:
                        print("KEEPING XML FILE")
                        f.close()
                    else:
                        print("REMOVING JSON FILE")
                        f.close()
                        non_event_files.append(str(i))
                        print(str(i))
                label_message(f"Verifying files pulled have Feed Event ID {value[3]}. Progress - {round((index + 1) / len(os.listdir(event_folder)) * 100, 2)}%", 320 , 420, round((index + 1) / len(os.listdir(event_folder)) * 100, 2))
            for files in non_event_files:
                print(f"REMOVING FILE{files}")
                os.remove(os.path.join(event_folder, str(files)))
        if value[0] == 9:
            non_event_files = []
            for num, i in enumerate(os.listdir(event_folder)):
                if ".json" in str(i):
                    f = open(f"{event_folder}/{i}")
                    content = json.load(f)
                    content2 = json.loads(content['content'])
                    feed_id_match = False
                    for pattern in content2:
                        if pattern == "BetgeniusFixtureId":
                            if str(content2[pattern]) == value[3]:
                                feed_id_match = True
                                break
                        elif pattern == "FixtureId":
                            if str(content2[pattern]) == value[3]:
                                feed_id_match = True
                                break
                    if feed_id_match == True:
                        print("KEEPING JSON FILE")
                        f.close()
                    else:
                        print("REMOVING JSON FILE")
                        f.close()
                        non_event_files.append(str(i))

                elif ".xml" in str(i):
                    f = open(f"{event_folder}/{i}")
                    soup = BeautifulSoup(f, 'xml')
                    FixtureId, Id = soup.find_all('FixtureId'), soup.find_all('Id')
                    feed_id_match = False
                    for g in FixtureId:
                        if g.text == value[3]:
                            feed_id_match = True
                            break

                    for g in Id:
                        if g.text == value[3]:
                            feed_id_match = True
                            break

                    if feed_id_match == True:
                        print("KEEPING XML FILE")
                        StartTimeUtc = len(soup.find_all('StartTimeUtc'))
                        if StartTimeUtc > 0:
                            print("START TIME UTC FOUND: RENAIMING WITH 1_")
                            f.close()
                            os.rename(os.path.join(event_folder, str(i)), os.path.join(event_folder + "/1_" + str(i)))
                        else:
                            f.close()
                    else:
                        print("REMOVING XML FILE")
                        f.close()
                        non_event_files.append(str(i))
                label_message(f"Verifying files pulled have Feed Event ID {value[3]}. Progress - {round((num + 1) / len(os.listdir(event_folder)) * 100, 2)}%", 320 , 420, round((num + 1) / len(os.listdir(event_folder)) * 100, 2))
            for files in non_event_files:
                print(f"REMOVING {files}")
                os.remove(os.path.join(event_folder, str(files)))
        zip_event_folder(event_folder)
    
    events.clear()

    options["state"] = "normal"
    event_id_input["state"] = "normal"
    feed_event_id_input["state"] = "normal"
    chosen_options_value.set(supplier_options[2][1])
    add_date_button["state"] = "normal"
    delete_date_button["state"] = "disabled"
    add_event_details["state"] = "normal"
    start_gathering_packets_details["state"] = "disabled"
    server_options["state"] = "normal"
    progress_label_string.set("")
    progress_bar["value"] = 0
    duration = time.strftime("%H:%M:%S", time.gmtime(time.time() - start))
    # Popup to let people know all packets have been gathered
    newWindow = Toplevel(root)
    newWindow.title("888 Packet Handler")
    newWindow.geometry("600x110")
    Label(newWindow, text =f"Event(s) have been gathered.\nTotal Time (H:M:S): {duration}\nThanks for using the 888 Packet Handler.",font=("Arial", 20)).pack()

def create_folders(event):
    if getattr(sys, 'frozen', False):
        event_folder = os.path.dirname(os.path.realpath(sys.executable)) + "/Packets_for_Event_" + event
    else:
        event_folder = os.path.abspath(os.path.dirname(__file__)) + "/Packets_for_Event_" + event

    if os.path.isdir(event_folder):
      shutil.rmtree(event_folder)
      os.makedirs(event_folder)
    else:
      os.makedirs(event_folder)
    return event_folder

def label_message(message, x, y, percentage):
    if percentage == 100.00:
        progress_label.place(x=400, y=420)
        progress_label_string.set("Waiting for next process to start")
        percentage = 0
        progress_bar["value"] = percentage
    else:
        progress_label.place(x=x, y=y)
        progress_label_string.set(message)
        progress_bar["value"] = percentage

def download_zip_progress(transferred, toBeTransferred, message, positionX, positionY, percentage, overall_progress, current_event):
    if percentage == 100.00:
        progress_label.place(x=400, y=420)
        progress_label_string.set("Waiting for next process to start")
        percentage = 0
        progress_bar["value"] = percentage
    else:
        progress_label.place(x=positionX, y=positionY)
        progress_label_string.set(message)
        progress_bar["value"] = percentage


def zip_event_folder(event_folder):
    print("ZIPPING FOLDER")
    print(event_folder)
    shutil.make_archive(os.path.join(event_folder), 'zip', os.path.join(event_folder))
    shutil.rmtree(event_folder)

root = Tk()
root.title("888 Packet Handler (v11.3)")
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
