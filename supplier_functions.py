from datetime import date
import paramiko
import os
import re
import tarfile
import shutil
import json

def lsports(hostname, username, password, feedEventID, eventDates, event_folder, chosen_directories, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    ftp_client = connect_to_host(hostname, username, password)
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        try:
            folder_one_verify = f"/mnt/feeds_data/fi_lsports_connector/{feedEventID}"
            for index, filename in enumerate(ftp_client.listdir(folder_one_verify)):
                ftp_client.get(f"{folder_one_verify}/{filename}", os.path.join(event_folder, filename))
                message = f"Event {currentEvent} of {total_event_count} - Pulling file {index + 1} of {len(ftp_client.listdir(folder_one_verify))} from directory {folder_one_verify}."
                label_message(progress_label, progress_label_string, message, 275, 420)
        except FileNotFoundError:
            folder_two_verify = f"/mnt/feeds_data/fi_lsports_connector/{year}/{month}/{day}/{feedEventID}"
            try:
                event_in_folder_check(ftp_client, folder_two_verify, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420)
            except FileNotFoundError:
                print("Folder does not exist - please double check the event")

    progress_bar["value"] += (50 / total_event_count)

    # Need to check this because the length of feed event ID for outright event is great than 7 characters long
    if len(feedEventID) > 7:
        feedEventID = feedEventID[0:7]

    # Check the markets_meta, outright_league_markets_meta, outright_leagues_meta, outright_meta folders 
    # for any files in relation to the event being pulled
    for number, directory in enumerate(chosen_directories):
        for index, filename in enumerate(ftp_client.listdir(directory)):
            ftp_client.get(f"{directory}/{filename}", os.path.join(event_folder, filename))
            opened_file = open(os.path.join(event_folder, filename), "r")
            read_opened_file = opened_file.read()
            pattern = re.search(str('"FixtureId":' + " " + feedEventID), str(read_opened_file))
            if pattern != None:
                read_opened_file = opened_file.close()
                os.rename(os.path.join(event_folder, filename), os.path.join(event_folder, "1_" + filename))
            else:
                read_opened_file = opened_file.close()
                os.remove(os.path.join(event_folder, filename))
            message = f"Event {currentEvent} of {total_event_count} - Checking file {index + 1} of {len(ftp_client.listdir(directory))} from directory {directory} for Fixture ID {feedEventID}."
            label_message(progress_label, progress_label_string, message, 150, 420)
        progress_bar["value"] += (12.5 / total_event_count)
    return progress_bar

def sportscast(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    ftp_client = connect_to_host(hostname, username, password)
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        remote_folder = f"/mnt/feeds_data/fi_sportcast_connector/{year}/{month}/{day}/{feedEventID}"
        event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420)
    progress_bar["value"] += (100 / total_event_count)
    return progress_bar

def sportsradar(hostname, username, password, feedEventID, event_folder, chosen_directories, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    ftp_client = connect_to_host(hostname, username, password)
    for number, directory in enumerate(chosen_directories):
        try:
            remote_event_folder = f"{directory}/sr_match{feedEventID[-8:]}"
            event_in_folder_check(ftp_client, remote_event_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420)
            break
        except IOError:
            print("Remote Folder not in this directory")
    progress_bar["value"] += (100 / total_event_count)
    return progress_bar

def swish(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    ftp_client = connect_to_host(hostname, username, password)
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        remote_folder = f"/mnt/feeds_data/fi_swish_connector/{year}/{month}/{day}/{feedEventID}"
        event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420)
    progress_bar["value"] += (100 / total_event_count)
    return progress_bar

def metric(hostname, username, password, feedEventID, eventDates, event_folder, chosen_directories, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        for i in chosen_directories:
            ftp_client = connect_to_host(hostname, username, password)
            if str(format) == str(date.today()):
                remote_folder = f"{i}/{year}/{month}/{day}"
                event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420)
            else:
                ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(175, 420, f"Event {currentEvent} of {total_event_count} - Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", progress_label, progress_label_string))
                ftp_client.close()
                tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                # This list will be used to store any files that have been found with the pattern needed.
                files = []
                for index, member in enumerate(tar.getmembers()):
                    f = tar.extractfile(member)
                    if f is not None:
                        pattern = re.search(str(feedEventID), str(f.read()))
                        if pattern != None:
                            files.append(member)
                
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
    progress_bar["value"] += (33.3 / total_event_count) 

    # Verify JSON and XML packets pulled
    non_event_files = []
    for index, i in enumerate(os.listdir(event_folder)):
        message = f"Event {currentEvent} of {total_event_count} - Checking file {index + 1} of {len(ftp_client.listdir(event_folder))} from directory {event_folder} for Fixture ID {feedEventID}."
        label_message(progress_label, progress_label_string, message, 150, 420)
        if ".json" in str(i):
            f = open(f"{event_folder}/{i}")
            content = json.loads(f.read())
            feed_id_match = False
            if content["eventId"] == feedEventID:
                feed_id_match = True
            if feed_id_match == True:
                f.close()
            else:
                f.close()
                non_event_files.append(str(i))
    progress_bar["value"] += (33.3 / total_event_count) 

    for files in non_event_files:
        message = f"Event {currentEvent} of {total_event_count} - Removing file {files} from directory {event_folder}. Fixture ID {feedEventID} is not in the file."
        label_message(progress_label, progress_label_string, message, 120, 420)
        os.remove(os.path.join(event_folder, str(files)))
    progress_bar["value"] += (33.4 / total_event_count)
    return progress_bar 

def other_suppliers(hostname, username, password, feedEventID, eventDates, event_folder, chosen_directories , progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        for i in chosen_directories:
            ftp_client = connect_to_host(hostname, username, password)
            if str(format) == str(date.today()):
                remote_folder = f"{i}/{year}/{month}/{day}/{feedEventID}"
                event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420)
            else:
                ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(175, 420, f"Event {currentEvent} of {total_event_count} - Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", progress_label, progress_label_string))
                ftp_client.close()
                message = f"Event {currentEvent} of {total_event_count} - Checking {day}.tgz file for Fixture ID {feedEventID}. This may take some time depending on supplier."
                label_message(progress_label, progress_label_string, message, 250, 420)
                
                with tarfile.open(f"{event_folder}/{day}.tgz", "r:gz") as tar:
                    files = [
                        tarinfo for tarinfo in tar.getmembers()
                        if f"/{feedEventID}/" in tarinfo.name
                    ]
                    tar.extractall(path = event_folder, members=files)

                message = f"Event {currentEvent} of {total_event_count} - Removing ZIP file from {event_folder}. This may take some time depending on supplier."
                label_message(progress_label, progress_label_string, message, 30, 420)

                if os.path.exists(f"{event_folder}/{day}/{feedEventID}/"):
                    files_list = os.listdir(f"{event_folder}/{day}/{feedEventID}/")
                    for files in files_list:
                        shutil.move(os.path.join(f"{event_folder}/{day}/{feedEventID}/", files), os.path.join(f"{event_folder}", files))
                    shutil.rmtree(f"{event_folder}/{day}/")
                    os.remove(f"{event_folder}/{day}.tgz")
                else:
                    os.remove(f"{event_folder}/{day}.tgz")
        progress_bar["value"] += ((100 / len(eventDates))/ total_event_count)
    return progress_bar

def event_in_folder_check(ftp_client, remote_event_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, x, y):
    try:
        for index, filename in enumerate(ftp_client.listdir(remote_event_folder)):
            ftp_client.get(f"{remote_event_folder}/{filename}", os.path.join(event_folder, filename))
            message = f"Event {currentEvent} of {total_event_count} - Pulling file {index + 1} of {len(ftp_client.listdir(remote_event_folder))} from directory {remote_event_folder}."
            label_message(progress_label, progress_label_string, message, x, y)
    except FileNotFoundError:
        print(f"{remote_event_folder} does not exist")

def download_zip_progress(x, y, message, progress_label, progress_label_string):
    progress_label.place(x=x, y=y)
    progress_label_string.set(message)

def connect_to_host(hostname, username, password):
    ssh_client=paramiko.SSHClient()
    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
    ssh_client.connect(hostname=hostname, username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
    ftp_client=ssh_client.open_sftp()
    return ftp_client

def label_message(progress_label, progress_label_string, message, x, y):
    progress_label.place(x=x, y=y)
    progress_label_string.set(message)