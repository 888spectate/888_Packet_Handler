from datetime import date
import paramiko
import os
import re
import tarfile
import shutil
import json

total_progress = 0

# Code specifically for grabbing LSPORTS events packets
def lsports(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    global total_progress

    # Open up the ftp_client
    ftp_client = connect_to_host(hostname, username, password)
    for index, format in enumerate(eventDates):
        # Split the dates into separte variables
        year, month, day = format[0:4], format[5:7], format[8:10]
        try:
            # Lsports has two different folders for events
            remote_folder = f"/mnt/feeds_data/fi_lsports_connector/{year}/{month}/{day}/{feedEventID}"
            remote_folder_2 = f"/mnt/feeds_data/fi_lsports_connector/{year}/{month}/{day}/outright_leagues_meta"
            # On outright events, the feed event id is longer than 7 characters, but on the packets its exactly 7
            # This if statement is needed so we get matching files.
            if len(feedEventID) > 7:
                feedEventID = feedEventID[0:7]
            
            # Grab all the events in the folder
            event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420, progress_bar, 50 / len(eventDates))
            
            # Move the files added to the root of the event folder
            # Searches for the FixtureID of the feed event id in the outright_leagues_meta folder
            # If theres a match, we grab that file, otherwise we dont
            # A progress bar will keep you up to date with the events progress.
            for index, filename in enumerate(ftp_client.listdir(remote_folder_2)):
                ftp_client.get(f"{remote_folder_2}/{filename}", os.path.join(event_folder, filename))
                opened_file = open(os.path.join(event_folder, filename), "r")
                read_opened_file = opened_file.read()
                pattern = re.search(str('"FixtureId":' + " " + feedEventID), str(read_opened_file))
                if pattern != None:
                    read_opened_file = opened_file.close()
                    os.rename(os.path.join(event_folder, filename), os.path.join(event_folder, "1_" + filename))
                else:
                    read_opened_file = opened_file.close()
                    os.remove(os.path.join(event_folder, filename))
                message = f"Event {currentEvent} of {total_event_count} - Checking file {index + 1} of {len(ftp_client.listdir(remote_folder_2))} from directory {remote_folder_2} for Fixture ID {feedEventID}."
                label_message(progress_label, progress_label_string, message, 150, 420)
                progress_bar["value"] = ((((50 / len(eventDates)) / len(ftp_client.listdir(remote_folder_2))) * (index + 1)) / total_event_count) + total_progress
            total_progress = progress_bar["value"]
        # If no directory or files are found, the event data may be wrong so we need to reset progress
        except FileNotFoundError:
            print("Folder does not exist - please double check the event")
            progress_bar["value"] = ((50 / len(eventDates)) / total_event_count) + total_progress
            total_progress = progress_bar["value"]

# Sportscast, sportsradar and swish all operate the exact same way
# Once the directory is set, it will extract all the files.
def sportscast(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    global total_progress
    ftp_client = connect_to_host(hostname, username, password)
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        remote_folder = f"/mnt/feeds_data/fi_sportcast_connector/{year}/{month}/{day}/{feedEventID}"
        event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420, progress_bar, 100)

def sportsradar(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    global total_progress
    ftp_client = connect_to_host(hostname, username, password)
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        remote_folder = f"/mnt/feeds_data/fi_sportradar_connector/{year}/{month}/{day}/{feedEventID}"
        event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420, progress_bar, 100)

def swish(hostname, username, password, feedEventID, eventDates, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    global total_progress
    ftp_client = connect_to_host(hostname, username, password)
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        remote_folder = f"/mnt/feeds_data/fi_swish_connector/{year}/{month}/{day}/{feedEventID}"
        event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420, progress_bar, 100)

def metric(hostname, username, password, feedEventID, eventDates, event_folder, chosen_directories, progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    global total_progress
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        for i in chosen_directories:
            ftp_client = connect_to_host(hostname, username, password)
            
            # Based on the dates supplier, the directory could be a zipped file of a normal folder.
            # We need to check this for both scenarios
            # If the date is today, the folder hasnt been zipped so like previously
            # We grab the packets like previous suppliers are set
            if str(format) == str(date.today()):
                remote_folder = f"{i}/{year}/{month}/{day}"
                event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420, progress_bar, 25)
            
            # If the date is not today, we need to download the zip file
            # A progress bar will keep you up to date with the downloads progress.
            else:
                ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(175, 420, f"Event {currentEvent} of {total_event_count} - Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", progress_label, progress_label_string, progress_bar, x, y, 25, total_event_count, len(eventDates)))
                ftp_client.close()
                
                # Once downloaded, we open the zip file.
                tar = tarfile.open(f"{event_folder}/{day}.tgz", "r:gz")
                # This list will be used to store any files that have been found with the pattern needed.
                files = []

                # We go through every file in the zip to look for the feed event ID pattern.
                # If it is found we add the file to the files list.
                for index, member in enumerate(tar.getmembers()):
                    f = tar.extractfile(member)
                    if f is not None:
                        pattern = re.search(str(feedEventID), str(f.read()))
                        if pattern != None:
                            files.append(member)
                    progress_bar["value"] = (((((25 / len(eventDates)) / len(tar.getmembers()))) * (index + 1)) / total_event_count) + total_progress
                    if len(tar.getmembers()) / (index + 1) == 1.00:
                        total_progress = progress_bar["value"]
                
                # Based on which files have been added to the files list, we extract them from the zip to
                # The root of the events folder
                tar.extractall(path = event_folder, members=files)
                tar.close()

                # This will move all extracted files to the root of the folder and delete the
                # Zip and the files extracted from the zip
                # When a file is extracted, it is move into the day folder. We need to move them to the root
                # for the next step
                if os.path.exists(f"{event_folder}/{day}"):
                    files_list = os.listdir(f"{event_folder}/{day}")
                    for files in files_list:
                        shutil.move(os.path.join(f"{event_folder}/{day}", files), os.path.join(event_folder, files))
                    shutil.rmtree(f"{event_folder}/{day}")
                    os.remove(f"{event_folder}/{day}.tgz")
                else:
                    os.remove(f"{event_folder}/{day}.tgz")

    # Verify JSON and XML packets pulled.
    # This could have been done in the previous step, but the json module behaved weird and wouldnt 
    # approve files that should have been pulled.
    # This code will add files to the non_event_files list, if the eventId key doesnt have the feed event id
    # We then delete all the files in that list in the second for loop
    non_event_files = []
    for index, i in enumerate(os.listdir(event_folder)):
        message = f"Event {currentEvent} of {total_event_count} - Checking file {index + 1} of {len(os.listdir(event_folder))} from directory {event_folder} for Fixture ID {feedEventID}."
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
        progress_bar["value"] = (((50 / len(os.listdir(event_folder))) * (index + 1)) / total_event_count) + total_progress
    total_progress = progress_bar["value"]

    for files in non_event_files:
        message = f"Event {currentEvent} of {total_event_count} - Removing file {files} from directory {event_folder}. Fixture ID {feedEventID} is not in the file."
        label_message(progress_label, progress_label_string, message, 120, 420)
        os.remove(os.path.join(event_folder, str(files)))

def other_suppliers(hostname, username, password, feedEventID, eventDates, event_folder, chosen_directories , progress_label, progress_label_string, currentEvent, total_event_count, progress_bar):
    global total_progress
    for index, format in enumerate(eventDates):
        year, month, day = format[0:4], format[5:7], format[8:10]
        for i in chosen_directories:
            ftp_client = connect_to_host(hostname, username, password)
            if str(format) == str(date.today()):
                remote_folder = f"{i}/{year}/{month}/{day}/{feedEventID}"
                event_in_folder_check(ftp_client, remote_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, 175, 420, progress_bar, (100 / len(chosen_directories)))
            else:
                ftp_client.get(f"{i}/{year}/{month}/{day}.tgz", f"{event_folder}/{day}.tgz", callback=lambda x, y : download_zip_progress(175, 420, f"Event {currentEvent} of {total_event_count} - Downloading zip file {i}/{year}/{month}/{day}.tgz. Progress - {round(x / y * 100, 2)}%", progress_label, progress_label_string, progress_bar, x, y, 100 / len(chosen_directories), total_event_count, len(eventDates)))
                ftp_client.close()
                total_progress = progress_bar["value"]
                message = f"Event {currentEvent} of {total_event_count} - Checking {day}.tgz file for Fixture ID {feedEventID}. This may take some time depending on supplier."
                label_message(progress_label, progress_label_string, message, 250, 420)
                
                # All other suppliers now have thier folder strcuture as the following
                # feed_event_id/all files releated to that id
                # This code will look through the zip for a folder with the provided feed event id
                # and extract it to the root of our events folder.
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
                total_progress = progress_bar["value"]

# This function will look for every file in the remote directory specified and extract them to our
# event folder
def event_in_folder_check(ftp_client, remote_event_folder, event_folder, progress_label, progress_label_string, currentEvent, total_event_count, x, y, progressBar, incrementValue):
    global total_progress
    try:
        for index, filename in enumerate(ftp_client.listdir(remote_event_folder)):
            ftp_client.get(f"{remote_event_folder}/{filename}", os.path.join(event_folder, filename))
            message = f"Event {currentEvent} of {total_event_count} - Pulling file {index + 1} of {len(ftp_client.listdir(remote_event_folder))} from directory {remote_event_folder}."
            label_message(progress_label, progress_label_string, message, x, y)
            progressBar["value"] = (((incrementValue / len(ftp_client.listdir(remote_event_folder))) * (index + 1)) / total_event_count) + total_progress
        total_progress = progressBar["value"]
    except FileNotFoundError:
        print(f"{remote_event_folder} does not exist")

# Keep the user up to date on the current progress of a zip file download
def download_zip_progress(x, y, message, progress_label, progress_label_string, progressBar, zip_progress, zip_total_progress, incrementValue, total_event_count, dateCounts):
    global total_progress
    progress_label.place(x=x, y=y)
    progress_label_string.set(message)
    progressBar["value"] = ((((incrementValue / dateCounts) / zip_total_progress) * zip_progress) / total_event_count) + total_progress

# Connect to the jumpvox specified and open an ftp_client
def connect_to_host(hostname, username, password):
    ssh_client=paramiko.SSHClient()
    ssh_client.load_host_keys(filename=os.path.expanduser('~/.ssh/known_hosts'))
    ssh_client.connect(hostname=hostname, username=username, password=password, key_filename=os.path.expanduser('~/.ssh/id_rsa'))
    ftp_client=ssh_client.open_sftp()
    return ftp_client

# Any update in the label regarding the progress bar created and set here
def label_message(progress_label, progress_label_string, message, x, y):
    progress_label.place(x=x, y=y)
    progress_label_string.set(message)

# When all events have been gathered, we reset the progress bad
def reset_progress(progressBar):
    global total_progress
    total_progress = 0
    progressBar["value"] = total_progress