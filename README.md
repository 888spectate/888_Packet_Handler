# 888_Packet_Handler

To be able to run the handler in VS Code, you will need the following installed

1. Python 3
2. Python 3 module - tkinter and tkcalender - need these for all UI elements to work.
3. Python 3 module - paramiko - need this to be able to test logging into jumpboxes and creating the ftp_client to download files from jumpboxes.
4. Python 3 module - shutil - handier way to create/move/delete files and folders
5. Python 3 module - json and tarfile - these are so that we can verify the JSON files for some suppliers and tarfile is so that we can check the downloaded zip for the feed event id withut having to extract the zip. The code can traverse through the zip without having to unzip it (Saves a ton of space)
6. Python 3 module - pyinstaller - we need this so that we can build the exe file after code is finished.

HOW TO BUILD THE PACKET HANDLER.
1. In VS Code, make sure you are on the Packet_Handler_v12.py file.
2. Run the following command to build the exe ---> pyinstaller --onefile --hidden-import babel.numbers --windowed Packet_Handler_v12.py ---> The exe file will be directed to a dist folder that is created in the same dir as the pythpn files are in.

