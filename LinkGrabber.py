import asyncio                  # For the asynchronous functions of Telethon
import re                       # Link Recognition Regex
import sqlite3                  # Local DBMS and Primary Storage
import os                       # For System Calls
from getpass import getpass     # 2FA password input

try:                            # Importing external libraries
    import xlsxwriter           # For writing the final data into xlsx file
    from telethon import TelegramClient, functions     # Client Creation Method
    from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError     # Telethon errors for handling 2FA and Invalid Numbers respectively
    from telethon.tl.functions.messages import (GetHistoryRequest)      # For requesting and receiving the dialogs from Telegram API
    from telethon.tl.types import PeerChannel       # For finding the entity object of the required channel
except ImportError:     # In case the extra libraries are not already present
    if os.name == "posix":      # If its a UNIX system
        os.system("sudo apt install python3-pip")       # Ensuring presence of pip
   
    os.system("pip3 install -r requirements.txt")       # Installing required libraries
    os.system("clear" if os.name == "posix" else "cls")     # Clearing console/terminal and importing required libraries
    import xlsxwriter
    from telethon import TelegramClient
    from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError
    from telethon.tl.functions.messages import (GetHistoryRequest)
    from telethon.tl.types import PeerChannel


class LinkGrabber:          # LinkGrabber Class for easier method handling
    def __init__(self):     # Default Constructor
        # Setting API ID and API Hash from Telegram MLBot
        # Sign up for Telegram using any application.
        # Log in to your Telegram core: https://my.telegram.org.
        # Go to 'API development tools' and fill out the form.
        # You will get basic addresses as well as the api_id and api_hash parameters required for user authorization.
        self.api_id = # API ID GOOES HERE
        self.api_hash = # API HASH GOES HERE

        self.username = "Client"

        print("The Phone Number must be the one linked to your Telegram Account")
        print("It must have its Country Code and a '+' sign added before of it.")
        self.phone = input("Enter Phone Number : ")     # User's Phone Number for OTP authentication

        self.client = TelegramClient(self.username, self.api_id, self.api_hash)     # Client Creation

        self.dialogs = []       # Initial Dialog Storage Variable

        self.mydb = sqlite3.connect(':memory:')     # Initializing DB connection with temporary DB in memory

        self.subjects = ['OS_LAB', 'OS_THEORY', 'DSDA_LAB', 'DSDA_THEORY', 'NETWORK_THEORY', 'NETWORK_LAB', 'SDP',
                    'SOFTWARE_LAB', 'SOFTWARE_THEORY', 'GRAPHICS', 'MANAGEMENT']        # Subject List

        self.loop = asyncio.get_event_loop()    # Loop Variable for async functions
        try:
            self.loop.run_until_complete(self.client.connect())     # Checking Client Connection
        except ConnectionError:     # If no connection available.
            print("\n\nCONNECTION FAILED. YOU NEED AN ACTIVE INTERNET CONNECTION TO PROCEED.")
            input("Press any key to continue......")
            exit(-1)
        self.loop.run_until_complete(self.getMessages(self.phone))  # Initiating message parsing

        os.system('clear' if os.name == 'posix' else 'cls')

        print("Messages collected. Scrapping Links.\n")
        self.processLinks()     # Function call to find YT links and store them in primary storage (DB)
        print("Links have been parsed and stored in a temporary DB. Shifting them to XML file.\n")
        self.sqlite3ToXML()     # Function call to move data from DB to XML file
        self.loop.run_until_complete(self.client(functions.auth.LogOutRequest()))
        print("Processing complete.\n")
        print(f"Links stored subject-wise in 'class_links' in {os.getcwd()}")
        input("Press any key to continue......")

    # Function Name : getMessages
    # Parameters    : phone (type - str, purpose - contains user's phone number)
    # Function      : To authenticate user and create a successful connection to Telegram
    #                 and parse dialogs, then store them in the initial storage
    # Return Value  : None
    async def getMessages(self, phone):
        # await self.client.start()
        # Ensuring Authorization
        if not await self.client.is_user_authorized():      # Checking Client Authorization
            try:
                await self.client.send_code_request(phone)
            except PhoneNumberInvalidError:     # Phone number entered is wrong
                print("\n\nThe Phone Number you provided did not match our records.")
                print("Please start over and ensure the number is correct and is entered in the specified format.")
                input("Press any key to continue......")
                exit(19)
            print("Your Session is not Verified. Enter the OTP sent to your Telegram.")
            try:
                await self.client.sign_in(phone, input("Enter The Code : "))
            except SessionPasswordNeededError:      # Client has 2FA
                await self.client.sign_in(password=getpass(prompt='Enter 2FA Password : '))     # 2FA password entry
        # user_input_channel = input("Enter Entity(Telegram URL or Entity ID) : ")
        user_input_channel = "1413828830"       # UEM CS 2018 Batch Group ID
        if user_input_channel.isdigit():
            entity = PeerChannel(int(user_input_channel))
        else:
            entity = user_input_channel
        my_channel = await self.client.get_entity(entity)   # Finding channel entity

        offset_id = 0   # Latest message id i.e. 0
        limit = 100     # Batch size limit for Telegram

        while True:
            # Parsing Dialogs within given parameters
            history = await self.client(GetHistoryRequest(
                peer=my_channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=1655,
                hash=0
            ))
            if not history.messages:    # Dialog presence check
                break
            messages = history.messages
            for message in messages:
                self.dialogs.append(message.to_dict())  # Dialog storage in dict() format
            offset_id = messages[-1].id     # Updating offset id to the last id in current batch for further retrieval
        self.dialogs.sort(key=lambda x: x["date"])      # Sorting dialogs based on date

    # Function Name : processLinks
    # Parameters    : None
    # Function      : To find dialogs with youtube links and sorting them subject-wise
    #                 and storing them in local DB
    # Return Value  : None
    def processLinks(self):
        mycursor = self.mydb.cursor()       # Cursor for the current DB i.e. local DB created in constructor
        for tablename in self.subjects:     # Creating tables for each subject
            mycursor.execute(f"CREATE TABLE {tablename} (CLASS_NO INTEGER PRIMARY KEY AUTOINCREMENT, DATE varchar(255), TEACHER varchar(255), YOUTUBE_LINK varchar(255));")

        # Pre-written SQL statements for each table entry
        dsda_lab = "INSERT INTO DSDA_LAB (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        dsda_theory = "INSERT INTO DSDA_THEORY (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        sdp = "INSERT INTO SDP (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        graphics = "INSERT INTO GRAPHICS (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        management = "INSERT INTO MANAGEMENT (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        network_theory = "INSERT INTO NETWORK_THEORY (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        network_lab = "INSERT INTO NETWORK_LAB (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        os_lab = "INSERT INTO OS_LAB (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        os_theory = "INSERT INTO OS_THEORY (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        software_lab = "INSERT INTO SOFTWARE_LAB (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"
        software_theory = "INSERT INTO SOFTWARE_THEORY (DATE, TEACHER, YOUTUBE_LINK) VALUES (?, ?, ?);"

        # Parsing through dialogs and finding ones with links
        for message in self.dialogs:
            regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)youtu\.be(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|youtu)+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
            url = re.findall(regex, message['message'])
            if not url:     # If current dialog has no Youtube link, we move on.
                continue
            if message['post_author'] in ['Sankhadeep Chatterjee', 'DEBKUMAR CHOWDHURY']:  # DSDA
                if 'Data Science & Data Analytics Laboratory' in message['message']:  # DSDA LAB
                    mycursor.execute(dsda_lab, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
                elif 'Data Science & Data Analytics' in message['message']:  # DSDA THEORY
                    mycursor.execute(dsda_theory, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
            elif message['post_author'] in ["Sukanya Roy"] and 'Computer Graphics & Multimedia' in message['message']:  # COMPUTER GRAPHICS
                mycursor.execute(graphics, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
            elif message['post_author'] in ['Abhilash Jain'] and 'PRINCIPLES OF MANAGEMENT' in message['message']:  # PRINCIPLES OF MANAGEMENT
                mycursor.execute(management, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
            elif message['post_author'] in ['Panchali Datta Choudhury'] and 'Operating System Lab' in message['message']:  # OPERATING SYSTEMS LAB
                mycursor.execute(os_lab, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
            elif message['post_author'] in ['Moumita Basu', 'Bipasha'] and 'Operating System' in message['message']:  # OPERATING SYSTEMS THEORY
                mycursor.execute(os_theory, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
            elif message['post_author'] in ['Anay Ghosh']:  # SOFTWARE ENGINEERING
                if 'Software Engineering Lab' in message['message']:  # SOFTWARE ENGINEERING LAB
                    mycursor.execute(software_lab, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
                elif 'Software Engineering' in message['message']:  # SOFTWARE ENGINEERING THEORY
                    mycursor.execute(software_theory, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
            elif message['post_author'] in ['Deb'] and 'SDP' in message['message']:  # SDP
                mycursor.execute(sdp, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
            elif message['post_author'] in ['Nilanjan Byabarta', 'Subhalaxmi Chakraborty', 'Sumit Anand']:  # NETWORKING
                if 'Computer Networks Laboratory' in message['message']:  # NETWROKING LAB
                    mycursor.execute(network_lab, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
                else:  # NETWORKING THEORY
                    mycursor.execute(network_theory, (message['date'].strftime("%Y-%b-%d"), message['post_author'].title(), url[0][0]))
        self.mydb.commit()      # Committing changes to current DB

    # Function Name : sqlite3ToXML
    # Parameters    : None
    # Function      : To transfer data, table-wise, from local DB to XML file.
    # Return Value  : None
    def sqlite3ToXML(self):
        mycursor = self.mydb.cursor()       # Cursor for the current DB i.e. local DB created in constructor
        with xlsxwriter.Workbook('class_links.xlsx') as outfile:    # Opening file object for writing
            for subject in self.subjects:       # Traversing through each subjects
                mycursor.execute(f"SELECT * from {subject}")
                rows = mycursor.fetchall()      # Receiving the data from each table
                currSheet = outfile.add_worksheet(subject)      # Creating new worksheet in XML file
                for rowNo, row in enumerate(rows):      # Storing data in XML File
                    for columnNo, value in enumerate(row):
                        currSheet.write(rowNo, columnNo, value)
        self.mydb.close()       # Ending connection with DB


if __name__ == '__main__':
    LinkGrabber()       # Calling class to initiate application
