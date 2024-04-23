import paho.mqtt.client as mqtt
import time
import datetime
import requests
import json
from datetime import datetime
from prettytable import PrettyTable
import pandas as pd
import os

# Initialize the heartbeat table
device_table = PrettyTable()
device_table.field_names = ["RPI-ID", "MAC", "ONLINE", "START_TIME", "HEARTBEAT_TIME",  "LAST_TEST_ETH", "LAST_TEST_WLAN"]

#  List of Mac addresses of devices
mac_pi = [
    {
        "RPI-10": "8E-51-F9-59-3F-8D",
        "RPI-20": "D8-3A-DD-41-AA-94",
        "RPI-30": "D8-3A-DD-4C-C4-9E",
        "RPI-40": "D8-3A-DD-4C-C5-19",
        "RPI-50": "D8-3A-DD-4C-C5-B5",
        "RPI-60": "D8-3A-DD-4C-C5-CE",
        "RPI-70": "D8-3A-DD-5C-A3-9A",
        "RPI-80": "D8-3A-DD-5C-E3-DB",
        "RPI-90": "D8-3A-DD-63-C6-76",
        "RPI-100": "D8-3A-DD-63-C8-5A",
        "RPI-110": "DC-A6-32-1D-A4-E0",
        "RPI-02": "D8-3A-DD-5C-E1-94"
    }]

# Initialize a heartbeat-file list
file_path = "/var/www/html/viz/device_list.json"


def get_last_data(fpath):
    try:
        # Attempt to open and load the JSON data
        with open(fpath, 'r') as file:
            last_data = json.load(file)
        #print(last_data)
    except FileNotFoundError:
        # Handle missing file error
        print(f"Error: The file '{fpath}' does not exist.")
    except json.JSONDecodeError:
        # Handle errors in JSON decoding
        print(f"Error: The file '{fpath}' contains invalid JSON.")
    except Exception as e:
        # Handle other possible exceptions (e.g., permission errors)
        print(f"An unexpected error occurred: {e}")
    return last_data


# This function takes a dictionary as input and add rows to the existing table
def update_table(message):
    global device_table
    report_values = list(message.values())
    device_table.add_row(report_values)

device_list = get_last_data(file_path)
list2 = device_list

# Flatten the dictionary inside the mac_pi list for easier access
lookup = {v: k for d in mac_pi for k, v in d.items()}

# Add the RPI identifier to each entry in list2
filtered_list = []

for item in list2:
    mac_address = item['mac']
    rpi_identifier = lookup.get(mac_address)
    if rpi_identifier:
        # Create a new dictionary starting with the RPI_ID
        new_item = {'RPI_ID': rpi_identifier}
        # Merge in the rest of the data from the original item
        new_item.update(item)
        # Append the new item to the filtered list
        filtered_list.append(new_item)


# Convert timestap to human-readable format
def convert_timestamp(ms):
    """Convert milliseconds since the epoch to a formatted date-time string."""
    if ms == 'NaN':
        return 'NaN'
    # Convert milliseconds to seconds
    seconds = ms / 1000.0
    return datetime.utcfromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')


# Update each entry with formatted timestamps
for entry in filtered_list:
    for key in ['start_timestamp', 'last_timestamp', 'last_test_eth', 'last_test_wlan']:
        if key in entry:
            entry[key] = convert_timestamp(entry[key])


# Convert LoD into a table
for j in range(len(filtered_list)):
    update_table(filtered_list[j])


# Define which columns to keep (i.e., drop 'Age' column)
columns_to_keep = ["RPI-ID", "MAC", "ONLINE", "HEARTBEAT_TIME",  "LAST_TEST_ETH", "LAST_TEST_WLAN"]


# Create a new table with selected columns
dev_table = PrettyTable()
dev_table.field_names = columns_to_keep


# Copy data from original table, excluding the 'Age' column
for row in device_table._rows:
    new_row = [row[device_table.field_names.index(col)] for col in columns_to_keep]
    dev_table.add_row(new_row)

print(dev_table)


def load_slack_config():
    with open('.slack-config.json', 'r') as file:
        cf = json.load(file)
    return cf


# This function takes table as input and converts it into string
def send_slack_msg(input_table):
    table = input_table.get_string()
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{table}```"
                }
            }
        ]
    }
    slack_conf = load_slack_config()
    requests.post(slack_conf['url'], json=payload)


send_slack_msg(dev_table)
