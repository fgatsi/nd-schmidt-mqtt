import paho.mqtt.client as mqtt
import time
import requests
import json
from prettytable import PrettyTable
from datetime import datetime, timedelta, timezone
import pytz

# Initialize the heartbeat table
device_table = PrettyTable()
device_table.field_names = ["RPI-ID", "MAC", "ONLINE", "START_TIME", "HEARTBEAT_TIME", "LAST_TEST_ETH",
                            "LAST_TEST_WLAN"]

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

# Get the last device data from the server as a json object
def get_last_data(fpath):
    try:
        # Attempt to open and load the JSON data
        with open(fpath, 'r') as file:
            last_data = json.load(file)
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


# Convert Timestamp to human-readable format: '%Y-%m-%d %H:%M:%S'
def convert_timestamp(ms):
    """Convert milliseconds since the epoch to a formatted date-time string."""
    if ms == 'NaN':
        return 'NaN'
    # Convert milliseconds to seconds
    seconds = ms / 1000.0

    timestamp_datetime = datetime.fromtimestamp(seconds, timezone.utc)
    formatted_datetime = timestamp_datetime.strftime('%Y-%m-%d %H:%M:%S')
    # print(formatted_datetime)
    return formatted_datetime


# Compute the ages of the last three time fields
def compute_age(data):
    current_time = datetime.now(pytz.utc)
    time_keys = ['last_timestamp', 'last_test_eth', 'last_test_wlan']

    for item in data:
        for key in time_keys:
            timestamp_str = item.get(key)
            if timestamp_str and timestamp_str != 'NaN':
                timestamp_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                timestamp_dt = pytz.utc.localize(timestamp_dt)
                age_delta = current_time - timestamp_dt
                age_minutes = int(age_delta.total_seconds() / 60.0)
                item[key] = f"{age_minutes} min(s) ago"
                # # Update the time to a readable format
                # if age_minutes < 2:
                #     item[key] = f"{age_minutes} min ago"
                # elif age_minutes < 60:
                #     item[key] = f"{age_minutes} mins ago"
                # else:
                #     h = age_minutes // 60
                #     m = age_minutes % 60
                #     item[key] = f"{h} hr {m} min ago"
                # # age_string = f"{age_delta.days} d, {age_delta.seconds // 3600} h ago"
                # # item[key] = age_string
            elif timestamp_str == 'NaN':
                item[key] = 'N/A'
    return data


# Main function
def main():
    # Create a list of dictionaries (LoD) from the json file
    device_list = get_last_data(file_path)

    # Flatten the dictionary inside the mac_pi list for easier access
    lookup = {v: k for d in mac_pi for k, v in d.items()}

    # Add the appropriate RPI identifier to each entry in device_list
    augmented_list = []
    for item in device_list:
        mac_address = item['mac']
        rpi_identifier = lookup.get(mac_address)
        if rpi_identifier:
            # Create a new dictionary starting with the RPI_ID
            new_item = {'RPI_ID': rpi_identifier}
            # Merge in the rest of the data from the original item
            new_item.update(item)
            # Append the new item to the filtered list
            augmented_list.append(new_item)

    # Update each entry with formatted (converted) timestamps
    for entry in augmented_list:
        for key in ['start_timestamp', 'last_timestamp', 'last_test_eth', 'last_test_wlan']:
            if key in entry:
                entry[key] = convert_timestamp(entry[key])

    # Convert the timestamp entries into ages
    updated_data = compute_age(augmented_list)

    # Define and print the new table with age strings
    table = PrettyTable()
    table.field_names = ['RPI_ID', 'ONLINE', 'LAST_HB', 'LAST_TEST_ETH', 'LAST_TEST_WLAN']
    table.title = "DATA FROM FIREBASE"
    for item in updated_data:
        if item['online']:
            item['online'] = 'YES'
        else:
            item['online'] = 'NO'
        table.add_row(
            [item['RPI_ID'], item['online'], item['last_timestamp'], item['last_test_eth'], item['last_test_wlan']])

    print(table)
    send_slack_msg(table)


if __name__ == "__main__":
    main()
