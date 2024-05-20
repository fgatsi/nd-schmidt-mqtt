import requests
import json
from prettytable import PrettyTable
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import math

#  List of Mac addresses of devices
mac_pi = [
    {
        "RPI-01": "DC-A6-32-1D-A4-E0",
        "RPI-02": "8E-51-F9-59-3F-8D",
        "RPI-03": "D8-3A-DD-41-AA-94",
        "RPI-04": "D8-3A-DD-4C-C4-9E",
        "RPI-05": "D8-3A-DD-4C-C5-19",
        "RPI-06": "D8-3A-DD-4C-C5-B5",
        "RPI-07": "D8-3A-DD-4C-C5-CE",
        "RPI-11": "D8-3A-DD-63-C6-76",
        "RPI-12": "D8-3A-DD-5C-E1-94",
        "RPI-13": "D8-3A-DD-5C-E3-DB",
        "RPI-14": "D8-3A-DD-5C-A3-9A",
        "RPI-15": "D8-3A-DD-63-C8-5A",
        #"RPI-16": "D8-3A-DD-63-C6-5F",
        #"RPI-17": "D8-3A-DD-5C-E4-0B",
        #"RPI-18": "D8-3A-DD-5C-E2-87",
        #"RPI-19": "D8-3A-DD-5C-A5-54",
        #"RPI-20": "D8-3A-DD-5C-E3-EE",
        #"RPI-21": "D8-3A-DD-5C-E2-AE",
        #"RPI-22": "D8-3A-DD-63-C7-C3",
        #"RPI-23": "D8-3A-DD-63-C6-CB",
        #"RPI-24": "D8-3A-DD-63-C7-93",
        #"RPI-25": "D8-3A-DD-5C-E1-F1",
        #"RPI-26": "D8-3A-DD-5C-E5-98",
        #"RPI-27": "D8-3A-DD-63-C7-20",
        #"RPI-28": "D8-3A-DD-5C-E4-D5",
        #"RPI-29": "D8-3A-DD-5C-E2-0C",
        #"RPI-30": "D8-3A-DD-63-C7-BA"
    }]

# Path to device-list file
file_path = "/Users/fgatsi/device_list.json"


def load_config():
    with open('.rpi-config.json', 'r') as file:
        devlist = json.load(file)
    print(devlist)
    return devlist


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


def ms_to_iso(timestamp_ms):
    if isinstance(timestamp_ms, float) and math.isnan(timestamp_ms):
        return 'NaN'
    else:
        # Convert milliseconds to seconds
        timestamp_seconds = timestamp_ms / 1000.0

        # Convert timestamp to a datetime object
        dt = datetime.fromtimestamp(timestamp_seconds, timezone.utc)

        # Convert datetime to ISO format
        iso_format = dt.isoformat()

    return iso_format


# Calculate the time difference given ISO format times with timezones as a string
def calculate_iso_difference(time_str1, time_str2):
    # Convert strings to datetime objects, ensuring they are timezone aware
    time1 = datetime.fromisoformat(time_str1)
    time2 = datetime.fromisoformat(time_str2)

    # Convert both times to the same timezone (UTC) for comparison
    time1_utc = time1.astimezone(ZoneInfo('UTC'))
    time2_utc = time2.astimezone(ZoneInfo('UTC'))

    # Calculate the difference
    time_diff = time1_utc - time2_utc
    time_diff = time_diff.total_seconds()
    # Return total seconds
    return time_diff


def calculate_age(data):
    current_time = datetime.now(timezone.utc).astimezone().isoformat()
    time_keys = ['last_timestamp', 'last_test_eth', 'last_test_wlan']

    for item in data:
        for key in time_keys:
            timestamp_str = item.get(key)
            if timestamp_str and timestamp_str != 'NaN':
                timestamp_dt = ms_to_iso(timestamp_str)
                age_delta = calculate_iso_difference(current_time, timestamp_dt)
                age_minutes = int(age_delta / 60.0)
                item[key] = f"{age_minutes} min(s) ago"
            elif timestamp_str == 'NaN':
                item[key] = 'N/A'
    return data


# Main function
def main():
    # Create a list of dictionaries (LoD) from the json file
    device_list = get_last_data(file_path)
    pi_dict = [load_config()]
    # Convert dictionary to a JSON object
    # pi_dev = [json.dumps(pi_dict, indent=4)]
    # print(pi_dev)
    # Flatten the dictionary inside the mac_pi list for easier access
    lookup = {v: k for d in pi_dict for k, v in d.items()}

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
    # print(augmented_list)
    updated_data = calculate_age(augmented_list)

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

    table.sortby = "RPI_ID"
    print(table)
    send_slack_msg(table)


if __name__ == "__main__":
    main()
