import requests
import json
from prettytable import PrettyTable
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import math
import argparse


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
                age_delta = calculate_iso_difference(
                    current_time, timestamp_dt)
                age_suffix = "mins"
                age_str = int(age_delta / 60.0)     # minutes
                if age_str > 120:
                    age_str = int(age_str / 60.0)     # hours
                    age_suffix = "hrs"
                if age_str > 48:
                    age_str = int(age_str / 24.0)     # days
                    age_suffix = "days"
                item[key] = f"{age_str} {age_suffix} ago"
            elif timestamp_str == 'NaN':
                item[key] = 'N/A'
    return data


# Main function
def main(device_file_path):
    # Create a list of dictionaries (LoD) from the json file
    device_list = get_last_data(device_file_path)
    pi_dict = [load_config()]
    # Convert dictionary to a JSON object
    # pi_dev = [json.dumps(pi_dict, indent=4)]
    # print(pi_dev)
    # Flatten the dictionary inside the device dictionary for easier access
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
    table.field_names = ['RPI_ID', 'UP', 'LAST_HB', 'LAST_TEST_ETH',
                         'LAST_TEST_WLAN', '#DAY', '#WEEK', 'CAP']
    table.title = "DATA FROM FIREBASE"
    updated_data = sorted(updated_data, key=lambda x: x["RPI_ID"])
    for item in updated_data:
        table.add_row(
            [item['RPI_ID'],
             "YES" if item["online"] else "NO",
             item['last_timestamp'],
             item['last_test_eth'],
             item['last_test_wlan'],
             item['total_day'],
             item['total_consecutive_week'],
             "YES" if item["data_used_gbytes"] > 100 else "NO"])
        if len(table.get_string()) > 2800:
            # Split data due to Slack 3000-characters limit
            print(table)
            if not args.experimental:
                send_slack_msg(table)
            table.clear_rows()

    print(table)
    if not args.experimental:
        send_slack_msg(table)


if __name__ == "__main__":
    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description='Receive an input file.')
    # Expected argument
    parser.add_argument('input_file', type=str,
                        help='The path to the device file')
    parser.add_argument("--experimental", action="store_true",
                        help="Enable experimental mode")
    # Parse the arguments
    args = parser.parse_args()
    # Use the input file
    device_file_path = args.input_file
    main(device_file_path)
