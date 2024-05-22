from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
import time
import requests
import json
from datetime import datetime, timezone
from prettytable import PrettyTable
import argparse

# Topic expression using a single wildcard
topic = "Schmidt/+/report/status"

# Initialize a report dictionary
report = {}

# Initialize a pretty_table object
report_table = PrettyTable()
report_table.field_names = ["RPI_ID", "MAC", "ETH_STATUS", "WIFI_STATUS",
                            "LAST_REPORT", "ATTN"]


# Calculate the time difference given ISO format times with timezones as a
# string
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


# This function takes a dictionary as input and add rows to the existing table
def update_table(message):
    global report_table
    report_values = list(message.values())
    report_table.add_row(report_values)
    return report_table


attn_list = []


def test_for_attention(table, column_name):
    global attn_list
    # Get the index of the column using the column name
    column_index = table.field_names.index(column_name)
    for row in table._rows:
        attn_list.append(row[column_index])
    if "MAYBE" not in attn_list and "YES" not in attn_list:
        attn = False  # No pi needs attention
    else:
        attn = True  # At least one pi needs attention
    return attn


def attn_table(table, column_name):
    attn_table = PrettyTable()
    # Get the index of the column using the column name
    column_index = table.field_names.index(column_name)
    attn_table.field_names = table.field_names
    attn_table.title = "@Francis: The following RPIs need attention."
    table.header_style = "title"
    # Loop through old table rows and filter by 'attn' column
    for row in table._rows:
        if row[column_index] in ["MAYBE", "YES"]:
            attn_table.add_row(row)
    return attn_table


def load_mqtt_config():
    with open('.mqtt-config.json', 'r') as file:
        conf = json.load(file)
    return conf


def load_slack_config():
    with open('.slack-config.json', 'r') as file:
        cf = json.load(file)
    return cf


def load_devlist():
    with open('.rpi-config.json', 'r') as file:
        devlist = json.load(file)
    return devlist


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


# This function takes table as input and converts it into string
def send_slack_msgtxt(input_table):
    table = input_table.get_string()
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (f"```{table}\n\nALL GOOD! No node needs "
                             f"attention right now```")
                }
            }
        ]
    }
    slack_conf = load_slack_config()
    requests.post(slack_conf['url'], json=payload)


# This function takes MAC of a Pi as input and retrieves the corresponding ID
def retrieve_id(mac):
    pi_list = load_devlist()  # Load the list of devices
    for key, value in pi_list.items():
        if value == mac:
            return key  # Return immediately when the MAC address matches
    return None  # Return None if no match is found


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("------------Connected successfully, please wait for the "
              "results -------------")
    client.subscribe(topic, qos=1)


# The Callback function to execute whenever messages are received
def on_message(client, userdata, msg):
    global report, report_table, eth0_data, wlan0_data, wlan1_data, wifi_status1

    try:
        pi_mac = msg.topic.split("/")[1]
        if len(pi_mac) == 17 and retrieve_id(pi_mac):
            id = retrieve_id(pi_mac)

            # get the published msg, extract the timestamp and calculate age
            retained_msg = json.loads(msg.payload.decode())
            current_time = datetime.now(timezone.utc).astimezone().isoformat()
            last_msg_time = retained_msg["timestamp"]
            age = round(calculate_iso_difference(
                current_time, last_msg_time) / 60)  # in minutes

            # Get the Ethernet and Wi-Fi status
            out_data = retained_msg['out']
            interfaces = out_data['ifaces']

            eth0_data = {}
            wlan0_data = {}
            wlan1_data = {}

            # Iterate through the list of interfaces
            for interface in interfaces:
                if interface["name"] == "eth0":
                    eth0_data = {
                        'up': interface['up'],
                        'ip_address': interface['ip_address'],
                        'mac_address': interface['mac_address']
                    }
                elif interface["name"] == "wlan0":
                    wlan0_data = {
                        'up': interface['up'],
                        'ip_address': interface['ip_address'],
                        'mac_address': interface['mac_address']
                    }
                elif interface["name"] == "wlan1":
                    wlan1_data = {
                        'up': interface['up'],
                        'ip_address': interface['ip_address'],
                        'mac_address': interface['mac_address']
                    }

            # Testing
            eth_status = ("UP" if eth0_data['up'] and eth0_data['ip_address']
                          else "DOWN")
            wifi_status = (
                True if wlan0_data['up'] and wlan0_data['ip_address']
                else False)

            if wlan1_data:
                wifi_status1 = (
                    True if wlan1_data.get('up') and wlan1_data.get('ip_address')
                    else False
                )

                actual_wifi_status = (
                    "UP" if wifi_status or wifi_status1
                    else "DOWN"
                )
            else:
                actual_wifi_status = (
                    "UP" if wifi_status
                    else "DOWN"
                )

            if age > 120:
                attention_needed = "YES"
                eth_status = 'UNKNOWN'
                wifi_status = 'UNKNOWN'
            elif age < 120 and actual_wifi_status == "DOWN":
                attention_needed = 'MAYBE'
            else:
                attention_needed = "NO"

            # Generate the row  corresponding to this raspberry pi
            report["RPI_ID"] = id  # Pi_id
            report["MAC"] = retained_msg["mac"]  # for eth0
            report["ETH_Status"] = eth_status
            report["WiFi_Status"] = actual_wifi_status

            if age < 2:
                report["Last_Report"] = f"{age} min ago"
            else:
                report["Last_Report"] = f"{age} mins ago"

            report["Attention"] = attention_needed

            # Add row to table
            report_table = update_table(report)

    except json.decoder.JSONDecodeError as e:
        print("Error decoding JSON:", e)


def main(experimental=False):
    # Define client and Callbacks
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    # Configure the client username and password
    config = load_mqtt_config()
    client.username_pw_set(config['username'], config['password'])
    client.connect(config['broker_addr'], int(config['broker_port']), 60)

    client.loop_start()

    try:
        time.sleep(10)
        test = test_for_attention(report_table, "ATTN")
        if test:
            # Sort the table on RPI_ID to enhance presentation
            report_table.sortby = "RPI_ID"
            print(report_table)
            if not experimental:
                send_slack_msg(report_table)

            # Generate the attention table (list of devices needing  attention)
            attn_tab = attn_table(report_table, "ATTN")
            attn_tab.sortby = "RPI_ID"
            print(attn_tab)
            if not experimental:
                send_slack_msg(attn_tab)
        else:
            print(report_table)
            if not experimental:
                print("SEE SLACK CHANNEL")
                send_slack_msgtxt(report_table)

        client.disconnect()
        client.loop_stop()

    except KeyboardInterrupt:
        print("\nDisconnecting from the broker ...")
        client.disconnect()
        client.loop_stop()


# Main script combining all the components
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--experimental", action="store_true",
                        help="Enable experimental mode")
    args = parser.parse_args()
    main(args.experimental)
