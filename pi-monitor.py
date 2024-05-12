import math
from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
import time
import datetime
import requests
import json
from datetime import datetime, timezone
from prettytable import PrettyTable

# Topic expression using a single wildcard
topic1 = "Schmidt/D8-3A-DD-63-C6-76/report/status"
topic2 = "Schmidt/D8-3A-DD-5C-E1-94/report/status"
topic3 = "Schmidt/D8-3A-DD-5C-E3-DB/report/status"
topic4 = "Schmidt/D8-3A-DD-5C-A3-9A/report/status"
topic5 = "Schmidt/D8-3A-DD-63-C8-5A/report/status"


# Initialize a report dictionary
report = {}

# Initialize a pretty_table object
test_table = PrettyTable()
test_table.field_names = ["RPI_ID", "MAC", "ETH_STATUS", "WIFI_STATUS", "LAST_REPORT", "ATTN"]


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


# This function takes a dictionary as input and add rows to the existing table
def update_table(message):
    global test_table
    report_values = list(message.values())
    test_table.add_row(report_values)
    return test_table

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


# This function takes MAC of a Pi as input and retrieves the corresponding ID
def retrieve_id(mac):
    pi_list = load_devlist()  # Load the list of devices
    for key, value in pi_list.items():
        if value == mac:
            return key  # Return immediately when the MAC address matches
    return None  # Return None if no match is found


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("------------Connected successfully, please wait for the results -------------")
    client.subscribe(topic1, qos=1)
    client.subscribe(topic2, qos=1)
    client.subscribe(topic3, qos=1)
    client.subscribe(topic4, qos=1)
    client.subscribe(topic5, qos=1)

# The Callback function to execute whenever messages are received
def on_message(client, userdata, msg):
    global report, report_table, eth0_data, wlan0_data
    try:
        pi_mac = msg.topic.split("/")[1]
        id = retrieve_id(pi_mac)
        #print(f'{id} : {pi_mac}')
        # get the published msg, extract the timestamp and calculate age
        retained_msg = json.loads(msg.payload.decode())
        current_time = datetime.now(timezone.utc).astimezone().isoformat()
        last_msg_time = retained_msg["timestamp"]
        age = round(calculate_iso_difference(current_time, last_msg_time) / 60)  # in minutes

        # Get the Ethernet and Wi-Fi status
        out_data = retained_msg['out']
        interfaces = out_data['ifaces']
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

        # Testing
        eth_status = "UP" if eth0_data['up'] and eth0_data['ip_address'] else "DOWN"
        wifi_status = "UP" if wlan0_data['up'] and wlan0_data['ip_address'] else "DOWN"

        #if age > 120 or wifi_status == "DOWN":
        #    attention_needed = "YES"
        #else:
        #    attention_needed = "NO"
        if age > 120:
            attention_needed = "YES"
            eth_status = 'UNKNOWN'
            wifi_status = 'UNKNOWN'
        elif age < 120 and wifi_status == "DOWN":                         
            attention_needed = 'MAYBE'
        else:
            attention_needed = "NO"

        # Generate the row  corresponding to this raspberry pi
        report["RPI_ID"] = id  # Pi_id
        report["MAC"] = retained_msg["mac"]  # for eth0
        report["ETH_Status"] = eth_status
        report["WiFi_Status"] = wifi_status

        if age < 2:
            report["Last_Report"] = f"{age} min ago"
        else:
            report["Last_Report"] = f"{age} mins ago"

        report["Attention"] = attention_needed

        # Add row to table
        report_table = update_table(report)
    
    except json.decoder.JSONDecodeError as e:
        print("Error decoding JSON:", e)


def main():
    # Define client and Callbacks
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Configure the client username and password
    config = load_mqtt_config()
    client.username_pw_set(config['username'], config['password'])
    client.connect(config['broker_addr'], int(config['broker_port']), 60)

    client.loop_start()

    try:
        time.sleep(5)
        print(report_table)
        report_table.sortby = "RPI_ID"
        #print("Table sorted")
        #send_slack_msg(report_table)
        client.disconnect()
        client.loop_stop()

    except KeyboardInterrupt:
        print("\nDisconnecting from the broker ...")
        client.disconnect()


# Main script combining all the components
if __name__ == '__main__':
    main()
