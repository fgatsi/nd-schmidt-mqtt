
import paho.mqtt.client as mqtt
import time
import datetime
import requests
import os
import json
from datetime import datetime
from prettytable import PrettyTable


# Topic expression using a single wildcard
topic = "Schmidt/+/report/status"

# Initialize a report dictionary
report = {}

# Initialize a pretty_table object
test_table = PrettyTable()
test_table.field_names = ["RPI-ID", "MAC", "ETH_STATUS", "WIFI_STATUS", "LAST_REPORT", "ATTN"]


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("------------Connected successfully, please wait for the results -------------")
    client.subscribe(topic, qos=1)


# The Callback function to execute whenever messages are received
def on_message(client, userdata, msg):
    global report
    try:
        retained_msg = json.loads(msg.payload.decode())
        last_msg_time = datetime.strptime(retained_msg["Timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        current_time = datetime.now()
        age = current_time - last_msg_time
        age = int(age.total_seconds() / 60.0)

        # Testing
        if retained_msg["SSID"] == 'NONE':
            wifi_status = "DOWN"
        else:
            wifi_status = "UP"

        if retained_msg["Ethernet_Status"] == '1':
            eth_status = "UP"
        else:
            eth_status = "DOWN"

        if age > 120:
            attention_needed = "YES"
            eth_status = 'UNKNOWN'
            wifi_status = 'UNKNOWN'
        elif age < 120 and wifi_status == "DOWN":                         
            attention_needed = 'MAYBE'
        else:
            attention_needed = "NO"

        # Generate the row  corresponding to this raspberry pi
        report["RPI_ID"] = retained_msg["RPI_ID"]
        report["MAC"] = retained_msg["MAC_eth0"]
        report["ETH_Status"] = eth_status
        report["WiFi_Status"] = wifi_status
        if age < 2:
            report["Last_Report"] = f"{age} min ago"
        elif age < 60:
            report["Last_Report"] = f"{age} mins ago"
        else: 
            h = age//60
            m = age%60
            report["Last_Report"] = f"{h}:{m} hrs ago"

        report["Attention"] = attention_needed

        # Add row to table
        update_table(report)

    except json.decoder.JSONDecodeError as e:
        print("Error decoding JSON:", e)


# This function add rows to the existing table
def update_table(message):
    global test_table
    report_values = list(message.values())
    test_table.add_row(report_values)

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
        attn = True   # At least one pi needs attention
    return attn

def attn_table(table, column_name):
    attn_table = PrettyTable()
    # Get the index of the column using the column name
    column_index = table.field_names.index(column_name)
    attn_table.field_names = table.field_names
    attn_table.title = "ATTENTION TABLE"
    table.header_style = "title"
    # Loop through old table rows and filter by 'attn' column
    for row in table._rows:
        if row[column_index] in ["MAYBE", "YES"]:
            attn_table.add_row(row)
    print(attn_table)
    return attn_table


def load_mqtt_config():
    with open('.mqtt-config.json', 'r') as file:
        conf = json.load(file)
    return conf


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


# This function takes table as input and converts it into string
def send_slack_msgtxt(input_table):
    table = input_table.get_string()
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{table}\n\nALL GOOD! No node needs attention right now```"
                }
            }
        ]
    }
    slack_conf = load_slack_config()
    requests.post(slack_conf['url'], json=payload)


def send_slack_txt():
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "ALL GOOD! No node needs attention right now"
                }
            }
        ]
    }
    slack_conf = load_slack_config()
    requests.post(slack_conf['url'], json=payload)


def main():
    # Define client and Callbacks
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    # Configure the client username and password
    config = load_mqtt_config()
    client.username_pw_set(config['username'], config['password'])
    client.connect(config['broker_address'], int(config['broker_port']), 60)

    client.loop_start()

    try:
        time.sleep(10)
        test = test_for_attention(test_table, "ATTN")
        if test:
            print(test_table)
            send_slack_msg(test_table)
            attn_tab = attn_table(test_table, "ATTN")      
            send_slack_msg(attn_tab)
        else:
            print(test_table)
            print("ALL GOOD! No node needs attention right now\n")
            send_slack_msgtxt(test_table)

        client.disconnect()
        client.loop_stop()

    except KeyboardInterrupt:
        print("\nDisconnecting from the broker ...")
        client.disconnect()


# Main script combining all the components
if __name__ == '__main__':
    main()
