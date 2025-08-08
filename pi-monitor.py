from zoneinfo import ZoneInfo
import paho.mqtt.client as mqtt
import time
import requests
import json
from datetime import datetime, timezone
from prettytable import PrettyTable
import argparse
import firebase

# Topic expression using a single wildcard
topic = "Schmidt/+/report/status"

reports = list()
seen = set()
rpi_ids = dict()
TABLE_FIELD_NAMES = ["RPI-ID", "MAC", "ETH", "WIFI", "LAST REPORT", "ATTN"]


def format_minutes_to_human_readable(total_minutes: int) -> str:
    """
    Converts a given number of minutes into a human-readable string
    using the largest appropriate unit (minutes, hours, days, or months).

    Args:
        total_minutes (int): The total number of minutes.

    Returns:
        str: A formatted string representing the duration.
             Returns "0 minutes" if total_minutes is 0.
             Returns an empty string if total_minutes is negative.
    """
    if total_minutes <= 0:
        return "0 mins"

    # Define conversion constants
    MINUTES_IN_HOUR = 60
    MINUTES_IN_DAY = MINUTES_IN_HOUR * 24
    MINUTES_IN_WEEK = MINUTES_IN_DAY * 7
    # Approximating a month as 30 days for simplicity
    MINUTES_IN_MONTH = MINUTES_IN_DAY * 30

    # Prioritize larger units
    if total_minutes >= MINUTES_IN_MONTH:
        # Calculate months and remaining minutes
        months = total_minutes // MINUTES_IN_MONTH
        # For simplicity, we'll just show months here.
        return f"{months} Mth{'s' if months != 1 else ''}"
    elif total_minutes >= MINUTES_IN_WEEK:
        days = total_minutes // MINUTES_IN_WEEK
        return f"{days} wk{'s' if days != 1 else ''}"
    elif total_minutes >= MINUTES_IN_DAY:
        days = total_minutes // MINUTES_IN_DAY
        return f"{days} day{'s' if days != 1 else ''}"
    elif total_minutes >= MINUTES_IN_HOUR:
        hours = total_minutes // MINUTES_IN_HOUR
        return f"{hours} hr{'s' if hours != 1 else ''}"
    else:
        # If less than an hour, show in minutes
        return f"{total_minutes} min{'s' if total_minutes != 1 else ''}"


# Create a report table from list of dict
def create_report_table(input_list):
    table = PrettyTable()
    table.field_names = TABLE_FIELD_NAMES
    table.add_rows([row.values() for row in input_list])
    return table


def load_mqtt_config():
    with open('.mqtt-config.json', 'r') as file:
        conf = json.load(file)
    return conf


def load_slack_config():
    with open('.slack-config.json', 'r') as file:
        cf = json.load(file)
    return cf


# Send a string
def send_slack_msg_str(input_str):
    payload = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": input_str
                }
            }
        ]
    }
    slack_conf = load_slack_config()
    requests.post(slack_conf['url'], json=payload)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("------------Connected successfully, please wait for the "
              "results -------------")
    client.subscribe(topic, qos=1)


# The Callback function to execute whenever messages are received
def on_message(client, userdata, msg):
    global reports, seen, rpi_ids
    try:
        pi_mac = msg.topic.split("/")[1]
        rpi_id = rpi_ids[pi_mac] if pi_mac in rpi_ids else None
        if (rpi_id is not None and rpi_id.startswith("RPI-")):
            report = dict()
            # get the published msg, extract the timestamp and calculate age
            retained_msg = json.loads(msg.payload.decode())
            current_time = datetime.now(timezone.utc)
            last_msg_time = datetime.fromisoformat(
                retained_msg["timestamp"]).astimezone(ZoneInfo('UTC'))
            age = round((current_time - last_msg_time).total_seconds() / 60)

            # Get the Ethernet and Wi-Fi status
            out_data = retained_msg['out']
            interfaces = out_data['ifaces']

            default_iface = {
                'up': None,
                'ip_address': None,
                'mac_address': None
            }
            eth0_data = next(
                (iface for iface in interfaces if iface["name"] == "eth0"),
                default_iface)
            wlan0_data = next(
                (iface for iface in interfaces if iface["name"] == "wlan0"),
                default_iface)
            wlan1_data = next(
                (iface for iface in interfaces if iface["name"] == "wlan1"),
                default_iface)
            is_eth_up = (eth0_data['up'] and eth0_data['ip_address'])
            is_wlan_up = (
                (wlan0_data['up'] and wlan0_data['ip_address'])
                or (wlan1_data['up'] and wlan1_data['ip_address']))

            report["RPI-ID"] = rpi_id
            report["MAC"] = retained_msg["mac"]  # eth0 MAC
            report["ETH_Status"] = "UP" if is_eth_up else "DOWN"
            report["WiFi_Status"] = "UP" if is_wlan_up else "DOWN"

            # Determine whether attention is required, considering
            # age of report, ethernet or Wi-Fi status
            if age > 20160:
                # Ignore if RPI age is more than 2 weeks
                attention_needed = "IGNR"
            elif age > 120:
                attention_needed = "YES"
            elif age < 120 and report["WiFi_Status"] == "DOWN":
                attention_needed = 'MAYBE'
            elif age < 120 and report["ETH_Status"] == "DOWN":
                attention_needed = 'MAYBE'
            else:
                attention_needed = "NO"

            # Format the last report time
            report["LAST REPORT"] = format_minutes_to_human_readable(age)

            # Add the attention column (make it the last column)
            report["Attention"] = attention_needed

            # Add only unique rows to reports
            if report["RPI-ID"] not in seen:
                seen.add(report["RPI-ID"])
                reports.append(report)

    except json.decoder.JSONDecodeError as e:
        print("Error decoding JSON:", e)


def main(experimental=False, timeout_sec=10, include_ignored=False):
    global reports, rpi_ids
    rpi_ids = firebase.get_rpi_ids()

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
        # Wait for reports to be populated
        time.sleep(timeout_sec)

        # Filter out ignored rows
        if not include_ignored:
            reports = [row for row in reports if row["Attention"] != "IGNR"]

        # Print/send table to slack
        report_table = create_report_table(reports)
        report_table.sortby = "RPI-ID"
        print(report_table)
        if not experimental:
            print("SENDING REPORT TABLE TO SLACK CHANNEL ...")
            # Split data due to Slack 3000-characters limit
            table_size = len(report_table.rows)
            start_idx = 0
            for i in range(1, table_size + 1):
                temp_table = report_table[start_idx:i]
                if (i == table_size
                        or len(temp_table.get_string()) > 2800):
                    start_idx = i
                    send_slack_msg_str(
                        f"```{temp_table.get_string()}```")

        # Generate the attention table (list of devices needing  attention)
        attn_table = create_report_table(
            [row for row in reports if row["Attention"] == "YES"])
        attn_table.sortby = "RPI-ID"
        if (len(attn_table.rows) > 0):
            print(f"Attention table:\n{attn_table}")
            if not experimental:
                print("SENDING ATTN TABLE TO SLACK CHANNEL ...")
                send_slack_msg_str(
                    "<@U048TQS3XUK> <@U05QKN65PEY>: The following RPIs need "
                    "attention.")
                # Split data due to Slack 3000-characters limit
                table_size = len(attn_table.rows)
                start_idx = 0
                for i in range(1, table_size + 1):
                    temp_table = attn_table[start_idx:i]
                    if (i == table_size
                            or len(temp_table.get_string()) > 2800):
                        start_idx = i
                        send_slack_msg_str(
                            f"```{temp_table.get_string()}```")

        else:
            print("ALL GOOD! No node needs attention right now.")
            if not experimental:
                send_slack_msg_str(
                    "ALL GOOD! No node needs attention right now.")

    except KeyboardInterrupt:
        print("\nKeyboard Interrupt !")

    finally:
        print("Disconnecting from the broker ...")
        client.disconnect()
        client.loop_stop()


# Main script combining all the components
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--experimental", action="store_true",
                        help="Enable experimental mode")
    parser.add_argument("--include-ignored", action="store_true",
                        help="Include ignored RPIs")
    parser.add_argument("--timeout", type=int, default=10,
                        help=("Timeout to wait for reports in seconds, "
                              "default=10s"))
    args = parser.parse_args()
    main(args.experimental, args.timeout, args.include_ignored)
