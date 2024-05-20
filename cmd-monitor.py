import argparse
import json
from paho.mqtt import client as mqtt
from slack_bolt import App
import logging
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument("--experimental", action="store_true",
                    help="Enable experimental mode")
parser.add_argument("-l", "--log-level", default="debug",
                    help="Provide logging level, default is warning'")
args = parser.parse_args()
logging.basicConfig(level=args.log_level.upper())

command_string = "/pi"
client_id = "cmd-monitor"
if args.experimental:
    print("Running in experimental mode! Message reply will not be sent to "
          "the Slack channel.")
    command_string += "exp"
    client_id += "-exp"

with open('.slack-config.json', 'r') as file:
    slack_conf = json.load(file)
with open('.mqtt-config.json', 'r') as file:
    mqtt_conf = json.load(file)
with open('.rpi-config.json', 'r') as file:
    rpi_ids = json.load(file)

app = App(
    token=slack_conf["bot_token"],
    signing_secret=slack_conf["signing_secret"]
)

topic_report_conf = f"Schmidt/+/report/config"


def get_rpi_id(mac):
    logging.info("Get Pi ID for mac: %s", mac)
    rpi_id = [key for key, val in rpi_ids.items() if val == mac]
    logging.debug("Got rpi_id: %s", rpi_id)
    if len(rpi_id) == 0:
        logging.warning("mac %s not found in the list of Pi IDs", mac)
        return mac
    else:
        return rpi_id[0]


def get_mac(rpi_id):
    logging.info("Get mac for Pi ID: %s", rpi_id)
    if rpi_id not in rpi_ids:
        logging.warning("Pi ID %s not found in the list of Pi IDs", rpi_id)
        return rpi_id
    else:
        logging.debug("Got mac: %s", rpi_ids[rpi_id])
        return rpi_ids[rpi_id]


def create_markdown_block(text):
    return [{
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": text
        }
    }]


help_text = create_markdown_block(
    "`/pi ping RPI-ID texts`: ping the selected Pi, which will be replied "
    "with the same input texts.\n"
    "\n"
    "`/pi status RPI-ID [ssid|iface|up|ip|mac]`: get the status of selected "
    "Pi, can be refined by selecting a parameter.\n"
    "\n"
    "`/pi logs RPI-ID (mqtt|speedtest) [n]`: Get the log stored in the "
    "selected Pi, must specify either `mqtt` or `speedtest` log. "
    "Additionally, specify last `n` lines of the logs (default=20).\n"
    "\n"
    "`/pi gitreset RPI-ID (main|testing|experimental)`: Reset the "
    "`sigcap-buddy` repository in the selected Pi to the latest of the "
    "selected branch.\n"
    "\n"
    "`/pi restartsrv RPI-ID [mqtt|speedtest]`: Restart all services in the "
    "selected Pi, can specify which service to restart.\n"
    "\n"
    "`/pi update RPI-ID`: Run `pi-install.sh` script to update the selected "
    "Pi.\n"
    "\n"
    "`/pi reboot RPI-ID`: Reboot the selected Pi.")
help_text.insert(0, {
    "type": "header",
    "text": {
        "type": "plain_text",
        "text": "List of commands"
    }
})


def send_slack_attachment(rpi_id, content, filename, title):
    logging.info("Sending as %s, attachment filename: %s", rpi_id, filename)
    if args.experimental:
        # Quit without actually sending the message.
        return

    try:
        # Call the files.upload method using the WebClient
        # Uploading files requires the `files:write` scope
        result = app.client.files_upload(
            channels=["C06TNJBSB52"],
            content=content,
            filename=filename,
            filetype="text",
            title=title
        )
        logging.info(result)

    except Exception as e:
        logging.error(f"Error uploading file: {e}")


def send_slack_blocks(rpi_id, blocks):
    logging.info("Sending as %s, message: %s", rpi_id, blocks)
    if args.experimental:
        # Quit without actually sending the message.
        return

    try:
        # Call the chat.postMessage method using the WebClient
        result = app.client.chat_postMessage(
            channel="C06TNJBSB52",
            blocks=blocks,
            username=rpi_id
        )
        logging.info(result)

    except Exception as e:
        logging.error(f"Error posting message: {e}")


def print_indent(count):
    out_str = ""
    for i in range(count):
        out_str += "  "
    return out_str


def dict_to_string(dict_obj, indent=0):
    out_str = ""
    for key, value in dict_obj.items():
        if isinstance(value, list):
            count = 1
            for child in value:
                out_str += f"{print_indent(indent)}{key}-{count}:\n"
                count += 1
                out_str += dict_to_string(child, indent + 1)
        elif isinstance(value, dict):
            out_str += f"{print_indent(indent)}{key}:\n"
            out_str += dict_to_string(value, indent + 1)
        else:
            out_str += f"{print_indent(indent)}{key}: {value}\n"
    return out_str


@app.command(command_string)
def respond_cmd(ack, respond, command):
    logging.debug("Command: %s", command)
    ack()
    # Parse request body data
    splits = command["text"].split(" ", 2)
    cmd = splits[0]
    if cmd == "help":
        respond(blocks=help_text)
        return

    rpi_id = splits[1]
    extras = splits[2] if len(splits) > 2 else ""

    if rpi_id not in rpi_ids:
        logging.warning("Invalid Pi ID: %s", rpi_id)
        respond(f"Error: {rpi_id} is invalid!")
        return

    # Immediately reply to give acknowledgment
    respond(f"Sending {cmd} command to {rpi_id}...")

    if cmd == "ping":
        topic = f"Schmidt/{rpi_ids[rpi_id]}/config/{cmd}"
        logging.info("Publishing to topic %s, message %s", topic, extras)
        client.publish(topic, extras, qos=1)
    else:
        if extras:
            cmd = f"{cmd}/{extras.replace(' ', '/')}"
        topic = f"Schmidt/{rpi_ids[rpi_id]}/config/{cmd}"
        logging.info("Publishing to topic %s", topic)
        client.publish(topic, "", qos=1)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        # Subscribe for commands replies
        client.subscribe(topic_report_conf)
    else:
        logging.error(f"Connection failed with code {rc}")


def on_message(client, userdata, msg):
    topic = msg.topic
    msg_str = msg.payload.decode("utf-8")
    logging.info("Message received: %s, %s", topic, msg_str)

    # Try parse payload as JSON
    try:
        msg_payload = json.loads(msg_str)
    except Exception:
        logging.error("Cannot parse payload %s", msg_str)
        return

    # Check payload parameters
    for param in ["mac", "timestamp", "type", "result", "out", "err"]:
        if param not in msg_payload:
            logging.error("%s not in MQTT payload!", param)
            return

    # Check if payload is outdated
    span = abs(datetime.now(timezone.utc) - datetime.fromisoformat(
        msg_payload["timestamp"]))
    logging.debug("span: %d s", span.seconds)
    # Check if payload is posted more than 10 minutes ago
    if span.seconds > 600:
        logging.info("Discarding old payload with timestamp %s",
                     msg_payload["timestamp"])
        return

    rpi_id = get_rpi_id(msg_payload["mac"])
    match msg_payload["type"]:
        case "ping":
            if msg_payload["result"] != "success":
                text = f"```Ping error: {json.dumps(msg_payload['err'])}```"
            else:
                if ("pong" in msg_payload["out"]):
                    text = f"```Pong: {msg_payload['out']['pong']}```"
                else:
                    text = f"```Pong: <empty>```"
            slack_block = create_markdown_block(text)
            send_slack_blocks(rpi_id, slack_block)

        case msg_type if msg_type.startswith("status"):
            if msg_payload["result"] != "success":
                text = f"```Status error: {json.dumps(msg_payload['err'])}```"
            else:
                if msg_type.startswith("status/"):
                    splits = msg_type.split("/")
                    out_dict = {splits[1]: msg_payload['out']}
                else:
                    out_dict = msg_payload['out']
                text = f"```{dict_to_string(out_dict)}```"
            slack_block = create_markdown_block(text)
            send_slack_blocks(rpi_id, slack_block)

        case msg_type if msg_type.startswith("logs"):
            if msg_payload["result"] != "success":
                text = f"```Logs error: {json.dumps(msg_payload['err'])}```"
                slack_block = create_markdown_block(text)
                send_slack_blocks(rpi_id, slack_block)
            else:
                if msg_type.startswith("logs/"):
                    splits = msg_type.split("/")
                    filename = f"{rpi_id}_{splits[1]}.log"
                    title = f"{rpi_id} {splits[1]} log"
                else:
                    filename = f"{rpi_id}.log"
                    title = f"{rpi_id} log"
                content = msg_payload["out"]["log"]

                if len(content) > 3000:
                    send_slack_attachment(rpi_id, content, filename, title)
                else:
                    text = f"{title}\n```{content}```"
                    slack_block = create_markdown_block(text)
                    send_slack_blocks(rpi_id, slack_block)

        case msg_type if msg_type.startswith("gitreset"):
            if msg_payload["result"] != "success":
                text = ("```gitreset error: "
                        f"{json.dumps(msg_payload['err'])}```")
            else:
                branch_name = ""
                if msg_type.startswith("gitreset/"):
                    splits = msg_type.split("/")
                    branch_name = f"{splits[1]} "
                text = (f"```gitreset success: {branch_name}"
                        f"{msg_payload['out']['stdout']}```")
            slack_block = create_markdown_block(text)
            send_slack_blocks(rpi_id, slack_block)

        case msg_type if msg_type.startswith("restartsrv"):
            if msg_payload["result"] != "success":
                text = ("```Restart service error: "
                        f"{json.dumps(msg_payload['err'])}```")
            else:
                # returncode -> bool
                out_dict = {
                    "Restart service": {
                        key: "success" if value == 0 else "error"
                        for key, value in msg_payload[
                            'out']['returncode'].items()}
                }
                text = f"```{dict_to_string(out_dict)}```"
            slack_block = create_markdown_block(text)
            send_slack_blocks(rpi_id, slack_block)

        case "update":
            if msg_payload["result"] != "success":
                text = f"```Update error: {json.dumps(msg_payload['err'])}```"
            else:
                text = f"```Update success!```"
            slack_block = create_markdown_block(text)
            send_slack_blocks(rpi_id, slack_block)

        case "reboot":
            if msg_payload["result"] != "success":
                text = f"```Reboot error: {json.dumps(msg_payload['err'])}```"
            else:
                text = f"```Reboot success!```"
            slack_block = create_markdown_block(text)
            send_slack_blocks(rpi_id, slack_block)

        case _:
            logging.warning("Unknown mqtt command %s", msg_payload["type"])
            text = f"```Err: Unknown mqtt command {msg_payload['type']}```"
            slack_block = create_markdown_block(text)
            send_slack_blocks(rpi_id, slack_block)


if __name__ == '__main__':

    client = mqtt.Client(
        client_id=client_id,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set(mqtt_conf['username'], mqtt_conf['password'])
    client.connect(mqtt_conf['broker_addr'],
                   int(mqtt_conf['broker_port']),
                   60)
    client.loop_start()

    try:
        app.start(port=int(slack_conf["slack_port"]))

    except KeyboardInterrupt:
        print("\nDisconnecting from the broker ...")
        client.disconnect()
