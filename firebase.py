import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

cred = credentials.Certificate(
    "nd-schmidt-firebase-adminsdk-d1gei-43db929d8a.json")
firebase_admin.initialize_app(cred, {
    "databaseURL": "https://nd-schmidt-default-rtdb.firebaseio.com"
})


def get_rpi_ids():
    """
    Retrieve a dict with MAC: RPI-ID pairs.

    Returns:
        A dict containing pairs of MAC key and RPI-ID value.
    """
    query = db.reference("config").get()
    values = list(query.values())
    db_keys = list(query.keys())
    rpi_ids = dict()
    if (len(values) > 0 and len(db_keys) > 0):
        rpi_ids = {val["mac"].replace(":", "-"): val["rpi_id"]
                   for val in values
                   if "rpi_id" in val and val["rpi_id"]}
    return rpi_ids


def get_mac_from_rpi_id(rpi_id):
    """
    Retrieve MAC based on RPI-ID from Firebase DB.

    Args:
        rpi_id (string): RPI-ID.

    Returns:
        MAC as string or None if not found
    """
    query = db.reference("config").order_by_child("rpi_id").equal_to(
        rpi_id).get()
    values = list(query.values())
    db_keys = list(query.keys())
    mac = None
    if (len(values) > 0 and len(db_keys) > 0):
        val = values[0]
        if "mac" in val:
            mac = val["mac"].replace(":", "-")
    return mac


def get_rpi_id_from_mac(mac):
    """
    Retrieve RPI-ID based on MAC from Firebase DB.

    Args:
        mac (string): MAC.

    Returns:
        RPI-ID as string or None if not found
    """
    mac = mac.replace("-", ":")
    query = db.reference("config").order_by_child("mac").equal_to(
        mac).get()
    values = list(query.values())
    db_keys = list(query.keys())
    rpi_id = None
    if (len(values) > 0 and len(db_keys) > 0):
        val = values[0]
        if "rpi_id" in val and val["rpi_id"]:
            rpi_id = val["rpi_id"]
    return rpi_id
