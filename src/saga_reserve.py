import os, time, datetime, boto3
from botocore.exceptions import ClientError

ddb = boto3.resource("dynamodb")
seats = ddb.Table(os.environ["SEATS_TABLE"])
orders = ddb.Table(os.environ["ORDERS_TABLE"])
events = ddb.Table(os.environ["EVENTS_TABLE"])


def log(txn, step, msg):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    events.put_item(Item={"txnId": txn, "ts": now, "flow": "SAGA", "step": step, "message": msg})


def handler(event, context):
    """Saga step 1 - RESERVE. Idempotent on orderKey; holds the seat with a TTL before any payment."""
    ctx = event["ctx"]
    key, eid, sid = ctx["orderKey"], ctx["eventId"], ctx["seatId"]
    now = int(time.time())
    try:
        orders.put_item(
            Item={"orderId": key, "status": "HELD", "eventId": eid, "seatId": sid,
                  "mode": ctx["mode"], "createdAt": now, "updatedAt": now, "attempts": 0},
            ConditionExpression="attribute_not_exists(orderId)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        old = orders.get_item(Key={"orderId": key}).get("Item", {})
        log(key, "DUPLICATE_IGNORED",
            "same idempotency key again (order is %s): no second hold, no second charge" % old.get("status"))
        return {**ctx, "stage": "DUPLICATE"}
    try:
        seats.put_item(
            Item={"eventId": eid, "seatId": sid, "txnId": key, "status": "HELD",
                  "holdExpiresAt": now + int(os.environ["HOLD_SECONDS"])},
            ConditionExpression="attribute_not_exists(seatId) OR (attribute_exists(holdExpiresAt) AND holdExpiresAt < :now)",
            ExpressionAttributeValues={":now": now},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        orders.update_item(Key={"orderId": key}, UpdateExpression="SET #s = :s, updatedAt = :t",
                           ExpressionAttributeNames={"#s": "status"},
                           ExpressionAttributeValues={":s": "SEAT_TAKEN", ":t": now})
        log(key, "SEAT_TAKEN", "seat already held or booked: nothing was charged")
        return {**ctx, "stage": "SEAT_TAKEN"}
    log(key, "RESERVED", "seat held for %ss before any payment (conditional write + TTL)" % os.environ["HOLD_SECONDS"])
    return {**ctx, "stage": "RESERVED"}
