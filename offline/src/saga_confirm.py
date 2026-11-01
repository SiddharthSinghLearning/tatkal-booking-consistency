import os, time, datetime, boto3
from botocore.exceptions import ClientError

ddb = boto3.resource("dynamodb")
seats = ddb.Table(os.environ["SEATS_TABLE"])
bookings = ddb.Table(os.environ["BOOKINGS_TABLE"])
orders = ddb.Table(os.environ["ORDERS_TABLE"])
events = ddb.Table(os.environ["EVENTS_TABLE"])


class BookingFailed(Exception):
    pass


def log(txn, step, msg):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    events.put_item(Item={"txnId": txn, "ts": now, "flow": "SAGA", "step": step, "message": msg})


def handler(event, context):
    """Saga step 3 - CONFIRM. Turns the hold into a booking. Failing here triggers the refund branch."""
    ctx = event["ctx"]
    key = ctx["orderKey"]
    now = int(time.time())
    if ctx["mode"] in ("BOOKING_FAILS_AFTER_PAY", "BOOKING_AND_REFUND_FAIL"):
        log(key, "CONFIRM_FAILED", "booking service error after payment: compensation starts")
        raise BookingFailed("booking service error")
    try:
        seats.update_item(
            Key={"eventId": ctx["eventId"], "seatId": ctx["seatId"]},
            UpdateExpression="SET #s = :b REMOVE holdExpiresAt",
            ConditionExpression="txnId = :me AND (holdExpiresAt >= :now OR #s = :b)",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":b": "BOOKED", ":me": key, ":now": now},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        log(key, "CONFIRM_FAILED", "the hold expired before confirmation: compensation starts")
        raise BookingFailed("hold expired")
    try:
        bookings.put_item(
            Item={"bookingId": "bk-" + key, "txnId": key, "paymentId": ctx["paymentId"], "seatId": ctx["seatId"]},
            ConditionExpression="attribute_not_exists(bookingId)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise  # already booked by an earlier attempt: fine, confirm is idempotent
    orders.update_item(Key={"orderId": key}, UpdateExpression="SET #s = :s, updatedAt = :t",
                       ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":s": "CONFIRMED", ":t": now})
    log(key, "BOOKED", "seat confirmed, ticket issued")
    return {**ctx, "stage": "BOOKED"}
