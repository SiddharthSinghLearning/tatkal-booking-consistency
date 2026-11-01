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
    """Compensation - RELEASE the seat hold and close the order (final = DECLINED or REFUNDED)."""
    ctx, final = event["ctx"], event["final"]
    key = ctx["orderKey"]
    try:
        seats.delete_item(Key={"eventId": ctx["eventId"], "seatId": ctx["seatId"]},
                          ConditionExpression="txnId = :me", ExpressionAttributeValues={":me": key})
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise  # the seat is no longer ours: nothing to release
    orders.update_item(Key={"orderId": key}, UpdateExpression="SET #s = :s, updatedAt = :t",
                       ExpressionAttributeNames={"#s": "status"},
                       ExpressionAttributeValues={":s": final, ":t": int(time.time())})
    log(key, "RELEASED", "seat hold released, order closed as %s" % final)
    return {**ctx, "stage": final}
