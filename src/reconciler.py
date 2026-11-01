import json, os, time, datetime, boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr

ddb = boto3.resource("dynamodb")
seats = ddb.Table(os.environ["SEATS_TABLE"])
orders = ddb.Table(os.environ["ORDERS_TABLE"])
bookings = ddb.Table(os.environ["BOOKINGS_TABLE"])
events = ddb.Table(os.environ["EVENTS_TABLE"])
lam = boto3.client("lambda")
sqs = boto3.client("sqs")
MAX_ATTEMPTS = 5


def log(txn, step, msg):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    events.put_item(Item={"txnId": txn, "ts": now, "flow": "RECONCILER", "step": step, "message": msg})


def gateway(body):
    r = lam.invoke(FunctionName=os.environ["GATEWAY_FN"], Payload=json.dumps(body))
    out = json.loads(r["Payload"].read())
    if r.get("FunctionError"):
        raise RuntimeError(out.get("errorMessage"))
    return out


def close(o, status, now, note):
    orders.update_item(Key={"orderId": o["orderId"]},
                       UpdateExpression="SET #s = :s, updatedAt = :t, resolveSecs = :r",
                       ExpressionAttributeNames={"#s": "status"},
                       ExpressionAttributeValues={":s": status, ":t": now, ":r": now - int(o["updatedAt"])})
    log(o["orderId"], status, note)


def put(table, **kw):
    try:
        table.put_item(**kw)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return False


def take_seat(o, now):
    return put(seats, Item={"eventId": o["eventId"], "seatId": o["seatId"], "txnId": o["orderId"], "status": "BOOKED"},
               ConditionExpression="attribute_not_exists(seatId) OR txnId = :me OR "
                                   "(attribute_exists(holdExpiresAt) AND holdExpiresAt < :now)",
               ExpressionAttributeValues={":me": o["orderId"], ":now": now})


def resolve(o, now):
    pid = o.get("paymentId") or "pay-" + o["orderId"]
    st = gateway({"action": "status", "paymentId": pid})["status"]
    if st == "CAPTURED":
        if take_seat(o, now):
            put(bookings, Item={"bookingId": "bk-" + o["orderId"], "txnId": o["orderId"],
                                "paymentId": pid, "seatId": o["seatId"]},
                ConditionExpression="attribute_not_exists(bookingId)")
            close(o, "CONFIRMED", now, "money was taken: seat booked after the fact")
        else:
            gateway({"action": "refund", "paymentId": pid})
            close(o, "REFUNDED", now, "money taken but seat gone: refunded")
    elif st == "PENDING":
        return 0  # bank not settled yet
    elif st in ("DECLINED", "NOT_FOUND"):
        close(o, "RELEASED", now, "no money was taken: order released")
    elif st == "REFUNDED":
        close(o, "REFUNDED", now, "already refunded")


def handler(event, context):
    now = int(time.time())
    k = event.get("orderId")  # set by the recheck workflow
    pending = [orders.get_item(Key={"orderId": k}).get("Item")] if k else \
        orders.scan(FilterExpression=Attr("status").eq("PAYMENT_UNKNOWN"))["Items"]
    pending = [o for o in pending if o and o["status"] == "PAYMENT_UNKNOWN"]
    done = 0
    for o in pending:
        try:
            done += resolve(o, now) != 0
        except Exception as e:  # after MAX_ATTEMPTS ask a human
            n = int(o.get("attempts", 0)) + 1
            stop = n >= MAX_ATTEMPTS
            orders.update_item(Key={"orderId": o["orderId"]}, UpdateExpression="SET attempts = :n, #s = :s",
                               ExpressionAttributeNames={"#s": "status"},
                               ExpressionAttributeValues={":n": n, ":s": "NEEDS_REVIEW" if stop else o["status"]})
            log(o["orderId"], "RETRY", "attempt %d failed: %s" % (n, e))
            if stop:
                sqs.send_message(QueueUrl=os.environ["DLQ_URL"], MessageBody=json.dumps(o, default=str))
    return {"checked": len(pending), "resolved": done, "open": len(pending) - done}
