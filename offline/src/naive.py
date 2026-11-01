import json, os, uuid, datetime, boto3
from botocore.exceptions import ClientError

ddb = boto3.resource("dynamodb")
seats = ddb.Table(os.environ["SEATS_TABLE"])
bookings = ddb.Table(os.environ["BOOKINGS_TABLE"])
events = ddb.Table(os.environ["EVENTS_TABLE"])
lam = boto3.client("lambda")


def log(txn, step, msg):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    events.put_item(Item={"txnId": txn, "ts": now, "flow": "NAIVE", "step": step, "message": msg})


def charge(txn, mode):
    pid = "pay-" + uuid.uuid4().hex[:10]  # naive: a brand-new payment id on every attempt
    body = {"paymentId": pid, "txnId": txn, "amount": 500, "mode": mode}
    r = lam.invoke(FunctionName=os.environ["GATEWAY_FN"], Payload=json.dumps(body))
    return json.loads(r["Payload"].read())


def handler(event, context):
    """Works from console tests ({"mode": ...}) and from the function URL (?mode=...)."""
    via_url = "requestContext" in event
    if via_url:
        if event.get("rawPath", "/") != "/":  # ignore /favicon.ico and other stray browser requests
            return {"statusCode": 404, "body": ""}
        event = event.get("queryStringParameters") or {}
    out = run(event)
    if not via_url:
        return out
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(out)}


def run(event):
    """NAIVE flow: charge first, then book. Nothing repairs a failure in between.
    mode = SUCCESS | DECLINE | TIMEOUT_BUT_CHARGED | BOOKING_FAILS_AFTER_PAY
           | DUPLICATE_CLICK | HOLD_EXPIRES_DURING_PAYMENT"""
    txn = event.get("txnId") or "txn-" + uuid.uuid4().hex[:8]
    eid = event.get("eventId", "E1")
    sid = event.get("seatId") or "S-" + uuid.uuid4().hex[:6]
    mode = event.get("mode", "SUCCESS")

    log(txn, "START", "naive flow, mode=%s: charge first, then book" % mode)
    clicks = 2 if mode == "DUPLICATE_CLICK" else 1
    pay = None
    for i in range(clicks):
        pay = charge(txn, mode)
        log(txn, "CHARGE", "click %d -> gateway says %s" % (i + 1, pay["status"]))

    if pay["status"] != "SUCCESS":
        log(txn, "END", "user is shown: payment failed, please retry")
        return {"txnId": txn, "outcome": "PAYMENT_FAILED_SHOWN", "gateway": pay["status"]}

    try:
        if mode in ("BOOKING_FAILS_AFTER_PAY", "BOOKING_AND_REFUND_FAIL"):
            raise RuntimeError("booking service error")
        if mode == "HOLD_EXPIRES_DURING_PAYMENT":  # someone else took the seat while we paid
            seats.put_item(Item={"eventId": eid, "seatId": sid, "txnId": "someone-else"})
        seats.put_item(
            Item={"eventId": eid, "seatId": sid, "txnId": txn},
            ConditionExpression="attribute_not_exists(seatId)",
        )
        bookings.put_item(
            Item={"bookingId": "bk-" + uuid.uuid4().hex[:8], "txnId": txn,
                  "paymentId": pay["paymentId"], "seatId": sid}
        )
        log(txn, "BOOKED", "ticket issued")
        return {"txnId": txn, "outcome": "BOOKED"}
    except (RuntimeError, ClientError) as e:
        log(txn, "BOOK_FAILED", "%s - money already taken, no refund, no retry" % e)
        return {"txnId": txn, "outcome": "PAID_NOT_BOOKED"}
