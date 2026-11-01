import json, os, time, datetime, boto3

ddb = boto3.resource("dynamodb")
seats = ddb.Table(os.environ["SEATS_TABLE"])
orders = ddb.Table(os.environ["ORDERS_TABLE"])
events = ddb.Table(os.environ["EVENTS_TABLE"])
lam = boto3.client("lambda")


class TransientError(Exception):
    pass


def log(txn, step, msg):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    events.put_item(Item={"txnId": txn, "ts": now, "flow": "SAGA", "step": step, "message": msg})


def handler(event, context):
    """Saga step 2 - PAY. The payment id is derived from the order key, so a retry or a
    double click can never create a second charge."""
    ctx = event["ctx"]
    key, mode = ctx["orderKey"], ctx["mode"]
    pid = "pay-" + key
    seat = {"eventId": ctx["eventId"], "seatId": ctx["seatId"]}
    now = int(time.time())

    if mode == "HOLD_EXPIRES_DURING_PAYMENT":  # the payment page was slow: the hold ran out meanwhile
        seats.update_item(Key=seat, UpdateExpression="SET holdExpiresAt = :p",
                          ExpressionAttributeValues={":p": now - 1})
        log(key, "HOLD_EXPIRED", "payment took too long: the seat hold ran out while paying")

    try:  # "Rate Exceeded" must become a retryable error, not a dead-letter message
        r = lam.invoke(FunctionName=os.environ["GATEWAY_FN"],
                       Payload=json.dumps({"paymentId": pid, "txnId": key, "amount": 500, "mode": mode}))
    except Exception as e:
        raise TransientError("gateway not reachable: %s" % e)
    body = json.loads(r["Payload"].read())
    if r.get("FunctionError"):
        raise TransientError("gateway error: %s" % body.get("errorMessage"))

    status = {"SUCCESS": "PAID", "DECLINED": "DECLINED", "TIMEOUT": "PAYMENT_UNKNOWN"}[body["status"]]
    orders.update_item(Key={"orderId": key}, UpdateExpression="SET #s = :s, paymentId = :p, updatedAt = :t",
                       ExpressionAttributeNames={"#s": "status"},
                       ExpressionAttributeValues={":s": status, ":p": pid, ":t": now})
    log(key, "PAY", "gateway says %s -> order %s" % (body["status"], status))
    out = {**ctx, "paymentId": pid, "payStatus": body["status"]}
    if body["status"] == "TIMEOUT":  # keep the seat while the reconciler finds out the truth
        seats.update_item(Key=seat, UpdateExpression="SET holdExpiresAt = :p",
                          ExpressionAttributeValues={":p": now + 600})
        log(key, "PENDING", "no reply from the gateway: order parked for the reconciler, hold extended")
        out["stage"] = "PENDING_RECONCILE"
    return out
