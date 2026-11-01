import json, os, time, datetime, boto3

ddb = boto3.resource("dynamodb")
orders = ddb.Table(os.environ["ORDERS_TABLE"])
events = ddb.Table(os.environ["EVENTS_TABLE"])
lam = boto3.client("lambda")


class RefundFailed(Exception):
    pass


def log(txn, step, msg):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    events.put_item(Item={"txnId": txn, "ts": now, "flow": "SAGA", "step": step, "message": msg})


def handler(event, context):
    """Compensation - REFUND the captured payment because the booking could not be completed."""
    ctx = event["ctx"]
    key = ctx["orderKey"]
    try:  # "Rate Exceeded" is a refund failure the workflow retries, not a dead-letter message
        r = lam.invoke(FunctionName=os.environ["GATEWAY_FN"],
                       Payload=json.dumps({"action": "refund", "paymentId": ctx["paymentId"],
                                           "failRefund": ctx["mode"] == "BOOKING_AND_REFUND_FAIL"}))
    except Exception as e:
        raise RefundFailed("gateway not reachable: %s" % e)
    body = json.loads(r["Payload"].read())
    if r.get("FunctionError"):
        log(key, "REFUND_FAILED", "refund call failed: %s" % body.get("errorMessage"))
        raise RefundFailed(str(body.get("errorMessage")))
    orders.update_item(Key={"orderId": key}, UpdateExpression="SET #s = :s, updatedAt = :t",
                       ExpressionAttributeNames={"#s": "status"},
                       ExpressionAttributeValues={":s": "REFUNDED", ":t": int(time.time())})
    log(key, "REFUNDED", "payment refunded automatically (%s)" % body["status"])
    return {**ctx, "refundStatus": body["status"]}
