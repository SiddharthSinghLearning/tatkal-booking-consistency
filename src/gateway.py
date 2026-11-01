import json, os, time, zlib, boto3
from botocore.exceptions import ClientError

pay = boto3.resource("dynamodb").Table(os.environ["PAYMENTS_TABLE"])
bus = boto3.client("events")


def handler(event, context):
    """Mock payment gateway. action = charge (default) | status | refund."""
    action = event.get("action", "charge")
    pid = event["paymentId"]
    old = pay.get_item(Key={"paymentId": pid}).get("Item")

    if action == "status":
        if not old:
            return {"paymentId": pid, "status": "NOT_FOUND"}
        pay.update_item(Key={"paymentId": pid}, UpdateExpression="ADD statusChecks :one",
                        ExpressionAttributeValues={":one": 1})  # counts every call to the bank
        if time.time() < int(old.get("settleAt", 0)):  # a real bank needs a few seconds to settle
            return {"paymentId": pid, "status": "PENDING"}
        return {"paymentId": pid, "status": old["status"]}

    if action == "refund":
        if event.get("failRefund"):
            raise RuntimeError("refund endpoint unavailable")
        if not old or old["status"] != "CAPTURED":
            return {"paymentId": pid, "status": old["status"] if old else "NOT_FOUND"}
        pay.update_item(
            Key={"paymentId": pid},
            UpdateExpression="SET #s = :r",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":r": "REFUNDED"},
        )
        # EventBridge PutEvents is a management event, so CloudTrail records every refund.
        bus.put_events(Entries=[{
            "Source": "tatkal.gateway", "DetailType": "RefundIssued",
            "Detail": json.dumps({"paymentId": pid, "txnId": old["txnId"], "amount": int(old["amount"])}),
        }])
        return {"paymentId": pid, "status": "REFUNDED"}

    mode = event.get("mode", "SUCCESS")
    if old:  # idempotent replay: the same paymentId is never charged twice
        s = "SUCCESS" if old["status"] == "CAPTURED" else old["status"]
        return {"paymentId": pid, "status": s, "replayed": True}

    status = "DECLINED" if mode == "DECLINE" else "CAPTURED"
    now = int(time.time())
    settle = now + 2 + zlib.crc32(pid.encode()) % 29 if mode == "TIMEOUT_BUT_CHARGED" else 0  # 2-30 s
    try:
        pay.put_item(
            Item={
                "paymentId": pid,
                "txnId": event["txnId"],
                "amount": int(event.get("amount", 500)),
                "status": status,
                "mode": mode,
                "createdAt": now,
                "settleAt": settle,
            },
            ConditionExpression="attribute_not_exists(paymentId)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return {"paymentId": pid, "status": "SUCCESS", "replayed": True}

    if mode == "TIMEOUT_BUT_CHARGED":
        return {"paymentId": pid, "status": "TIMEOUT"}  # money taken, caller never hears back
    return {"paymentId": pid, "status": "SUCCESS" if status == "CAPTURED" else "DECLINED"}
