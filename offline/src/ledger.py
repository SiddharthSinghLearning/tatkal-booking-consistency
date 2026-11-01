import json, os, time, boto3
from boto3.dynamodb.conditions import Key

ddb = boto3.resource("dynamodb")
T = {n: ddb.Table(os.environ[n.upper() + "_TABLE"]) for n in ("payments", "bookings", "seats", "events", "orders")}
KEYS = {"payments": ["paymentId"], "bookings": ["bookingId"], "seats": ["eventId", "seatId"],
        "events": ["txnId", "ts"], "orders": ["orderId"]}
PREFIX = {"NAIVE": "txn-", "SAGA": "sg-"}


def scan(t):
    out, kw = [], {}
    while True:
        r = t.scan(**kw)
        out += r["Items"]
        if "LastEvaluatedKey" not in r:
            return out
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]


def totals(flow):
    keep = lambda txn: flow == "ALL" or txn.startswith(PREFIX[flow])
    pays = [p for p in scan(T["payments"]) if keep(p["txnId"])]
    books = [b for b in scan(T["bookings"]) if keep(b["txnId"])]
    ords = [o for o in scan(T["orders"]) if keep(o["orderId"])]
    captured = {}
    for p in pays:
        if p["status"] == "CAPTURED":
            captured.setdefault(p["txnId"], []).append(p)
    booked = {b["txnId"] for b in books}
    secs = [float(o["resolveSecs"]) for o in ords if "resolveSecs" in o]
    out = {
        "flow": flow,
        "payments": len(pays),
        "debitedRupees": sum(int(p["amount"]) for ps in captured.values() for p in ps),
        "ticketsIssued": len(books),
        "orphanedPayments": sum(1 for t in captured if t not in booked),
        "duplicateCharges": sum(len(ps) - 1 for ps in captured.values()),
        "refunded": sum(1 for p in pays if p["status"] == "REFUNDED"),
        "pendingReconcile": sum(1 for o in ords if o["status"] == "PAYMENT_UNKNOWN"),
        "needsReview": sum(1 for o in ords if o["status"] == "NEEDS_REVIEW"),
        "avgResolveSecs": round(sum(secs) / len(secs), 1) if secs else None,
        "maxResolveSecs": max(secs) if secs else None,
        "bankStatusCalls": sum(int(p.get("statusChecks", 0)) for p in pays),
        "paymentsChecked": sum(1 for p in pays if p.get("statusChecks")),
        "reconcilerMode": os.environ.get("RECON_MODE", "SCAN"),
    }
    if flow in PREFIX:  # CloudWatch Embedded Metric Format: becomes dashboard metrics for free
        m = ("OrphanedPayments", "DuplicateCharges", "PendingReconcile")
        print(json.dumps({"_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": [
            {"Namespace": "Tatkal", "Dimensions": [["Flow"]], "Metrics": [{"Name": n, "Unit": "Count"} for n in m]}]},
            "Flow": flow, "OrphanedPayments": out["orphanedPayments"],
            "DuplicateCharges": out["duplicateCharges"], "PendingReconcile": out["pendingReconcile"]}))
    return out


def handler(event, context):
    """{} / ?flow=NAIVE|SAGA -> totals. ?events=<txnId> -> step timeline.
    {"action":"reset"} -> empty all tables (console tests and the batch runner only, never the public URL)."""
    via_url = "requestContext" in event
    if via_url:
        if event.get("rawPath", "/") != "/":
            return {"statusCode": 404, "body": ""}
        event = event.get("queryStringParameters") or {}
    if not via_url and event.get("action") == "reset":
        for name, keys in KEYS.items():
            with T[name].batch_writer() as b:
                for it in scan(T[name]):
                    b.delete_item(Key={k: it[k] for k in keys})
        return {"reset": True}
    if event.get("events"):
        rows = T["events"].query(KeyConditionExpression=Key("txnId").eq(event["events"]))["Items"]
        out = [{k: r.get(k) for k in ("ts", "flow", "step", "message")} for r in rows]
    else:
        out = totals(str(event.get("flow", "ALL")).upper())
    if not via_url:
        return out
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(out)}
