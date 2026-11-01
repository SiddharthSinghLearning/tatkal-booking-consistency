import json, os, uuid, boto3
from concurrent.futures import ThreadPoolExecutor

sfn = boto3.client("stepfunctions")


def start(inp, standard):
    if standard:  # asynchronous: watch the execution graph in the Step Functions console
        r = sfn.start_execution(stateMachineArn=os.environ["STANDARD_SM"],
                                name=inp["orderKey"] + "-" + uuid.uuid4().hex[:6], input=json.dumps(inp))
        return {"status": "STARTED", "executionArn": r["executionArn"]}
    r = sfn.start_sync_execution(stateMachineArn=os.environ["EXPRESS_SM"], input=json.dumps(inp))
    out = json.loads(r["output"]) if r.get("output") else {}
    return {"status": r["status"], "stage": out.get("stage") or r.get("error", "FAILED")}


def handler(event, context):
    """Starts the saga. Works from console tests ({"mode": ...}) and from the function URL (?mode=...&std=1).
    DUPLICATE_CLICK starts the same order twice at the same moment, like a double click."""
    via_url = "requestContext" in event
    if via_url:
        if event.get("rawPath", "/") != "/":
            return {"statusCode": 404, "body": ""}
        event = event.get("queryStringParameters") or {}
    mode = event.get("mode", "SUCCESS")
    inp = {"orderKey": event.get("key") or "sg-" + uuid.uuid4().hex[:8],
           "eventId": event.get("eventId", "E1"),
           "seatId": event.get("seatId") or "S-" + uuid.uuid4().hex[:6], "mode": mode}
    standard = str(event.get("std", "")).lower() in ("1", "true")
    clicks = 2 if mode == "DUPLICATE_CLICK" else 1
    with ThreadPoolExecutor(clicks) as ex:
        res = list(ex.map(lambda _: start(inp, standard), range(clicks)))
    main = next((r for r in res if r.get("stage") != "DUPLICATE"), res[0])
    out = {"orderKey": inp["orderKey"], "outcome": main.get("stage") or main["status"], "executions": res}
    if not via_url:
        return out
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(out)}
