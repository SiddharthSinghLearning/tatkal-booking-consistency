import json, os, random, time, uuid, boto3
from concurrent.futures import ThreadPoolExecutor

lam = boto3.client("lambda")
sfn = boto3.client("stepfunctions")
FAILS = ["TIMEOUT_BUT_CHARGED", "BOOKING_FAILS_AFTER_PAY", "DUPLICATE_CLICK", "HOLD_EXPIRES_DURING_PAYMENT"]


def call(fn, payload):
    for i in range(6):  # new accounts have low concurrency limits: back off and retry on throttling
        try:
            r = lam.invoke(FunctionName=fn, Payload=json.dumps(payload))
            return json.loads(r["Payload"].read())
        except lam.exceptions.TooManyRequestsException:
            time.sleep(0.5 * 2 ** i)
    raise RuntimeError("throttled")


def run_naive(i, mode):
    call(os.environ["NAIVE_FN"], {"txnId": "txn-b%03d" % i, "eventId": "NAIVE-SHOW", "seatId": "B%03d" % i, "mode": mode})


def saga_once(inp):
    for k in range(6):
        r = sfn.start_sync_execution(stateMachineArn=os.environ["EXPRESS_SM"], input=json.dumps(inp))
        if r["status"] != "FAILED" or r.get("error") == "NeedsManualReview":
            return r
        time.sleep(0.5 * 2 ** k)


def run_saga(i, mode):
    inp = {"orderKey": "sg-b%03d" % i, "eventId": "SAGA-SHOW", "seatId": "B%03d" % i, "mode": mode}
    with ThreadPoolExecutor(2 if mode == "DUPLICATE_CLICK" else 1) as ex:  # a double click = two executions
        list(ex.map(lambda _: saga_once(inp), range(2 if mode == "DUPLICATE_CLICK" else 1)))


def handler(event, context):
    """Runs the SAME N scenarios through the naive flow and then the saga, and returns both ledgers."""
    via_url = "requestContext" in event
    if via_url:
        if event.get("rawPath", "/") != "/":
            return {"statusCode": 404, "body": ""}
        event = event.get("queryStringParameters") or {}
    n = max(1, min(int(event.get("n", 200)), 300))
    pct = max(0, min(int(event.get("failPct", 30)), 100))
    rnd = random.Random(42)  # fixed seed: both flows and every rerun get identical scenarios
    modes = []
    for _ in range(n):
        x = rnd.random() * 100
        modes.append(rnd.choice(FAILS) if x < pct else ("DECLINE" if x < pct + 5 else "SUCCESS"))
    if str(event.get("reset", "1")) != "0":
        call(os.environ["LEDGER_FN"], {"action": "reset"})
    t0 = time.time()
    with ThreadPoolExecutor(3) as ex:
        list(ex.map(lambda a: run_naive(*a), enumerate(modes)))
    t1 = time.time()
    with ThreadPoolExecutor(3) as ex:
        list(ex.map(lambda a: run_saga(*a), enumerate(modes)))
    t2 = time.time()
    out = {"n": n, "failPct": pct,
           "scenarios": {m: modes.count(m) for m in sorted(set(modes))},
           "naive": call(os.environ["LEDGER_FN"], {"flow": "NAIVE"}),
           "saga": call(os.environ["LEDGER_FN"], {"flow": "SAGA"}),
           "seconds": {"naive": round(t1 - t0, 1), "saga": round(t2 - t1, 1)},
           "note": "Saga orders with pendingReconcile > 0 are payments whose result was unknown; "
                   "the reconciler (every minute) resolves them. Re-open the ledger a minute later."}
    if not via_url:
        return out
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(out)}
