"""Tatkal Booking Consistency - OFFLINE emulation of the whole AWS pipeline on this computer.

What is real: the exact Lambda source files and the exact Step Functions definitions (src/) that run on AWS.
What is emulated: DynamoDB, SQS and EventBridge (moto, in memory), Lambda invocation, the Step Functions
interpreter (asl_sim.py), and the once-a-minute EventBridge schedule. No internet is used while it runs.

Usage:  python tatkal_offline.py                 start the local stack and the demo page
        python tatkal_offline.py --mode BACKOFF  use per-payment backoff timers instead of the 1-minute sweep
        python tatkal_offline.py --selftest      run the 200-booking batch and compare with the recorded AWS run
"""
import argparse, importlib.util, io, json, os, pathlib, sys, threading, time, webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = pathlib.Path(__file__).resolve().parent
SRC = HERE / "src"
sys.path.insert(0, str(HERE))

os.environ.update(AWS_DEFAULT_REGION="ap-southeast-2", AWS_ACCESS_KEY_ID="offline", AWS_SECRET_ACCESS_KEY="offline",
                  PAYMENTS_TABLE="tatkal-payments", BOOKINGS_TABLE="tatkal-bookings", SEATS_TABLE="tatkal-seats",
                  EVENTS_TABLE="tatkal-events", ORDERS_TABLE="tatkal-orders", GATEWAY_FN="tatkal-gateway",
                  HOLD_SECONDS="120", NAIVE_FN="tatkal-naive", LEDGER_FN="tatkal-ledger",
                  EXPRESS_SM="tatkal-saga-express", STANDARD_SM="tatkal-saga")
for k in ("AWS_ENDPOINT_URL", "AWS_PROFILE"):
    os.environ.pop(k, None)

import boto3
from moto import mock_aws
import asl_sim

LOCK = threading.RLock()  # boto3 resources are not thread-safe; real Lambdas run in separate containers


def load(name):
    spec = importlib.util.spec_from_file_location(name, SRC / (name + ".py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class FakeLambda:
    """Behaves like the boto3 Lambda client: the target's exception becomes FunctionError in the reply."""
    class exceptions:
        class TooManyRequestsException(Exception):
            pass

    def __init__(self, table):
        self.table = table

    def invoke(self, FunctionName, Payload):
        try:
            with LOCK:
                out = self.table[FunctionName](json.loads(Payload), None)
            return {"Payload": io.BytesIO(json.dumps(out, default=str).encode())}
        except Exception as e:
            body = {"errorMessage": str(e), "errorType": type(e).__name__}
            return {"Payload": io.BytesIO(json.dumps(body).encode()), "FunctionError": "Unhandled"}


class SqsSink:
    def __init__(self, url, sqs):
        self.url, self.sqs = url, sqs

    def append(self, body):
        with LOCK:
            self.sqs.send_message(QueueUrl=self.url, MessageBody=body)


def locked(fn):
    def run(ev, ctx):
        with LOCK:
            return fn(ev, ctx)
    return run


class Stack:
    def __init__(self, mode="SCAN", sweep_seconds=60):
        self.mode, self.sweep = mode, sweep_seconds
        self.mock = mock_aws()
        self.mock.start()
        d = boto3.client("dynamodb")
        for name, keys in [("tatkal-payments", [("paymentId", "HASH")]), ("tatkal-bookings", [("bookingId", "HASH")]),
                           ("tatkal-orders", [("orderId", "HASH")]),
                           ("tatkal-seats", [("eventId", "HASH"), ("seatId", "RANGE")]),
                           ("tatkal-events", [("txnId", "HASH"), ("ts", "RANGE")])]:
            d.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                           AttributeDefinitions=[{"AttributeName": k, "AttributeType": "S"} for k, _ in keys],
                           KeySchema=[{"AttributeName": k, "KeyType": t} for k, t in keys])
        self.sqs = boto3.client("sqs")
        self.dlq_url = self.sqs.create_queue(QueueName="tatkal-dlq")["QueueUrl"]
        os.environ["DLQ_URL"] = self.dlq_url
        names = ("gateway", "naive", "saga_reserve", "saga_pay", "saga_confirm", "saga_refund", "saga_release",
                 "saga_start", "reconciler", "ledger", "batch")
        self.M = {n: load(n) for n in names}
        M = self.M
        M["ledger"].print = lambda *a, **k: None  # silence the CloudWatch metric lines in the console
        table = {"tatkal-gateway": M["gateway"].handler, "tatkal-naive": M["naive"].handler,
                 "tatkal-ledger": M["ledger"].handler}
        self.fake = FakeLambda(table)
        for m in ("naive", "saga_pay", "saga_refund", "reconciler", "batch"):
            M[m].lam = self.fake
        self.sink = SqsSink(self.dlq_url, self.sqs)
        self.arns = {"ReserveFn": "R", "PayFn": "P", "ConfirmFn": "C", "RefundFn": "F", "ReleaseFn": "L",
                     "DlqUrl": self.dlq_url, "ReconMode": mode, "RecheckSm": "RS", "ReconcilerFn": "RECON"}
        self.handlers = {"R": locked(M["saga_reserve"].handler), "P": locked(M["saga_pay"].handler),
                         "C": locked(M["saga_confirm"].handler), "F": locked(M["saga_refund"].handler),
                         "L": locked(M["saga_release"].handler)}
        self.saga_asl = (SRC / "saga.asl.yaml").read_text()
        self.recheck_asl = (SRC / "recheck.asl.yaml").read_text()
        stack = self

        class FakeSfn:
            def start_sync_execution(self, stateMachineArn, input):
                r = stack.execute(json.loads(input))
                if r["status"] == "FAILED":
                    return {"status": "FAILED", "error": r["stage"]}
                return {"status": "SUCCEEDED", "output": json.dumps({"stage": r["stage"]})}

            def start_execution(self, stateMachineArn, name, input):
                threading.Thread(target=stack.execute, args=(json.loads(input),), daemon=True).start()
                return {"executionArn": "offline:execution:" + name}

        M["saga_start"].sfn = M["batch"].sfn = FakeSfn()

    def execute(self, inp):
        mach = asl_sim.Machine(self.saga_asl, self.arns, self.handlers, self.sink, self.start_recheck)
        try:
            return {"status": "SUCCEEDED", "stage": mach.run(inp)["stage"], "trace": mach.trace}
        except asl_sim.Failed as f:
            return {"status": "FAILED", "stage": f.error, "trace": mach.trace}

    def start_recheck(self, arn, inp):  # BACKOFF mode: one recheck workflow per unknown payment, real waits
        def run():
            mach = asl_sim.Machine(self.recheck_asl, self.arns, {"RECON": locked(self.M["reconciler"].handler)},
                                   self.sink, on_wait=time.sleep)
            try:
                mach.run(inp)
            except asl_sim.Failed:
                pass
        threading.Thread(target=run, daemon=True).start()

    def start_schedule(self):  # SCAN mode: the EventBridge rule rate(1 minute) -> reconciler
        def loop():
            while True:
                time.sleep(self.sweep)
                try:
                    with LOCK:
                        self.M["reconciler"].handler({}, None)
                except Exception as e:
                    print("reconciler sweep failed:", e)
        if self.mode == "SCAN":
            threading.Thread(target=loop, daemon=True).start()

    def dlq_count(self):
        with LOCK:
            a = self.sqs.get_queue_attributes(QueueUrl=self.dlq_url, AttributeNames=["ApproximateNumberOfMessages"])
        return int(a["Attributes"]["ApproximateNumberOfMessages"])

    def call(self, route, query):
        h = {"naive": self.M["naive"].handler, "saga": self.M["saga_start"].handler,
             "ledger": self.M["ledger"].handler, "batch": self.M["batch"].handler}[route]
        ev = {"requestContext": {"http": {"method": "GET"}}, "rawPath": "/", "queryStringParameters": query}
        if route in ("naive", "ledger"):
            with LOCK:
                return h(ev, None)
        return h(ev, None)


# ----------------------------------------------------------------------------------------------- web server
def make_server(stack, port):
    routes = ("naive", "saga", "ledger", "batch")

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code, body, ctype="application/json"):
            data = body if isinstance(body, bytes) else body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_OPTIONS(self):
            self.send_response(204)
            for k, v in (("Access-Control-Allow-Origin", "*"), ("Access-Control-Allow-Headers", "*"),
                         ("Access-Control-Allow-Methods", "GET, OPTIONS")):
                self.send_header(k, v)
            self.end_headers()

        def do_GET(self):
            u = urlparse(self.path)
            name = u.path.strip("/").split("/")[0]
            if name in routes:
                q = {k: v[0] for k, v in parse_qs(u.query).items()}
                if name == "batch":
                    print("batch started:", q)
                t0 = time.time()
                try:
                    r = stack.call(name, q)
                except Exception as e:
                    return self.send(500, json.dumps({"error": str(e)}))
                if name == "batch":
                    print("batch finished in %.1fs" % (time.time() - t0))
                return self.send(r.get("statusCode", 200), r.get("body", ""))
            if name in ("", "index.html"):
                if not u.query:
                    base = "http://127.0.0.1:%d" % port
                    loc = "/index.html?" + "&".join("%s=%s/%s" % (r, base, r) for r in routes)
                    self.send_response(302)
                    self.send_header("Location", loc)
                    self.end_headers()
                    return
                return self.send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
            if name == "status":
                return self.send(200, json.dumps({"mode": stack.mode, "dlqMessages": stack.dlq_count()}))
            self.send(404, "")

    return ThreadingHTTPServer(("127.0.0.1", port), H)


# ---------------------------------------------------------------------------------------------- self test
# Recorded from the real AWS run (Sydney, final clean batch): n=200, failPct=30, seed 42.
AWS = {"mix": {"SUCCESS": 126, "DECLINE": 4, "DUPLICATE_CLICK": 17, "TIMEOUT_BUT_CHARGED": 21,
               "BOOKING_FAILS_AFTER_PAY": 14, "HOLD_EXPIRES_DURING_PAYMENT": 18},
       "naive": {"orphanedPayments": 53, "duplicateCharges": 17, "ticketsIssued": 143, "debitedRupees": 106500},
       "saga": {"orphanedPayments": 0, "duplicateCharges": 0, "ticketsIssued": 164, "debitedRupees": 82000,
                "pendingReconcile": 0}}


def selftest():
    stack = Stack("SCAN", sweep_seconds=5)
    stack.start_schedule()
    print("Running the 200-booking batch (30% failures) through both flows ...")
    t0 = time.time()
    out = stack.M["batch"].handler({"n": 200, "failPct": 30}, None)
    print("batch done in %.1fs; waiting for the reconciler to settle unknown payments ..." % (time.time() - t0))
    for _ in range(40):
        s = stack.M["ledger"].handler({"flow": "SAGA"}, None)
        if s.get("pendingReconcile", 0) == 0:
            break
        time.sleep(2)
    n = stack.M["ledger"].handler({"flow": "NAIVE"}, None)
    ok = True
    rows = [("scenario mix", AWS["mix"], out["scenarios"])]
    for k in ("orphanedPayments", "duplicateCharges", "ticketsIssued", "debitedRupees"):
        rows.append(("naive " + k, AWS["naive"][k], n.get(k)))
    for k in ("orphanedPayments", "duplicateCharges", "ticketsIssued", "debitedRupees", "pendingReconcile"):
        rows.append(("saga  " + k, AWS["saga"][k], s.get(k)))
    print("\n%-34s %-14s %-14s %s" % ("", "AWS (Sydney)", "Offline", "match"))
    for label, a, b in rows:
        same = a == b
        ok &= same
        if isinstance(a, dict):
            a, b = "see below", "see below"
        print("%-34s %-14s %-14s %s" % (label, a, b, "yes" if same else "NO"))
    print("\nDead-letter queue messages:", stack.dlq_count())
    print("\nRESULT:", "offline output matches the recorded AWS run" if ok else "DIFFERENCE FOUND (see NO rows)")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="SCAN", choices=["SCAN", "BACKOFF"])
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--sweep-seconds", type=int, default=60)
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--no-browser", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(selftest())
    stack = Stack(a.mode, a.sweep_seconds)
    stack.start_schedule()
    srv = make_server(stack, a.port)
    url = "http://127.0.0.1:%d/" % a.port
    print("Tatkal offline stack running (reconciler mode %s). Open %s" % (a.mode, url))
    print("The page's four URLs are already filled in. Press Ctrl+C to stop.")
    if not a.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
