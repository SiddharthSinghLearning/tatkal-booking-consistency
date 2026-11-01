"""Runs every Lambda and the saga definition locally against fake AWS (moto), for all scenarios and both flows."""
import importlib.util, io, json, os, pathlib, sys, time

os.environ.update(AWS_DEFAULT_REGION="ap-southeast-2", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x",
                  PAYMENTS_TABLE="tatkal-payments", BOOKINGS_TABLE="tatkal-bookings", SEATS_TABLE="tatkal-seats",
                  EVENTS_TABLE="tatkal-events", ORDERS_TABLE="tatkal-orders", GATEWAY_FN="tatkal-gateway",
                  HOLD_SECONDS="120", NAIVE_FN="tatkal-naive", LEDGER_FN="tatkal-ledger",
                  EXPRESS_SM="express", STANDARD_SM="standard")
import boto3
from moto import mock_aws

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import asl_sim

SRC = HERE.parent / "src"
failed = 0


def check(label, ok, extra=""):
    global failed
    failed += not ok
    print("%-4s %s %s" % ("PASS" if ok else "FAIL", label, extra))


def load(name):
    spec = importlib.util.spec_from_file_location(name, SRC / (name + ".py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class FakeLambda:
    """Behaves like boto3 lambda client: raises nothing, reports FunctionError in the response."""
    class exceptions:
        class TooManyRequestsException(Exception):
            pass

    def __init__(self, table):
        self.table = table

    def invoke(self, FunctionName, Payload):
        try:
            out = self.table[FunctionName](json.loads(Payload), None)
            return {"Payload": io.BytesIO(json.dumps(out, default=str).encode())}
        except Exception as e:
            body = {"errorMessage": str(e), "errorType": type(e).__name__}
            return {"Payload": io.BytesIO(json.dumps(body).encode()), "FunctionError": "Unhandled"}


def make_tables():
    d = boto3.client("dynamodb")
    for name, keys in [("tatkal-payments", [("paymentId", "HASH")]), ("tatkal-bookings", [("bookingId", "HASH")]),
                       ("tatkal-orders", [("orderId", "HASH")]),
                       ("tatkal-seats", [("eventId", "HASH"), ("seatId", "RANGE")]),
                       ("tatkal-events", [("txnId", "HASH"), ("ts", "RANGE")])]:
        d.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                       AttributeDefinitions=[{"AttributeName": k, "AttributeType": "S"} for k, _ in keys],
                       KeySchema=[{"AttributeName": k, "KeyType": t} for k, t in keys])


with mock_aws():
    make_tables()
    sqs = boto3.client("sqs")
    dlq_url = sqs.create_queue(QueueName="tatkal-dlq")["QueueUrl"]
    os.environ["DLQ_URL"] = dlq_url
    M = {n: load(n) for n in ("gateway", "naive", "saga_reserve", "saga_pay", "saga_confirm", "saga_refund",
                              "saga_release", "saga_start", "reconciler", "ledger", "batch")}
    lam_table = {"tatkal-gateway": M["gateway"].handler, "tatkal-naive": M["naive"].handler,
                 "tatkal-ledger": M["ledger"].handler}
    fake = FakeLambda(lam_table)
    for m in ("naive", "saga_pay", "saga_refund", "reconciler", "batch"):
        M[m].lam = fake
    sink = []
    class FakeTime:
        """Real time plus an offset that the recheck workflow's Wait states advance (no real sleeping)."""
        offset = 0
        def time(self): return time.time() + self.offset
        def sleep(self, s): time.sleep(s)
    clock = FakeTime()
    M["gateway"].time = M["reconciler"].time = clock
    pay_t = boto3.resource("dynamodb").Table("tatkal-payments")

    def age_payments():  # pretend the bank has settled everything
        for it in pay_t.scan()["Items"]:
            pay_t.update_item(Key={"paymentId": it["paymentId"]}, UpdateExpression="SET settleAt = :z",
                              ExpressionAttributeValues={":z": 0})

    arns = {"ReserveFn": "R", "PayFn": "P", "ConfirmFn": "C", "RefundFn": "F", "ReleaseFn": "L", "DlqUrl": dlq_url,
            "ReconMode": "SCAN", "RecheckSm": "RS", "ReconcilerFn": "RECON"}
    handlers = {"R": M["saga_reserve"].handler, "P": M["saga_pay"].handler, "C": M["saga_confirm"].handler,
                "F": M["saga_refund"].handler, "L": M["saga_release"].handler}
    asl_text = (SRC / "saga.asl.yaml").read_text()
    recheck_text = (SRC / "recheck.asl.yaml").read_text()
    started, rechecks = [], []

    def start_recheck(arn, inp):
        started.append((arn, inp))

    def run_recheck(inp):
        mach = asl_sim.Machine(recheck_text, arns, {"RECON": M["reconciler"].handler}, sink,
                               on_wait=lambda sec: setattr(clock, "offset", clock.offset + sec))
        try:
            mach.run(inp)
            return "SUCCEEDED", mach
        except asl_sim.Failed as f:
            return f.error, mach

    def execute(inp, mode="SCAN"):
        mach = asl_sim.Machine(asl_text, {**arns, "ReconMode": mode}, handlers, sink, start_recheck)
        try:
            return {"status": "SUCCEEDED", "stage": mach.run(inp)["stage"], "trace": mach.trace}
        except asl_sim.Failed as f:
            return {"status": "FAILED", "stage": f.error, "trace": mach.trace}

    class FakeSfn:
        def start_sync_execution(self, stateMachineArn, input):
            r = execute(json.loads(input))
            if r["status"] == "FAILED":
                return {"status": "FAILED", "error": r["stage"]}
            return {"status": "SUCCEEDED", "output": json.dumps({"stage": r["stage"]})}

    M["saga_start"].sfn = FakeSfn()
    M["batch"].sfn = FakeSfn()
    ledger = M["ledger"].handler
    reset = lambda: ledger({"action": "reset"}, None)

    def order(key):
        return boto3.resource("dynamodb").Table("tatkal-orders").get_item(Key={"orderId": key}).get("Item", {})

    print("=== NAIVE flow (charge first, then book) ===")
    NAIVE = {"SUCCESS": (500, 1, 0, 0), "DECLINE": (0, 0, 0, 0), "TIMEOUT_BUT_CHARGED": (500, 0, 1, 0),
             "BOOKING_FAILS_AFTER_PAY": (500, 0, 1, 0), "DUPLICATE_CLICK": (1000, 1, 0, 1),
             "HOLD_EXPIRES_DURING_PAYMENT": (500, 0, 1, 0), "BOOKING_AND_REFUND_FAIL": (500, 0, 1, 0)}
    for mode, want in NAIVE.items():
        reset()
        M["naive"].handler({"txnId": "txn-x", "mode": mode}, None)
        L = ledger({"flow": "NAIVE"}, None)
        got = (L["debitedRupees"], L["ticketsIssued"], L["orphanedPayments"], L["duplicateCharges"])
        check("naive %-28s" % mode, got == want, "debited/tickets/orphans/dup=%s" % (got,))

    print("=== SAGA flow (reserve, pay, confirm, compensate) ===")
    SAGA = {"SUCCESS": ("BOOKED", (500, 1, 0, 0)), "DECLINE": ("DECLINED", (0, 0, 0, 0)),
            "BOOKING_FAILS_AFTER_PAY": ("REFUNDED", (0, 0, 0, 0)),
            "HOLD_EXPIRES_DURING_PAYMENT": ("REFUNDED", (0, 0, 0, 0)),
            "DUPLICATE_CLICK": ("BOOKED", (500, 1, 0, 0))}
    for mode, (stage, want) in SAGA.items():
        reset(); sink.clear()
        inp = {"orderKey": "sg-x", "eventId": "S", "seatId": "A1", "mode": mode}
        r = execute(inp)
        if mode == "DUPLICATE_CLICK":  # the double click arrives again after the first finished
            r2 = execute(inp)
            check("saga  duplicate click 2nd run is ignored", r2["stage"] == "DUPLICATE", r2["trace"][-1])
        L = ledger({"flow": "SAGA"}, None)
        got = (L["debitedRupees"], L["ticketsIssued"], L["orphanedPayments"], L["duplicateCharges"])
        check("saga  %-28s" % mode, r["stage"] == stage and got == want, "stage=%s %s" % (r["stage"], got))

    # unknown payment result -> parked -> reconciler finishes the job
    reset()
    r = execute({"orderKey": "sg-t", "eventId": "S", "seatId": "A2", "mode": "TIMEOUT_BUT_CHARGED"})
    L = ledger({"flow": "SAGA"}, None)
    check("saga  TIMEOUT parks the order", r["stage"] == "PENDING_RECONCILE" and L["pendingReconcile"] == 1
          and L["orphanedPayments"] == 1, "stage=%s pending=%s" % (r["stage"], L["pendingReconcile"]))
    M["reconciler"].handler({}, None)
    L = ledger({"flow": "SAGA"}, None)
    check("bank not settled yet -> stays pending, 1 bank call counted",
          L["pendingReconcile"] == 1 and L["bankStatusCalls"] == 1 and order("sg-t")["status"] == "PAYMENT_UNKNOWN",
          "calls=%s" % L["bankStatusCalls"])
    check("SCAN mode never starts a recheck workflow", started == [])
    age_payments()
    time.sleep(1.1)
    M["reconciler"].handler({}, None)
    L = ledger({"flow": "SAGA"}, None)
    check("saga  reconciler confirms the booking", L["pendingReconcile"] == 0 and L["orphanedPayments"] == 0
          and L["ticketsIssued"] == 1 and L["debitedRupees"] == 500 and L["maxResolveSecs"] is not None,
          "resolveSecs=%s" % L["maxResolveSecs"])

    # reconciler: money taken but the seat was sold to someone else meanwhile -> automatic refund
    reset()
    execute({"orderKey": "sg-t2", "eventId": "S", "seatId": "A3", "mode": "TIMEOUT_BUT_CHARGED"})
    seats = boto3.resource("dynamodb").Table("tatkal-seats")
    seats.put_item(Item={"eventId": "S", "seatId": "A3", "txnId": "someone-else", "status": "BOOKED"})
    age_payments()
    M["reconciler"].handler({}, None)
    L = ledger({"flow": "SAGA"}, None)
    check("saga  reconciler refunds if seat is gone", L["debitedRupees"] == 0 and L["refunded"] == 1
          and order("sg-t2")["status"] == "REFUNDED")

    # saga: seat already taken -> nothing is charged
    reset()
    seats.put_item(Item={"eventId": "S", "seatId": "A4", "txnId": "someone-else", "status": "BOOKED"})
    r = execute({"orderKey": "sg-t3", "eventId": "S", "seatId": "A4", "mode": "SUCCESS"})
    check("saga  seat taken -> no charge", r["stage"] == "SEAT_TAKEN" and ledger({"flow": "SAGA"}, None)["payments"] == 0)

    # hold TTL: an expired hold can be re-reserved even though DynamoDB has not deleted it yet
    reset()
    seats.put_item(Item={"eventId": "S", "seatId": "A5", "txnId": "old", "status": "HELD", "holdExpiresAt": 1})
    r = execute({"orderKey": "sg-t4", "eventId": "S", "seatId": "A5", "mode": "SUCCESS"})
    check("saga  expired hold can be re-reserved", r["stage"] == "BOOKED")

    # compensation itself fails -> dead-letter queue + failed execution
    reset(); sink.clear()
    r = execute({"orderKey": "sg-t5", "eventId": "S", "seatId": "A6", "mode": "BOOKING_AND_REFUND_FAIL"})
    msgs = len(sink)
    L = ledger({"flow": "SAGA"}, None)
    check("saga  refund fails -> DLQ + NeedsManualReview", r["status"] == "FAILED" and r["stage"] == "NeedsManualReview"
          and msgs == 1 and L["orphanedPayments"] == 1, "trace tail=%s" % r["trace"][-3:])

    # reconciler gives up after 5 attempts and uses the real DLQ
    reset()
    execute({"orderKey": "sg-t6", "eventId": "S", "seatId": "A7", "mode": "TIMEOUT_BUT_CHARGED"})
    real_gateway = M["reconciler"].gateway
    M["reconciler"].gateway = lambda body: (_ for _ in ()).throw(RuntimeError("gateway down"))
    for _ in range(5):
        M["reconciler"].handler({}, None)
    M["reconciler"].gateway = real_gateway
    n = int(sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"])
            ["Attributes"]["ApproximateNumberOfMessages"])
    check("reconciler  5 failed attempts -> real SQS DLQ", n == 1 and order("sg-t6")["status"] == "NEEDS_REVIEW")

    # NOVELTY: BACKOFF mode - every unclear payment gets its own recheck workflow
    import zlib
    for key, seat in (("sg-b1", "B1"), ("sg-b2", "B2"), ("sg-b3", "B3")):
        reset(); started.clear(); sink.clear(); clock.offset = 0
        r = execute({"orderKey": key, "eventId": "S", "seatId": seat, "mode": "TIMEOUT_BUT_CHARGED"}, "BACKOFF")
        ok_start = len(started) == 1 and started[0][0] == "RS" and started[0][1] == {"orderKey": key}
        d = 2 + zlib.crc32(("pay-" + key).encode()) % 29
        res, mach = run_recheck(started[0][1])
        waits, cum, calls = [2, 4, 8, 16, 32, 64], 0, 0
        for w in waits:
            cum += w; calls += 1
            if cum >= d:
                break
        L = ledger({"flow": "SAGA"}, None)
        check("backoff %s settle=%2ds: saga starts recheck, it resolves" % (key, d),
              r["stage"] == "PENDING_RECONCILE" and ok_start and res == "SUCCEEDED" and order(key)["status"] == "CONFIRMED"
              and L["bankStatusCalls"] == calls and L["ticketsIssued"] == 1 and L["orphanedPayments"] == 0
              and L["reconcilerMode"] in ("SCAN", "BACKOFF"), "bank calls=%d waited=%ss" % (calls, getattr(mach, "waited", 0)))
    # the bank never settles -> after the last check the recheck workflow gives up and a human is told
    reset(); started.clear(); sink.clear(); clock.offset = 0
    execute({"orderKey": "sg-b9", "eventId": "S", "seatId": "B9", "mode": "TIMEOUT_BUT_CHARGED"}, "BACKOFF")
    pay_t.update_item(Key={"paymentId": "pay-sg-b9"}, UpdateExpression="SET settleAt = :z",
                      ExpressionAttributeValues={":z": 4102444800})
    res, mach = run_recheck(started[0][1])
    check("backoff never settles -> DLQ + NeedsManualReview", res == "NeedsManualReview" and len(sink) == 1
          and mach.trace[-2:] == ["GiveUp", "NeedsManualReview"],
          "total wait=%ss" % getattr(mach, "waited", 0))
    clock.offset = 0
    # the recheck's own Lambda call is throttled more often than its retries allow: it must skip that check, not die
    reset(); started.clear(); sink.clear(); clock.offset = 0
    execute({"orderKey": "sg-b8", "eventId": "S", "seatId": "B8", "mode": "TIMEOUT_BUT_CHARGED"}, "BACKOFF")
    Throttled = type("Lambda.TooManyRequestsException", (Exception,), {})
    realh, left = M["reconciler"].handler, [6]
    def flaky(ev, ctx):
        if left[0]:
            left[0] -= 1
            raise Throttled("Rate Exceeded")
        return realh(ev, ctx)
    mach = asl_sim.Machine(recheck_text, arns, {"RECON": flaky}, sink,
                           on_wait=lambda sec: setattr(clock, "offset", clock.offset + sec))
    mach.run(started[0][1])
    check("backoff throttled check is skipped, next check resolves", order("sg-b8")["status"] == "CONFIRMED"
          and mach.trace[-1] == "Resolved" and "Check2" in mach.trace, "trace tail=%s" % mach.trace[-4:])
    clock.offset = 0

    # throttled call to the bank inside Pay is retried by the workflow (no dead-letter message)
    reset(); sink.clear()
    real, left = M["saga_pay"].lam, [2]
    class Throttle:
        def invoke(self, **kw):
            if left[0]:
                left[0] -= 1
                raise real.exceptions.TooManyRequestsException("Rate Exceeded")
            return real.invoke(**kw)
    M["saga_pay"].lam = Throttle()
    r = execute({"orderKey": "sg-th", "eventId": "S", "seatId": "T1", "mode": "SUCCESS"})
    M["saga_pay"].lam = real
    check("saga  throttled bank call is retried, not dead-lettered", r["stage"] == "BOOKED" and sink == []
          and ledger({"flow": "SAGA"}, None)["debitedRupees"] == 500, "stage=%s" % r["stage"])

    # throttled refund is retried too
    reset(); sink.clear()
    realr, left = M["saga_refund"].lam, [2]
    class ThrottleR:
        def invoke(self, **kw):
            if left[0]:
                left[0] -= 1
                raise realr.exceptions.TooManyRequestsException("Rate Exceeded")
            return realr.invoke(**kw)
    M["saga_refund"].lam = ThrottleR()
    r = execute({"orderKey": "sg-thr", "eventId": "S", "seatId": "T2", "mode": "BOOKING_FAILS_AFTER_PAY"})
    M["saga_refund"].lam = realr
    check("saga  throttled refund is retried, not dead-lettered", r["stage"] == "REFUNDED" and sink == []
          and ledger({"flow": "SAGA"}, None)["debitedRupees"] == 0, "stage=%s" % r["stage"])

    # function-URL entry points
    reset()
    u = M["saga_start"].handler({"requestContext": {}, "rawPath": "/", "queryStringParameters": {"mode": "SUCCESS"}}, None)
    check("saga_start URL", u["statusCode"] == 200 and json.loads(u["body"])["outcome"] == "BOOKED")
    u = M["saga_start"].handler({"requestContext": {}, "rawPath": "/favicon.ico"}, None)
    n1 = M["naive"].handler({"requestContext": {}, "rawPath": "/favicon.ico"}, None)
    check("favicon requests ignored", u["statusCode"] == 404 and n1["statusCode"] == 404)
    reset()
    d = M["saga_start"].handler({"mode": "DUPLICATE_CLICK"}, None)
    check("saga_start double click = 2 executions, 1 charge", len(d["executions"]) == 2
          and ledger({"flow": "SAGA"}, None)["debitedRupees"] == 500)
    ev = ledger({"events": d["orderKey"]}, None)
    check("timeline events readable", len(ev) >= 4 and {"ts", "flow", "step", "message"} <= set(ev[0]), "%d rows" % len(ev))
    check("reset blocked via URL", "reset" not in json.dumps(
        ledger({"requestContext": {}, "rawPath": "/", "queryStringParameters": {"action": "reset"}}, None)))

    # batch runner: 40 identical scenarios through both flows
    out = M["batch"].handler({"n": 40, "failPct": 30}, None)
    N, S = out["naive"], out["saga"]
    print("batch n=40 scenarios:", out["scenarios"])
    print("  naive:", {k: N[k] for k in ("debitedRupees", "ticketsIssued", "orphanedPayments", "duplicateCharges")})
    print("  saga :", {k: S[k] for k in ("debitedRupees", "ticketsIssued", "orphanedPayments", "duplicateCharges", "pendingReconcile")})
    check("batch: naive has orphans and duplicates", N["orphanedPayments"] > 0 and N["duplicateCharges"] > 0)
    check("batch: saga orphans are only the pending ones", S["orphanedPayments"] == S["pendingReconcile"]
          and S["duplicateCharges"] == 0)
    age_payments()
    time.sleep(1.1)
    M["reconciler"].handler({}, None)
    S2 = ledger({"flow": "SAGA"}, None)
    check("batch: after reconciler saga has 0 orphans, 0 duplicates, 0 pending",
          S2["orphanedPayments"] == 0 and S2["duplicateCharges"] == 0 and S2["pendingReconcile"] == 0,
          "tickets naive=%s saga=%s" % (N["ticketsIssued"], S2["ticketsIssued"]))

print("FAILED: %d" % failed)
sys.exit(1 if failed else 0)
