"""A tiny Amazon States Language interpreter, just enough to run saga.asl.yaml locally.
Supports Task (Lambda ARN, lambda:invoke, states:startExecution, sqs:sendMessage), Pass, Wait, Choice, Succeed, Fail, Parameters, Retry, Catch, ResultPath."""
import json, re
import yaml


class Failed(Exception):
    def __init__(self, error, cause):
        super().__init__("%s: %s" % (error, cause))
        self.error, self.cause = error, cause


def path(doc, p):
    cur = doc
    for part in [x for x in p.lstrip("$").split(".") if x]:
        cur = cur[part]
    return cur


def resolve(params, state):
    if isinstance(params, dict):
        out = {}
        for k, v in params.items():
            if k.endswith(".$"):
                v = str(v)
                if v.startswith("States.JsonToString("):
                    out[k[:-2]] = json.dumps(path(state, v[len("States.JsonToString("):-1]), default=str)
                else:
                    out[k[:-2]] = path(state, v)
            else:
                out[k] = resolve(v, state)
        return out
    return params


def set_path(state, rp, value):
    if rp is None:
        return state
    if rp == "$":
        return value
    new = dict(state)
    new[rp.replace("$.", "", 1)] = value
    return new


def matches(err, names):
    return "States.ALL" in names or err in names


class Machine:
    def __init__(self, asl_text, subs, lambdas, sqs_sink, starter=None, on_wait=None):
        for k, v in subs.items():
            asl_text = asl_text.replace("${%s}" % k, v)
        self.asl = yaml.safe_load(asl_text)
        self.lambdas, self.sqs, self.trace, self.starter, self.on_wait = lambdas, sqs_sink, [], starter, on_wait

    def run(self, state):
        name = self.asl["StartAt"]
        while True:
            s = self.asl["States"][name]
            self.trace.append(name)
            t = s["Type"]
            if t == "Succeed":
                return state
            if t == "Fail":
                raise Failed(s["Error"], s["Cause"])
            if t == "Pass":
                state = set_path(state, s.get("ResultPath", "$"), s["Result"]) if "Result" in s else state
                name = s["Next"]
                continue
            if t == "Wait":  # no real waiting in tests; remember the total
                self.waited = getattr(self, "waited", 0) + s["Seconds"]
                if self.on_wait:
                    self.on_wait(s["Seconds"])
                name = s["Next"]
                continue
            if t == "Choice":
                nxt = s.get("Default")
                for c in s["Choices"]:
                    v = path(state, c["Variable"])
                    if v == c.get("StringEquals", object()) or v == c.get("NumericEquals", object()):
                        nxt = c["Next"]
                        break
                name = nxt
                continue
            # Task
            params = resolve(s.get("Parameters", state), state)
            attempts = {}
            while True:
                try:
                    if s["Resource"].startswith("arn:aws:states:::sqs"):
                        self.sqs.append(params["MessageBody"])
                        result = {}
                    elif s["Resource"] == "arn:aws:states:::states:startExecution":
                        self.starter(params["StateMachineArn"], params["Input"])
                        result = {}
                    elif s["Resource"] == "arn:aws:states:::lambda:invoke":
                        out = self.lambdas[params["FunctionName"]](params["Payload"], None)
                        result = {"Payload": out}
                        if "ResultSelector" in s:
                            result = resolve(s["ResultSelector"], result)
                    else:
                        result = self.lambdas[s["Resource"]](params, None)
                    break
                except Exception as e:  # Lambda error name = exception class name
                    err = type(e).__name__
                    retried = False
                    for i, r in enumerate(s.get("Retry", [])):
                        if matches(err, r["ErrorEquals"]) and attempts.get(i, 0) < r.get("MaxAttempts", 3):
                            attempts[i] = attempts.get(i, 0) + 1
                            retried = True
                            break
                    if retried:
                        continue
                    caught = next((c for c in s.get("Catch", []) if matches(err, c["ErrorEquals"])), None)
                    if not caught:
                        raise Failed(err, str(e))
                    state = set_path(state, caught.get("ResultPath", "$"), {"Error": err, "Cause": str(e)})
                    name = caught["Next"]
                    result = None
                    break
            if result is None:
                continue
            state = set_path(state, s.get("ResultPath", "$"), result) if "ResultPath" in s else result
            name = s["Next"]
