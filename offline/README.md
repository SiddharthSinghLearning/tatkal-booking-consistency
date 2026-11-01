# Tatkal Booking Consistency - OFFLINE BACKUP

Runs the same booking pipeline on your laptop with no internet and no AWS account.
It uses the project's real Lambda code and real state-machine definitions.
DynamoDB and SQS are provided by `moto` (a local AWS stand-in).

## One-time setup (do this at home, WITH internet)
1. Install Python 3.10 or newer from python.org (tick "Add Python to PATH" on Windows).
2. Open a terminal in this folder and run:
       pip install -r requirements.txt
   (If pip complains, try: pip install --break-system-packages -r requirements.txt)

## Before the presentation (works offline)
1. Windows: double-click run.bat.   Mac/Linux: ./run.sh
2. Your browser opens the same web page as the online demo, with all four
   URLs already filled in. Pick a scenario and press "Run both systems".
3. For the batch: press "Run batch", then "Refresh totals" about 20-60 s later.
4. Stop with Ctrl+C.

## Check it matches AWS
    python tatkal_offline.py --selftest
Runs the 200-booking batch and compares it with the numbers recorded from the
real AWS run (naive 53 paid-no-ticket / 17 charged twice; saga 164 tickets,
0 and 0, Rs 82,000). Every row should say match.

## Options
    --mode SCAN|BACKOFF   reconciler style (default SCAN)
    --sweep-seconds N     how often the sweep runs (default 60; run.* use 20)
    --port N              default 8787
    --no-browser

## Honest differences from AWS (say this out loud)
- Same: Lambda code, saga definition, mock bank, DynamoDB conditions, the
  failure mix, and every count and rupee total in the batch.
- Different: Step Functions is run by a local interpreter, not AWS; no console
  graph, CloudWatch dashboard or alarms; the batch finishes faster; the
  "average settle" timing depends on the sweep interval.
