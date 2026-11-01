# Tatkal Booking Consistency

Resilient payment-booking consistency for high-demand ticket booking on AWS.
Course project, Cloud Architecture, VIT Vellore.

Booking a seat and taking a payment live in two systems that cannot share a transaction.
Under Tatkal-style load they sometimes disagree: customers get charged with no ticket,
charged twice, or told a payment failed when it went through. This project builds the
naive flow that does this, then a saga-based flow that repairs itself, on AWS, and
measures both on the same 200 seeded bookings.

| 200 seeded bookings | Naive flow | Saga flow |
|---|---|---|
| Paid, no ticket | 53 | 0 |
| Charged twice | 17 | 0 |
| Tickets issued | 143 | 164 |
| Automatic refunds | 0 | 32 |

The payment gateway is a **mock bank** written for this project. No real money or customer data is involved.
Diagrams and screenshots from the AWS run are in [`docs/figures/`](docs/figures/).

## What is in this repository

| Path | Purpose |
|---|---|
| `template.yaml` | Complete AWS SAM / CloudFormation template (stack `tatkal-baseline`, ap-southeast-2) |
| `template.base.yaml`, `build.py` | Template source and the script that inlines `src/` into it |
| `src/` | Lambda functions (mock bank, naive flow, saga steps, reconciler, ledger, batch) and the two Step Functions definitions |
| `index.html` | The demonstration page (talks to the four Lambda Function URLs) |
| `frontend.yaml` | Optional S3 + CloudFront hosting for the page (not deployed in our run) |
| `tests/` | Local test suite (moto + our own Amazon States Language interpreter) |
| `offline/` | Full local emulation: runs the same pipeline on a laptop, no AWS account needed |
| `docs/figures/` | Architecture and flow diagrams, AWS console screenshots, and demo-page captures |

## AWS services used

Lambda (Function URLs), Step Functions (Standard and Express), DynamoDB (5 tables, on-demand, TTL),
SQS (dead-letter queue), EventBridge (1-minute sweep, refund events), CloudWatch (EMF metrics, 2 alarms, 1 dashboard),
CloudFormation / SAM, IAM (least-privilege per function).

## Run it locally (no AWS account)

```bash
cd offline
pip install -r requirements.txt
python tatkal_offline.py --selftest        # replays the 200 bookings and compares with the AWS run
python tatkal_offline.py --sweep-seconds 20   # opens the demo page at http://127.0.0.1:8787/
```
Options: `--mode SCAN|BACKOFF`, `--port N`, `--no-browser`. See [`offline/README.md`](offline/README.md).

## Deploy to AWS

1. (Only if you change `src/`) `python build.py` regenerates `template.yaml`.
2. In the CloudFormation console choose *Create stack* and upload `template.yaml`. Name it `tatkal-baseline`, region `ap-southeast-2`.
   Parameters: `HoldSeconds` (default 120) and `ReconcilerMode` (`SCAN` or `BACKOFF`).
   Accept the IAM capabilities prompt.
3. When the stack finishes, copy the four Function URLs (naive, saga-start, ledger, batch) from *Outputs* into the settings of `index.html`.
4. Open `index.html` in a browser, pick a scenario, press *Run both systems*.
5. **Tear down:** delete the stack, then delete the leftover CloudWatch log groups.

Costs: everything is pay-per-use; the whole project used about US$0.06 of free credit.

**Security note:** the Function URLs use auth type `NONE` and open CORS. That is fine for a short demonstration
and not for production. Delete the stack when you are done.

## Limitations

Simulated bank, 200-booking scale, new-account Lambda throttling, one run per reconciler mode.


## Authors

Siddharth Singh (24BCI0158) and team members, VIT Vellore.
