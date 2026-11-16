# Progress log

Add to the entry at the end of every class, and whenever you stop working. Short lines are fine. Include the raw record count when you have one.

## Session 1 · Thu 05 Nov
- Done: caught up on Wed 11 Nov: tested the class quakes pipeline (2,235 events landed, flow and chart), created ds3022-streaming, pushed the kit, probed Coinbase (11.77/s) and Bluesky (55.38/s), chose Coinbase, started SOURCE_CARD.md
- Raw records so far: 0
- Next: producer for coinbase_trades
- Blocked: nothing

## Session 2 · Mon 09 Nov
- Done: caught up on Mon 16 Nov: lander for coinbase_trades (checkpoint C), four_numbers.py
- Raw records so far: see Session 4
- Next: chaos cards
- Blocked: nothing

## Session 3 · Thu 12 Nov
- Done: caught up on Mon 16 Nov: producer and lander ran 16:34–16:51 (47,979 messages), passed 50,000 raw records at 17:34
- Raw records so far: see Session 4
- Next: Prefect flow
- Chart swap: not done, I did not swap charts with a classmate
- Blocked: nothing

## Session 4 · Mon 16 Nov
- Done: producer (key product_id, reconnect loop), lander (13 columns), chaos cards D1, D2, D4 with numbers, moved the project out of iCloud (files were locked), Prefect flow with stage/entities/marts/charts, sample, screenshots, README
- Raw records so far: 69,264 (malformed 1, unique Kafka records 68,263, unique trades 68,242)
- Also: flow.py fixes (the sample gets its own database and charts, `sequence` parsed from the raw JSON, chart titles state the finding). Reran the full data twice ('legendary-hare', 'fuzzy-lionfish') and the sample twice ('humongous-taipan', 'uber-ostrich'), the four numbers were the same each time. New screenshots in docs/. Wrote the Reflection.
- Peer test: I could not get a classmate to follow my README, so I had Claude follow How to run from a fresh copy of the repo. Setup, pip install and `python flow.py data/sample` (twice) were tested; Docker, the producer and the lander were not, since there was no Docker there.
- Where it got stuck: the Prefect server crashed (`Body() got an unexpected keyword argument 'min_items'`) because pip installed the newest FastAPI, which does not work with Prefect. Fix: added `"fastapi<=0.150"` to the pip install line in How to run. After that the server started and both sample runs gave 5,000 / 0 / 4,994 / 4,992.
- Also added the Windows activate lines and the `prefect config set PREFECT_API_URL` line to How to run.
- Next: submit (dp3-submit tag, Canvas)
- Blocked: nothing
