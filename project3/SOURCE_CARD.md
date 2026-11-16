# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

**Name:** Khalid Bakhit
**Date:** 11 Nov 2026

## 1. The source
- Source and URL: Coinbase Exchange WebSocket feed, `wss://ws-feed.exchange.coinbase.com`, channel `matches` (one message per trade), products BTC-USD, ETH-USD, SOL-USD, XRP-USD, DOGE-USD, ADA-USD
- Method (WebSocket push, REST polling or REST history pull): WebSocket push
- Account, key or User-Agent needed: none, it is public market data
- Limits I know about (rate limits, paging, reconnects): the subscribe message must be sent within 5 seconds of connecting or Coinbase closes the connection; Coinbase warns that messages can be dropped from this channel; no replay, so anything missed while disconnected is lost; the producer needs a reconnect loop with a pause

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
  "type": "match",
  "trade_id": 1131972399,
  "maker_order_id": "65d3a654-43fb-4b64-a3ac-638c7cf5ddcd",
  "taker_order_id": "221a22fc-04dc-40a9-99fd-4a1aea6ee009",
  "side": "buy",
  "size": "0.0001569",
  "price": "64099.59",
  "product_id": "BTC-USD",
  "sequence": 139572835589,
  "time": "2026-11-16T15:34:56.638465Z"
}
```

## 3. Volume maths
- Measured rate (from the probe): 11.77 messages per second (706 messages in 60 s, 0 not JSON)
- Live capture needed for 50,000: about 71 minutes (4,250 s)
- History / backfill available? How many days? None. The WebSocket has no history, so every record is collected live
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): Run the producer and lander together for about 2 hours, which is about 85,000 messages at the measured rate. Since trading can be quieter at night, 2 hours gives margin over the 71-minute minimum, and with the lander running at the same time I can check the raw count before Thursday. What actually happened: I did not collect before Thursday. I started on Mon 16 Nov in the afternoon, the rate was 47.7 messages/s (about 4 times the probe) so the raw count was already over 50,000 by 17:34 (60,117 raw records).

## 4. Topic design
- Topic name: coinbase_trades (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: product_id
- Why this key (ordering, spread across partitions): every trade of one product goes to the same partition, so the trades of each product stay in order and gaps in its `sequence` number show dropped messages. My question compares products, so grouping by product fits. Limit: with only 6 keys over 3 partitions, the partitions may not hold equal numbers of records.

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) The same kafka_partition + kafka_offset. This happens when the lander writes a file and crashes before storing its offsets, so the same records are read and landed again.
- What makes two records **the same thing** (the same trade, post, aircraft position)? The same product_id + trade_id.
- If the same thing can arrive in several versions, which one do I keep? A trade never changes after it happens, so there is only one version and any copy can be kept. The `subscriptions` and `last_match` messages are not new trades (last_match repeats a past trade) and are dropped in the stage step.

## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: Is there more buying or selling pressure, and does it differ by product? Share of buy volume per product in 1-minute tumbling windows by trade time.

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [ ] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
  - Not ticked: the plan had margin, but I did not collect before Thursday. I passed 50,000 on Mon 16 Nov (see section 3).
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
