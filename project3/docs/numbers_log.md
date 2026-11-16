# Numbers log (evidence for the README)

Raw numbers as I measured them, in order. The README quotes from here.

## Probes (Wed 11 Nov)
- Coinbase: 706 messages in 60 s = 11.77/s, 0 not JSON, 50,000 in ~71 min
- Bluesky: 3,324 messages in 60 s = 55.38/s, 0 not JSON, 50,000 in ~15 min

## Topic coinbase_trades (Mon 16 Nov, ~16:41, producer and lander running since ~16:34)
`rpk topic describe coinbase_trades -p`

| Partition | Leader | Epoch | High watermark |
|---|---|---|---|
| 0 | 0 | 1 | 12,726 |
| 1 | 0 | 2 | 879 |
| 2 | 1 | 2 | 2,665 |
| **Total** | | | **16,270** |

- Partition 0 holds ~78% (BTC-USD and ADA-USD hash there; BTC trades most). Partition 1 holds only XRP-USD.
- Note: the Redpanda Console showed 10,264 in every partition (30,792 total): a display bug. rpk is correct.
- First landed batches: 1,993 match, 6 last_match, 1 subscriptions (no product_id, sent without key), 0 malformed.

## Producer run 1 (Mon 16 Nov)
- Started 16:34:39, stopped 16:51:24 (Ctrl+C, for card D1): **47,979 messages sent** to coinbase_trades
- Live rate: 47,979 in 16 min 45 s = **about 47.7 messages/s** (about 4x the 11.77/s probe rate on Wed evening; Monday afternoon is busier)
- So the topic holds 47,979 records. The lander had landed 34,000 before it was stopped, so **13,979 records waited in the topic** for card D1.

## Move out of iCloud (Mon 16 Nov, ~16:55)
- The project lived in ~/Documents, which iCloud Drive syncs. While starting D1, `python lander.py crash` hung before printing anything; even `import quixstreams` hung, and reading Parquet files failed with "Resource deadlock avoided" (iCloud locking files while it uploads them).
- Fix: fresh `git clone` of ds3022-streaming into ~/dev/learn-kafka (outside iCloud) and a new .venv.
- The 34,000 landed records stayed in the old folder, but nothing was lost: all 47,979 records were still in the Kafka topic. The lander got a new consumer group (`coinbase_trades_lander_v2`), which starts at offset 0 and lands the whole topic again into the new data/raw.

## Four numbers (four_numbers.py)
| When | raw | malformed | unique Kafka records | unique product_id+trade_id |
|---|---|---|---|---|
| 16:46, first count (producer + lander running) | 31,000 | 0 | 31,000 | 30,999 |
| ~16:48, before D1 (lander stopped) | 34,000 | 0 | 34,000 | 33,999 |
| 17:05, before D1 redo (new folder, nothing landed) | 0 | 0 | 0 | 0 |
| 17:19, after D1 | 48,979 | 0 | 47,979 | 47,978 |

## Chaos round

### D1 · Crash between file and offsets
- First attempt (old folder, before the iCloud move): before = raw 34,000 · malformed 0 · unique records 34,000 · unique trades 33,999; the crash run hung (iCloud), so the card was redone below.
- Redone in ~/dev/learn-kafka with the new consumer group: before = raw 0 · malformed 0 · unique records 0 · unique trades 0 (no files yet)
- Waiting in the topic: all 47,979 records (producer stopped at 16:51:24)
- Expected after: raw 47,979 + 1,000 (crashed batch landed twice) = **48,979** · malformed 0 · unique records **47,979** · unique trades **47,978**
- After: 
- My prediction is that the crashed batch will appear double as many times, but kafka will only keep the unique values

### D2 · Poison message
- Before: raw 48,979 · malformed 0 · unique records 47,979 · unique trades 47,978
- My prediction is that the lander keeps running and lands the poisoned message as malformed
- What I did: `docker exec -it redpanda-0 rpk topic produce coinbase_trades -k bad`, typed `this is not JSON`, Enter, Ctrl+C. The lander landed it 17:23:58 as `Landed 1 records -> data/raw/coinbase_20261116T162358_b98d77.parquet` and kept running.
- After: raw **48,980** · malformed **1** · unique records **47,979** · unique trades **47,978**: as expected
- The row: partition 1 · offset 2748 · key `bad` · action `malformed` · raw `this is not JSON`
- Explanation: land everything, decide later. to_row() caught the JSON error and landed the text with action = 'malformed' instead of crashing, so the offset was stored and the lander moved on. A lander that crashed on it would read it again after every restart and block partition 1 (a poison message). The count excludes malformed rows from unique records and trades, so those did not change.

### D4 · The source drops
- Before: raw 48,980 · malformed 1 · unique records 47,979 · unique trades 47,978
- My prediction is that when Wi-Fi goes off, the producer will log that the connection failed and retry every 5 seconds. Then it will reconnect by itself when Wi-Fi comes back. Then the trades made while I was offline don't go through. Nothing will be duplicated, and the lander keeps running because Kafka is local.
- What I did: `python producer.py` at 17:27:17 (live), Wi-Fi off for about 30 s around 17:33, then on again. Screenshot: docs/d4_reconnect.png
- Producer log: 17:33:22 "The server closed the connection" → "Reconnecting in 5 s" → 17:33:27, :32, :37 "Connection failed: gaierror(8, 'nodename nor servname provided, or not known')" (no DNS without Wi-Fi) → 17:33:42 "Connected and subscribed" again, by itself. Sent so far before the drop: 10,095.
- After (17:34:44, producer and lander running): raw **60,117** · malformed 1 · unique records **59,116** · unique trades **59,113**
- The gap: the largest gap between two trades after 17:30 is **55 s** (last trade 17:32:48, next 17:33:43). Every other gap is about 1 s or less. The socket only noticed the drop at 17:33:22, about 34 s after the last trade, because nothing tells a WebSocket the network is gone until a ping fails.
- Unique records − unique trades = 3: the three `subscriptions` messages (one per connection: 16:34, 17:27, 17:33), which are Kafka records but not trades.
- Explanation: push vs. pull. The reconnect loop with a 5 s pause brought the producer back without me; nothing was duplicated (unique records = raw − 1,000 D1 copy − 1 malformed). But Coinbase has no replay, so the ~55 s of trades during the outage are lost for good. A source with history (like the earthquake catalogue) could fill that gap; this one cannot.

## 50,000 reached
- 17:34:44: **60,117 raw records landed** (target 50,000)

