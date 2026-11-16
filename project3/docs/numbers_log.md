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

## Four numbers (four_numbers.py)
| When | raw | malformed | unique Kafka records | unique product_id+trade_id |
|---|---|---|---|---|

## Chaos round
