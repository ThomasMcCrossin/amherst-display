# Judge leaderboard: highlight quality (39 incidents, judges: sonnet-1, sonnet-2, sonnet-3)

Headline = mean overall highlight score (0-10) from the blinded judges, with spend per incident beside it. Sub-scores and flags are diagnostics.

| contestant | incidents | **highlight score** | tokens / incident | cost $ / incident | wall s / incident | build-up | moment | ending | cleanliness | win rate (equiv.) | starts late (missing build-up) | wrong event | missing event | ends early | too long | dropped (event seen) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| pi-glm-5.3-flash | 35 | **7.09** | 672161 | 0.0 | 277.8 | 7.31 | 7.96 | 7.34 | 7.38 | 0.295 | 0.038 | 0.0 | 0.038 | 0.051 | 0.342 | 4 (0) |
| pi-deepseek-v4.1-flash-lean | 36 | **6.39** | 359524 | 0.0 | 109.1 | 6.77 | 7.22 | 6.36 | 6.79 | 0.231 | 0.11 | 0.061 | 0.098 | 0.146 | 0.378 | 3 (0) |
| cu-deepseek-v4.1-flash | 36 | **6.04** | 814072 | 4.6679 | 504.9 | 5.75 | 7.46 | 6.76 | 7.14 | 0.171 | 0.222 | 0.049 | 0.074 | 0.111 | 0.16 | 3 (0) |
| pi-deepseek-v4.1-flash | 38 | **5.81** | 1317360 | 0.0 | 382.8 | 5.54 | 7.1 | 6.28 | 6.84 | 0.145 | 0.233 | 0.058 | 0.116 | 0.116 | 0.163 | 1 (0) |
| pi-gemma4-31b | 37 | **5.6** | 199462 | 0.0 | 123.2 | 6.0 | 6.42 | 5.6 | 6.48 | 0.234 | 0.095 | 0.167 | 0.214 | 0.143 | 0.155 | 2 (0) |
| escalate (hand-off 59%) | 36 | **5.39** | 229265 | 0.0 | 93.4 | 5.49 | 6.85 | 5.44 | 7.02 | 0.13 | 0.232 | 0.085 | 0.122 | 0.341 | 0.159 | 3 (0) |
| engine | 39 | **4.56** | None | None | None | 5.9 | 5.99 | 4.38 | 5.96 | 0.154 | 0.159 | 0.091 | 0.17 | 0.42 | 0.193 | 0 (0) |
| api-deepseek-flash | 39 | **3.88** | 10650 | None | 22.5 | 3.67 | 5.58 | 4.25 | 6.12 | 0.051 | 0.432 | 0.148 | 0.261 | 0.386 | 0.023 | 0 (0) |

Judge agreement: best pick 0.492 over 59 judge pairs; mean |score difference| 0.84.
