# HKJC 熱錢流 — Phase 0 collector（自用）

目的：由今日（2026-11-02）開始，靜靜咁把馬會的**彩池投注額（投注額 / investment）**同**每匹馬賠率**
變化記錄落本地資料庫；非賽馬日慢速、賽馬日自動加密到 20 秒（閘前 10 秒），賽馬日並同時開
MQTT 推送錄音，之後就用呢批數據做「熱錢流」指標同 backtest。

只讀、不下注、無登入、無 API key。

## Repo 與本機部署

原始碼：https://github.com/alice-creator-lab/hkjc-moneyflow（`main`）

- Repo 只放**原始碼與設定**；`data/`（SQLite、快照、賠率）、`logs/`、`bin/`、`.pages/` 一律不入 repo，見 `.gitignore`。
- 本機工作副本：`~/.hermes/profiles/friend/workspace/hkjc-moneyflow`，改完直接 `git push`；換機就 `git clone` 返落嚟。
- 重建步驟：`git clone` → 按 `config.env` 改 `PYTHON` / `NODE` 路徑 → 自行下載 `cloudflared` 放入 `bin/`（見「即時版」）→ 跑 `./watchdog.sh`。歷史數據（`data/moneyflow.db`）不在 repo，要由舊機複製。
- 靜態快照是另一個 repo：https://github.com/alice-creator-lab/moneyflow-monitor，由 `pages_push.sh` 每 2 分鐘自動 commit，不要手動改。

## 檔案

| 檔案 | 作用 |
|---|---|
| `collect.py` | GraphQL collector（`--once` / `--daemon` / `--summary`） |
| `mqtt_recorder.mjs` | 賽馬日推送錄音（MQTT v5 over WebSocket，gzip JSONL） |
| `watchdog.sh` | 每 3 分鐘由 cron 檢查，死咗自動重開、日誌輪替 |
| `data/moneyflow.db` | SQLite：變動記錄（只在數值有變時寫入） |
| `data/snap/<date>.jsonl.gz` | 每次 poll 的壓縮快照（可重播） |
| `data/push/<date>_<venue>.jsonl.gz` | MQTT 原始訊息（賽馬日） |
| `data/query.txt` | 由 bet.hkjc.com bundle 抽出的原封 GraphQL query（會自動更新） |
| `logs/collector.log`、`logs/mqtt.log`、`logs/watchdog.log` | 日誌 |

## 取樣頻率（自動判斷）

| 情況 | 間隔 |
|---|---|
| 無賽事 / 未開售 | 600 秒 |
| 今日有賽事、未開售 | 120 秒 |
| 明日有賽事、已開售 | 60 秒 |
| 今日有賽事、已開售 | 20 秒 |
| 距開跑 15 分鐘內 | 10 秒 |
| 距開跑 5 分鐘內 | **10 秒**（只追即將開跑嗰場，唔再輪流掃其他場） |
| 距開跑 30 秒內 | **1 秒**（sprint：只問 WIN/PLA，回應約 78ms，避開 rate limit） |
| 賽馬日 + MQTT 推送 | 即時（每條訊息） |

Webapp 刷新頻率亦跟住開跑時間自動調整：>15 分鐘 15 秒、15 分鐘內 6 秒、
5 分鐘內 3 秒、30 秒內 1.5 秒。

## 賽果與凍結

- 每場跑完約 2 分鐘後，collector 會抓名次＋派彩寫入 `race_result`。**主路係 GraphQL**（`results_gql.py`，POST `https://info.cld.hkjc.com/graphql/base/`，query `RaceResultsProfile`）——一次 call 拎齊全場次，本地同越洋轉播都得；抓唔到（舊賽日已過 GQL 視窗）就 fallback 去 LocalResults HTML。
- ⚠️ 個 GraphQL 有 query whitelist（送錯字會 `WHITELIST_ERROR`），所以 query 文字要原封不動由 `data/results_query.json` replay，唔好自己改。
- 前端喺該場最頂顯示「賽果」卡：前 4 名（馬號／馬名／騎師／開跑賠率／勝負距離）＋ 獨贏、位置、連贏、位置Q 派彩。
- 有賽果嘅場次會被**凍結**：唔再重繪、唔再 poll（`refreshMs()` 回 `null`），collector 亦會將該場剔出輪詢名單；切換場次或賽事會重新開始。
- 轉播賽事（S1–S9）馬會冇本地賽果頁，`race_result` 會記 `ok:false`，賽果卡自動隱藏。

## 20 秒切片（最後 3 分鐘）

BAR 圖最後 3 分鐘切成 9 格，每格 20 秒一種顏色（`s1`…`s9`，熱度色階），再加 `tail`（anchor 之後殘留）。
Anchor **唔係排位時間**：池一凍結就取「首次達到最終金額」嗰刻做實際開跑，所以遲開跑都唔會扭曲。

官方排位時間 **T** 做界：T-3→T 每 20 秒一格（`s1`…`s9`，黃→紅），**T 之後每 10 秒一格**（`p1`…`p12`，紫→藍→綠）直到池真正凍結（＝實際開跑），最後嘅殘留入 `tail`。咁遲開跑嗰段額外資金就唔會再擠埋一格。

**只追下一場**：空檔每小時一次；只有 T-15 分鐘內才加密（T-15→20s、T-5→10s、T-60s→2s），而且衝刺時只拉當前 meeting。完賽超過 10 分鐘嘅場次（odds、彩池、賠率）一律停止抓取，唔會再寫 DB。

密集 poll 係**自適應**：T-70s 起每 **2 秒**一次（WIN／PLA），一直做到池真正關閉（最長 T+180s）。準時開跑嘅場次同以前一樣，遲開跑嘅場次先會自動延長，所以額外成本只喺需要嘅時候付。

馬會公開數據**冇**「入閘／紅燈」欄位（race status 只有 DEFINED/STARTED/CLOSED，pool status 只有 START_SELL→CLOSED，今日全日未見過 SUSPENDED），所以 R 只可以靠資金／賠率流動推。真要紅燈要靠 push feed（MQTT）——現時錄出嚟嘅 frame 未解得到，要下次賽馬日再試。

## 長期特徵表（後台儲，唔上畫面）

`race_signal` 每個（場次, 馬號）一行：7 段 band、share / share_prev、late_pct、delta_pct、賠率、名次、是否份額前三 / 尾段前三。由 `snapshot_signals.py` 每 10 分鐘把新完賽場次 freeze 落去，**retention 唔會清**，所以 raw odds／pool 兩日後被 purge 都仍然可以做長期回測。畫面（web/）完全冇改，照舊只顯示現有邏輯。

## 賽事狀態（唔用馬會原字）

馬會 API 賣緊飛時仍然報 `DEFINED`，跑完才轉 `CLOSED`，直接顯示會好混亂。所以 serve 自己計 `phase`：

| phase | 顯示 | 判斷 |
|---|---|---|
| `presale` | 未開售 | 未見 sale_open |
| `selling` | 開售中 | 有 sale_open、未到第一場開跑 |
| `racing` | 賽事進行中 | 已過第一場開跑、未完結 |
| `finished` | 已完結 | status=CLOSED 或最後一場開跑後 10 分鐘 |
| `imminent`（單場） | 最後衝刺 | 距該場開跑 ≤15 分鐘 |
| `jumped`（單場） | 已開跑 | 已過開跑時間但賽事未完結 |

## 固定網址（nginx 反代，推薦）

`/moneyflow/` 已經反代去 `127.0.0.1:8787`，所以有一條唔會變嘅公開網址：

```
http://47.83.216.177/moneyflow/?k=<token>
```

- 設定檔：`deploy/nginx-moneyflow.conf` → `/etc/nginx/default.d/moneyflow.conf`
- 要 sudo 安裝：`sudo cp ... && sudo nginx -t && sudo nginx -s reload`
- 仍然要 `?k=<token>`（無 token 回 401）；HTTP 冇 TLS，因為係裸 IP，冇 domain 就冇 cert。

## 即時版（唔經 GitHub Pages）

快照最快都只可以做到 2–3 分鐘延遲（push + Pages build）。要真正即時就用 tunnel 直連本機 server：

```
./live.sh          # 印出即時版 / 本機 / 快照三條 link
```

- `bin/cloudflared tunnel --url http://127.0.0.1:8787` 開 quick tunnel，URL 隨機，寫入 `data/live_url.txt`。
- watchdog 每次跑都會檢查 tunnel，死咗自動重開（重開後 URL 會變，`./live.sh` 會讀新的）。
- API 仍然要 `?k=<token>`；冇 token 回 401。
- 即時版刷新節奏：最後 1 分鐘 1 秒、≤5 分鐘 2 秒、≤15 分鐘 4 秒、其餘 12 秒。

## GitHub Pages 點解「唔郁」

Pages 每個檔案都有 `cache-control: max-age=600`，所以硬刷新之外，瀏覽器會用足 10 分鐘舊檔。處理方法：

1. `pages_push.sh` 由 cron **每 2 分鐘**跑（有變才 commit + push）。
2. 前端在快照模式**不靠 `<script src="data.js">`**，改用 `fetch('data.js?ts=…')`：新 query string 對 CDN 嚟講係 cache miss，所以拎到最新 build，每 30 秒（開跑前 20 分鐘內）或 120 秒重拉一次，直接重繪，唔需要 reload。
3. 預設選「開售中／賽事進行中」的賽事，唔會再因為舊 snapshot 停在已完結的場次。

改完 app.js 之後第一次仍然要硬刷新（Ctrl/Cmd+Shift+R）一次，因為舊 HTML 還被 CDN 快取住。

## 資料保留

`retention.py` 只保留近 2 個賽日嘅監察數據（poll / pool_change / odds_change /
dist_snapshot / combo_odds / runner_meta / race_meta / baseline / meeting）。
`hist_start`（往績、開跑賠率）**唔會刪**，因為上仗分析要用。collector 每 6 小時自動跑一次。

## 熱錢分佈（per-horse bet distribution）

`dist.py` 把彩池錢攤去每匹馬：

- WIN 池 × 隱含機率份額（1/賠率 正規化）
- PLA 池 × 位置賠率份額
- QIN / QPL：每個組合（`01,02`）用 1/賠率 正規化後分錢，兩匹馬各一半
- 四項相加 = 該馬的「熱錢」，即分佈 bar 的總高

collector 每輪（最多 3 場、輪流）抓 WIN/PLA/QIN/QPL，`combo_odds` 記組合賠率，
`dist_snapshot` 每場每 60 秒存一次分佈快照 → 前端可顯示「對比 x 分鐘前」的資金變化。

## 監察 webapp（serve.py）

```bash
python3 serve.py --port 8787 --host 0.0.0.0     # watchdog 會自動拉起
# 本機： http://127.0.0.1:8787/?k=<data/webapp_token.txt>
```

- `GET /api/summary` 賽事清單 + collector 健康
- `GET /api/meeting/<id>` 排位、賠率、彩池、走勢（前端 15 秒自動更新）
- 前端零依賴（`web/index.html` + `app.js` + `style.css`），相對路徑，可掛在任何路徑前綴（例如 nginx `/moneyflow/`）

想公開上網：nginx 已經有 port 80，加一個 location 反代即可（見 `deploy/nginx-moneyflow.conf`）。

## 常用指令

```bash
cd ~/.hermes/profiles/friend/workspace/hkjc-moneyflow

python3 collect.py --once --verbose   # 手動 poll 一次
python3 collect.py --summary          # 睇已記錄了幾多、投注額範圍、幾時開售
tail -f logs/collector.log            # 睇 daemon 運作

./watchdog.sh                          # 確認 daemon 在跑（cron 每 3 分鐘會叫）
```

賽馬日手動開推送錄音（daemon 通常會自動開）：

```bash
node mqtt_recorder.mjs --date 20261104 --venue hv --out data/push --verbose
```

## 解釋器（重要）

`/usr/bin/python3` 係 3.6，呢個 collector 要 3.10+；cron 嘅 PATH 又係最小。所以 `config.env` 釘死：

```
PYTHON=/home/admin/.hermes/tools/python-3.14.9+.../bin/python3
NODE=/usr/bin/node
```

`watchdog.sh` 會 source 佢；換機／升級時只需改 `config.env`。

## 停止／重啟

```bash
touch data/.stop      # daemon 會在下一次檢查（≤2 秒）自行退出
./watchdog.sh         # 或等 cron 3 分鐘內自動重開
```

## 安裝 cron（已安裝）

```
*/3 * * * * /home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/watchdog.sh
@reboot     /home/admin/.hermes/profiles/friend/workspace/hkjc-moneyflow/watchdog.sh
```

## 資料表

- `poll`：每次 poll 一行（status、totalInvestment、彩池數、runner 數、耗時）
- `pool_change`：彩池投注額／開售狀態**有變**才寫（`odds_type`, `leg`, `inv`, `sell`, `last_upd`）
- `odds_change`：每匹馬 winOdds **有變**才寫
- `event`：`meeting_first_seen`、`sale_open` 等事件
- `meeting`：首次／最後見到時間

查詢例子：

```sql
-- 每場 WIN 池投注額變化
SELECT ts, inv FROM pool_change
 WHERE meeting_id='2026-11-04_HV' AND odds_type='WIN' AND leg='1' ORDER BY ts;

-- 賽日總投注額走勢
SELECT ts, total_inv FROM poll WHERE meeting_id='2026-11-04_HV' ORDER BY ts;
```

## 賽馬日 checklist

1. 開賽前 2 小時：`python3 collect.py --summary` 確認 `sale open` 出現、`totalInvestment` > 0。
2. `ps -p $(cat data/mqtt.pid)` 確認推送錄音在跑（daemon 自動開；冇就手動開）。
3. 完賽後：`ls -la data/push/` 睇檔案大小，`zcat data/push/*.jsonl.gz | wc -l` 睇訊息數。
4. 若見到 `WHITELIST_ERROR`：`rm data/query.txt && python3 collect.py --once` 會自動重新抽取。

## 注意

- 資料版權屬香港賽馬會；本工具**只供個人研究**，不要公開發佈原始賠率／投注額數據，也不要轉售。
- 馬會前端一旦改版，query 字串就要重新抽取（collector 會自動做，但 MQTT topic 若有變就要人手跟）。
- 非賽馬日 `investment` 係 `null`、`totalInvestment` 係 `0` 係正常，唔係壞。
