# 量化：价值因子和质量因子设计方案
# Finnhub 财务数据接入

> 目标：为策略引擎接入 Finnhub 财务数据，使策略能引用 **PE（价值因子）、ROE/负债率（质量因子）** 字段。
> 适用：美股多因子策略 `custom_us_multifactor_v2` 的价值/质量因子扩展。
> 本文档供引擎开发者落地实施用。
>
> **状态：已用真实 API Key 实测验证**（2026-11-16）。接口连通、字段可用、批量拉取、因子排名逻辑均已验证通过。

---

## 1. 背景与目标

当前多因子策略 `custom_us_multifactor_v2` 只使用了 **动量 + 低波动 + 趋势** 三个价格类因子，原因在于策略引擎的日线特征表里**没有财务数据**（PE/PB/ROE/资产负债率），无法实现：

- **价值因子**：PE 低的股票排名前 30
- **质量因子**：被低估（低 PE/PB）且负债少（低负债率）的股票排名前 30

本方案通过接入 **Finnhub**（免费 REST API，60 次/分钟）获取美股财务指标，同步进策略引擎的日线特征表，使策略能引用这些字段。

---

## 2. Finnhub API 端点

### 2.1 公司基本指标（核心）

```
GET https://finnhub.io/api/v1/stock/metric?symbol=AAPL&metric=all&token=<API_KEY>
```

返回 JSON，关键字段在 `metric` 对象内：

```json
{
  "metric": {
    "peTTM": 28.5,
    "pb": 6.2,
    "roeTTM": 0.35,
    "totalDebtToEquity": 1.8,
    "currentRatio": 1.5,
    "revenueGrowthTTMYoy": 0.12,
    ...
  }
}
```

### 2.2 认证方式

- Header：`X-Finnhub-Token: <API_KEY>`（推荐）
- 或 Query 参数：`?token=<API_KEY>`

### 2.3 免费额度

| 项目 | 免费版 | 付费版 |
|---|---|---|
| 调用频率 | 60 次/分钟 | 300 次/分钟 |
| 美股实时行情 | ✅ | ✅ |
| 公司基本面（PE/ROE等） | ✅ | ✅ |
| 费用 | $0 | $11.99–99.99/月 |

60 次/分钟可支撑逐票查询。全市场扫描需分批（如 500 只票 ≈ 8 分钟/轮）。

---

## 3. 字段映射（Finnhub → 引擎特征列）

> 已用真实 API 验证（2026-11-16）：`/stock/metric` 返回 133 个字段。负债/流动性字段在 Finnhub 里带斜杠命名（`totalDebt/totalEquityAnnual`），非 `totalDebtToEquity`。

| Finnhub 字段（实测） | 引擎特征列 | 类型 | 因子用途 | 实测值(AAPL) |
|---|---|---|---|---|
| `peTTM` | `pe` | float | 价值因子（PE 低→排名前 30） | 39.17 |
| `pb` | `pb` | float | 估值辅助（质量因子低估判断） | 46.19 |
| `roeTTM` | `roe` | float | 质量因子（盈利质量高） | 127.61 |
| `totalDebt/totalEquityAnnual` | `debt_to_equity` | float | 质量因子（负债少） | 0.7277 |
| `currentRatioAnnual` | `current_ratio` | float | 辅助（流动性） | 1.0232 |

> 建议只保留策略实际用到的列。`pe`/`roe`/`debt_to_equity` 是核心，`pb`/`current_ratio` 可选。
> 注：`roeTTM` 为百分比数值（如 127.61 表示 127.61%），策略引用时可按需 `/100` 归一。

---

## 4. 数据流向架构

```
Finnhub REST API (stock/metric)
        │
        ▼ ① 拉取
[同步脚本 fetch_finnhub_fundamentals.py]
        │
        ▼ ② 清洗/映射 (peTTM→pe, roeTTM→roe, ...)
[数据清洗层]
        │
        ▼ ③ 落库
[fundamentals 表 / parquet]
  (按 symbol 维度, 公司级指标, 按财报周期更新)
        │
        ▼ ④ JOIN
[日线特征表 daily_features]  ←LEFT JOIN→  [fundamentals ON symbol]
        │
        ▼ ⑤ 策略引用
[filter_history / filter 里 pl.col("pe"), pl.col("roe"), pl.col("debt_to_equity")]
```

### 4.1 关键设计决策

1. **财务指标是公司级，不是每日变化**：按 `symbol` 维度存储，每只股票一条（或按最新财报周期更新），**不需要**按日期冗余存储。
2. **JOIN 时机**：在日线特征表构建时，用 `symbol` 做 LEFT JOIN，把 `pe`/`roe`/`debt_to_equity` 注入日线行。
3. **更新频率**：财务指标随财报季变化，建议**每日收盘后**刷新一次（或每周），拉取全市场/自选股最新指标。
4. **容错**：缺失财务指标的股票，`pe`/`roe`/`debt_to_equity` 为 null，策略需 `.fill_null()` 处理或跳过（避免 500，参考此前 ColumnNotFound 教训）。

---

## 5. 同步脚本设计（fetch_finnhub_fundamentals.py）

```python
"""Finnhub 财务数据同步脚本（示例骨架，供引擎开发者落地）。
拉取公司基本指标 → 清洗 → 落库到 fundamentals 表。
"""
import requests
import time
from typing import Dict

FINNHUB_BASE = "https://finnhub.io/api/v1"
API_KEY = "<YOUR_FINNHUB_API_KEY>"  # 从环境变量读取, 不要硬编码

def fetch_metrics(symbol: str) -> Dict:
    """拉取单只股票的基本财务指标."""
    resp = requests.get(
        f"{FINNHUB_BASE}/stock/metric",
        params={"symbol": symbol, "metric": "all"},
        headers={"X-Finnhub-Token": API_KEY},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("metric", {})

def map_fields(metric: Dict) -> Dict:
    """清洗映射: Finnhub字段 → 引擎特征列名 (实测字段名)."""
    return {
        "symbol": None,  # 由调用方传入
        "pe": metric.get("peTTM"),
        "pb": metric.get("pb"),
        "roe": metric.get("roeTTM"),
        "debt_to_equity": metric.get("totalDebt/totalEquityAnnual"),
        "current_ratio": metric.get("currentRatioAnnual"),
    }

def sync_fundamentals(symbols: list[str], rate_limit=60):
    """批量同步（注意限流: 60次/分钟, 需要 sleep 节流）."""
    results = []
    for i, sym in enumerate(symbols):
        try:
            metric = fetch_metrics(sym)
            row = map_fields(metric)
            row["symbol"] = sym
            results.append(row)
        except Exception as e:
            print(f"[warn] {sym} 拉取失败: {e}")
        # 限流: 60次/分钟 → 每轮间隔约1秒
        if (i + 1) % rate_limit == 0:
            time.sleep(60)
    return results
```

**要点**：
- API Key 从环境变量读，**不硬编码**（安全）。
- 限流：60 次/分钟，需节流 `sleep`。
- 容错：单票失败不影响整体，记录 warn 继续。

---

## 6. 策略侧因子逻辑（数据接入后生效）

> 前提：日线特征表已 JOIN 进 `pe`/`roe`/`debt_to_equity` 列。此逻辑在字段存在时才启用，避免 ColumnNotFound。

### 6.1 价值因子（PE 低 → 排名前 30）

```python
# 在 filter 或 filter_history 中:
# PE 为正且最低的股票
value_ok = (
    (pl.col("pe") > 0)  # 排除亏损(PE<0)公司
    & pl.col("pe").is_not_null()
)
# 排名: PE 越低越好 → 升序排名, 取前 30
value_rank = pl.col("pe").rank("ordinal")  # 按 PE 升序
```

### 6.2 质量因子（被低估且负债少 → 排名前 30）

```python
# 低估: 低 PE + 低 PB; 负债少: 低 debt_to_equity
# 综合质量分 = 低PE排名 + 低PB排名 + 低负债排名 (越低越好)
quality_score = (
    pl.col("pe").rank("ordinal")
    + pl.col("pb").rank("ordinal")
    + pl.col("debt_to_equity").rank("ordinal")
)
# 取 quality_score 前 30 (综合最优)
```

### 6.3 与现有因子组合

价值/质量因子作为**附加过滤或加权**，与现有动量/低波动/趋势叠加：

```python
final_entry = (
    raw_entry                    # 趋势+超跌
    & momentum_ok                # 动量过滤
    & lowvol_ok                  # 低波动过滤
    & value_ok                   # 价值因子(PE为正且排名靠前)
    & quality_ok                 # 质量因子(低估且低负债)
)
```

---

## 7. 落地步骤（给引擎开发者）

1. **注册 Finnhub**：https://finnhub.io/register → 获取免费 API Key（60 次/分钟）。
2. **写同步脚本**：按 §5 骨架实现，配置 API Key 到环境变量。
3. **落库**：创建 `fundamentals` 表（按 symbol），写入清洗后的 `pe`/`roe`/`debt_to_equity`。
4. **JOIN 进日线特征表**：构建 daily_features 时 LEFT JOIN `fundamentals ON symbol`，注入特征列。
5. **声明 REQUIRED_FEATURES**：策略里加入 `{"pe", "roe", "debt_to_equity"}`。
6. **验证**：先跑单票（AAPL）确认字段映射正确，再跑全市场。
7. **策略启用**：加入 §6 的价值/质量因子逻辑，验证不报 ColumnNotFound。

---

## 8. 风险与注意

| 风险 | 说明 | 缓解 |
|---|---|---|
| **数据接入是系统级改动** | 需要改引擎数据链路（JOIN 特征表），非策略文件可完成 | 由引擎开发者按本方案落地 |
| **免费额度限制** | 60 次/分钟，全市场需分批 | 分批拉取 + 节流；或升级付费 |
| **字段缺失** | 部分股票 PE/ROE 为 null | 策略 `.fill_null()` 或跳过，避免 500 |
| **财务数据延迟** | 财报季更新，非实时 | 每日/每周刷新，接受 T+1 |
| **API Key 安全** | 不可硬编码 | 环境变量读取，脱敏展示 |

---

## 9. 结论

- **Finnhub 免费版可提供**价值因子（PE）和质量因子（ROE/负债率）所需字段，60 次/分钟足够逐票查询。
- **关键约束**：数据必须 JOIN 进引擎日线特征表才能被策略引用，这是**系统级改动**，需引擎开发者落地。
- 落地后，价值/质量因子逻辑（§6）即可加入 `custom_us_multifactor_v2`，与现有动量/低波动/趋势叠加。

---

*文档版本：v1.0（2026-11-16）*

