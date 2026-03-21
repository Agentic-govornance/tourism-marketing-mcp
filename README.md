# CCDM MCP

日本の観光政策高度化のためのCCDMデータAPI。

## 利用資格
行政機関・DMO・観光協会・研究機関担当者に限定。

## アクセス申請
https://ccdm-mcp.teddykmk.workers.dev/apply

## 使い方
1. `get_dataset_url` でParquet URLを取得
2. DuckDBで直接クエリ

```python
import duckdb
url = "https://ccdm-mcp.teddykmk.workers.dev/data/v1/corpus_index.parquet"
duckdb.query(f"""
  SELECT narrative, COUNT(*) as n
  FROM read_parquet('{url}')
  WHERE list_contains(string_split(destinations,','), 'niigata')
    AND market = 'FR'
  GROUP BY narrative ORDER BY n DESC
""").df()
```

## 提供データ
| データセット | 内容 |
|---|---|
| corpus_index | メディア・SNS・OTA・タリフ・政府統計インデックス（body除去済み） |
| integrated_panel_v14 | FR×瀬戸内 統合時系列パネル |
| setouchi_market_panel | 瀬戸内×4市場 市場パネル |
| granger_v13 | Granger因果分析結果 |
| dmo_database | 世界DMOデータベース |

研究代表：Rei Ogata (ORCID: 0009-0003-0506-0295)
