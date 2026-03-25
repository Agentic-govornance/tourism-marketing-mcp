# Tourism Marketing MCP

観光マーケティング・政策立案のためのデータレイクAPI。メディア・SNS・OTA・政府統計を統合した639万件のコーパスをMCP経由で提供します。

## 何ができるか

- 市場別ナラティブトレンドの時系列分析
- 目的地別メディア露出量・ナラティブ構造の比較
- ナラティブ変数→市場指標（価格・訪問者数）のGranger因果分析
- 複数市場横断CCDMシグナル比較

## 利用資格

行政機関・DMO・観光協会・研究機関担当者に限定。

## アクセス申請

https://ccdm.patent-space.dev/apply

## 使い方

1. `get_dataset_url` でParquet URLを取得
2. DuckDBで直接クエリ
```python
import duckdb
url = "https://ccdm.patent-space.dev/data/v1/corpus_index.parquet"
duckdb.query(f"""
  SELECT narrative, COUNT(*) as n
  FROM read_parquet('{url}')
  WHERE market = 'FR'
  GROUP BY narrative ORDER BY n DESC
""").df()
```

## 提供データ

### コーパスインデックス（639万件）

メディア・SNS・OTA・政府統計のメタデータインデックス（本文除去済み）。

| 軸 | 内訳 |
|---|---|
| 市場 | INTL 228万 / TW 214万 / US 110万 / FR 62万 / AU 19万 / JP 5.6万 |
| メディア種別 | magazine 376万 / SNS 228万 / forum 23万 / review 6.8万 / blog 2.9万 |
| ナラティブ | culture_depth / nature_outdoor / gastronomy / template 他 |

### パネルデータ・分析結果

| データセット | 内容 | 規模 |
|---|---|---|
| 統合時系列パネル | ナラティブ×市場指標 統合パネル | 122四半期 × 957変数 |
| 市場パネル | 複数市場統合パネル | 88四半期 × 32変数 |
| CCDMシグナル | 市場横断シグナル比較 | 44四半期 × 複数市場 |
| Granger因果分析結果 | ナラティブ→市場指標の因果経路 | 1,119ペア |
| 世界DMOデータベース | 観光局マスター | 1,869件 |
