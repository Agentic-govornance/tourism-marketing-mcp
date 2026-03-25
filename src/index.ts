import { Hono } from 'hono'
import { cors } from 'hono/cors'

interface Env {
  CCDM_KV: KVNamespace
  CCDM_R2: R2Bucket
  ADMIN_SECRET: string
  R2_BASE: string
}

const app = new Hono<{ Bindings: Env }>()
app.use('*', cors({ origin: '*' }))

// ─── ユーティリティ ────────────────────────────────────

function genKey() {
  const c = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
  const a = new Uint8Array(32)
  crypto.getRandomValues(a)
  return 'ccdm_' + [...a].map(b => c[b % c.length]).join('')
}

async function auth(req: Request, env: Env): Promise<{ok:boolean; err?:string}> {
  const key = req.headers.get('X-API-Key')
    || req.headers.get('Authorization')?.replace('Bearer ','')
    || new URL(req.url).searchParams.get('api_key')
  if (!key) return {ok:false, err:'API key required. Apply: https://ccdm.patent-space.dev/apply'}

  const d = await env.CCDM_KV.get<any>(`key:${key}`, 'json')
  if (!d)                                  return {ok:false, err:'Invalid API key'}
  if (d.status !== 'active')               return {ok:false, err:'API key suspended'}
  if (new Date(d.expires_at) < new Date()) return {ok:false, err:'API key expired'}

  d.usage_count++; d.last_used = new Date().toISOString()
  await env.CCDM_KV.put(`key:${key}`, JSON.stringify(d))
  return {ok:true}
}

// ─── R2プロキシ（/data/v1/*） ──────────────────────────
app.get('/data/v1/:filename', async c => {
  const filename = c.req.param('filename')
  const object = await c.env.CCDM_R2.get(`v1/${filename}`)
  if (!object) return c.json({error: 'Not found'}, 404)

  const headers = new Headers()
  headers.set('Content-Type', 'application/octet-stream')
  headers.set('Cache-Control', 'public, max-age=3600')
  headers.set('Access-Control-Allow-Origin', '*')
  object.writeHttpMetadata(headers)

  return new Response(object.body, { headers })
})

// ─── ダッシュボード ────────────────────────────────────
app.get('/dashboard', async c => {
  const secret = c.req.query('secret')
  if (!secret || secret !== c.env.ADMIN_SECRET) {
    return c.json({error: 'Unauthorized'}, 401)
  }
  const R2 = c.env.R2_BASE
  return c.html(DASHBOARD_HTML.replace(/__R2_BASE__/g, R2))
})

// ─── ヘルスチェック ────────────────────────────────────
app.get('/', c => c.json({
  name: 'CCDM MCP', version: '2.0.0',
  description: '日本の観光政策高度化のためのCCDMデータAPI',
  apply: 'https://ccdm.patent-space.dev/apply',
}))

// ─── 申請フォーム（HTML）─────────────────────────────
app.get('/apply', c => c.html(`<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8"><title>CCDM MCP アクセス申請</title>
<style>
body{font-family:sans-serif;max-width:560px;margin:48px auto;padding:0 20px}
h1{color:#1e2761}
.note{background:#f0f4ff;border-left:4px solid #1e2761;padding:12px 16px;margin:16px 0;font-size:14px}
label{display:block;margin-top:16px;font-weight:600;font-size:14px}
input,select,textarea{width:100%;padding:8px;margin-top:4px;border:1px solid #ccc;box-sizing:border-box}
button{margin-top:24px;background:#1e2761;color:#fff;padding:12px 28px;border:none;cursor:pointer;font-size:15px}
#res{margin-top:20px;padding:16px;background:#e8f5e9;display:none}
</style></head><body>
<h1>CCDM MCP アクセス申請</h1>
<div class="note">
  <strong>対象：国・都道府県・市町村の行政担当者、観光局・DMO担当者</strong><br>
  承認後、APIキーをご登録メールアドレスにお送りします（3営業日以内）。
</div>
<form id="f">
  <label>所属組織名 *<input name="organization" required placeholder="例：新潟県観光企画課"></label>
  <label>担当者氏名 *<input name="contact_name" required></label>
  <label>連絡先メール（公式） *<input name="contact_email" type="email" required></label>
  <label>組織区分 *
    <select name="org_type" required>
      <option value="">選択してください</option>
      <option value="government">行政機関（国・都道府県・市町村）</option>
      <option value="dmo">観光局・DMO・観光協会</option>
      <option value="research_institution">大学・研究機関</option>
    </select>
  </label>
  <label>都道府県<input name="prefecture" placeholder="例：新潟県"></label>
  <label>利用目的 *<textarea name="purpose" required rows="3"></textarea></label>
  <button type="submit">申請する</button>
</form>
<div id="res"></div>
<script>
document.getElementById('f').onsubmit=async e=>{
  e.preventDefault()
  const r=await fetch('/api/apply',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify(Object.fromEntries(new FormData(e.target)))})
  const j=await r.json()
  const d=document.getElementById('res'); d.style.display='block'
  d.innerHTML='<strong>申請ID: '+j.application_id+'</strong><br>'+j.message
}
</script></body></html>`))

// ─── 申請API ─────────────────────────────────────────
app.post('/api/apply', async c => {
  const body = await c.req.json()
  const id = `app_${Date.now()}_${Math.random().toString(36).slice(2,8)}`
  await c.env.CCDM_KV.put(`application:${id}`, JSON.stringify({
    id, ...body, applied_at: new Date().toISOString(), status: 'pending'
  }))
  const list = await c.env.CCDM_KV.get<string[]>('applications:pending','json') || []
  list.push(id)
  await c.env.CCDM_KV.put('applications:pending', JSON.stringify(list))
  return c.json({
    application_id: id,
    message: '申請を受け付けました。3営業日以内にAPIキーをメールでお送りします。',
  })
})

// ─── 管理者：承認 ─────────────────────────────────────
app.post('/admin/approve', async c => {
  const {application_id, admin_secret} = await c.req.json()
  if (admin_secret !== c.env.ADMIN_SECRET) return c.json({error:'Unauthorized'}, 401)

  const appData = await c.env.CCDM_KV.get<any>(`application:${application_id}`, 'json')
  if (!appData) return c.json({error:'Not found'}, 404)

  const apiKey = genKey()
  await c.env.CCDM_KV.put(`key:${apiKey}`, JSON.stringify({
    organization: appData.organization,
    contact_name: appData.contact_name,
    contact_email: appData.contact_email,
    org_type: appData.org_type,
    prefecture: appData.prefecture,
    approved_at: new Date().toISOString(),
    status: 'active',
    usage_count: 0,
    expires_at: new Date(Date.now() + 365*24*60*60*1000).toISOString(),
  }))
  appData.status = 'approved'
  await c.env.CCDM_KV.put(`application:${application_id}`, JSON.stringify(appData))
  return c.json({success: true, api_key: apiKey, organization: appData.organization})
})

app.get('/admin/applications', async c => {
  if (c.req.header('X-Admin-Secret') !== c.env.ADMIN_SECRET) return c.json({error:'Unauthorized'}, 401)
  const ids = await c.env.CCDM_KV.get<string[]>('applications:pending','json') || []
  const apps = await Promise.all(ids.map(id => c.env.CCDM_KV.get(`application:${id}`,'json')))
  return c.json({applications: apps.filter(Boolean)})
})

// ─── MCP ─────────────────────────────────────────────
app.get('/mcp', c => c.text('CCDM MCP v2.0 — POST /mcp for JSON-RPC'))

app.post('/mcp', async c => {
  const {jsonrpc, id, method, params} = await c.req.json()

  const PUBLIC = ['initialize','notifications/initialized','tools/list']
  const isPublic = PUBLIC.includes(method)
    || (method==='tools/call' && params?.name==='apply_for_access')

  if (!isPublic) {
    const {ok, err} = await auth(c.req.raw, c.env)
    if (!ok) return c.json({jsonrpc, id, error:{code:-32001, message:err}}, 401)
  }

  if (method === 'initialize') return c.json({jsonrpc, id, result:{
    protocolVersion:'2024-11-05', capabilities:{tools:{}},
    serverInfo:{name:'ccdm-mcp', version:'2.0.0'},
  }})

  if (method === 'tools/list') return c.json({jsonrpc, id, result:{tools: TOOLS}})

  if (method === 'tools/call') {
    const result = await handleTool(params.name, params.arguments||{}, c.env)
    return c.json({jsonrpc, id, result:{
      content:[{type:'text', text:JSON.stringify(result,null,2)}]
    }})
  }

  return c.json({jsonrpc, id, error:{code:-32601, message:`Unknown: ${method}`}})
})

// ─── ツール定義 ───────────────────────────────────────
const TOOLS = [
  {
    name: 'list_datasets',
    description: '利用可能なデータセット一覧・スキーマ・DuckDB接続方法を返す',
    inputSchema: {type:'object', properties:{}, required:[]},
  },
  {
    name: 'get_dataset_url',
    description: '指定データセットのParquet URLを返す。AgentはこのURLでDuckDBから直接クエリする。',
    inputSchema: {
      type: 'object',
      properties: {
        dataset: {
          type: 'string',
          enum: ['corpus_index','integrated_panel_v14','setouchi_market_panel','market_signals_4market','granger_v13','dmo_database'],
          description: 'データセット名',
        },
      },
      required: ['dataset'],
    },
  },
  {
    name: 'suggest_query',
    description: '分析目的に応じたDuckDBクエリ例を返す',
    inputSchema: {
      type: 'object',
      properties: {
        goal: {type:'string', description:'分析したいこと（例: niigataのFR市場ナラティブ分布 2020-2024年）'},
      },
      required: ['goal'],
    },
  },
  {
    name: 'describe_schema',
    description: '指定データセットの列定義・値の種類・例を返す',
    inputSchema: {
      type: 'object',
      properties: {
        dataset: {type:'string', enum:['corpus_index','integrated_panel_v14','setouchi_market_panel','market_signals_4market','granger_v13','dmo_database']},
      },
      required: ['dataset'],
    },
  },
  {
    name: 'apply_for_access',
    description: 'MCPアクセス申請を送信する（認証不要）。行政・DMO担当者向け。',
    inputSchema: {
      type: 'object',
      properties: {
        organization:  {type:'string', description:'所属組織名'},
        contact_name:  {type:'string', description:'担当者氏名'},
        contact_email: {type:'string', description:'連絡先メール（公式）'},
        org_type:      {type:'string', enum:['government','dmo','research_institution']},
        prefecture:    {type:'string', description:'都道府県'},
        purpose:       {type:'string', description:'利用目的'},
      },
      required: ['organization','contact_name','contact_email','org_type','purpose'],
    },
  },
]

// ─── スキーマ定義 ─────────────────────────────────────
const SCHEMAS: Record<string,any> = {
  corpus_index: {
    description: 'CCDMコーパスインデックス（body/title除去済み）。6.39M件。destinationsでデスティネーションフィルタ可能。',
    columns: {
      source_id:      '媒体識別子（匿名化済み）',
      market:         '市場: FR/US/TW/AU/JP/ES/IT/DE/KR',
      date_published: '発行日 YYYY-MM-DD',
      lang:           '言語コード',
      word_count:     '文字数',
      medium_type:    'magazine/sns/forum/review/ota_listing/blog/video/government_stat',
      media_category: 'luxury/travel_specialist/lifestyle/japan_specialist/news_general/b2b',
      type_ab:        'A=一次取材 / B=二次引用 / null',
      actor_type:     'professional_media/community_interpreter/social_diffusion/consumer_experience/market_actor/official_stat',
      narrative:      'culture_depth/nature_outdoor/gastronomy/template/uncategorized',
      destinations:   'カンマ区切り (例: niigata,sado)',
      year:           '発行年（整数）',
      pro_private:    'professional/private',
    },
    filter_tip: "list_contains(string_split(destinations,','), 'niigata')",
  },
  integrated_panel_v14: {
    description: 'FR市場×瀬戸内の統合時系列パネル（集計済み）。121Q×957変数。',
    key_variables: {
      template_score_q: 'CCDMテンプレートスコア（四半期）',
      experience_depth_q: 'CCDM深度スコア（四半期）',
      agency_japan_price_median_q: '旅行代理店価格中央値',
      gt_naoshima: 'Google Trends 直島',
    },
  },
  market_signals_4market: {
    description: 'FR/TW/AU/US 4市場CCDMシグナル比較パネル。44四半期（2015Q1-2025Q4）×20変数。',
    columns: {
      quarter: '四半期（例: 2015Q1）',
      fr_n: 'FR記事数', fr_template: 'FRテンプレートスコア', fr_depth: 'FR深度スコア', fr_entropy: 'FRエントロピー',
      tw_n: 'TW記事数', tw_template: 'TWテンプレートスコア', tw_depth: 'TW深度スコア', tw_entropy: 'TWエントロピー', tw_japan_pct: 'TW日本比率',
      us_n: 'US記事数', us_template: 'USテンプレートスコア', us_depth: 'US深度スコア', us_entropy: 'USエントロピー', us_japan_pct: 'US日本比率',
      au_n: 'AU記事数', au_template: 'AUテンプレートスコア', au_depth: 'AU深度スコア', au_entropy: 'AUエントロピー', au_japan_pct: 'AU日本比率',
    },
  },
  granger_v13: {
    description: 'Granger因果分析結果。1119ペア。',
    columns: {
      variable_x: '原因変数', variable_y: '結果変数',
      lag_quarters: 'ラグ（四半期）', p_value: 'p値',
      is_significant: '有意フラグ (1=p<0.05)',
    },
  },
}

// ─── ツール実行 ───────────────────────────────────────
async function handleTool(name: string, args: any, env: Env): Promise<unknown> {
  const base = env.R2_BASE

  switch(name) {
    case 'list_datasets':
      return {
        datasets: [
          { id:'corpus_index',          url:`${base}/corpus_index.parquet`,          rows:'~6.39M', description:'CCDMコーパスインデックス（body/title除去済み）' },
          { id:'integrated_panel_v14',  url:`${base}/integrated_panel_v14.parquet`,  rows:121,      description:'FR市場×瀬戸内 統合時系列パネル' },
          { id:'setouchi_market_panel', url:`${base}/setouchi_market_panel.parquet`, rows:88,       description:'瀬戸内×4市場 市場パネル' },
          { id:'market_signals_4market', url:`${base}/market_signals_4market.parquet`, rows:44,       description:'FR/TW/AU/US 4市場CCDMシグナル比較パネル' },
          { id:'granger_v13',           url:`${base}/granger_v13.parquet`,           rows:1119,     description:'Granger因果分析結果' },
          { id:'dmo_database',          url:`${base}/dmo_database.parquet`,          rows:1869,     description:'世界DMOデータベース' },
        ],
        usage: 'get_dataset_urlでURLを取得 → DuckDBで直接クエリ',
        example: `duckdb -c "SELECT * FROM read_parquet('${base}/corpus_index.parquet') LIMIT 5"`,
      }

    case 'get_dataset_url': {
      const map: Record<string,string> = {
        corpus_index:          'corpus_index.parquet',
        integrated_panel_v14:  'integrated_panel_v14.parquet',
        setouchi_market_panel: 'setouchi_market_panel.parquet',
        market_signals_4market: 'market_signals_4market.parquet',
        granger_v13:           'granger_v13.parquet',
        dmo_database:          'dmo_database.parquet',
      }
      const file = map[args.dataset]
      if (!file) return {error:`Unknown dataset: ${args.dataset}`}
      const url = `${base}/${file}`
      const schema = SCHEMAS[args.dataset]
      return {
        dataset: args.dataset,
        url,
        format: 'parquet / snappy',
        schema: schema || {},
        example: args.dataset === 'corpus_index'
          ? `SELECT narrative, COUNT(*) as n FROM read_parquet('${url}') WHERE list_contains(string_split(destinations,','),'niigata') AND market='FR' GROUP BY narrative ORDER BY n DESC`
          : `SELECT * FROM read_parquet('${url}') LIMIT 10`,
      }
    }

    case 'describe_schema': {
      const schema = SCHEMAS[args.dataset]
      if (!schema) return {error:`Unknown: ${args.dataset}`}
      return schema
    }

    case 'suggest_query': {
      const g = (args.goal||'').toLowerCase()
      const url = `${base}/corpus_index.parquet`

      if (g.includes('ナラティブ') || g.includes('narrative'))
        return {query:`SELECT narrative, market, COUNT(*) as n, ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(PARTITION BY market),1) as pct FROM read_parquet('${url}') WHERE list_contains(string_split(destinations,','),'niigata') AND year BETWEEN 2020 AND 2024 GROUP BY narrative, market ORDER BY market, n DESC`}

      if (g.includes('type') || g.includes('一次'))
        return {query:`SELECT year, market, type_ab, COUNT(*) as n FROM read_parquet('${url}') WHERE list_contains(string_split(destinations,','),'niigata') GROUP BY year, market, type_ab ORDER BY year, market`}

      if (g.includes('granger') || g.includes('因果'))
        return {query:`SELECT variable_x, variable_y, lag_quarters, p_value FROM read_parquet('${base}/granger_v13.parquet') WHERE is_significant=1 ORDER BY p_value LIMIT 20`}

      return {
        query: `SELECT * FROM read_parquet('${url}') LIMIT 5`,
        tips: [
          `destinationsフィルタ: list_contains(string_split(destinations,','), 'niigata')`,
          `市場フィルタ: market = 'FR'`,
          `期間フィルタ: year BETWEEN 2020 AND 2024`,
          `medium_typeフィルタ: medium_type = 'magazine'`,
        ],
      }
    }

    case 'apply_for_access': {
      const id = `app_${Date.now()}_${Math.random().toString(36).slice(2,8)}`
      await env.CCDM_KV.put(`application:${id}`, JSON.stringify({
        id, ...args, applied_at: new Date().toISOString(), status: 'pending'
      }))
      const list = await env.CCDM_KV.get<string[]>('applications:pending','json') || []
      list.push(id)
      await env.CCDM_KV.put('applications:pending', JSON.stringify(list))
      return {
        application_id: id,
        message: '申請を受け付けました。3営業日以内にAPIキーをメールでお送りします。',
        contact: 'ccdm-access@agentic-governance.jp',
      }
    }

    default: return {error:`Unknown tool: ${name}`}
  }
}

// ─── ダッシュボードHTML ───────────────────────────────
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CCDM Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<script src="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0f1117;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:24px}
h1{font-size:22px;margin-bottom:8px;color:#fff}
.subtitle{color:#888;font-size:13px;margin-bottom:24px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}
.panel{background:#1a1d27;border:1px solid #2a2d3a;border-radius:10px;padding:20px;position:relative;min-height:320px}
.panel.wide{grid-column:span 2}
.panel h2{font-size:15px;color:#a0a8c0;margin-bottom:14px;font-weight:500}
.spinner{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);display:flex;flex-direction:column;align-items:center;gap:10px;color:#666;font-size:13px}
.spinner::before{content:'';width:28px;height:28px;border:3px solid #333;border-top-color:#6c8aff;border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.hide{display:none}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:#6c8aff;padding:8px 12px;border-bottom:1px solid #2a2d3a;font-weight:500}
td{padding:8px 12px;border-bottom:1px solid #1e2130}
tr:hover td{background:#1e2130}
.num{text-align:right;font-variant-numeric:tabular-nums}
.stat-row{display:flex;gap:24px;margin-bottom:16px}
.stat{background:#12141c;border-radius:8px;padding:14px 18px;flex:1}
.stat .val{font-size:26px;font-weight:700;color:#fff}
.stat .lbl{font-size:11px;color:#666;margin-top:2px}
.err{color:#ff6b6b;font-size:13px}
canvas{max-height:260px}
</style></head><body>
<h1>CCDM Data Lake Dashboard</h1>
<div class="subtitle">Tourism Marketing MCP — Admin Overview</div>

<div class="stat-row" id="stats">
  <div class="stat"><div class="val" id="st-total">—</div><div class="lbl">Total Records</div></div>
  <div class="stat"><div class="val" id="st-markets">—</div><div class="lbl">Markets</div></div>
  <div class="stat"><div class="val" id="st-sources">—</div><div class="lbl">Sources</div></div>
  <div class="stat"><div class="val" id="st-datasets">6</div><div class="lbl">Datasets</div></div>
</div>

<div class="grid">
  <div class="panel" id="p1">
    <h2>Corpus by Market</h2>
    <div class="spinner" id="sp1">Loading corpus…</div>
    <canvas id="c1"></canvas>
  </div>
  <div class="panel" id="p2">
    <h2>Media Type Distribution</h2>
    <div class="spinner" id="sp2">Loading media…</div>
    <canvas id="c2"></canvas>
  </div>
  <div class="panel" id="p3">
    <h2>Narrative × Market</h2>
    <div class="spinner" id="sp3">Loading narratives…</div>
    <canvas id="c3"></canvas>
  </div>
  <div class="panel" id="p4">
    <h2>Data Freshness</h2>
    <div class="spinner" id="sp4">Loading freshness…</div>
    <div id="t4"></div>
  </div>
  <div class="panel wide" id="p5">
    <h2>Agency Pricing &amp; Tours (FR Panel)</h2>
    <div class="spinner" id="sp5">Loading panel…</div>
    <canvas id="c5"></canvas>
  </div>
</div>

<script type="module">
const R2 = '__R2_BASE__';
const CORPUS = R2 + '/corpus_index.parquet';
const PANEL  = R2 + '/integrated_panel_v14.parquet';

const COLORS = ['#6c8aff','#ff6c8a','#8aff6c','#ffc46c','#c46cff','#6cffc4','#ff8a6c'];

function done(id){ document.getElementById('sp'+id).classList.add('hide') }
function err(id,e){ const s=document.getElementById('sp'+id); s.textContent=e; s.classList.add('err') }
function fmt(n){ return n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'K':n.toString() }

async function initDuckDB(){
  const JSDELIVR_BUNDLES = {
    mvp: { mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-mvp.wasm',
           mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-mvp.worker.js' },
    eh:  { mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-eh.wasm',
           mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-eh.worker.js' }
  };
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);
  return await db.connect();
}

async function query(conn, sql){
  const result = await conn.query(sql);
  const rows = result.toArray().map(r => {
    const obj = {};
    for (const field of result.schema.fields) {
      const v = r[field.name];
      obj[field.name] = typeof v === 'bigint' ? Number(v) : v;
    }
    return obj;
  });
  return rows;
}

(async()=>{
  let conn;
  try {
    conn = await initDuckDB();
  } catch(e) {
    ['1','2','3','4','5'].forEach(i => err(i,'DuckDB init failed: '+e.message));
    return;
  }

  // ① Corpus by Market
  try {
    const rows = await query(conn, \`SELECT market, COUNT(*)::INTEGER as n FROM '\${CORPUS}' GROUP BY market ORDER BY n DESC\`);
    document.getElementById('st-total').textContent = fmt(rows.reduce((s,r)=>s+r.n,0));
    document.getElementById('st-markets').textContent = rows.length;
    new Chart(document.getElementById('c1'),{type:'bar',data:{
      labels:rows.map(r=>r.market), datasets:[{data:rows.map(r=>r.n),backgroundColor:COLORS}]
    },options:{indexAxis:'y',responsive:true,plugins:{legend:{display:false}},
      scales:{x:{ticks:{callback:v=>fmt(v)},grid:{color:'#1e2130'}},y:{grid:{display:false}}}}});
    done(1);
  } catch(e){ err(1,e.message) }

  // sources count
  try {
    const sr = await query(conn, \`SELECT COUNT(DISTINCT source_id)::INTEGER as n FROM '\${CORPUS}'\`);
    document.getElementById('st-sources').textContent = sr[0].n;
  } catch(e){}

  // ② Media Type
  try {
    const rows = await query(conn, \`SELECT medium_type, COUNT(*)::INTEGER as n FROM '\${CORPUS}' GROUP BY medium_type ORDER BY n DESC\`);
    new Chart(document.getElementById('c2'),{type:'bar',data:{
      labels:rows.map(r=>r.medium_type), datasets:[{data:rows.map(r=>r.n),backgroundColor:COLORS}]
    },options:{indexAxis:'y',responsive:true,plugins:{legend:{display:false}},
      scales:{x:{ticks:{callback:v=>fmt(v)},grid:{color:'#1e2130'}},y:{grid:{display:false}}}}});
    done(2);
  } catch(e){ err(2,e.message) }

  // ③ Narrative × Market
  try {
    const rows = await query(conn, \`SELECT market, narrative, COUNT(*)::INTEGER as n FROM '\${CORPUS}' WHERE narrative != 'uncategorized' GROUP BY market, narrative ORDER BY market, n DESC\`);
    const markets = [...new Set(rows.map(r=>r.market))];
    const narratives = [...new Set(rows.map(r=>r.narrative))];
    const datasets = narratives.map((nar,i) => ({
      label: nar,
      data: markets.map(m => { const r = rows.find(x=>x.market===m&&x.narrative===nar); return r?r.n:0 }),
      backgroundColor: COLORS[i % COLORS.length],
    }));
    new Chart(document.getElementById('c3'),{type:'bar',data:{labels:markets,datasets},
      options:{responsive:true,plugins:{legend:{position:'bottom',labels:{color:'#888',boxWidth:12,font:{size:11}}}},
        scales:{x:{stacked:true,grid:{display:false}},y:{stacked:true,ticks:{callback:v=>fmt(v)},grid:{color:'#1e2130'}}}}});
    done(3);
  } catch(e){ err(3,e.message) }

  // ④ Data Freshness
  try {
    const rows = await query(conn, \`SELECT market, MAX(year)::INTEGER as latest_year, MIN(year)::INTEGER as earliest_year, COUNT(*)::INTEGER as n FROM '\${CORPUS}' WHERE year IS NOT NULL GROUP BY market ORDER BY n DESC\`);
    let html = '<table><tr><th>Market</th><th>Earliest</th><th>Latest</th><th class="num">Records</th></tr>';
    rows.forEach(r => { html += '<tr><td>'+r.market+'</td><td>'+r.earliest_year+'</td><td>'+r.latest_year+'</td><td class="num">'+fmt(r.n)+'</td></tr>' });
    html += '</table>';
    document.getElementById('t4').innerHTML = html;
    done(4);
  } catch(e){ err(4,e.message) }

  // ⑤ Agency Pricing & Tours
  try {
    const rows = await query(conn, \`SELECT quarter, agency_japan_price_median_q as price, agency_japan_tours_q as tours FROM '\${PANEL}' WHERE agency_japan_price_median_q IS NOT NULL ORDER BY quarter\`);
    new Chart(document.getElementById('c5'),{type:'line',data:{
      labels:rows.map(r=>r.quarter),
      datasets:[
        {label:'Median Price (€)',data:rows.map(r=>r.price),borderColor:'#6c8aff',backgroundColor:'rgba(108,138,255,0.1)',fill:true,tension:0.3,yAxisID:'y'},
        {label:'Tours Count',data:rows.map(r=>r.tours),borderColor:'#ff6c8a',backgroundColor:'rgba(255,108,138,0.1)',fill:true,tension:0.3,yAxisID:'y1'}
      ]
    },options:{responsive:true,interaction:{mode:'index',intersect:false},
      plugins:{legend:{position:'bottom',labels:{color:'#888',boxWidth:12,font:{size:11}}}},
      scales:{
        x:{ticks:{maxRotation:45,autoSkip:true,maxTicksLimit:20,color:'#666'},grid:{display:false}},
        y:{position:'left',title:{display:true,text:'Price (€)',color:'#6c8aff'},ticks:{color:'#6c8aff'},grid:{color:'#1e2130'}},
        y1:{position:'right',title:{display:true,text:'Tours',color:'#ff6c8a'},ticks:{color:'#ff6c8a'},grid:{display:false}}
      }}});
    done(5);
  } catch(e){ err(5,e.message) }

})();
<\/script></body></html>`;

export default app
