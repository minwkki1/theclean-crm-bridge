const sql = require("mssql");

const ALLOW_ORIGINS = [
  "https://your-imweb-domain.com",
  "https://www.your-imweb-domain.com"
];

function pickOrigin(headers) {
  const origin = headers?.origin || headers?.Origin || "";
  return ALLOW_ORIGINS.includes(origin) ? origin : "";
}

function corsHeaders(origin) {
  const h = {
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  };
  if (origin) h["Access-Control-Allow-Origin"] = origin;
  return h;
}

function safeJsonParse(str) {
  try { return JSON.parse(str); } catch { return null; }
}

exports.handler = async (event) => {
  const origin = pickOrigin(event.headers);

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: corsHeaders(origin), body: "" };
  }

  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers: corsHeaders(origin),
      body: "Method Not Allowed"
    };
  }

  const raw = event.body || "";
  const payload = safeJsonParse(raw) || {};

  console.log("[collect] payload =", payload);

  // 🔥 MSSQL 연결 설정
  const config = {
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASSWORD,
    server: process.env.MSSQL_HOST,
    port: parseInt(process.env.MSSQL_PORT || "1433"),
    database: process.env.MSSQL_DB,
    options: {
      encrypt: false,
      trustServerCertificate: true
    }
  };

  try {
    await sql.connect(config);

    await sql.query`
      INSERT INTO dbo.TB_CLN_CUSTOMER_test (
        DB_STATUS,
        CMPNY_CD,
        REG_DT,
        USE_YN,
        AIRCON_WALL,
        AIRCON_STAND,
        AIRCON_2IN1,
        AIRCON_1WAY,
        AIRCON_4WAY,
        REG_SOURCE
      )
      VALUES (
        'NEW',
        'TEST',
        GETDATE(),
        'Y',
        'N',
        'N',
        'N',
        'N',
        'N',
        'NETLIFY'
      )
    `;

    console.log("✅ INSERT SUCCESS");

    return {
      statusCode: 200,
      headers: corsHeaders(origin),
      body: JSON.stringify({ ok: true })
    };

  } catch (err) {
    console.error("❌ DB ERROR:", err);

    return {
      statusCode: 500,
      headers: corsHeaders(origin),
      body: JSON.stringify({ error: "DB insert failed" })
    };
  }
};
