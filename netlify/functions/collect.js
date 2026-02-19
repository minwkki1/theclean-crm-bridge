const sql = require("mssql");

const ALLOW_ORIGINS = [
  "https://ming709826297.imweb.me",
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

  try {
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

    await sql.connect(config);

    const payload = JSON.parse(event.body);

    console.log("payload:", payload);

    // 🔥 테스트용 INSERT
    await sql.query`
      INSERT INTO TB_CLN_CUSTOMER_TEST (name, phone)
      VALUES (${payload.name}, ${payload.phone})
    `;

    return {
      statusCode: 200,
      headers: corsHeaders(origin),
      body: "OK"
    };

  } catch (err) {
    console.error("DB ERROR:", err);
    return {
      statusCode: 500,
      headers: corsHeaders(origin),
      body: "DB ERROR"
    };
  }
};
