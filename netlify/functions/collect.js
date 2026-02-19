const sql = require("mssql");

const ALLOW_ORIGINS = [
  "https://theclean-crm-bridge.netlify.app",
  "https://ming709826297.imweb.me",
  // ✅ 실제 운영 도메인도 추가 (필요시 더 넣기)
  "https://xn--9m1bq4jd2k55kh7g.kr",
  "https://www.xn--9m1bq4jd2k55kh7g.kr",
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

function isPlainObject(v){
  return v && typeof v === "object" && !Array.isArray(v);
}

function toYN(v, def="N"){
  if(typeof v === "string"){
    const s = v.trim().toUpperCase();
    if(s === "Y" || s === "N") return s;
  }
  if(typeof v === "boolean") return v ? "Y" : "N";
  return def;
}

function toNullableDate(v){
  // expects "YYYY-MM-DD" or ISO
  if(!v) return null;
  const s = String(v).trim();
  if(!s) return null;
  const d = new Date(s);
  if(Number.isNaN(d.getTime())) return null;
  return d;
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
      body: "Method Not Allowed",
    };
  }

  const raw = event.body || "";
  const payload = safeJsonParse(raw) || {};

  console.log("[collect] raw body =", raw);
  console.log("[collect] payload =", payload);

  // 기대 스키마:
  // { table, lock:{key,timeoutMs}, idempotencyKey, flat:{...} }
  const table = payload.table || "TB_CLN_CUSTOMER_test";
  const lockKey = payload?.lock?.key || `TB_CLN_CUSTOMER_test:${Date.now()}`;
  const lockTimeoutMs = Number(payload?.lock?.timeoutMs || 8000);
  const idempotencyKey = String(payload.idempotencyKey || "").trim();
  const flat = isPlainObject(payload.flat) ? payload.flat : {};

  // 고정 규칙 강제
  const DB_STATUS = "0";
  const REG_SOURCE = "홈페이지";
  const CMPNY_CD = "TEST";

  // 입력값(가능한 것만)
  const phone = (flat.DB_CMPNY_REG_PHONE || "").toString();
  const region = (flat.REGION || "").toString();
  const address = (flat.ADDRESS || region || "").toString();
  const feedback = (flat.FEED_BACK || "").toString();
  const extJson = (flat.EXT_ATTR_JSON || "").toString();

  const reservationDate = toNullableDate(flat.RESERVATION_DATE);

  // 에어컨 N 고정(요청)
  const airconWall  = "N";
  const airconStand = "N";
  const aircon2in1  = "N";
  const aircon1way  = "N";
  const aircon4way  = "N";

  const useYn = toYN(flat.USE_YN, "Y");

  // 🔥 MSSQL 연결 설정
  const config = {
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASSWORD,
    server: process.env.MSSQL_HOST,
    port: parseInt(process.env.MSSQL_PORT || "1433", 10),
    database: process.env.MSSQL_DB,
    options: { encrypt: false, trustServerCertificate: true },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
  };

  let pool;
  try {
    pool = await sql.connect(config);

    // 트랜잭션 + 비관적 락
    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      // 1) 비관적 락(앱락)
      const lockReq = new sql.Request(tx);
      lockReq.input("Resource", sql.NVarChar(255), lockKey);
      lockReq.input("LockMode", sql.NVarChar(32), "Exclusive");
      lockReq.input("LockOwner", sql.NVarChar(32), "Transaction");
      lockReq.input("LockTimeout", sql.Int, lockTimeoutMs);

      const lockResult = await lockReq.query(`
        DECLARE @res INT;
        EXEC @res = sp_getapplock
          @Resource = @Resource,
          @LockMode = @LockMode,
          @LockOwner = @LockOwner,
          @LockTimeout = @LockTimeout;
        SELECT @res AS lockResult;
      `);

      const lr = lockResult?.recordset?.[0]?.lockResult;
      console.log("[collect] applock result =", lr, "lockKey=", lockKey);

      if (lr < 0) {
        await tx.rollback();
        return {
          statusCode: 423,
          headers: corsHeaders(origin),
          body: JSON.stringify({ ok: false, error: "LOCK_TIMEOUT", lockResult: lr }),
        };
      }

      // 2) idempotency 체크 (EXT_ATTR_JSON에 키가 있으면 기존 SEQ 반환)
      if (idempotencyKey) {
        const idemReq = new sql.Request(tx);
        // LIKE 패턴으로 검색(우선 구조)
        const pattern = `%\"idempotencyKey\":\"${idempotencyKey.replace(/"/g, '\\"')}\"%`;
        idemReq.input("pattern", sql.NVarChar(4000), pattern);

        const exists = await idemReq.query(`
          SELECT TOP 1 SEQ
          FROM dbo.TB_CLN_CUSTOMER_test WITH (UPDLOCK, HOLDLOCK)
          WHERE EXT_ATTR_JSON LIKE @pattern
          ORDER BY SEQ DESC
        `);

        const existedSeq = exists?.recordset?.[0]?.SEQ;
        if (existedSeq) {
          console.log("[collect] ✅ idempotency hit. existed SEQ =", existedSeq);
          await tx.commit();
          return {
            statusCode: 200,
            headers: corsHeaders(origin),
            body: JSON.stringify({ ok: true, dup: true, seq: existedSeq }),
          };
        }
      }

      // 3) INSERT
      const ins = new sql.Request(tx);

      // ✅ NVARCHAR로 한글 보존: REG_SOURCE/ADDRESS/REGION/FEED_BACK/EXT_ATTR_JSON 등
      ins.input("DB_STATUS", sql.VarChar(20), DB_STATUS);
      ins.input("CMPNY_CD", sql.VarChar(20), CMPNY_CD);
      ins.input("USE_YN", sql.VarChar(1), useYn);

      ins.input("DB_CMPNY_REG_PHONE", sql.NVarChar(100), phone || null);
      ins.input("REGION", sql.NVarChar(100), region || null);
      ins.input("ADDRESS", sql.NVarChar(300), address || null);
      ins.input("FEED_BACK", sql.NVarChar(sql.MAX), feedback || null);
      ins.input("EXT_ATTR_JSON", sql.NVarChar(sql.MAX), extJson || null);

      ins.input("AIRCON_WALL", sql.Char(1), airconWall);
      ins.input("AIRCON_STAND", sql.Char(1), airconStand);
      ins.input("AIRCON_2IN1", sql.Char(1), aircon2in1);
      ins.input("AIRCON_1WAY", sql.Char(1), aircon1way);
      ins.input("AIRCON_4WAY", sql.Char(1), aircon4way);

      ins.input("REG_SOURCE", sql.NVarChar(50), REG_SOURCE); // ✅ 핵심(???) 방지

      if (reservationDate) ins.input("RESERVATION_DATE", sql.DateTime, reservationDate);
      else ins.input("RESERVATION_DATE", sql.DateTime, null);

      const result = await ins.query(`
        INSERT INTO dbo.TB_CLN_CUSTOMER_test (
          DB_STATUS,
          CMPNY_CD,
          REG_DT,
          USE_YN,

          DB_CMPNY_REG_PHONE,
          REGION,
          ADDRESS,
          RESERVATION_DATE,

          FEED_BACK,
          EXT_ATTR_JSON,

          AIRCON_WALL,
          AIRCON_STAND,
          AIRCON_2IN1,
          AIRCON_1WAY,
          AIRCON_4WAY,

          REG_SOURCE
        )
        OUTPUT INSERTED.SEQ
        VALUES (
          @DB_STATUS,
          @CMPNY_CD,
          GETDATE(),
          @USE_YN,

          @DB_CMPNY_REG_PHONE,
          @REGION,
          @ADDRESS,
          @RESERVATION_DATE,

          @FEED_BACK,
          @EXT_ATTR_JSON,

          @AIRCON_WALL,
          @AIRCON_STAND,
          @AIRCON_2IN1,
          @AIRCON_1WAY,
          @AIRCON_4WAY,

          @REG_SOURCE
        )
      `);

      const seq = result?.recordset?.[0]?.SEQ;
      console.log("[collect] ✅ INSERT SUCCESS seq =", seq);

      await tx.commit();

      return {
        statusCode: 200,
        headers: corsHeaders(origin),
        body: JSON.stringify({ ok: true, seq }),
      };

    } catch (e) {
      console.error("[collect] ❌ TX ERROR:", e);
      try { await tx.rollback(); } catch {}
      return {
        statusCode: 500,
        headers: corsHeaders(origin),
        body: JSON.stringify({ ok: false, error: "TX_FAILED" }),
      };
    }

  } catch (err) {
    console.error("[collect] ❌ DB ERROR:", err);
    return {
      statusCode: 500,
      headers: corsHeaders(origin),
      body: JSON.stringify({ ok: false, error: "DB_CONNECT_OR_QUERY_FAILED" }),
    };
  }
};
