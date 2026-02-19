const sql = require("mssql");

const ALLOW_ORIGINS = [
  "https://theclean-crm-bridge.netlify.app",
  "https://ming709826297.imweb.me",
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

function isPlainObject(v) {
  return v && typeof v === "object" && !Array.isArray(v);
}

function toYN(v, def = "N") {
  if (typeof v === "string") {
    const s = v.trim().toUpperCase();
    if (s === "Y" || s === "N") return s;
  }
  if (typeof v === "boolean") return v ? "Y" : "N";
  return def;
}

function ynToKorYN(v) {
  // FEED_BACK 표기용: Y/N만
  return (String(v || "N").toUpperCase() === "Y") ? "Y" : "N";
}

function toNullableDate(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (!s) return null;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function safeStr(v, maxLen = 4000) {
  const s = (v === null || typeof v === "undefined") ? "" : String(v);
  const t = s.trim();
  if (!t) return "";
  return t.length > maxLen ? t.slice(0, maxLen) : t;
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
  const lockTimeoutMs = Number(payload?.lock?.timeoutMs || 8000);
  const idempotencyKey = safeStr(payload.idempotencyKey || "", 200);
  const flat = isPlainObject(payload.flat) ? payload.flat : {};

  // ✅ 고정 규칙 강제
  const DB_STATUS = "0";          // 무조건 0
  const REG_SOURCE = "홈페이지";  // 무조건 홈페이지
  const CMPNY_CD = "TEST";

  // ✅ 세션키(=DB_ADKEY): flat에서 우선 받고, 없으면 payload.sessionKey도 시도
  const dbAdkey =
    safeStr(flat.DB_ADKEY || "", 120) ||
    safeStr(payload.sessionKey || "", 120) ||
    ""; // 최종 문자열

  // 입력값(가능한 것만)
  const phone = safeStr(flat.DB_CMPNY_REG_PHONE || "", 100);
  const region = safeStr(flat.REGION || "", 200);
  const address = safeStr(flat.ADDRESS || region || "", 300);

  // 날짜
  const reservationDate = toNullableDate(flat.RESERVATION_DATE);

  // 동의/연락선호 (FEED_BACK 구성용)
  const consentRequired = ynToKorYN(toYN(flat.CONSENT_REQUIRED ?? flat.consentRequired, "N"));
  const consentMarketing = ynToKorYN(toYN(flat.CONSENT_MARKETING ?? flat.consentMarketing, "N"));
  const consentMarketingReceive = ynToKorYN(toYN(flat.CONSENT_MARKETING_RECEIVE ?? flat.consentMarketingReceive, "N"));

  const contactPrefRaw =
    safeStr(
      flat.CONTACT_PREFERENCE ??
      flat.contactPreference ??
      flat.CONTACT_PREF ??
      "",
      50
    ) || "전화";

  // ✅ FEED_BACK 포맷 강제
  const feedback = `동의필수:${consentRequired} | 마케팅동의:${consentMarketing} | 마케팅수신:${consentMarketingReceive} | 연락선호:${contactPrefRaw}`;

  // ext json (그대로 저장)
  const extJson = safeStr(flat.EXT_ATTR_JSON || "", 20000);

  // 에어컨 N 고정(요청)
  const airconWall = "N";
  const airconStand = "N";
  const aircon2in1 = "N";
  const aircon1way = "N";
  const aircon4way = "N";

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

    // ✅ 비관적 락 키 전략:
    // - DB_ADKEY가 있으면 "테이블+세션키" 기준으로 락 (같은 세션 동시요청 충돌 방지)
    // - 없으면 요청별 랜덤 락 (동시 신규 insert 정도만 보호)
    const lockKey =
      (dbAdkey ? `${table}:DB_ADKEY:${dbAdkey}` : `${table}:NO_ADKEY:${Date.now()}`);

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

      // 2) (옵션) idempotency 체크: 기존 로직 유지
      //    단, 지금은 “DB_ADKEY 업서트”가 핵심이라 idem은 참고용으로만 둠
      if (idempotencyKey) {
        const idemReq = new sql.Request(tx);
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

      // 3) 업서트(덮어쓰기) by DB_ADKEY
      //    - DB_ADKEY 있으면: UPDATE 먼저 시도 -> @@ROWCOUNT=0이면 INSERT
      //    - DB_ADKEY 없으면: 무조건 INSERT (매칭 불가)
      const req = new sql.Request(tx);

      // inputs
      req.input("DB_STATUS", sql.VarChar(20), DB_STATUS);
      req.input("CMPNY_CD", sql.VarChar(20), CMPNY_CD);
      req.input("USE_YN", sql.VarChar(1), useYn);

      req.input("DB_ADKEY", sql.NVarChar(120), dbAdkey || null);

      req.input("DB_CMPNY_REG_PHONE", sql.NVarChar(100), phone || null);
      req.input("REGION", sql.NVarChar(200), region || null);
      req.input("ADDRESS", sql.NVarChar(300), address || null);
      req.input("FEED_BACK", sql.NVarChar(sql.MAX), feedback || null);
      req.input("EXT_ATTR_JSON", sql.NVarChar(sql.MAX), extJson || null);

      req.input("AIRCON_WALL", sql.Char(1), airconWall);
      req.input("AIRCON_STAND", sql.Char(1), airconStand);
      req.input("AIRCON_2IN1", sql.Char(1), aircon2in1);
      req.input("AIRCON_1WAY", sql.Char(1), aircon1way);
      req.input("AIRCON_4WAY", sql.Char(1), aircon4way);

      req.input("REG_SOURCE", sql.NVarChar(50), REG_SOURCE);

      if (reservationDate) req.input("RESERVATION_DATE", sql.DateTime, reservationDate);
      else req.input("RESERVATION_DATE", sql.DateTime, null);

      console.log("[collect] mapped =", {
        table,
        DB_STATUS,
        REG_SOURCE,
        dbAdkey: dbAdkey || null,
        phone: phone || null,
        region: region || null,
        address: address || null,
        reservationDate: reservationDate ? reservationDate.toISOString() : null,
        FEED_BACK: feedback,
      });

      const upsertSql = `
        DECLARE @out TABLE (SEQ INT, ACTION NVARCHAR(10));

        -- ✅ DB_ADKEY가 있으면 "같은 키" UPDATE 우선 (비관적 락: UPDLOCK+HOLDLOCK)
        IF (@DB_ADKEY IS NOT NULL AND LTRIM(RTRIM(@DB_ADKEY)) <> '')
        BEGIN
          UPDATE T
          SET
            T.DB_STATUS = @DB_STATUS,               -- 규칙 강제
            T.CMPNY_CD = @CMPNY_CD,
            T.USE_YN = @USE_YN,

            T.DB_CMPNY_REG_PHONE = @DB_CMPNY_REG_PHONE,
            T.REGION = @REGION,
            T.ADDRESS = @ADDRESS,
            T.RESERVATION_DATE = @RESERVATION_DATE,

            T.FEED_BACK = @FEED_BACK,
            T.EXT_ATTR_JSON = @EXT_ATTR_JSON,

            T.AIRCON_WALL = @AIRCON_WALL,
            T.AIRCON_STAND = @AIRCON_STAND,
            T.AIRCON_2IN1 = @AIRCON_2IN1,
            T.AIRCON_1WAY = @AIRCON_1WAY,
            T.AIRCON_4WAY = @AIRCON_4WAY,

            T.REG_SOURCE = @REG_SOURCE,            -- 규칙 강제
            T.UPD_DT = GETDATE()
          OUTPUT INSERTED.SEQ, 'UPDATE' INTO @out(SEQ, ACTION)
          FROM dbo.TB_CLN_CUSTOMER_test T WITH (UPDLOCK, HOLDLOCK)
          WHERE T.DB_ADKEY = @DB_ADKEY;

          IF (@@ROWCOUNT = 0)
          BEGIN
            INSERT INTO dbo.TB_CLN_CUSTOMER_test (
              DB_STATUS,
              CMPNY_CD,
              REG_DT,
              USE_YN,

              DB_ADKEY,

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
            OUTPUT INSERTED.SEQ, 'INSERT' INTO @out(SEQ, ACTION)
            VALUES (
              @DB_STATUS,
              @CMPNY_CD,
              GETDATE(),
              @USE_YN,

              @DB_ADKEY,

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
            );
          END
        END
        ELSE
        BEGIN
          -- ✅ DB_ADKEY가 NULL/빈값이면 매칭 불가: 무조건 신규 INSERT
          INSERT INTO dbo.TB_CLN_CUSTOMER_test (
            DB_STATUS,
            CMPNY_CD,
            REG_DT,
            USE_YN,

            DB_ADKEY,

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
          OUTPUT INSERTED.SEQ, 'INSERT' INTO @out(SEQ, ACTION)
          VALUES (
            @DB_STATUS,
            @CMPNY_CD,
            GETDATE(),
            @USE_YN,

            NULL,

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
          );
        END

        SELECT TOP 1 SEQ, ACTION FROM @out;
      `;

      const result = await req.query(upsertSql);
      const row = result?.recordset?.[0] || {};
      const seq = row.SEQ;
      const action = row.ACTION || "UNKNOWN";

      console.log("[collect] ✅ UPSERT SUCCESS", { seq, action, dbAdkey: dbAdkey || null });

      await tx.commit();

      return {
        statusCode: 200,
        headers: corsHeaders(origin),
        body: JSON.stringify({ ok: true, seq, action }),
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
