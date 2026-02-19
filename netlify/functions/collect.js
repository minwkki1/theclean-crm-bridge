const sql = require("mssql");

const ALLOW_ORIGINS = [
  "https://your-imweb-domain.com",
  "https://ming709826297.imweb.me",
  // 필요하면 추가
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

function toYN(v, defaultValue = "N") {
  if (v === true) return "Y";
  if (v === false) return "N";
  if (typeof v === "string") {
    const s = v.trim().toUpperCase();
    if (s === "Y" || s === "YES" || s === "TRUE" || s === "1") return "Y";
    if (s === "N" || s === "NO" || s === "FALSE" || s === "0") return "N";
  }
  if (typeof v === "number") return v ? "Y" : "N";
  return defaultValue;
}

function cleanText(v, max = 500) {
  if (v === null || typeof v === "undefined") return null;
  const s = String(v).trim();
  if (!s) return null;
  return s.length > max ? s.slice(0, max) : s;
}

function normalizePhone(v) {
  const s = cleanText(v, 50);
  if (!s) return null;
  // DB에는 원문(하이픈 포함) 그대로 넣되, 공백만 정리
  return s.replace(/\s+/g, "");
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

  // ✅ 폼에서 보내는 body 구조(네가 콘솔에 찍은 것 기준)
  // body: { type, sentAt, table, lock:{...}, flat:{...} }
  const type = payload.type || "";
  const sentAt = payload.sentAt || null;
  const table = payload.table || "TB_CLN_CUSTOMER_test";
  const lock = payload.lock || {};
  const flat = payload.flat || {};

  // 🔥 규칙: table은 무조건 test로 고정
  const TARGET_TABLE = "dbo.TB_CLN_CUSTOMER_test";

  // 🔥 규칙: DB_STATUS / REG_SOURCE 강제
  const DB_STATUS = "0";      // 신규
  const REG_SOURCE = "홈페이지";

  // ✅ lockKey (세션키) - 없으면 임시 키 생성(그래도 중복방지 약해짐)
  const lockKey =
    cleanText(lock.lockKey, 80) ||
    cleanText(flat.__lockKey, 80) ||
    cleanText(flat.sessionKey, 80) ||
    `tmp-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  // ✅ 폼 데이터 매핑 (현재 컬럼에 최대한 맞춤)
  // - DB_CMPNY_* 는 "업체" 관련인데 폼에는 없으니 null 유지
  // - REGION / ADDRESS / ADDRESS_DETAIL / WORK_DATE(희망일자) / FEED_BACK(메모) / DB_CMPNY_REG_PHONE(전화) 위주로 채움
  const region = cleanText(flat.region, 100);
  const phone = normalizePhone(flat.phone);

  // preferredDate는 "YYYY-MM-DD" 일 수 있음 -> datetime으로 넣기
  // null이면 null로
  const preferredDateRaw = cleanText(flat.preferredDate, 30);
  const preferredDate = preferredDateRaw ? preferredDateRaw : null;

  const memo = cleanText(flat.memo, 1000);

  // contactPreference(전화/문자/카카오톡)
  const contactPreference = cleanText(flat.contactPreference, 30);

  // 에어컨 타입들 (기본 N)
  const AIRCON_WALL = toYN(flat.aircon_wall, "N");
  const AIRCON_STAND = toYN(flat.aircon_stand, "N");
  const AIRCON_2IN1 = toYN(flat.aircon_2in1, "N");
  const AIRCON_1WAY = toYN(flat.aircon_1way, "N");
  const AIRCON_4WAY = toYN(flat.aircon_4way, "N");

  // consent는 DB 컬럼이 없으니 우선 memo에 같이 넣어도 됨(원하면)
  // (디자인 안 바꾸고 데이터만 더 보존하려고)
  const consentRequired = toYN(flat.consentRequired, "N");
  const consentMarketing = toYN(flat.consentMarketing, "N");
  const consentMarketingReceive = toYN(flat.consentMarketingReceive, "N");

  const mergedMemo = (() => {
    const base = memo || "";
    const extra = [
      contactPreference ? `연락선호:${contactPreference}` : null,
      `동의필수:${consentRequired}`,
      `마케팅동의:${consentMarketing}`,
      `마케팅수신:${consentMarketingReceive}`,
      `lockKey:${lockKey}`,
      sentAt ? `sentAt:${sentAt}` : null,
      type ? `type:${type}` : null,
    ].filter(Boolean).join(" | ");

    if (!base && !extra) return null;
    if (!base) return extra;
    return `${base}\n---\n${extra}`;
  })();

  console.log("[collect] type/table/lockKey =", { type, table, lockKey });
  console.log("[collect] flat(mapped) =", {
    DB_STATUS,
    REG_SOURCE,
    region,
    phone,
    preferredDate,
    AIRCON_WALL,
    AIRCON_STAND,
    AIRCON_2IN1,
    AIRCON_1WAY,
    AIRCON_4WAY,
  });

  // 🔥 MSSQL 연결 설정
  const config = {
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASSWORD,
    server: process.env.MSSQL_HOST,
    port: parseInt(process.env.MSSQL_PORT || "1433", 10),
    database: process.env.MSSQL_DB,
    options: {
      encrypt: false,
      trustServerCertificate: true
    }
  };

  let pool;
  try {
    pool = await sql.connect(config);

    // ✅ 비관적 락 + 중복 방지 전략
    // - 트랜잭션
    // - SERIALIZABLE
    // - UPDLOCK,HOLDLOCK로 lockKey 기준으로 "이미 들어간 건지"를 잠그고 확인
    // - 있으면 insert 스킵(멱등성)
    //
    // ⚠️ 전제: 우리 테이블에는 lockKey 컬럼이 없으니,
    // 지금은 FEED_BACK 안에 lockKey를 저장해놓고 LIKE로 검사한다.
    // 나중에 lockKey 전용 컬럼 추가하면 훨씬 깔끔해짐.
    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    const req = new sql.Request(tx);

    // 락 잡고(UPDLOCK/HOLDLOCK), 동일 lockKey 이미 저장됐는지 확인
    req.input("lockKey", sql.NVarChar(120), lockKey);

    const existsRs = await req.query(`
      SELECT TOP 1 SEQ
      FROM ${TARGET_TABLE} WITH (UPDLOCK, HOLDLOCK)
      WHERE FEED_BACK LIKE '%' + @lockKey + '%'
      ORDER BY SEQ DESC
    `);

    const already = existsRs?.recordset?.[0]?.SEQ;

    if (already) {
      console.log("🟡 already inserted (pessimistic lock hit). SEQ =", already);
      await tx.commit();
      return {
        statusCode: 200,
        headers: corsHeaders(origin),
        body: JSON.stringify({ ok: true, dedup: true, seq: already })
      };
    }

    // ✅ INSERT (payload 반영)
    const ins = new sql.Request(tx);

    ins.input("DB_STATUS", sql.VarChar(10), DB_STATUS); // 규칙
    ins.input("REG_SOURCE", sql.VarChar(50), REG_SOURCE); // 규칙

    ins.input("REGION", sql.NVarChar(100), region);
    ins.input("PHONE", sql.NVarChar(50), phone);

    ins.input("WORK_DATE", sql.DateTime, preferredDate ? new Date(preferredDate) : null);
    ins.input("FEED_BACK", sql.NVarChar(1000), mergedMemo);

    ins.input("AIRCON_WALL", sql.Char(1), AIRCON_WALL);
    ins.input("AIRCON_STAND", sql.Char(1), AIRCON_STAND);
    ins.input("AIRCON_2IN1", sql.Char(1), AIRCON_2IN1);
    ins.input("AIRCON_1WAY", sql.Char(1), AIRCON_1WAY);
    ins.input("AIRCON_4WAY", sql.Char(1), AIRCON_4WAY);

    // CMPNY_CD는 테스트용으로 유지 가능(원하면 "TEST")
    ins.input("CMPNY_CD", sql.VarChar(30), "TEST");

    const insertRs = await ins.query(`
      INSERT INTO ${TARGET_TABLE} (
        DB_STATUS,
        CMPNY_CD,
        REGION,
        DB_CMPNY_REG_PHONE,
        WORK_DATE,
        FEED_BACK,
        REG_DT,
        USE_YN,
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
        @REGION,
        @PHONE,
        @WORK_DATE,
        @FEED_BACK,
        GETDATE(),
        'Y',
        @AIRCON_WALL,
        @AIRCON_STAND,
        @AIRCON_2IN1,
        @AIRCON_1WAY,
        @AIRCON_4WAY,
        @REG_SOURCE
      )
    `);

    const seq = insertRs?.recordset?.[0]?.SEQ || null;

    await tx.commit();

    console.log("✅ INSERT SUCCESS. SEQ =", seq);

    return {
      statusCode: 200,
      headers: corsHeaders(origin),
      body: JSON.stringify({ ok: true, seq })
    };

  } catch (err) {
    console.error("❌ DB ERROR:", err);
    try {
      // 트랜잭션 중 에러면 rollback 시도
      // (tx 변수가 scope 밖일 수 있어, 여기서는 안전하게 무시)
    } catch (e) {}

    return {
      statusCode: 500,
      headers: corsHeaders(origin),
      body: JSON.stringify({
        ok: false,
        error: "DB insert failed",
        detail: String(err?.message || err)
      })
    };
  } finally {
    // netlify에서는 pool.close()를 매번 하면 느려질 수 있지만,
    // 테스트 단계에서는 명시적으로 닫아도 괜찮음
    try { await sql.close(); } catch (e) {}
  }
};
