const sql = require("mssql");

/* =========================================================
  TCXM / TheCleanAtHome - Collect (MSSQL)  [A안 v2.3 FULL]
  ✅ DB_ADKEY 완전 제거 (읽기/쓰기 X)
  ✅ 전화번호(DB_CMPNY_REG_PHONE) 기준
  ✅ 서버 메모리(인스턴스 내) "짧은 락"으로 연타/중복 클릭 방지
     - 짧은 락에 걸리면: DUP 체크/모달 트리거 금지 → LOCKED_SHORT 응답
  ✅ 24시간 중복이면 최근 REG_DT 반환 + force=true 일 때만 신규 INSERT
  ✅ CMPNY_CD = "9000011" 하드코딩
  ✅ EXT_ATTR_JSON = NULL 고정
  ✅ console.log 상세 출력
========================================================= */

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

function escapeForLog(v, maxLen = 800) {
  const s = safeStr(v, maxLen);
  return s.length > maxLen ? s.slice(0, maxLen) + "..." : s;
}

function fmtKst(dt) {
  // DB에서 넘어오는 Date(UTC로 찍힐 수 있음)를 "KST 표기" 문자열로 반환
  try {
    const d = (dt instanceof Date) ? dt : new Date(dt);
    if (Number.isNaN(d.getTime())) return null;
    // KST = UTC+9 (간단 표기)
    const k = new Date(d.getTime() + 9 * 60 * 60 * 1000);
    const yyyy = k.getUTCFullYear();
    const mm = String(k.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(k.getUTCDate()).padStart(2, "0");
    const hh = String(k.getUTCHours()).padStart(2, "0");
    const mi = String(k.getUTCMinutes()).padStart(2, "0");
    const ss = String(k.getUTCSeconds()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} (KST)`;
  } catch {
    return null;
  }
}

/* =========================================================
  ✅ 서버 메모리 락 (A안)
  - 같은 인스턴스 안에서만 보장
  - 목적: "연타 클릭"으로 동일 요청이 거의 동시에 2번 들어오는 것 방지
========================================================= */
const __TCXM_LOCK_MAP__ = global.__TCXM_LOCK_MAP__ || new Map();
global.__TCXM_LOCK_MAP__ = __TCXM_LOCK_MAP__;

function nowMs() { return Date.now(); }

function acquireShortLock(key, ttlMs) {
  const now = nowMs();
  const exp = __TCXM_LOCK_MAP__.get(key);

  if (exp && exp > now) {
    return { ok: false, remainMs: exp - now };
  }

  __TCXM_LOCK_MAP__.set(key, now + ttlMs);
  return { ok: true, remainMs: ttlMs };
}

function releaseLock(key) {
  __TCXM_LOCK_MAP__.delete(key);
}

/* =========================================================
  ✅ MSSQL 연결 설정
========================================================= */
function mssqlConfig() {
  return {
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASSWORD,
    server: process.env.MSSQL_HOST,
    port: parseInt(process.env.MSSQL_PORT || "1433", 10),
    database: process.env.MSSQL_DB,
    options: { encrypt: false, trustServerCertificate: true },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
  };
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

  console.log("[collect-v2.3] raw body =", escapeForLog(raw, 1600));
  console.log("[collect-v2.3] payload keys =", Object.keys(payload || {}));

  // ✅ table 외부 입력 금지
  const table = "TB_CLN_CUSTOMER";

  const flat = isPlainObject(payload.flat) ? payload.flat : {};

  // ✅ 고정 규칙 강제
  const DB_STATUS = "0";
  const REG_SOURCE = "홈페이지";
  const CMPNY_CD = "9000011"; // ✅ 하드코딩 (서버가 넣음)

  // ✅ force: 중복(24h)일 때 새로 접수 버튼 누르면 true로 재호출
  const force = !!(payload.force ?? flat.force);

  // ✅ 짧은락 TTL (연타 방지). 기본 2500ms 추천
  const shortLockTtlMs = Number(payload?.lock?.shortTtlMs || 2500);

  // 입력값
  const phone = safeStr(flat.DB_CMPNY_REG_PHONE || "", 100); // '-' 포함 포맷 유지
  const region = safeStr(flat.REGION || "", 200);
  const address = safeStr(flat.ADDRESS || region || "", 300);
  const reservationDate = toNullableDate(flat.RESERVATION_DATE);

  // 동의/연락선호
  const consentRequired = ynToKorYN(toYN(flat.CONSENT_REQUIRED ?? flat.consentRequired, "N"));
  const consentMarketing = ynToKorYN(toYN(flat.CONSENT_MARKETING ?? flat.consentMarketing, "N"));
  const consentMarketingReceive = ynToKorYN(
    toYN(flat.CONSENT_MARKETING_RECEIVE ?? flat.consentMarketingReceive, "N")
  );

  const contactPrefRaw =
    safeStr(
      flat.CONTACT_PREFERENCE ??
      flat.contactPreference ??
      flat.CONTACT_PREF ??
      "",
      50
    ) || "전화";

  // ✅ EXT_ATTR_JSON은 만들지도/저장하지도 않음 → NULL 고정
  const extJson = null;

  // ✅ 메모는 "선택" (EXT_ATTR_JSON 만들지 않으니, 필요하면 flat.MEMO로만 받는다)
  const memo = safeStr(flat.MEMO || flat.memo || "", 2000);
  const memoPart = memo ? ` | 메모:${memo}` : ` | 메모:-`;

  // ✅ FEED_BACK 포맷 강제
  const feedback =
    `동의필수:${consentRequired} | 마케팅동의:${consentMarketing} | 마케팅수신:${consentMarketingReceive} | 연락선호:${contactPrefRaw}` +
    memoPart;

  // 에어컨 N 고정
  const airconWall = "N";
  const airconStand = "N";
  const aircon2in1 = "N";
  const aircon1way = "N";
  const aircon4way = "N";

  const useYn = toYN(flat.USE_YN, "Y");

  // ✅ 필수값 체크 (최소)
  if (!phone) {
    console.log("[collect-v2.3] ❌ missing phone");
    return {
      statusCode: 400,
      headers: corsHeaders(origin),
      body: JSON.stringify({ ok: false, error: "MISSING_PHONE" }),
    };
  }

  // =========================================================
  // ✅ (A안) 짧은락: 같은 전화번호로 들어오는 "연타" 요청은 여기서 컷
  // - 이때는 "중복(24h)" 안내 모달을 띄우면 안되므로
  //   DUP 체크 전에 바로 LOCKED_SHORT로 반환한다.
  // =========================================================
  const lockKey = `${table}:PHONE:${phone}`;
  const lock = acquireShortLock(lockKey, shortLockTtlMs);

  if (!lock.ok) {
    console.log("[collect-v2.3] ⛔ locked_short", { phone, remainMs: lock.remainMs });
    return {
      statusCode: 200,
      headers: corsHeaders(origin),
      body: JSON.stringify({
        ok: false,
        status: "LOCKED_SHORT",
        phone,
        remainMs: lock.remainMs,
        message: "요청 처리 중입니다. 잠시만 기다려주세요.",
      }),
    };
  }

  let pool;
  try {
    pool = await sql.connect(mssqlConfig());

    // ✅ 트랜잭션: 24h 체크 + (조건부) INSERT 를 한 덩어리로
    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

    try {
      // ---------------------------------------------------------
      // 1) 24시간 중복 체크 (REG_DT 기준)
      //    - 여기서는 락이 이미 잡혀있으므로 "연타에 의한 2중 실행"은 방지됨
      //    - 하지만 (서로 다른 인스턴스) 동시 요청 가능성은 남는다.
      //      → 그래도 요구사항(A안)대로 DB PK 변경 없이 진행.
      // ---------------------------------------------------------
      const dupReq = new sql.Request(tx);
      dupReq.input("PHONE", sql.NVarChar(100), phone);

      const dupSql = `
        SELECT TOP 1
          SEQ,
          REG_DT
        FROM dbo.TB_CLN_CUSTOMER WITH (READPAST)
        WHERE DB_CMPNY_REG_PHONE = @PHONE
          AND REG_DT >= DATEADD(HOUR, -24, GETDATE())
        ORDER BY REG_DT DESC;
      `;

      const dupResult = await dupReq.query(dupSql);
      const dupRow = dupResult?.recordset?.[0] || null;
      const isDup24h = !!dupRow;

      console.log("[collect-v2.3] dup-check", {
        phone,
        isDup24h,
        latestSeq: dupRow?.SEQ,
        latestRegDt: dupRow?.REG_DT,
        latestRegDtKst: dupRow?.REG_DT ? fmtKst(dupRow.REG_DT) : null,
        force,
      });

      // ✅ 중복인데 force=false면 저장 금지 + 최근 신청시간 반환
      if (isDup24h && !force) {
        await tx.commit();

        // ✅ 여기서 락 해제:
        // - 프론트가 모달 띄우고 "새로 접수" 누르면 force=true로 재호출할 것
        releaseLock(lockKey);

        return {
          statusCode: 200,
          headers: corsHeaders(origin),
          body: JSON.stringify({
            ok: false,
            status: "DUPLICATE_24H",
            phone,
            latest: {
              seq: dupRow.SEQ,
              regDt: dupRow.REG_DT,
              regDtKst: dupRow.REG_DT ? fmtKst(dupRow.REG_DT) : null,
            },
            message: "24시간 이내 동일 전화번호 접수가 있습니다.",
          }),
        };
      }

      // ---------------------------------------------------------
      // 2) INSERT (항상 새 row)
      //    - PK(SEQ)는 IDENTITY라 DB가 자동 생성
      //    - CMPNY_CD = 9000011
      //    - EXT_ATTR_JSON = NULL
      // ---------------------------------------------------------
      const insReq = new sql.Request(tx);

      insReq.input("DB_STATUS", sql.VarChar(20), DB_STATUS);
      insReq.input("CMPNY_CD", sql.VarChar(20), CMPNY_CD);
      insReq.input("USE_YN", sql.VarChar(1), useYn);

      insReq.input("DB_CMPNY_REG_PHONE", sql.NVarChar(100), phone || null);
      insReq.input("REGION", sql.NVarChar(200), region || null);
      insReq.input("ADDRESS", sql.NVarChar(300), address || null);

      insReq.input("FEED_BACK", sql.NVarChar(sql.MAX), feedback || null);
      insReq.input("EXT_ATTR_JSON", sql.NVarChar(sql.MAX), null); // ✅ 항상 NULL

      insReq.input("AIRCON_WALL", sql.Char(1), airconWall);
      insReq.input("AIRCON_STAND", sql.Char(1), airconStand);
      insReq.input("AIRCON_2IN1", sql.Char(1), aircon2in1);
      insReq.input("AIRCON_1WAY", sql.Char(1), aircon1way);
      insReq.input("AIRCON_4WAY", sql.Char(1), aircon4way);

      insReq.input("REG_SOURCE", sql.NVarChar(50), REG_SOURCE);

      if (reservationDate) insReq.input("RESERVATION_DATE", sql.DateTime, reservationDate);
      else insReq.input("RESERVATION_DATE", sql.DateTime, null);

      console.log("[collect-v2.3] insert mapped =", {
        table,
        DB_STATUS,
        CMPNY_CD,
        REG_SOURCE,
        phone,
        region,
        address,
        reservationDate: reservationDate ? reservationDate.toISOString() : null,
        FEED_BACK: feedback,
        EXT_ATTR_JSON: extJson,
        force,
      });

      const insertSql = `
        INSERT INTO dbo.TB_CLN_CUSTOMER (
          DB_STATUS, CMPNY_CD, REG_DT, USE_YN,
          DB_CMPNY_REG_PHONE, REGION, ADDRESS, RESERVATION_DATE,
          FEED_BACK, EXT_ATTR_JSON,
          AIRCON_WALL, AIRCON_STAND, AIRCON_2IN1, AIRCON_1WAY, AIRCON_4WAY,
          REG_SOURCE
        )
        OUTPUT INSERTED.SEQ AS SEQ, INSERTED.REG_DT AS REG_DT
        VALUES (
          @DB_STATUS, @CMPNY_CD, GETDATE(), @USE_YN,
          @DB_CMPNY_REG_PHONE, @REGION, @ADDRESS, @RESERVATION_DATE,
          @FEED_BACK, NULL,
          @AIRCON_WALL, @AIRCON_STAND, @AIRCON_2IN1, @AIRCON_1WAY, @AIRCON_4WAY,
          @REG_SOURCE
        );
      `;

      const insRes = await insReq.query(insertSql);
      const seq = insRes?.recordset?.[0]?.SEQ;
      const regDt = insRes?.recordset?.[0]?.REG_DT;

      await tx.commit();

      console.log("[collect-v2.3] ✅ INSERT SUCCESS", {
        seq,
        phone,
        force,
        regDt,
        regDtKst: regDt ? fmtKst(regDt) : null,
      });

      return {
        statusCode: 200,
        headers: corsHeaders(origin),
        body: JSON.stringify({
          ok: true,
          status: "INSERTED",
          seq,
          phone,
          force,
          regDt,
          regDtKst: regDt ? fmtKst(regDt) : null,
        }),
      };

    } catch (e) {
      console.error("[collect-v2.3] ❌ TX ERROR:", e);
      try { await tx.rollback(); } catch {}
      return {
        statusCode: 500,
        headers: corsHeaders(origin),
        body: JSON.stringify({ ok: false, error: "TX_FAILED" }),
      };
    } finally {
      // ✅ TX 끝나면 락 해제
      releaseLock(lockKey);
    }

  } catch (err) {
    console.error("[collect-v2.3] ❌ DB ERROR:", err);
    // ✅ DB 연결 실패시도 락 해제
    releaseLock(lockKey);
    return {
      statusCode: 500,
      headers: corsHeaders(origin),
      body: JSON.stringify({ ok: false, error: "DB_CONNECT_OR_QUERY_FAILED" }),
    };
  }
};
