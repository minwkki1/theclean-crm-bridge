// netlify/functions/collect.js

const ALLOW_ORIGINS = [
  "https://ming709826297.imweb.me",   // ✅ 여기 너 아임웹 도메인으로 바꿔
  "https://www.your-imweb-domain.com"
];

function pickOrigin(headers) {
  const origin = headers?.origin || headers?.Origin || "";
  return ALLOW_ORIGINS.includes(origin) ? origin : "";
}

function corsHeaders(origin) {
  // sendBeacon은 보통 preflight가 없지만, 혹시 대비용
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

  // sendBeacon은 content-type이 text/plain 이거나 form-urlencoded로 오는 경우가 있어
  const raw = event.body || "";
  const contentType = (event.headers["content-type"] || event.headers["Content-Type"] || "").toLowerCase();

  let payload = null;

  if (contentType.includes("application/json")) {
    payload = safeJsonParse(raw);
  } else if (contentType.includes("application/x-www-form-urlencoded")) {
    // a=b&c=d 형태
    const params = new URLSearchParams(raw);
    payload = Object.fromEntries(params.entries());
    // payload.json 같은 키로 JSON이 들어오는 패턴도 대비
    if (payload.json) {
      const j = safeJsonParse(payload.json);
      if (j) payload = j;
    }
  } else {
    // text/plain 등: 그냥 JSON 문자열로 오는 경우가 많음
    payload = safeJsonParse(raw) || { raw };
  }

  // ✅ 서버 로그 (Netlify Functions logs에서 확인 가능)
  console.log("[collect] headers=", event.headers);
  console.log("[collect] payload=", payload);

  // TODO: 여기서 DB INSERT 붙일 것 (다음 단계)

  return {
    statusCode: 204,
    headers: corsHeaders(origin),
    body: ""
  };
};
