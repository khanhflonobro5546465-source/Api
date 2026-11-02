const express = require("express");

const app = express();
app.use(express.json({ limit: "200kb" }));

// ===== Lưu trữ =====
// Có biến môi trường Upstash/Vercel KV -> dùng Redis (bền trên Vercel).
// Không có -> dùng RAM (chỉ phù hợp chạy local / VPS).
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const HASH = "pets";
const TTL_MS = 10 * 60 * 1000; // 10 phút không cập nhật thì xóa
const MAX_ENTRIES = 500;

const mem = new Map();

async function redis(cmd) {
  const r = await fetch(REDIS_URL, {
    method: "POST",
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify(cmd),
  });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}

const db = {
  async all() {
    if (!REDIS_URL) return [...mem.entries()];
    const flat = (await redis(["HGETALL", HASH])) || [];
    const out = [];
    for (let i = 0; i < flat.length; i += 2) out.push([flat[i], JSON.parse(flat[i + 1])]);
    return out;
  },
  async set(entries) {
    if (!entries.length) return;
    if (!REDIS_URL) return entries.forEach(([k, v]) => mem.set(k, v));
    await redis(["HSET", HASH, ...entries.flatMap(([k, v]) => [k, JSON.stringify(v)])]);
  },
  async del(keys) {
    if (!keys.length) return;
    if (!REDIS_URL) return keys.forEach((k) => mem.delete(k));
    await redis(["HDEL", HASH, ...keys]);
  },
};

// ===== Helper =====
function clean(p) {
  if (!p || typeof p !== "object") return null;
  const petName = String(p.petName ?? "").trim().slice(0, 60);
  const jobId = String(p.jobId ?? "").trim().slice(0, 100);
  const value = Number(p.value);
  if (!petName || !jobId || !Number.isFinite(value) || value <= 0) return null;
  return {
    petName,
    jobId,
    value,
    iconId: String(p.iconId ?? "").replace(/[^\d]/g, "").slice(0, 20),
    timestamp: Number(p.timestamp) || Date.now(),
  };
}

const keyOf = (p) => `${p.jobId}|${p.petName}|${Math.floor(p.value)}`;
const wrap = (fn) => (req, res) =>
  fn(req, res).catch((e) => res.status(500).json({ error: String(e.message || e) }));

// ===== Routes =====
app.get("/", (req, res) => res.json({ ok: true }));

// Lấy danh sách (Lua gọi mỗi 1 giây)
let petsCache = { at: 0, data: null };
app.get("/api/getPets", wrap(async (req, res) => {
  const now = Date.now();
  res.set("Cache-Control", "no-store");
  // Cache 1.5s: nhiều người poll cùng lúc chỉ tốn 1 lần đọc Redis
  if (petsCache.data && now - petsCache.at < 1500) return res.json(petsCache.data);
  const list = [];
  const expired = [];
  for (const [k, e] of await db.all()) {
    if (now - e.receivedAt > TTL_MS) expired.push(k);
    else list.push(e);
  }
  await db.del(expired);
  list.sort((a, b) => b.value - a.value);
  const out = list.slice(0, MAX_ENTRIES).map(({ petName, jobId, value, iconId, timestamp }) => ({
    petName, jobId, value, iconId, timestamp,
  }));
  petsCache = { at: Date.now(), data: out };
  res.json(out);
}));

// Thêm pet
app.post("/api/addPet", wrap(async (req, res) => {
  const p = clean(req.body);
  if (!p) return res.status(400).json({ ok: false });
  await db.set([[keyOf(p), { ...p, receivedAt: Date.now() }]]);
  res.json({ ok: true });
}));

// Xóa pet
app.post("/api/removePet", wrap(async (req, res) => {
  const p = clean(req.body);
  if (!p) return res.status(400).json({ ok: false });
  await db.del([keyOf(p)]);
  res.json({ ok: true });
}));

// Đồng bộ toàn bộ pet của 1 server
app.post("/api/syncPets", wrap(async (req, res) => {
  const jobId = String(req.body?.jobId ?? "").trim().slice(0, 100);
  const pets = req.body?.pets;
  if (!jobId || !Array.isArray(pets)) return res.status(400).json({ ok: false });

  const old = (await db.all()).filter(([, e]) => e.jobId === jobId).map(([k]) => k);
  await db.del(old);

  const fresh = [];
  for (const raw of pets.slice(0, 50)) {
    const p = clean({ ...raw, jobId });
    if (p) fresh.push([keyOf(p), { ...p, receivedAt: Date.now() }]);
  }
  await db.set(fresh);
  res.json({ ok: true });
}));

// Vercel dùng export; chạy local thì listen
module.exports = app;
if (require.main === module) {
  const port = process.env.PORT || 3000;
  app.listen(port, () => console.log("Pet API chạy ở cổng " + port));
}
