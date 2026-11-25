// BAK CITY RP — сервер (Node.js 16+, БЕЗ внешних библиотек).
// Запуск:  node server.js        (порт 8080, или PORT=3000 node server.js)
// Если рядом лежит index.html — он открывается по адресу http://IP:8080/ и адрес сервера в игре подставится сам.
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 8080;
const DB_FILE = path.join(__dirname, 'data.json');

/* ---------------- база (один JSON-файл) ---------------- */
let DB = { users: {}, market: [], nextLot: 1, nextOffer: 1 };
try { DB = Object.assign(DB, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (e) {}
let dirty = false;
function touch() { dirty = true; }
setInterval(() => { if (!dirty) return; dirty = false; try { fs.writeFileSync(DB_FILE + '.tmp', JSON.stringify(DB)); fs.renameSync(DB_FILE + '.tmp', DB_FILE); } catch (e) { console.error('save', e.message); } }, 5000);
process.on('SIGINT', () => { try { fs.writeFileSync(DB_FILE, JSON.stringify(DB)); } catch (e) {} process.exit(0); });

/* ---------------- каталог ---------------- */
// f — прибавка веса (как в меню игры); shop — где продаётся
const ITEMS = [
  { id: 'water', name: 'Вода', w: 0.5, buy: 150, f: 0, shop: ['store'] },
  { id: 'chips', name: 'Чипсы', w: 0.2, buy: 400, f: 0.02, shop: ['store'] },
  { id: 'choco', name: 'Шоколад', w: 0.1, buy: 350, f: 0.015, shop: ['store'] },
  { id: 'salad', name: 'Салат', w: 0.3, buy: 900, f: 0.004, shop: ['cafe'] },
  { id: 'steak', name: 'Стейк с картошкой', w: 0.5, buy: 2500, f: 0.03, shop: ['cafe'] },
  { id: 'cake', name: 'Торт', w: 0.4, buy: 1200, f: 0.04, shop: ['cafe'] },
  { id: 'burger', name: 'Бургер', w: 0.3, buy: 1000, f: 0.03, shop: ['burger'] },
  { id: 'fries', name: 'Картошка фри', w: 0.2, buy: 600, f: 0.02, shop: ['burger'] },
  { id: 'combo', name: 'Комбо XXL', w: 0.8, buy: 2200, f: 0.07, shop: ['burger'] },
  { id: 'cola', name: 'Кола', w: 0.4, buy: 300, f: 0.01, shop: ['burger'] },
].map(i => Object.assign(i, { sell: Math.round(i.buy * 0.5), use: true }));
const ITEM = Object.fromEntries(ITEMS.map(i => [i.id, i]));
const CLIENT_ITEMS = ITEMS.map(i => ({ id: i.id, name: i.name, w: i.w, buy: i.buy, sell: i.sell, use: i.use, shop: i.shop }));
const DONATE = [
  { id: 'title:boss', type: 'item', name: 'Титул «Босс города»', gold: 500, vip: 0 },
  { id: 'title:legend', type: 'item', name: 'Титул «Легенда»', gold: 1500, vip: 0 },
  { id: 'skin:1', type: 'item', name: 'Скин №1', gold: 100, vip: 0 },
  { id: 'skin:2', type: 'item', name: 'Скин №2', gold: 200, vip: 0 },
];
const COMMISSION = 0.05, MAX_LOAN = lvl => Math.min(200000, 5000 + 1500 * lvl);

/* ---------------- WebSocket (минимальная реализация RFC 6455) ---------------- */
function wsFrame(str) {
  const p = Buffer.from(str), n = p.length; let h;
  if (n < 126) h = Buffer.from([0x81, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, p]);
}
const server = http.createServer((req, res) => {
  const f = path.join(__dirname, 'index.html');
  if ((req.url === '/' || req.url.startsWith('/index.html')) && fs.existsSync(f)) { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); fs.createReadStream(f).pipe(res); }
  else { res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('BAK CITY RP server. Online: ' + clients.size); }
});
const clients = new Set(); const byName = new Map(); let nextId = 1;
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) return sock.destroy();
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' +
    crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64') + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { sock, id: nextId++, user: null, name: '', pos: null, buf: Buffer.alloc(0), frag: '', rate: 0, rateT: Date.now(), task: null, offer: null, pendingOffers: {} };
  clients.add(c);
  sock.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > 4e6) return sock.destroy();
    for (;;) { const f = readFrame(c); if (!f) break; if (!handleFrame(c, f)) return; }
  });
  const bye = () => { if (!clients.delete(c)) return; if (c.name && byName.get(c.name) === c) byName.delete(c.name); if (c.user) { c.user.last = Date.now(); touch(); notifyFriends(c.user); } };
  sock.on('close', bye); sock.on('error', bye);
});
function readFrame(c) {
  const b = c.buf; if (b.length < 2) return null;
  const op = b[0] & 15, fin = !!(b[0] & 128), masked = !!(b[1] & 128); let len = b[1] & 127, o = 2;
  if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); o = 4; }
  else if (len === 127) { if (b.length < 10) return null; len = Number(b.readBigUInt64BE(2)); o = 10; }
  if (len > 2e6) { c.sock.destroy(); return null; }
  if (b.length < o + (masked ? 4 : 0) + len) return null;
  let pl;
  if (masked) { const m = b.slice(o, o + 4); pl = Buffer.from(b.slice(o + 4, o + 4 + len)); for (let i = 0; i < len; i++) pl[i] ^= m[i & 3]; o += 4 + len; }
  else { pl = b.slice(o, o + len); o += len; }
  c.buf = b.slice(o); return { op, fin, pl };
}
function handleFrame(c, f) {
  if (f.op === 8) { c.sock.end(); return false; }
  if (f.op === 9) { const p = Buffer.concat([Buffer.from([0x8a, f.pl.length]), f.pl]); if (f.pl.length < 126) c.sock.write(p); return true; }
  if (f.op === 1 || f.op === 0) {
    c.frag += f.pl.toString('utf8');
    if (!f.fin) return true;
    const s = c.frag; c.frag = '';
    let m; try { m = JSON.parse(s); } catch (e) { return true; }
    const t = Date.now(); if (t - c.rateT > 1000) { c.rateT = t; c.rate = 0; }
    if (++c.rate > 80) return true;                       // антифлуд
    try { onMsg(c, m); } catch (e) { console.error('msg', m && m.t, e); }
  }
  return true;
}
function send(c, o) { if (c && !c.sock.destroyed) c.sock.write(wsFrame(JSON.stringify(o))); }
function sendTo(name, o) { const c = byName.get(name); if (c) send(c, o); return !!c; }

/* ---------------- аккаунты ---------------- */
const num = (v, lo, hi, d) => (typeof v === 'number' && isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d);
const lc = s => String(s).toLowerCase();
function hash(pw, salt) { return crypto.scryptSync(pw, salt, 32).toString('hex'); }
function newUser(name, pw) {
  const salt = crypto.randomBytes(16).toString('hex'), card = Array.from({ length: 16 }, () => crypto.randomInt(10)).join('');
  return { name, salt, pass: hash(pw, salt), profile: null, don: { vip: 0, gold: 0, items: [] }, inv: {}, loan: null, card, friends: [], req: [], credits: [], nextCredit: 1, last: 0 };
}
function friendList(u) { return u.friends.map(n => { const f = DB.users[lc(n)]; return { n: f ? f.name : n, on: byName.has(f ? f.name : n) }; }); }
function frPayload(u) { return { list: friendList(u), req: u.req.map(n => (DB.users[lc(n)] || { name: n }).name) }; }
function notifyFriends(u) { u.friends.forEach(n => { const f = DB.users[lc(n)]; if (f) sendTo(f.name, { t: 'friends', fr: frPayload(f) }); }); }
function user(name) { return DB.users[lc(name)]; }
function onlineBy(name) { const u = user(name); return u ? byName.get(u.name) : null; }

function onMsg(c, m) {
  if (!m || typeof m.t !== 'string') return;
  if (!c.user) {
    if (m.t !== 'login' && m.t !== 'reg') return;
    const name = String(m.u || '').trim(), pw = String(m.p || '');
    const fail = msg => { send(c, { t: 'auth', ok: false, msg }); };
    if (!/^[A-Za-z0-9_А-Яа-яЁё]{3,16}$/.test(name)) return fail('Ник: 3–16 букв/цифр');
    if (pw.length < 4 || pw.length > 64) return fail('Пароль: от 4 символов');
    let u = user(name);
    if (m.t === 'reg') { if (u) return fail('Такой ник уже занят'); u = DB.users[lc(name)] = newUser(name, pw); touch(); }
    else if (!u || !crypto.timingSafeEqual(Buffer.from(hash(pw, u.salt), 'hex'), Buffer.from(u.pass, 'hex'))) return fail('Неверный логин или пароль');
    const old = byName.get(u.name); if (old) { send(old, { t: 'kick', msg: 'Вход с другого устройства' }); old.sock.end(); old.user = null; }
    c.user = u; c.name = u.name; byName.set(u.name, c);
    send(c, { t: 'auth', ok: true, name: u.name, profile: u.profile, don: u.don, credits: u.credits, shop: DONATE, items: CLIENT_ITEMS,
      inv: u.inv, loan: u.loan, card: u.card, fac: null, payReady: false, fr: frPayload(u) });
    send(c, { t: 'w', id: c.id }); notifyFriends(u); return;
  }
  const u = c.user, h = H[m.t]; if (h) h(c, u, m);
}

/* ---------------- экономика ---------------- */
function mergeSave(u, m) {                      // принимаем профиль от клиента с базовой проверкой
  const d = m.d; if (d && typeof d === 'object' && d.v === 2) {
    d.money = num(d.money, 0, 1e9, 0); d.bank = num(d.bank, 0, 1e9, 0);
    const prev = u.profile; if (prev && prev.v === 2) { d.bank = Math.min(d.bank, num(prev.bank, 0, 1e9, 0)); } // банк растёт только операциями сервера
    u.profile = d; touch();
  }
  if (typeof m.ack === 'number') { const n = u.credits.length; u.credits = u.credits.filter(x => x.id > m.ack); if (n !== u.credits.length) touch(); }
  return u.profile;
}
function econ(c, u, extra) {
  const p = u.profile || {};
  send(c, Object.assign({ t: 'econ', inv: u.inv, loan: u.loan, card: u.card, money: p.money, bank: p.bank, cars: p.cars, plates: p.plates, tune: p.tune }, extra));
}
function invWeight(u) { let w = 0; for (const k in u.inv) w += (ITEM[k] ? ITEM[k].w : 0) * u.inv[k]; return w; }
function invCap(u) { return 30 + Math.round(20 * ((u.profile && u.profile.muscle) || 0)); }
function addMoney(u, a) { u.profile.money += a; }
function takeMoney(u, a) {                      // наличные, затем банк
  const p = u.profile; if (p.money + p.bank < a) return false;
  const c = Math.min(p.money, a); p.money -= c; p.bank -= (a - c); return true;
}
function income(u, a) {                          // 25% дохода идёт на погашение кредита
  if (u.loan && a > 0) { const pay = Math.min(u.loan.owed, Math.floor(a * 0.25)); u.loan.owed -= pay; a -= pay; if (u.loan.owed <= 0) u.loan = null; }
  addMoney(u, a);
}
function giveCredit(toU, from, amount) {
  const cr = { id: toU.nextCredit++, amount, from }; toU.credits.push(cr);
  const c = byName.get(toU.name); if (c) send(c, { t: 'credit', id: cr.id, amount, from, });
  touch();
}
const need = (c, u, m) => { const p = mergeSave(u, m); if (!p) { send(c, { t: 'econ', msg: 'Профиль не сохранён' }); return null; } return p; };

const H = {
  s(c, u, m) { c.pos = { x: num(m.x, -3000, 3000, 0), y: num(m.y, -100, 500, 0), z: num(m.z, -3000, 3000, 0), f: num(m.f, -10, 10, 0), d: m.d ? 1 : 0, ct: m.ct | 0, cc: m.cc | 0, sk: m.sk | 0, fa: num(m.fa, 0, 1, 0), mu: num(m.mu, 0, 1, 0), tn: m.tn | 0 }; },
  save(c, u, m) { mergeSave(u, m); },
  c(c, u, m) { const s = String(m.m || '').slice(0, 80); if (s) broadcast({ t: 'c', n: u.name, m: s }); },
  pm(c, u, m) {
    const s = String(m.m || '').slice(0, 80), to = onlineBy(String(m.to || ''));
    if (!to) return send(c, { t: 'c', n: '', m: 'Игрок не в сети' });
    send(to, { t: 'c', n: '[ЛС] ' + u.name, m: s }); send(c, { t: 'c', n: '[ЛС → ' + to.name + ']', m: s });
  },
  fchat() {}, fac(c) { send(c, { t: 'fac', fac: null, duty: false }); }, calls(c) { send(c, { t: 'calls', list: [] }); },
  fr(c, u, m) {
    const n = String(m.n || '').trim(), o = user(n); const upd = () => { send(c, { t: 'friends', fr: frPayload(u) }); if (o) sendTo(o.name, { t: 'friends', fr: frPayload(o) }); touch(); };
    if (m.a === 'add') {
      if (!o || o === u) return send(c, { t: 'c', n: '', m: 'Игрок не найден' });
      if (u.friends.includes(o.name)) return;
      if (u.req.includes(o.name)) { u.req = u.req.filter(x => x !== o.name); u.friends.push(o.name); o.friends.push(u.name); }
      else if (!o.req.includes(u.name)) o.req.push(u.name);
      send(c, { t: 'c', n: '', m: 'Заявка отправлена' }); upd();
    } else if (m.a === 'accept' && o && u.req.includes(o.name)) { u.req = u.req.filter(x => x !== o.name); u.friends.push(o.name); o.friends.push(u.name); upd(); }
    else if (m.a === 'decline' && o) { u.req = u.req.filter(x => x !== o.name); upd(); }
    else if (m.a === 'remove' && o) { u.friends = u.friends.filter(x => x !== o.name); o.friends = o.friends.filter(x => x !== u.name); upd(); }
  },
  bank(c, u, m) {
    const p = need(c, u, m); if (!p) return;
    let a = m.all ? (m.a === 'dep' ? p.money : p.bank) : Math.floor(+m.amt || 0); a = Math.floor(a);
    if (!(a >= 1)) return econ(c, u, { msg: 'Неверная сумма' });
    if (m.a === 'dep') { if (p.money < a) return econ(c, u, { msg: 'Не хватает наличных' }); p.money -= a; p.bank += a; }
    else if (m.a === 'wd') { if (p.bank < a) return econ(c, u, { msg: 'Не хватает на счёте' }); p.bank -= a; p.money += a; }
    touch(); econ(c, u, { msg: 'Готово' });
  },
  loan(c, u, m) {
    const p = need(c, u, m); if (!p) return;
    if (m.a === 'take') {
      const a = Math.floor(+m.amt || 0), max = MAX_LOAN(p.lvl | 0 || 1);
      if (u.loan) return econ(c, u, { msg: 'Сначала погасите кредит' });
      if (!(a >= 1) || a > max) return econ(c, u, { msg: 'Сумма кредита: 1–' + max });
      u.loan = { owed: Math.round(a * 1.1) }; p.money += a; touch(); econ(c, u, { msg: 'Кредит выдан' });
    } else if (m.a === 'repay' && u.loan) {
      const a = m.all ? u.loan.owed : Math.min(u.loan.owed, Math.floor(+m.amt || 0));
      if (!(a >= 1)) return econ(c, u, { msg: 'Неверная сумма' });
      if (!takeMoney(u, a)) return econ(c, u, { msg: 'Не хватает денег' });
      u.loan.owed -= a; if (u.loan.owed <= 0) u.loan = null; touch(); econ(c, u, { msg: 'Погашено' });
    }
  },
  xfer(c, u, m) {
    const p = need(c, u, m); if (!p) return; const to = user(String(m.to || '').trim()), a = Math.floor(+m.amount || 0);
    if (!to || to === u) return econ(c, u, { msg: 'Игрок не найден' });
    if (!(a >= 1) || !takeMoney(u, a)) return econ(c, u, { msg: 'Не хватает денег' });
    giveCredit(to, u.name, a); touch(); econ(c, u, { msg: 'Переведено ' + a + ' → ' + to.name });
  },
  ibuy(c, u, m) {
    const p = need(c, u, m); if (!p) return; const it = ITEM[m.id], q = Math.max(1, Math.min(99, Math.floor(+m.qty || 1)));
    if (!it || !it.shop.includes(m.shop)) return econ(c, u, { msg: 'Нет такого товара' });
    if (invWeight(u) + it.w * q > invCap(u)) return econ(c, u, { msg: 'Рюкзак переполнен' });
    if (!takeMoney(u, it.buy * q)) return econ(c, u, { msg: 'Не хватает денег' });
    u.inv[it.id] = (u.inv[it.id] || 0) + q; touch(); econ(c, u, { msg: it.name + ' в рюкзаке' });
  },
  isell(c, u, m) {
    const p = need(c, u, m); if (!p) return; const it = ITEM[m.id], q = Math.max(1, Math.floor(+m.qty || 1));
    if (!it || (u.inv[it.id] || 0) < q) return econ(c, u, { msg: 'Нет предмета' });
    removeInv(u, it.id, q); income(u, it.sell * q); touch(); econ(c, u, { msg: 'Продано' });
  },
  iuse(c, u, m) {
    const p = need(c, u, m); if (!p) return; const it = ITEM[m.id];
    if (!it || !u.inv[it.id]) return econ(c, u, { msg: 'Нет предмета' });
    removeInv(u, it.id, 1); p.fat = Math.min(1, (p.fat || 0) + it.f); touch(); econ(c, u, { fat: p.fat, msg: it.name + ': съедено' });
  },
  igive(c, u, m) {
    const p = need(c, u, m); if (!p) return; const it = ITEM[m.id], to = user(String(m.to || '').trim()), q = Math.max(1, Math.floor(+m.qty || 1));
    if (!it || !to || to === u || (u.inv[it.id] || 0) < q) return econ(c, u, { msg: 'Нельзя передать' });
    removeInv(u, it.id, q); to.inv[it.id] = (to.inv[it.id] || 0) + q; touch(); econ(c, u, { msg: 'Передано' });
    const oc = byName.get(to.name); if (oc) send(oc, { t: 'econ', inv: to.inv, msg: u.name + ' передал(а) ' + it.name + ' ×' + q });
  },
  mk(c, u, m) {
    if (m.a === 'browse') return send(c, { t: 'mk', list: DB.market });
    const p = need(c, u, m); if (!p) return;
    const done = msg => { touch(); econ(c, u, { msg }); send(c, { t: 'mk', list: DB.market }); };
    if (m.a === 'list') {
      const it = ITEM[m.id], q = Math.max(1, Math.floor(+m.qty || 1)), pr = Math.floor(+m.price || 0);
      if (!it || !(pr >= 1) || (u.inv[it.id] || 0) < q) return econ(c, u, { msg: 'Нельзя выставить' });
      removeInv(u, it.id, q); DB.market.push({ id: DB.nextLot++, item: it.id, qty: q, price: pr, seller: u.name }); done('Выставлено на рынок');
    } else if (m.a === 'cancel') {
      const i = DB.market.findIndex(l => l.id === m.lid && l.seller === u.name); if (i < 0) return;
      const l = DB.market.splice(i, 1)[0]; u.inv[l.item] = (u.inv[l.item] || 0) + l.qty; done('Лот снят');
    } else if (m.a === 'buy') {
      const l = DB.market.find(x => x.id === m.lid); if (!l || l.seller === u.name) return econ(c, u, { msg: 'Лот недоступен' });
      const q = Math.max(1, Math.min(l.qty, Math.floor(+m.qty || 1))), cost = l.price * q;
      if (invWeight(u) + ITEM[l.item].w * q > invCap(u)) return econ(c, u, { msg: 'Рюкзак переполнен' });
      if (!takeMoney(u, cost)) return econ(c, u, { msg: 'Не хватает денег' });
      u.inv[l.item] = (u.inv[l.item] || 0) + q; l.qty -= q; if (l.qty <= 0) DB.market.splice(DB.market.indexOf(l), 1);
      const s = user(l.seller); if (s) giveCredit(s, 'Рынок', Math.floor(cost * (1 - COMMISSION)));
      done('Куплено');
    }
  },
  offer(c, u, m) {
    const p = need(c, u, m); if (!p) return; const to = onlineBy(String(m.to || '').trim()), car = m.car | 0, pr = Math.floor(+m.price || 0);
    if (!to || to === c) return econ(c, u, { msg: 'Игрок не в сети' });
    if (!(pr >= 1) || car === 0 || !p.cars.includes(car) || p.car === car) return econ(c, u, { msg: 'Эту машину продать нельзя' });
    const id = DB.nextOffer++; to.offer = { id, from: u.name, car, price: pr }; send(to, { t: 'offerIn', id, from: u.name, car, price: pr });
    econ(c, u, { msg: 'Предложение отправлено' });
  },
  decline(c) { c.offer = null; },
  accept(c, u, m) {
    const p = need(c, u, m), o = c.offer; if (!p || !o || o.id !== m.id) return econ(c, u, { msg: 'Предложение устарело' }); c.offer = null;
    const s = user(o.from); if (!s || !s.profile || !s.profile.cars.includes(o.car) || s.profile.car === o.car) return econ(c, u, { msg: 'Сделка невозможна' });
    if (!takeMoney(u, o.price)) return econ(c, u, { msg: 'Не хватает денег' });
    const sp = s.profile, i = sp.cars.indexOf(o.car); sp.cars.splice(i, 1);
    const plate = sp.plates && sp.plates[o.car], tune = sp.tune && sp.tune[o.car];
    if (sp.plates) delete sp.plates[o.car]; if (sp.tune) delete sp.tune[o.car];
    if (!p.cars.includes(o.car)) p.cars.push(o.car);
    p.plates = p.plates || {}; p.tune = p.tune || {}; if (plate) p.plates[o.car] = plate; if (tune) p.tune[o.car] = tune;
    sp.money += o.price; touch();
    econ(c, u, { msg: 'Машина куплена' });
    const sc = byName.get(s.name); if (sc) send(sc, { t: 'econ', money: sp.money, cars: sp.cars, plates: sp.plates, tune: sp.tune, msg: 'Машину купил ' + u.name });
  },
  task(c, u, m) {
    const p = need(c, u, m); if (!p) return;
    if (m.op === 'start') { c.task = { x: num(m.x, -3000, 3000, 0), z: num(m.z, -3000, 3000, 0), t: Date.now() }; send(c, { t: 'task', op: 'start', ok: true, msg: 'Задание принято' }); }
    else if (m.op === 'cancel') { c.task = null; send(c, { t: 'task', op: 'cancel', ok: true }); }
    else if (m.op === 'done') {
      const k = c.task; c.task = null;
      if (!k || Date.now() - k.t < 8000) return send(c, { t: 'task', op: 'done', ok: false, msg: 'Задание не засчитано' });
      const pos = c.pos; if (!pos || Math.hypot(pos.x - k.x, pos.z - k.z) > 40) return send(c, { t: 'task', op: 'done', ok: false, msg: 'Вы не на месте' });
      const pay = Math.round((400 + 25 * (p.lvl || 1)) * (1 + 0.05 * u.don.vip)); income(u, pay); p.jobs = (p.jobs || 0) + 1; p.earned = (p.earned || 0) + pay; touch();
      send(c, { t: 'task', op: 'done', ok: true, msg: 'Задание выполнено: +' + pay }); econ(c, u, {});
    }
  },
  spend(c, u, m) {
    const it = DONATE.find(x => x.id === m.id); const r = msg => send(c, { t: 'buyres', ok: false, msg });
    if (!it || u.don.items.includes(it.id)) return r('Нельзя купить');
    if (u.don.gold < it.gold || u.don.vip < it.vip) return r('Не хватает золота или VIP');
    u.don.gold -= it.gold; u.don.items.push(it.id); touch(); send(c, { t: 'buyres', ok: true, don: u.don, msg: 'Куплено: ' + it.name });
  },
  buy(c) { send(c, { t: 'buyres', ok: false, msg: 'Приём платежей не подключён' }); },
  // полиция/медицина/фракции — заглушки, чтобы клиент не ждал ответа
  call(c) { send(c, { t: 'c', n: '', m: 'Служб на смене нет (фракции пока не включены на сервере)' }); },
};
function removeInv(u, id, q) { u.inv[id] -= q; if (u.inv[id] <= 0) delete u.inv[id]; }
function broadcast(o) { const s = wsFrame(JSON.stringify(o)); clients.forEach(c => { if (c.user) c.sock.write(s); }); }

/* ---------------- рассылка позиций игроков (10 раз/сек) ---------------- */
setInterval(() => {
  const all = []; clients.forEach(c => { if (c.user && c.pos) { const p = c.pos; all.push({ i: c.id, n: c.name, x: p.x, y: p.y, z: p.z, f: p.f, d: p.d, ct: p.ct, cc: p.cc, sk: p.sk, fa: p.fa, mu: p.mu, tn: p.tn, vip: c.user.don.vip, tn2: 0 }); } });
  clients.forEach(c => { if (c.user) send(c, { t: 'p', l: all.filter(a => a.i !== c.id) }); });
}, 100);

server.listen(PORT, () => console.log('BAK CITY RP server on port ' + PORT + '  (ws://IP:' + PORT + ')'));
