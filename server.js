// Scispare Mind — serveur de mise en relation ("En ligne" mondial)
//
// Version 2 (novembre 2026) : ajoute les AMIS, sans rien casser pour les anciennes versions du jeu.
//
// Ce serveur fait trois choses, très légères :
//   1. Mettre en relation deux joueurs qui cherchent une partie au hasard (matchmaking).
//   2. Relayer les messages de connexion WebRTC (offre/réponse) entre eux.
//   3. (v2) Savoir quels amis sont connectés, et transmettre les défis entre amis.
//
// Une fois les deux joueurs connectés l'un à l'autre, TOUS les coups de la partie
// passent directement d'un téléphone à l'autre (WebRTC), plus jamais par ce serveur.
// Le serveur ne garde AUCUNE donnée sur disque : tout est en mémoire et disparaît au redémarrage.
// Les listes d'amis sont enregistrées sur les téléphones, pas ici.
//
// Compatibilité :
//   - Ancienne version du jeu (adresse sans "?v=2") : mise en file d'attente dès la connexion,
//     exactement comme avant.
//   - Nouvelle version (adresse avec "?v=2") : envoie {type:'find'} pour chercher au hasard
//     (même file d'attente que les anciennes versions : les deux peuvent jouer ensemble),
//     ou {type:'join', room} pour rejoindre un ami qui a accepté un défi.
//   - Connexion "salon" (?v=2&lobby=1) : reste ouverte pendant que le jeu est ouvert,
//     pour la présence des amis et les défis.

const http = require('http');
const crypto = require('crypto');
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;

// Petite page de contrôle (utile pour vérifier que le serveur tourne, et pour le réveiller).
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Scispare Mind matchmaking v2 OK\n');
});
const wss = new WebSocket.Server({ server });

let waitingPlayer = null;
const lobby = new Map();      // id joueur -> socket salon
const watchers = new Map();   // id joueur -> Set(socket salon de ceux qui le suivent)
const invites = new Map();    // "deId>aId" -> { from, to, at }
const rooms = new Map();      // code salle -> { host: socket|null, guest: socket|null, at }

const ID_RE = /^[a-f0-9]{16,64}$/;
const WAITING_TIMEOUT_MS = 90 * 1000;     // libère un joueur resté seul trop longtemps (repris de la v1 en ligne)
const HEARTBEAT_INTERVAL_MS = 30 * 1000;  // fréquence de vérification des connexions mortes
const MAX_FRIENDS = 15;

function safeSend(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}
function cleanName(s) { return String(s || '').replace(/[\u0000-\u001f<>]/g, '').slice(0, 16); }
function cleanAvatar(s) { return String(s || '').slice(0, 8); }

/* ---------- Mise en relation au hasard (identique à la v1) ---------- */
// Si le joueur en attente l'est depuis trop longtemps (personne ne s'est connecté),
// on le libère plutôt que de le matcher avec un adversaire qui a peut-être quitté.
function clearWaitingIfStale() {
  if (waitingPlayer && Date.now() - waitingPlayer.waitingSince > WAITING_TIMEOUT_MS) {
    safeSend(waitingPlayer, { type: 'no_opponent' });
    waitingPlayer = null;
  }
}
function enqueue(ws) {
  clearWaitingIfStale();
  if (waitingPlayer === null || waitingPlayer.readyState !== WebSocket.OPEN || waitingPlayer === ws) {
    waitingPlayer = ws;
    waitingPlayer.waitingSince = Date.now();
    safeSend(ws, { type: 'waiting' });
  } else {
    const host = waitingPlayer;
    waitingPlayer = null;
    pair(host, ws);
  }
}
function pair(host, guest) {
  host.peer = guest;
  guest.peer = host;
  safeSend(host, { type: 'matched', role: 'host' });
  safeSend(guest, { type: 'matched', role: 'guest' });
}

/* ---------- Salon : présence des amis ---------- */
function notifyPresence(id, online) {
  const set = watchers.get(id);
  if (!set) return;
  set.forEach(w => safeSend(w, { type: 'presence', id, online }));
}
function setWatchList(ws, ids) {
  (ws.watching || []).forEach(id => {
    const set = watchers.get(id);
    if (set) { set.delete(ws); if (!set.size) watchers.delete(id); }
  });
  ws.watching = (Array.isArray(ids) ? ids : []).filter(x => ID_RE.test(String(x))).slice(0, MAX_FRIENDS);
  ws.watching.forEach(id => {
    if (!watchers.has(id)) watchers.set(id, new Set());
    watchers.get(id).add(ws);
  });
  safeSend(ws, { type: 'presence_list', online: ws.watching.filter(id => lobby.has(id)) });
}
function leaveLobby(ws) {
  setWatchList(ws, []);
  if (ws.pid && lobby.get(ws.pid) === ws) {
    lobby.delete(ws.pid);
    notifyPresence(ws.pid, false);
    // défis en attente envoyés par ce joueur : prévenir les destinataires
    invites.forEach((inv, key) => {
      if (inv.from === ws.pid) { invites.delete(key); safeSend(lobby.get(inv.to), { type: 'invite_cancel', from: ws.pid }); }
      else if (inv.to === ws.pid) { invites.delete(key); safeSend(lobby.get(inv.from), { type: 'invite_resp', from: ws.pid, accept: false, reason: 'offline' }); }
    });
  }
}

function handleLobbyMessage(ws, msg) {
  switch (msg.type) {
    case 'hello': {
      const id = String(msg.id || '');
      if (!ID_RE.test(id)) return;
      const old = lobby.get(id);
      if (old && old !== ws) { old.replaced = true; try { old.close(); } catch (e) {} }
      ws.pid = id;
      ws.name = cleanName(msg.name);
      ws.avatar = cleanAvatar(msg.avatar);
      ws.busy = !!msg.busy;
      lobby.set(id, ws);
      notifyPresence(id, true);
      if (Array.isArray(msg.watch)) setWatchList(ws, msg.watch);
      break;
    }
    case 'watch':
      if (ws.pid) setWatchList(ws, msg.ids);
      break;
    case 'status':
      ws.busy = !!msg.busy;
      if (msg.name !== undefined) ws.name = cleanName(msg.name);
      if (msg.avatar !== undefined) ws.avatar = cleanAvatar(msg.avatar);
      break;
    case 'invite': {
      // Un défi n'est transmis qu'entre deux joueurs qui se suivent mutuellement (amis des deux côtés).
      const to = String(msg.to || '');
      const target = lobby.get(to);
      if (!ws.pid || !target) { safeSend(ws, { type: 'invite_resp', from: to, accept: false, reason: 'offline' }); return; }
      if (!(target.watching || []).includes(ws.pid)) { safeSend(ws, { type: 'invite_resp', from: to, accept: false, reason: 'not_friend' }); return; }
      if (target.busy) { safeSend(ws, { type: 'invite_resp', from: to, accept: false, reason: 'busy' }); return; }
      invites.set(ws.pid + '>' + to, { from: ws.pid, to, at: Date.now() });
      safeSend(target, { type: 'invite', from: ws.pid, name: ws.name, avatar: ws.avatar });
      break;
    }
    case 'invite_cancel': {
      const to = String(msg.to || '');
      if (invites.delete(ws.pid + '>' + to)) safeSend(lobby.get(to), { type: 'invite_cancel', from: ws.pid });
      break;
    }
    case 'invite_resp': {
      const from = String(msg.to || '');           // celui qui avait lancé le défi
      const key = from + '>' + ws.pid;
      if (!invites.has(key)) return;
      invites.delete(key);
      const inviter = lobby.get(from);
      if (!inviter) { safeSend(ws, { type: 'invite_resp', from, accept: false, reason: 'offline' }); return; }
      if (!msg.accept) { safeSend(inviter, { type: 'invite_resp', from: ws.pid, accept: false, reason: msg.reason === 'busy' ? 'busy' : 'declined' }); return; }
      const room = crypto.randomBytes(12).toString('hex');
      rooms.set(room, { host: null, guest: null, at: Date.now(), hostId: from, guestId: ws.pid });
      safeSend(inviter, { type: 'invite_go', room, role: 'host', with: ws.pid });
      safeSend(ws, { type: 'invite_go', room, role: 'guest', with: from });
      break;
    }
    case 'ping':
      safeSend(ws, { type: 'pong' });
      break;
  }
}

/* ---------- Connexions ---------- */
wss.on('connection', (ws, req) => {
  ws.peer = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  let params;
  try { params = new URL(req.url, 'http://x').searchParams; } catch (e) { params = new URLSearchParams(); }
  const v2 = params.get('v') === '2';
  ws.isLobby = v2 && params.get('lobby') === '1';

  if (!v2) enqueue(ws);   // ancienne version du jeu : comportement d'origine

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch (e) { return; }
    if (!msg || typeof msg !== 'object') return;

    if (ws.isLobby) { handleLobbyMessage(ws, msg); return; }

    if (v2 && msg.type === 'find' && !ws.peer) { enqueue(ws); return; }
    if (v2 && msg.type === 'join' && !ws.peer) {
      const room = rooms.get(String(msg.room || ''));
      if (!room) { safeSend(ws, { type: 'room_gone' }); return; }
      const role = msg.role === 'host' ? 'host' : 'guest';
      room[role] = ws;
      if (room.host && room.guest && room.host.readyState === WebSocket.OPEN && room.guest.readyState === WebSocket.OPEN) {
        rooms.delete(String(msg.room));
        pair(room.host, room.guest);
      } else {
        safeSend(ws, { type: 'waiting' });
      }
      return;
    }
    if (ws.peer) safeSend(ws.peer, msg);   // offre / réponse WebRTC
  });

  ws.on('close', () => {
    if (waitingPlayer === ws) waitingPlayer = null;
    if (ws.isLobby) { if (!ws.replaced) leaveLobby(ws); else setWatchList(ws, []); }
    if (ws.peer) {
      safeSend(ws.peer, { type: 'peer_disconnected' });
      ws.peer.peer = null;
    }
  });
});

// Ferme les connexions mortes (perte réseau, appli tuée, veille du téléphone…), libère un joueur
// resté seul trop longtemps, et fait le ménage (défis et salles de plus de 2 minutes).
// Comme la v1, ce serveur doit tourner en UNE SEULE instance (tout est en mémoire).
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
  clearWaitingIfStale();
  const old = Date.now() - 120000;
  invites.forEach((inv, key) => { if (inv.at < old) invites.delete(key); });
  rooms.forEach((r, key) => { if (r.at < old) rooms.delete(key); });
}, HEARTBEAT_INTERVAL_MS);

server.listen(PORT, () => console.log('Serveur de mise en relation Scispare Mind (v2, amis) démarré sur le port ' + PORT));
