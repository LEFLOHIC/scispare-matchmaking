// Scispare Mind — serveur de mise en relation ("En ligne" mondial)
//
// Ce serveur ne fait QUE deux choses, très légères :
//   1. Mettre en relation deux joueurs qui cherchent une partie (matchmaking).
//   2. Relayer les messages de connexion WebRTC (offre/réponse/candidats) entre eux.
//
// Une fois les deux joueurs connectés l'un à l'autre, TOUS les coups de la partie
// passent directement d'un téléphone à l'autre (WebRTC), plus jamais par ce serveur.
// Le serveur reste donc très peu sollicité, même avec beaucoup de parties simultanées.

const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });

let waitingPlayer = null;

function safeSend(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

wss.on('connection', (ws) => {
  ws.peer = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  if (waitingPlayer === null || waitingPlayer.readyState !== WebSocket.OPEN) {
    waitingPlayer = ws;
    safeSend(ws, { type: 'waiting' });
  } else {
    const host = waitingPlayer;
    const guest = ws;
    waitingPlayer = null;
    host.peer = guest;
    guest.peer = host;
    safeSend(host, { type: 'matched', role: 'host' });
    safeSend(guest, { type: 'matched', role: 'guest' });
  }

  ws.on('message', (data) => {
    if (ws.peer) {
      safeSend(ws.peer, JSON.parse(data));
    }
  });

  ws.on('close', () => {
    if (waitingPlayer === ws) waitingPlayer = null;
    if (ws.peer) {
      safeSend(ws.peer, { type: 'peer_disconnected' });
      ws.peer.peer = null;
    }
  });
});

// Keep connections alive on hosts that close idle sockets (e.g. Render free tier)
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

console.log('Serveur de mise en relation Scispare Mind démarré sur le port ' + PORT);
