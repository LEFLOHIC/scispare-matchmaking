// Scispare Mind — serveur de mise en relation ("En ligne" mondial)
//
// Ce serveur ne fait QUE deux choses, très légères :
//   1. Mettre en relation deux joueurs qui cherchent une partie (matchmaking).
//   2. Relayer les messages de connexion WebRTC (offre/réponse) entre eux.
//
// Une fois les deux joueurs connectés l'un à l'autre, TOUS les coups de la partie
// passent directement d'un téléphone à l'autre (WebRTC), plus jamais par ce serveur.
// Le serveur reste donc très peu sollicité, même avec beaucoup de parties simultanées
// — à condition de tourner en une seule instance (voir le mémo d'archive à ce sujet).

const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });

const WAITING_TIMEOUT_MS = 90 * 1000;     // libère un joueur resté seul trop longtemps
const HEARTBEAT_INTERVAL_MS = 30 * 1000;  // fréquence de vérification des connexions mortes

let waitingPlayer = null;

function safeSend(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

// Si le joueur en attente l'est depuis trop longtemps (personne ne s'est connecté),
// on le libère plutôt que de le faire attendre indéfiniment ou de le matcher
// avec un adversaire qui a peut-être quitté sans que la coupure soit détectée.
function clearWaitingIfStale() {
  if (waitingPlayer && Date.now() - waitingPlayer.waitingSince > WAITING_TIMEOUT_MS) {
    safeSend(waitingPlayer, { type: 'no_opponent' });
    waitingPlayer = null;
  }
}

wss.on('connection', (ws) => {
  ws.peer = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  clearWaitingIfStale();

  if (waitingPlayer === null || waitingPlayer.readyState !== WebSocket.OPEN) {
    waitingPlayer = ws;
    waitingPlayer.waitingSince = Date.now();
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
      // Un message mal formé (ou une tentative malveillante) ne doit jamais
      // faire planter le serveur pour tout le monde — d'où le try/catch,
      // absent de la version précédente.
      try {
        safeSend(ws.peer, JSON.parse(data));
      } catch (e) {
        // message ignoré silencieusement
      }
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

// Ferme les connexions mortes (perte réseau, appli tuée en arrière-plan, veille du
// téléphone, etc.) — sans ça, une connexion "fantôme" pourrait rester considérée
// comme active et bloquer un vrai joueur dans une mise en relation qui n'aboutira
// jamais.
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
  clearWaitingIfStale();
}, HEARTBEAT_INTERVAL_MS);

console.log('Serveur de mise en relation Scispare Mind démarré sur le port ' + PORT);
