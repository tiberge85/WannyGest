#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
  PASSERELLE TRACCAR → WannyGest  (tracking GPS)
═══════════════════════════════════════════════════════════════════════════

Les balises GPS physiques (Concox / GT06 / Coban / TK103…) parlent un protocole
BINAIRE en TCP — elles ne savent pas appeler /tracking/position/update en JSON.
Traccar (https://www.traccar.org) reçoit ce binaire et sait « forwarder » chaque
position en JSON. Ce petit service :

    Balise GPS  ──TCP binaire──►  Traccar  ──JSON forward──►  CE SCRIPT  ──JSON──►  WannyGest
                                                              (traduit + convertit)

Il fait 3 choses :
  1. reçoit le « Position forwarding » de Traccar (POST JSON)
  2. extrait l'IMEI (uniqueId), lat, lng, vitesse (nœuds → km/h), adresse
  3. relaie vers POST {WANNY_URL}/tracking/position/update au format WannyGest

─── INSTALLATION ──────────────────────────────────────────────────────────
  pip install flask requests
  export WANNY_URL="https://TON-APP.onrender.com"     # SANS slash final
  export BRIDGE_TOKEN=""           # si tu ajoutes un token côté WannyGest (option a)
  export BRIDGE_PORT="5055"        # port d'écoute de cette passerelle
  python3 traccar_bridge.py

─── CÔTÉ TRACCAR (fichier conf/traccar.xml) ───────────────────────────────
  <entry key='forward.enable'>true</entry>
  <entry key='forward.url'>http://IP_DE_CE_SCRIPT:5055/forward</entry>
  <entry key='forward.type'>json</entry>
  Puis redémarre Traccar. (Traccar 5/6 : Paramètres serveur → Forwarding aussi possible.)

─── CÔTÉ WannyGest ────────────────────────────────────────────────────────
  Chaque véhicule (Tracking → Véhicules → Modifier) doit avoir son champ
  « ID balise GPS » (gps_device_id) = l'IMEI déclaré dans Traccar (uniqueId).
  C'est la clé d'appariement.
═══════════════════════════════════════════════════════════════════════════
"""
import os
import sys
import logging
import requests
from flask import Flask, request, jsonify

# ─── Configuration (via variables d'environnement) ───────────────────────────
WANNY_URL    = os.environ.get('WANNY_URL', 'http://localhost:5000').rstrip('/')
TARGET_URL   = WANNY_URL + '/tracking/position/update'
BRIDGE_TOKEN = os.environ.get('BRIDGE_TOKEN', '').strip()   # optionnel (sécurité)
BRIDGE_PORT  = int(os.environ.get('BRIDGE_PORT', '5055'))
HTTP_TIMEOUT = float(os.environ.get('BRIDGE_TIMEOUT', '8'))
KNOTS_TO_KMH = 1.852   # Traccar exprime la vitesse en nœuds

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('traccar-bridge')

app = Flask(__name__)


def _extract(payload):
    """Traduit un payload de forwarding Traccar en (device_id, lat, lng, speed_kmh, address).
    Tolérant aux variantes de structure entre versions de Traccar."""
    pos = payload.get('position') or payload  # certains forwards envoient la position à plat
    dev = payload.get('device') or {}

    # device_id : on privilégie l'IMEI (uniqueId), sinon l'id numérique Traccar
    device_id = (dev.get('uniqueId')
                 or pos.get('uniqueId')
                 or pos.get('deviceId')
                 or dev.get('id') or '')
    device_id = str(device_id).strip()

    lat = pos.get('latitude', pos.get('lat', 0))
    lng = pos.get('longitude', pos.get('lng', pos.get('lon', 0)))
    speed_knots = pos.get('speed', 0) or 0
    address = pos.get('address', '') or ''

    try:
        speed_kmh = round(float(speed_knots) * KNOTS_TO_KMH, 1)
    except (TypeError, ValueError):
        speed_kmh = 0.0

    return device_id, float(lat or 0), float(lng or 0), speed_kmh, address


@app.route('/forward', methods=['POST'])
def forward():
    payload = request.get_json(silent=True)
    if payload is None:
        log.warning("Payload non-JSON ignoré (Content-Type=%s)", request.content_type)
        return jsonify({'error': 'JSON attendu'}), 400

    try:
        device_id, lat, lng, speed, address = _extract(payload)
    except Exception as e:
        log.error("Extraction échouée: %s — payload=%s", e, payload)
        return jsonify({'error': 'payload illisible'}), 400

    if not device_id:
        log.warning("Position sans device_id/uniqueId ignorée")
        return jsonify({'error': 'device_id introuvable dans le payload Traccar'}), 422

    # Ignore les positions sans coordonnées valides (Traccar envoie parfois des events à 0,0)
    if lat == 0 and lng == 0:
        log.info("Position 0,0 ignorée pour device %s", device_id)
        return jsonify({'ok': True, 'skipped': 'coordonnées nulles'}), 200

    body = {'device_id': device_id, 'lat': lat, 'lng': lng, 'speed': speed, 'address': address}
    if BRIDGE_TOKEN:
        body['token'] = BRIDGE_TOKEN  # si tu actives la vérification côté WannyGest

    headers = {'Content-Type': 'application/json'}
    if BRIDGE_TOKEN:
        headers['X-Tracking-Token'] = BRIDGE_TOKEN  # ou en header, selon ton implémentation

    try:
        r = requests.post(TARGET_URL, json=body, headers=headers, timeout=HTTP_TIMEOUT)
    except requests.RequestException as e:
        log.error("Relais vers WannyGest échoué: %s", e)
        return jsonify({'error': 'WannyGest injoignable', 'detail': str(e)}), 502

    if r.status_code == 200:
        log.info("✅ %s → %.5f,%.5f %.0f km/h", device_id, lat, lng, speed)
    else:
        # 404 = device inconnu : le gps_device_id du véhicule ne correspond pas à l'IMEI
        log.warning("⚠️ WannyGest a répondu %s pour device %s : %s",
                    r.status_code, device_id, r.text[:200])
    return jsonify({'ok': r.status_code == 200, 'wannygest_status': r.status_code}), 200


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'ok': True, 'target': TARGET_URL, 'token': bool(BRIDGE_TOKEN)})


if __name__ == '__main__':
    log.info("Passerelle Traccar → WannyGest")
    log.info("  Écoute  : 0.0.0.0:%s  (endpoint POST /forward)", BRIDGE_PORT)
    log.info("  Relaie  : %s", TARGET_URL)
    log.info("  Token   : %s", "activé" if BRIDGE_TOKEN else "désactivé")
    if WANNY_URL.startswith('http://localhost'):
        log.warning("  ⚠️ WANNY_URL n'est pas défini — exporte WANNY_URL avant de lancer en prod.")
    app.run(host='0.0.0.0', port=BRIDGE_PORT)
