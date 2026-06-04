# ARCHITECTURE NOTIFICATIONS — WannyGest v129

## Vue d'ensemble

Système de notifications multi-canal qui dispatche automatiquement les alertes
sur les canaux activés par l'utilisateur. Tous les canaux passent par le
helper centralisé `notify_user()`.

```
┌──────────────────────────────────────────────────────────────┐
│  Code applicatif : module Projet, Intervention, Compta...   │
│                                                              │
│         notify_user(user_id, title, message, link, ...)     │
└────────────────────────────┬─────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌─────────────────┐  ┌─────────────────┐
│  Internal    │    │  Préférences    │  │   Outbox        │
│ (toujours)   │    │  utilisateur    │  │ (file d'envoi)  │
│              │    │  push/email/wap │  │                 │
│ notifications│    │                 │  │  retry, audit   │
│   (BDD)      │    └────────┬────────┘  └─────────────────┘
└──────────────┘             │
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
   ┌─────────┐         ┌──────────┐        ┌──────────┐
   │   FCM   │         │   SMTP   │        │  Twilio  │
   │ (push)  │         │ (email)  │        │ (WhatsApp)│
   └─────────┘         └──────────┘        └──────────┘
```

## Tables

### `notifications` (existante, enrichie en v129)
Stockage des notifications internes affichées dans `/notifications`.
- `module` (v129) : projets, interventions, rh, compta, etc.
- `priority` (v129) : low/normal/high
- `icon` (v129) : émoji optionnel
- `data` (v129) : JSON contextuel

### `notif_user_prefs` (v129)
Préférences canal par utilisateur (push/email/whatsapp).
PK composite (user_id, channel).

### `notif_fcm_tokens` (v129)
Tokens FCM enregistrés par les clients mobiles/web.
Plusieurs tokens possibles par user (un par appareil).

### `notif_outbox` (v129)
File d'envoi pour les canaux externes. Permet :
- Traçabilité (qui, quand, vers quel canal)
- Retry des échecs
- Audit par admin

### `app_settings`
Stocke la config admin :
- `smtp_host`, `smtp_port`, `smtp_user`, `smtp_password`, `smtp_from`, `smtp_tls`
- `fcm_server_key`, `fcm_web_config` (JSON)
- `twilio_sid`, `twilio_token`, `twilio_whatsapp_from`
- `app_url`

## Helper principal : `notify_user()`

```python
notify_user(
    user_id=123,
    title="Nouvelle intervention assignée",
    message="L'intervention RAMYA-INT-456 vous a été assignée par...",
    link="/interventions/456",
    type="info",                # info/success/warning/error
    module="interventions",      # pour filtres et stats
    priority="normal",           # low/normal/high
    icon="🛠️",
    channels=('internal','push','email','whatsapp'),  # canaux à utiliser
    force=False                  # True = ignore préférences user (sécurité)
)
```

Le helper :
1. Crée TOUJOURS la notification interne (table `notifications`)
2. Pour chaque canal externe, vérifie les préférences user
3. Ajoute dans `notif_outbox` pour traçabilité
4. Tente l'envoi immédiatement (synchrone)
5. Marque l'outbox comme `sent` ou `failed` selon le résultat

## Helpers auxiliaires

- `notify_role(role, ...)` : notifie tous les users d'un rôle
- `notify_roles([role1, role2], ...)` : notifie plusieurs rôles
- `get_user_notif_prefs(user_id)` : lit les préférences
- `set_user_notif_pref(user_id, channel, enabled)` : modifie une préférence

## Configuration côté admin

URL : `/admin/notifications-config` (admin uniquement)

3 sections :
1. **SMTP** : serveur, port, user, password, from, TLS
2. **FCM** : server key (Cloud Messaging API legacy)
3. **Twilio WhatsApp** : SID, token, numéro from

Plus une URL de base de l'application (pour les liens dans emails/WhatsApp).

### Test d'email
Bouton dans la page admin pour envoyer un email test à n'importe quelle adresse.

### Retry outbox
Bouton pour réessayer les envois `pending` ou `failed`.

## Préférences utilisateur

URL : `/notifications/preferences`

L'utilisateur peut activer/désactiver :
- ☑ Push mobile
- ☑ Email
- ☑ WhatsApp + saisie de son numéro

Affiche aussi la liste de ses appareils enregistrés (FCM tokens) avec
possibilité de les supprimer (déconnexion d'un appareil).

## Intégration côté client (web/PWA)

Pour activer le push web :

```javascript
// Dans le template ou un JS chargé par base.html
import { initializeApp } from "firebase/app";
import { getMessaging, getToken } from "firebase/messaging";

// La config est servie par /api/notifications/fcm-config
fetch('/api/notifications/fcm-config').then(r => r.json()).then(cfg => {
    if (!cfg.apiKey) return;  // FCM pas configuré
    const app = initializeApp(cfg);
    const messaging = getMessaging(app);
    getToken(messaging, { vapidKey: cfg.vapidKey }).then(token => {
        if (token) {
            // Enregistrer le token côté serveur
            const fd = new FormData();
            fd.append('token', token);
            fd.append('platform', 'web');
            fd.append('device_label', navigator.userAgent.slice(0,60));
            fetch('/api/notifications/fcm-register', { method:'POST', body: fd });
        }
    });
});
```

Le service worker `firebase-messaging-sw.js` est servi à la racine et géré
automatiquement par l'app (route `/firebase-messaging-sw.js`).

## Intégration côté mobile (Android/iOS — Capacitor / React Native / Flutter)

1. App native obtient son token FCM via Firebase SDK
2. App appelle `POST /api/notifications/fcm-register` avec le token
3. Le serveur enregistre dans `notif_fcm_tokens`
4. À chaque `notify_user()`, le serveur envoie via l'API HTTP FCM

## Workflow par module — exemples d'intégration

### Projet (Commercial → Coordinateur)
```python
# Dans la route de création de projet
notify_roles(['coordinateur', 'gestionnaire_projet'],
    title=f"📋 Nouveau projet : {project.reference}",
    message=f"{user.full_name} a créé le projet {project.title} pour {client.name}.",
    link=f"/projects/{project.id}",
    module="projets",
    icon="🎯")
```

### Intervention assignée
```python
notify_user(intervention.assigned_to,
    title="🛠️ Nouvelle intervention assignée",
    message=f"L'intervention {intervention.reference} chez {client.name} vous a été assignée pour le {intervention.date}.",
    link=f"/interventions/{intervention.id}",
    module="interventions",
    icon="🛠️",
    priority="high")
```

### Remontée d'information
```python
notify_roles(['gestionnaire_projet', 'admin'],
    title=f"📢 Nouvelle remontée : {report.reference}",
    message=f"{user.full_name} a remonté un problème chez {report.client_name}.",
    link=f"/field-reports/{report.id}",
    module="remontees",
    icon="📢")
```

## Limites actuelles et roadmap

### Limitations v129
- Envoi synchrone : si SMTP est lent, ralentit la requête HTTP. À terme, prévoir
  un worker background (Celery, RQ ou thread pool) qui dépile `notif_outbox`.
- FCM Legacy API : Google migrera vers HTTP v1 d'ici 2026. Adaptation nécessaire
  (utiliser un service account JWT au lieu de la server key).
- WhatsApp : Twilio impose une fenêtre 24h après dernier message client.
  Pour notif unilatérale, créer un template approuvé Twilio.

### Évolutions prévues
- Worker async pour l'outbox
- Templates email HTML personnalisés
- Templates WhatsApp pré-approuvés
- Statistiques détaillées de délivrabilité
- Digest quotidien (regroupement)

## Mode dégradé

Si un canal n'est pas configuré côté admin, `notify_user()` ne fait rien pour
ce canal (pas d'erreur). L'utilisateur garde toujours sa notification interne.
La page préférences indique clairement quels canaux sont configurés
("✓ Configuré" / "⚠️ Non configuré côté admin").
