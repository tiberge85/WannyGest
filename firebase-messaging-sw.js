// WannyGest v129 — Service Worker FCM
// Ce SW est chargé par le navigateur pour recevoir les notifications push
// même lorsque l'onglet est fermé.

importScripts('https://www.gstatic.com/firebasejs/9.6.1/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/9.6.1/firebase-messaging-compat.js');

// IMPORTANT : la config Firebase est injectée depuis /api/fcm-config par le serveur.
// Pour éviter de hardcoder les credentials ici, ce SW lit la config au démarrage.

self.addEventListener('install', () => {
    console.log('[FCM SW] Installed');
    self.skipWaiting();
});

self.addEventListener('activate', () => {
    console.log('[FCM SW] Activated');
});

// Récupération de la config Firebase puis init messaging
fetch('/api/notifications/fcm-config')
    .then(r => r.json())
    .then(cfg => {
        if (!cfg || !cfg.apiKey) {
            console.warn('[FCM SW] Pas de config Firebase — push non actif');
            return;
        }
        firebase.initializeApp(cfg);
        const messaging = firebase.messaging();
        messaging.onBackgroundMessage(payload => {
            const title = (payload.notification && payload.notification.title) || 'WannyGest';
            const options = {
                body: (payload.notification && payload.notification.body) || '',
                icon: '/static/icon-192.png',
                badge: '/static/badge.png',
                data: payload.data || {},
            };
            self.registration.showNotification(title, options);
        });
    })
    .catch(err => console.warn('[FCM SW] Init failed', err));

self.addEventListener('notificationclick', event => {
    event.notification.close();
    const link = (event.notification.data && event.notification.data.link) || '/dashboard';
    event.waitUntil(
        clients.matchAll({ type: 'window' }).then(clientList => {
            for (const c of clientList) {
                if (c.url.includes(link) && 'focus' in c) return c.focus();
            }
            if (clients.openWindow) return clients.openWindow(link);
        })
    );
});
