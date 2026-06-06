/* WannyGest v159 — Multi-fenêtres in-app (sans onglet navigateur)
   - Fenêtres draggables / redimensionnables avec iframe
   - Barre des tâches en bas
   - Z-index management
   - Minimize / Maximize / Close
   - Liens avec data-window="true" ou .floating-window s'ouvrent dans une fenêtre
*/

(function() {
    'use strict';
    
    if (window.FloatingWindowManager) return; // Singleton
    
    const STORAGE_KEY = 'wg_floating_windows';
    let windows = [];
    let nextId = 1;
    let highestZ = 5000;
    let activeWindow = null;
    // La persistance ne s'applique qu'à la fenêtre principale : une iframe ne doit
    // ni restaurer (imbrication) ni écraser l'état sauvegardé du parent.
    const IS_TOP = (() => { try { return window.self === window.top; } catch (e) { return false; } })();
    
    // CSS injecté
    const css = `
    .fw-taskbar {
        position: fixed; bottom: 0; left: 0; right: 0; height: 44px;
        background: linear-gradient(135deg, #1A7A6D, #0d5a51); color: white;
        display: flex; align-items: center; gap: 8px; padding: 0 12px;
        box-shadow: 0 -2px 12px rgba(0,0,0,0.2); z-index: 4999;
        font-family: -apple-system, system-ui, sans-serif;
        overflow-x: auto; overflow-y: hidden;
    }
    .fw-taskbar.empty { display: none; }
    .fw-task {
        background: rgba(255,255,255,0.15); color: white; padding: 6px 14px;
        border-radius: 8px; cursor: pointer; font-size: 12px; font-weight: 600;
        display: flex; align-items: center; gap: 6px; white-space: nowrap;
        border: 2px solid transparent; transition: all 0.15s;
    }
    .fw-task:hover { background: rgba(255,255,255,0.25); }
    .fw-task.active { background: rgba(242,159,47,0.9); border-color: #fff; }
    .fw-task-close {
        background: rgba(0,0,0,0.2); border: none; color: white; width: 18px; height: 18px;
        border-radius: 50%; cursor: pointer; font-size: 10px; padding: 0;
        display: inline-flex; align-items: center; justify-content: center;
    }
    .fw-task-close:hover { background: #c53030; }
    .fw-taskbar-label {
        font-size: 11px; color: rgba(255,255,255,0.8); margin-right: 6px;
        font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;
    }
    
    .fw-window {
        position: fixed; background: white; border-radius: 12px;
        box-shadow: 0 10px 40px rgba(0,0,0,0.25); display: flex; flex-direction: column;
        font-family: -apple-system, system-ui, sans-serif;
        min-width: 320px; min-height: 200px;
        animation: fwOpen 0.2s ease-out;
        overflow: hidden;
    }
    @keyframes fwOpen { from { opacity: 0; transform: scale(0.95); } to { opacity: 1; transform: scale(1); } }
    .fw-window.minimized { display: none !important; }
    /* Plein écran in-app (API Fullscreen) : la fenêtre remplit tout l'écran */
    .fw-window:fullscreen { width: 100vw !important; height: 100vh !important; top: 0 !important; left: 0 !important; border-radius: 0 !important; }
    .fw-window:fullscreen .fw-resize-handle { display: none; }
    .fw-window.maximized {
        top: 0 !important; left: 0 !important;
        width: 100vw !important; height: calc(100vh - 44px) !important;
        border-radius: 0 !important;
    }
    
    .fw-titlebar {
        background: linear-gradient(135deg, #1A7A6D, #0d5a51); color: white;
        padding: 9px 12px; border-radius: 12px 12px 0 0; cursor: move;
        display: flex; align-items: center; gap: 8px; user-select: none;
        font-size: 13px; font-weight: 600;
    }
    .fw-window.maximized .fw-titlebar { border-radius: 0; }
    .fw-title { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .fw-btn {
        background: rgba(255,255,255,0.15); border: none; color: white;
        width: 22px; height: 22px; border-radius: 4px; cursor: pointer; font-size: 12px;
        display: flex; align-items: center; justify-content: center;
        padding: 0; font-weight: 700; transition: background 0.1s;
    }
    .fw-btn:hover { background: rgba(255,255,255,0.3); }
    .fw-btn.close:hover { background: #c53030; }
    
    .fw-body { flex: 1; position: relative; background: #f8f9fa; }
    .fw-iframe {
        width: 100%; height: 100%; border: none; background: white;
    }
    
    .fw-loading {
        position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
        background: white; flex-direction: column; gap: 12px;
    }
    .fw-spinner {
        width: 40px; height: 40px; border: 4px solid #e0e0e0; border-top-color: #1A7A6D;
        border-radius: 50%; animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    
    .fw-resize-handle {
        position: absolute; bottom: 0; right: 0; width: 16px; height: 16px;
        cursor: nwse-resize; background: linear-gradient(135deg, transparent 50%, #1A7A6D 50%);
        z-index: 10;
    }
    .fw-window.maximized .fw-resize-handle { display: none; }
    
    .fw-overlay-mobile { display: none; }
    @media (max-width: 768px) {
        .fw-window {
            width: 100vw !important; height: calc(100vh - 44px) !important;
            top: 0 !important; left: 0 !important; border-radius: 0;
        }
        .fw-resize-handle { display: none; }
    }
    
    .fw-floating-hint {
        position: fixed; bottom: 50px; right: 12px; background: #1A7A6D; color: white;
        padding: 6px 10px; border-radius: 6px; font-size: 11px; opacity: 0;
        transition: opacity 0.3s; z-index: 5001; pointer-events: none;
    }
    .fw-floating-hint.visible { opacity: 1; }
    `;
    
    function injectCSS() {
        if (document.getElementById('fw-styles')) return;
        const style = document.createElement('style');
        style.id = 'fw-styles';
        style.textContent = css;
        document.head.appendChild(style);
    }
    
    function createTaskbar() {
        if (document.getElementById('fw-taskbar')) return;
        const tb = document.createElement('div');
        tb.id = 'fw-taskbar';
        tb.className = 'fw-taskbar empty';
        tb.innerHTML = '<span class="fw-taskbar-label">🪟 Fenêtres :</span>';
        document.body.appendChild(tb);
    }
    
    function updateTaskbar() {
        const tb = document.getElementById('fw-taskbar');
        if (!tb) return;
        // Garder le label, retirer les tasks
        const oldTasks = tb.querySelectorAll('.fw-task');
        oldTasks.forEach(t => t.remove());
        
        windows.forEach(w => {
            const task = document.createElement('div');
            task.className = 'fw-task' + (w === activeWindow ? ' active' : '');
            task.innerHTML = `
                <span>${w.icon || '📄'} ${escapeHTML(w.title)}</span>
                <button class="fw-task-close" title="Fermer">×</button>
            `;
            task.onclick = (e) => {
                if (!e.target.classList.contains('fw-task-close')) {
                    restoreWindow(w);
                }
            };
            task.querySelector('.fw-task-close').onclick = (e) => {
                e.stopPropagation();
                closeWindow(w);
            };
            tb.appendChild(task);
        });
        
        tb.classList.toggle('empty', windows.length === 0);
        // Ajuster le body pour ne pas être masqué
        document.body.style.paddingBottom = windows.length > 0 ? '44px' : '';
    }
    
    function escapeHTML(s) {
        return String(s || '').replace(/[&<>"']/g, m => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' })[m]);
    }

    function toPx(v, fallback) {
        if (v == null) return fallback;
        if (typeof v === 'number') return v;
        const n = parseInt(v, 10);
        return isNaN(n) ? fallback : n;
    }

    function serializeWindow(win) {
        const el = win.element;
        return {
            url: win.url, title: win.title, icon: win.icon,
            left: el.style.left, top: el.style.top,
            width: el.style.width, height: el.style.height,
            minimized: el.classList.contains('minimized'),
            maximized: el.classList.contains('maximized'),
            z: parseInt(el.style.zIndex, 10) || 0,
        };
    }

    function saveState() {
        if (!IS_TOP) return; // ne jamais écraser l'état depuis une iframe
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(windows.map(serializeWindow)));
        } catch (e) { /* quota / navigation privée — ignore */ }
    }

    function restoreState() {
        if (!IS_TOP) return; // pas de restauration dans une iframe (éviter l'imbrication)
        let data;
        try { data = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'); }
        catch (e) { data = []; }
        if (!Array.isArray(data) || !data.length) return;
        // Restaurer dans l'ordre d'empilement d'origine (z croissant)
        data.sort((a, b) => (a.z || 0) - (b.z || 0));
        data.forEach(d => {
            if (!d || !d.url) return;
            openWindow(d.url, {
                title: d.title, icon: d.icon,
                left: d.left, top: d.top, width: d.width, height: d.height,
                minimized: d.minimized, maximized: d.maximized,
                restoring: true,
            });
        });
    }

    function openWindow(url, opts) {
        opts = opts || {};
        const title = opts.title || 'Fenêtre';
        const icon = opts.icon || '📄';
        const width = toPx(opts.width, Math.min(1100, window.innerWidth - 80));
        const height = toPx(opts.height, Math.min(window.innerHeight - 100, 720));

        // Vérifier si déjà ouverte avec la même URL
        const existing = windows.find(w => w.url === url);
        if (existing) {
            restoreWindow(existing);
            return existing;
        }

        const id = nextId++;
        highestZ++;

        // Position : restaurée si fournie (opts.left/top), sinon en cascade
        const hasPos = (opts.left != null && opts.top != null);
        const offset = (windows.length % 5) * 30;
        const left = hasPos ? toPx(opts.left, 40) : Math.max(40, (window.innerWidth - width) / 2 + offset - 60);
        const top = hasPos ? toPx(opts.top, 30) : Math.max(30, (window.innerHeight - height) / 2 + offset - 60);
        
        const wDiv = document.createElement('div');
        wDiv.className = 'fw-window';
        wDiv.style.cssText = `width:${width}px;height:${height}px;left:${left}px;top:${top}px;z-index:${highestZ}`;
        wDiv.dataset.id = id;
        wDiv.innerHTML = `
            <div class="fw-titlebar">
                <span style="font-size:14px">${icon}</span>
                <span class="fw-title">${escapeHTML(title)}</span>
                <button class="fw-btn" data-action="reload" title="Actualiser">↻</button>
                <button class="fw-btn" data-action="popout" title="Ouvrir en plein écran">⬈</button>
                <button class="fw-btn" data-action="minimize" title="Réduire">_</button>
                <button class="fw-btn" data-action="maximize" title="Agrandir">▢</button>
                <button class="fw-btn close" data-action="close" title="Fermer">×</button>
            </div>
            <div class="fw-body">
                <div class="fw-loading">
                    <div class="fw-spinner"></div>
                    <div style="font-size:12px;color:#666">Chargement de ${escapeHTML(title)}...</div>
                </div>
                <iframe class="fw-iframe" src="${url}" allow="clipboard-read; clipboard-write"></iframe>
            </div>
            <div class="fw-resize-handle"></div>
        `;
        document.body.appendChild(wDiv);
        
        const iframe = wDiv.querySelector('.fw-iframe');
        const loading = wDiv.querySelector('.fw-loading');
        iframe.onload = () => {
            loading.style.display = 'none';
            // Essayer de récupérer le titre de la page chargée
            try {
                const doc = iframe.contentDocument || iframe.contentWindow.document;
                const pageTitle = doc.title || '';
                if (pageTitle && pageTitle.length < 60) {
                    const cleanTitle = pageTitle.replace(/^WannyGest[\s—-]*/, '').trim();
                    if (cleanTitle && cleanTitle !== title) {
                        wDiv.querySelector('.fw-title').textContent = cleanTitle;
                        const winObj = windows.find(w => w.id === id);
                        if (winObj) {
                            winObj.title = cleanTitle;
                            updateTaskbar();
                        }
                    }
                }
            } catch(e) { /* cross-origin or err — ignore */ }
        };
        
        const win = { id, url, title, icon, element: wDiv, iframe };
        windows.push(win);
        activeWindow = win;

        // État restauré : réduit / agrandi
        if (opts.maximized) wDiv.classList.add('maximized');
        if (opts.minimized) {
            wDiv.classList.add('minimized');
            if (activeWindow === win) {
                activeWindow = windows.filter(w => !w.element.classList.contains('minimized')).pop() || null;
            }
        }

        // Drag titlebar
        const titlebar = wDiv.querySelector('.fw-titlebar');
        let dragX = 0, dragY = 0, isDragging = false;
        titlebar.addEventListener('mousedown', (e) => {
            if (e.target.classList.contains('fw-btn')) return;
            if (wDiv.classList.contains('maximized')) return;
            isDragging = true;
            const rect = wDiv.getBoundingClientRect();
            dragX = e.clientX - rect.left;
            dragY = e.clientY - rect.top;
            focusWindow(win);
            // Disable iframe pointer events during drag (otherwise mouse events lost)
            iframe.style.pointerEvents = 'none';
            document.body.style.userSelect = 'none';
        });
        document.addEventListener('mousemove', (e) => {
            if (!isDragging) return;
            const newL = Math.max(0, Math.min(window.innerWidth - 100, e.clientX - dragX));
            const newT = Math.max(0, Math.min(window.innerHeight - 50, e.clientY - dragY));
            wDiv.style.left = newL + 'px';
            wDiv.style.top = newT + 'px';
        });
        document.addEventListener('mouseup', () => {
            if (isDragging) {
                isDragging = false;
                iframe.style.pointerEvents = '';
                document.body.style.userSelect = '';
                saveState();
            }
        });
        
        // Resize handle
        const resize = wDiv.querySelector('.fw-resize-handle');
        let isResizing = false, rX = 0, rY = 0, rW = 0, rH = 0;
        resize.addEventListener('mousedown', (e) => {
            e.preventDefault();
            isResizing = true;
            rX = e.clientX; rY = e.clientY;
            rW = wDiv.offsetWidth; rH = wDiv.offsetHeight;
            iframe.style.pointerEvents = 'none';
            focusWindow(win);
        });
        document.addEventListener('mousemove', (e) => {
            if (!isResizing) return;
            const newW = Math.max(320, rW + (e.clientX - rX));
            const newH = Math.max(200, rH + (e.clientY - rY));
            wDiv.style.width = newW + 'px';
            wDiv.style.height = newH + 'px';
        });
        document.addEventListener('mouseup', () => {
            if (isResizing) {
                isResizing = false;
                iframe.style.pointerEvents = '';
                saveState();
            }
        });
        
        // Focus on click anywhere
        wDiv.addEventListener('mousedown', () => focusWindow(win));
        
        // Boutons de la titlebar
        titlebar.querySelectorAll('.fw-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const action = btn.dataset.action;
                if (action === 'close') closeWindow(win);
                else if (action === 'minimize') minimizeWindow(win);
                else if (action === 'maximize') toggleMaximize(win);
                else if (action === 'reload') iframe.src = iframe.src;
                else if (action === 'popout') {
                    // Plein écran IN-APP (API Fullscreen) — pas de nouvel onglet navigateur
                    const el = win.element;
                    if (!document.fullscreenElement) {
                        focusWindow(win);
                        if (el.requestFullscreen) {
                            el.requestFullscreen().catch(() => toggleMaximize(win));
                        } else {
                            toggleMaximize(win); // navigateur sans Fullscreen API → repli agrandir
                        }
                    } else if (document.exitFullscreen) {
                        document.exitFullscreen();
                    }
                }
            });
        });
        
        // Double clic titlebar = maximize
        titlebar.addEventListener('dblclick', (e) => {
            if (!e.target.classList.contains('fw-btn')) toggleMaximize(win);
        });
        
        updateTaskbar();
        if (!opts.restoring) saveState();
        return win;
    }

    function closeWindow(win) {
        if (!win || !win.element) return;
        win.element.style.transition = 'opacity 0.15s, transform 0.15s';
        win.element.style.opacity = '0';
        win.element.style.transform = 'scale(0.95)';
        setTimeout(() => {
            if (win.element && win.element.parentNode) win.element.parentNode.removeChild(win.element);
        }, 150);
        windows = windows.filter(w => w !== win);
        if (activeWindow === win) {
            activeWindow = windows[windows.length - 1] || null;
        }
        updateTaskbar();
        saveState();
    }

    function minimizeWindow(win) {
        win.element.classList.add('minimized');
        if (activeWindow === win) activeWindow = null;
        updateTaskbar();
        saveState();
    }

    function restoreWindow(win) {
        win.element.classList.remove('minimized');
        focusWindow(win);
        saveState();
    }

    function focusWindow(win) {
        highestZ++;
        win.element.style.zIndex = highestZ;
        activeWindow = win;
        updateTaskbar();
        saveState();
    }

    function toggleMaximize(win) {
        win.element.classList.toggle('maximized');
        focusWindow(win);
        saveState();
    }
    
    function isLinkSuitableForFloating(href) {
        if (!href) return false;
        if (href.startsWith('#')) return false;
        if (href.startsWith('javascript:')) return false;
        if (href.startsWith('mailto:') || href.startsWith('tel:')) return false;
        if (href.startsWith('http') && !href.startsWith(location.origin)) return false;
        // Ne pas intercepter logout, login, etc.
        if (/\/(login|logout|register|api\/)/i.test(href)) return false;
        // Ne pas intercepter les fichiers à télécharger
        if (/\.(pdf|zip|xlsx|docx|csv|jpg|png)(\?|$)/i.test(href)) return false;
        return true;
    }
    
    function getIconForUrl(url) {
        if (/devis|proforma/i.test(url)) return '📋';
        if (/clients?/i.test(url)) return '👥';
        if (/intervention/i.test(url)) return '🔧';
        if (/facture|recouvrement|caissiere/i.test(url)) return '🧾';
        if (/projet/i.test(url)) return '🏗️';
        if (/notification/i.test(url)) return '🔔';
        if (/visite/i.test(url)) return '📍';
        if (/caisse|tresorerie/i.test(url)) return '💰';
        if (/rapport/i.test(url)) return '📊';
        if (/dashboard/i.test(url)) return '📈';
        return '📄';
    }
    
    // Intercepter les clics sur les liens marqués pour ouverture flottante
    document.addEventListener('click', function(e) {
        const link = e.target.closest('a');
        if (!link) return;
        
        const href = link.getAttribute('href');
        if (!isLinkSuitableForFloating(href)) return;
        
        // Cas 1 : Lien marqué explicitement (data-window ou class floating-window)
        const isExplicit = link.dataset.window === 'true' || link.classList.contains('floating-window');
        
        // Cas 2 : Ctrl+clic / Cmd+clic / Middle-click sur N'IMPORTE QUEL lien interne 
        //          → ouvrir en fenêtre flottante au lieu d'onglet navigateur
        const wantsModal = isExplicit || (e.ctrlKey || e.metaKey);
        
        if (!wantsModal) return;
        
        e.preventDefault();
        const title = link.title || link.textContent.trim().slice(0, 40) || 'Page';
        const icon = link.dataset.icon || getIconForUrl(href);
        openWindow(href, { title, icon });
    });
    
    // API publique
    window.FloatingWindowManager = {
        open: openWindow,
        close: closeWindow,
        list: () => windows.slice(),
        closeAll: () => { windows.slice().forEach(w => closeWindow(w)); },
    };
    
    // Raccourcis clavier : Ctrl+Shift+W = fermer fenêtre active
    document.addEventListener('keydown', (e) => {
        if (e.ctrlKey && e.shiftKey && e.key === 'W') {
            e.preventDefault();
            if (activeWindow) closeWindow(activeWindow);
        }
        // Échap = restaurer si maximized, sinon fermer
        if (e.key === 'Escape' && activeWindow && !e.target.matches('input, textarea')) {
            if (activeWindow.element.classList.contains('maximized')) {
                toggleMaximize(activeWindow);
            }
        }
    });
    
    // Initialisation
    function init() {
        injectCSS();
        createTaskbar();
        restoreState();
        // Filet de sécurité : capturer la dernière géométrie avant déchargement
        if (IS_TOP) window.addEventListener('beforeunload', saveState);
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
    
    console.log('[FloatingWindowManager v159] Multi-fenêtres in-app prêtes — Ctrl+clic sur tout lien = fenêtre flottante');
})();
