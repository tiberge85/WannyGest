#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAMYA TECHNOLOGIE & INNOVATION
Application Web v3 — Gestion des Rapports de Pointage
Auth + Rôles + Dashboard + Clients + Fichiers RH
"""

import os, uuid, shutil, functools, smtplib, json, time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from flask import (Flask, render_template, request, send_file, flash,
                   redirect, url_for, jsonify, session, send_from_directory)
from werkzeug.utils import secure_filename

from rapport_core import extract_from_excel, generate_full_pdf
from rapport_core import generate_presence_xlsx
from models import (init_db, create_user, authenticate_user, get_user_by_id,
                    get_all_users, update_user, delete_user,
                    create_client, get_all_clients, get_client_by_id,
                    find_client_by_name, update_client, delete_client,
                    create_job, get_jobs_by_status, get_all_jobs, mark_job_sent,
                    get_dashboard_stats, has_permission, get_role_permissions,
                    update_role_permissions,
                    reset_jobs, reset_clients, reset_users, reset_all,
                    log_activity, get_activity_logs,
                    add_job_comment, get_job_comments, update_job_notes,
                    get_job_by_id, get_db_path,
                    create_contract, get_client_contracts, get_all_contracts,
                    get_contract_by_id, update_contract, delete_contract,
                    get_client_monthly_stats,
                    save_smtp_settings, get_smtp_settings, get_admin_smtp,
                    create_invoice, get_invoices_by_status, get_all_invoices,
                    update_invoice_status, get_invoice_stats,
                    create_visit_report, get_visit_reports, get_visit_by_id,
                    update_visit_proforma, get_visit_stats,
                    create_devis, get_all_devis, get_devis_by_id,
                    update_devis_status, get_devis_stats)
from rapport_core import generate_devis_pdf

app = Flask(__name__, template_folder=BASE_DIR, static_folder=BASE_DIR, static_url_path='/static')
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(32).hex())
app.config['PERMANENT_SESSION_LIFETIME'] = 3600 * 8  # 8 hours
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
PERSISTENT_DIR = os.environ.get('PERSISTENT_DIR', BASE_DIR)
app.config['UPLOAD_FOLDER'] = os.path.join(PERSISTENT_DIR, 'uploads')
app.config['FILES_FOLDER'] = os.path.join(PERSISTENT_DIR, 'files')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['FILES_FOLDER'], exist_ok=True)

@app.errorhandler(413)
def too_large(e):
    flash("Fichier trop volumineux. Taille max : 50 Mo.", "error")
    return redirect(request.referrer or '/dashboard'), 302

init_db()

# Init extra tables
from models import (init_rh_tables, get_all_employees, get_employee_by_id,
                    create_employee, update_employee, get_employee_stats,
                    get_leaves, create_leave, update_leave_status,
                    get_payslips, create_payslip, update_payslip,
                    init_extra_tables, record_login_attempt, get_failed_attempts,
                    save_otp, verify_otp, db_count, db_sum, db_get_all)
init_rh_tables()
init_extra_tables()

from models import init_mg_tables
init_mg_tables()

from models import (init_chat_tables, get_messages, get_direct_messages, 
                    send_message, get_unread_count, mark_chat_read, db_get_all,
                    migrate_v4, get_payslip_detail, get_maintenance_due,
                    migrate_v5, log_audit, get_audit_trail, get_executive_stats,
                    get_devis_templates, get_devis_template,
                    migrate_payslip_v2, get_payslip_detail_v2,
                    migrate_caisse, gen_caisse_ref, get_caisse_sorties, get_caisse_stats)
from models import migrate_caisse_v2, delete_caisse
from models import migrate_v6
init_chat_tables()
migrate_v4()
migrate_payslip_v2()
migrate_v5()
migrate_caisse()
migrate_caisse_v2()
migrate_v6()
from models import migrate_v7
migrate_v7()
from models import migrate_v8
migrate_v8()
from models import migrate_v9
migrate_v9()
from models import migrate_v10
migrate_v10()
from models import migrate_v11
migrate_v11()
from models import migrate_v12
migrate_v12()
from models import migrate_v13
migrate_v13()
from models import migrate_v14
migrate_v14()
from models import migrate_v15
migrate_v15()
from models import migrate_v16
migrate_v16()
from models import migrate_v17
migrate_v17()
from models import migrate_v18
migrate_v18()
from models import migrate_v19
migrate_v19()
from models import migrate_v20
migrate_v20()
from models import migrate_v21
migrate_v21()
from models import migrate_v22
migrate_v22()
from models import migrate_v23
migrate_v23()
from models import migrate_v24
migrate_v24()
from models import migrate_v25
migrate_v25()
from models import migrate_v26
migrate_v26()
from models import migrate_v27
migrate_v27()
from models import migrate_v28
migrate_v28()
from models import migrate_v29
migrate_v29()
from models import migrate_v30
migrate_v30()
from models import migrate_v31
migrate_v31()
from models import migrate_v32
migrate_v32()
from models import migrate_v33
migrate_v33()
from models import migrate_v34
migrate_v34()
from models import migrate_v35
migrate_v35()
from models import migrate_v36
migrate_v36()
from models import migrate_v37
migrate_v37()
from models import migrate_v38
migrate_v38()
from models import migrate_v39
migrate_v39()
from models import migrate_v40
migrate_v40()
from models import migrate_v41
migrate_v41()
from models import migrate_v42
migrate_v42()
from models import migrate_v43
migrate_v43()
from models import migrate_v44
migrate_v44()
from models import migrate_v45
migrate_v45()
from models import migrate_v45
migrate_v45()
from models import migrate_v31
migrate_v31()
from models import migrate_v32
migrate_v32()
from models import migrate_v33
migrate_v33()
from models import migrate_v34
migrate_v34()
from models import migrate_v35
migrate_v35()
from models import migrate_v36
migrate_v36()
from models import migrate_v37
migrate_v37()
from models import migrate_v38
migrate_v38()
from models import migrate_v39
migrate_v39()
from models import migrate_v40
migrate_v40()
from models import migrate_v41
migrate_v41()
from models import migrate_v42
migrate_v42()
from models import migrate_v43
migrate_v43()
from models import migrate_v44
migrate_v44()
from models import migrate_v45
migrate_v45()
from models import migrate_v45
migrate_v45()
from models import migrate_v27
migrate_v27()
from models import migrate_v28
migrate_v28()
from models import migrate_v29
migrate_v29()
from models import migrate_v30
migrate_v30()
from models import migrate_v31
migrate_v31()
from models import migrate_v32
migrate_v32()
from models import migrate_v33
migrate_v33()
from models import migrate_v34
migrate_v34()
from models import migrate_v35
migrate_v35()
from models import migrate_v36
migrate_v36()
from models import migrate_v37
migrate_v37()
from models import migrate_v38
migrate_v38()
from models import migrate_v39
migrate_v39()
from models import migrate_v40
migrate_v40()
from models import migrate_v41
migrate_v41()
from models import migrate_v42
migrate_v42()
from models import migrate_v43
migrate_v43()
from models import migrate_v44
migrate_v44()
from models import migrate_v45
migrate_v45()
from models import migrate_v45
migrate_v45()
from models import migrate_v31
migrate_v31()
from models import migrate_v32
migrate_v32()
from models import migrate_v33
migrate_v33()
from models import migrate_v34
migrate_v34()
from models import migrate_v35
migrate_v35()
from models import migrate_v36
migrate_v36()
from models import migrate_v37
migrate_v37()
from models import migrate_v38
migrate_v38()
from models import migrate_v39
migrate_v39()
from models import migrate_v40
migrate_v40()
from models import migrate_v41
migrate_v41()
from models import migrate_v42
migrate_v42()
from models import migrate_v43
migrate_v43()
from models import migrate_v44
migrate_v44()
from models import migrate_v45
migrate_v45()
from models import migrate_v45
migrate_v45()
from models import migrate_v46
migrate_v46()
from models import migrate_v47
migrate_v47()
from models import migrate_v48
migrate_v48()
from models import migrate_v49
migrate_v49()
from models import migrate_v15
migrate_v15()
from models import migrate_v16
migrate_v16()
from models import migrate_v17
migrate_v17()
from models import migrate_v18
migrate_v18()
from models import migrate_v19
migrate_v19()

# Register module routes
from modules_routes import modules_bp
app.register_blueprint(modules_bp)

# Ensure admin and concierge roles have default permissions
try:
    update_role_permissions('admin', ['traitement', 'fichiers', 'clients', 'clients_edit', 'admin', 'dashboard', 'dashboard_general',
                   'envoyer', 'logs', 'concierge', 'concierge_edit',
                   'contrats', 'comptabilite', 'comptabilite_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit',
                   'moyens_generaux', 'moyens_generaux_edit', 'informatique', 'projets', 'caisse_sortie', 'rapports_j', 'convertir_devis',
                   'resp_projet', 'resp_projet_edit', 'centre_technique', 'centre_technique_edit', 'chat', 'tracking',
                   # v47/v48/v49 additions
                   'grand_livre', 'balance', 'caisse_multi', 'gps_itineraire',
                   'virement_demande', 'virement_valide',
                   'client_requests_view', 'client_users_approve',
                   'controle_qualite', 'livraison_intervention'])
    from models import get_role_permissions as _grp
    if not _grp('concierge'):
        update_role_permissions('concierge', ['dashboard', 'concierge', 'rapports_j', 'chat'])
    if not _grp('proprietaire'):
        update_role_permissions('proprietaire', ['dashboard', 'dashboard_general', 'concierge', 'fichiers', 'clients', 'comptabilite', 'proforma', 'moyens_generaux', 'rapports_j', 'chat', 'tracking', 'logs'])
    if not _grp('secretaire'):
        update_role_permissions('secretaire', ['dashboard', 'clients', 'clients_edit', 'proforma', 'rapports_j', 'chat', 'concierge', 'concierge_edit', 'visites', 'contrats'])
    if not _grp('client'):
        update_role_permissions('client', ['dashboard'])
    if 'informatique' not in _grp('technicien'):
        update_role_permissions('technicien', ['dashboard', 'traitement', 'informatique', 'centre_technique', 'centre_technique_edit', 'visites', 'visites_edit', 'rapports_j', 'chat'])
    if 'centre_technique' not in _grp('resp_projet'):
        update_role_permissions('resp_projet', ['dashboard', 'resp_projet', 'resp_projet_edit', 'projets', 'informatique', 'centre_technique', 'clients', 'visites', 'rapports_j', 'chat'])
except: pass

from models import (init_devis_tables, create_devis, get_all_devis, get_devis_by_id,
                    update_devis_status, get_devis_stats, get_next_devis_ref)
init_devis_tables()

ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
ALL_PERMISSIONS = ['traitement', 'fichiers', 'clients', 'clients_edit', 'admin', 'dashboard', 'dashboard_general',
                   'envoyer', 'logs', 'concierge', 'concierge_edit',
                   'contrats', 'comptabilite', 'comptabilite_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit',
                   'moyens_generaux', 'moyens_generaux_edit', 'informatique', 'projets', 'caisse_sortie', 'rapports_j', 'convertir_devis',
                   'resp_projet', 'resp_projet_edit', 'centre_technique', 'centre_technique_edit', 'chat', 'tracking',
                   # v47/v48 permissions
                   'grand_livre', 'balance', 'caisse_multi', 'gps_itineraire',
                   'virement_demande', 'virement_valide',
                   'client_requests_view', 'client_users_approve',
                   # v17 workflow intervention
                   'controle_qualite', 'livraison_intervention']

# Permission categories for admin display
PERM_CATEGORIES = {
    'Comptabilité': [
        ('comptabilite', 'Comptabilité lecture'),
        ('comptabilite_edit', 'Comptabilité modification'),
        ('convertir_devis', 'Convertir devis'),
        ('caisse_sortie', 'Caisse (entrées/sorties)'),
        ('caisse_multi', 'Multi-caisses'),
        ('grand_livre', 'Grand Livre'),
        ('balance', 'Balance comptable'),
        ('virement_demande', 'Demander virement banque→caisse'),
        ('virement_valide', 'Valider virement (DG uniquement)'),
    ],
    'Commercial / CRM': [
        ('clients', 'Clients lecture'),
        ('clients_edit', 'Clients modification'),
        ('proforma', 'Devis lecture'),
        ('proforma_edit', 'Devis modification'),
        ('visites', 'Visites lecture'),
        ('visites_edit', 'Visites modification'),
    ],
    'Portail client': [
        ('client_requests_view', 'Voir demandes clients'),
        ('client_users_approve', 'Valider comptes clients'),
    ],
    'Concierge': [('concierge', 'Consultation'), ('concierge_edit', 'Création/Modification')],
    'Technique': [
        ('centre_technique', 'Centre technique'),
        ('centre_technique_edit', 'Centre tech. modif'),
        ('traitement', 'Traitement/DPCI'),
        ('gps_itineraire', 'GPS / Itinéraire'),
        ('controle_qualite', 'Contrôle qualité'),
        ('livraison_intervention', 'Livraison chantier'),
    ],
    'Projets': [
        ('resp_projet', 'Resp. projet lecture'),
        ('resp_projet_edit', 'Resp. projet modif'),
        ('projets', 'Projets info'),
    ],
    'RH & Admin': [
        ('fichiers', 'Employés/RH'),
        ('contrats', 'Contrats'),
        ('envoyer', 'Envoi paie'),
        ('logs', 'Logs'),
    ],
    'Général': [
        ('dashboard', 'Dashboard'),
        ('dashboard_general', 'Dashboard général'),
        ('rapports_j', 'Rapports journaliers'),
        ('chat', 'Chat'),
        ('informatique', 'Informatique'),
        ('moyens_generaux', 'Stock lecture'),
        ('moyens_generaux_edit', 'Stock modification'),
        ('tracking', 'Tracking GPS véhicules'),
    ],
    'Administration': [('admin', 'Admin système')],
}

def allowed_file(fn):
    return '.' in fn and fn.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# ======================== AUTH HELPERS ========================

def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            flash("Veuillez vous connecter", "error")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def permission_required(perm):
    def decorator(f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            if 'user_id' not in session:
                return redirect(url_for('login'))
            user = get_user_by_id(session['user_id'])
            if not user or not has_permission(user['role'], perm):
                flash("Accès non autorisé", "error")
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated
    return decorator

@app.context_processor
def inject_globals():
    """Injecte les variables globales dans tous les templates."""
    ctx = {'current_user': None, 'permissions': [], 'pending_count': 0, 'unread_messages': 0, 'caisse_pending': 0, 'weekly_champion': None, 'show_champion': True, 'active_tenders': [], 'pending_absences': 0}
    if 'user_id' in session:
        user = get_user_by_id(session['user_id'])
        if user:
            perms = get_role_permissions(user['role'])
            ctx['current_user'] = user
            ctx['permissions'] = perms
            ctx['user_role'] = user['role']
            ctx['can_edit'] = lambda module: (module + '_edit') in perms or 'admin' in perms
            ctx['pending_count'] = len(get_jobs_by_status('traite'))
            try: ctx['unread_messages'] = get_unread_count(user['id'])
            except: pass
            # Notification count
            try:
                from models import get_db as _ndb
                _nc = _ndb()
                ctx['notif_count'] = _nc.execute("SELECT COUNT(*) FROM notifications WHERE (user_id=? OR employee_id IN (SELECT id FROM employees WHERE email=?)) AND read=0",
                    (user['id'], user.get('email',''))).fetchone()[0]
                _nc.close()
            except: ctx['notif_count'] = 0
            if 'fichiers' in perms or 'admin' in perms:
                try:
                    from models import get_db as _gdb_abs
                    _ca = _gdb_abs()
                    ctx['pending_absences'] = _ca.execute("SELECT COUNT(*) FROM absences WHERE status='en_attente'").fetchone()[0]
                    _ca.close()
                except: pass
            if user['role'] in ('admin', 'dg', 'directeur'):
                try:
                    from models import get_db as _gdb
                    _c = _gdb()
                    ctx['caisse_pending'] = _c.execute("SELECT COUNT(*) FROM caisse_sorties WHERE status='en_attente'").fetchone()[0]
                    # Comptes clients en attente de validation
                    try:
                        ctx['pending_clients_count'] = _c.execute("SELECT COUNT(*) FROM client_users WHERE account_status='pending'").fetchone()[0]
                    except: ctx['pending_clients_count'] = 0
                    # Virements banque→caisse en attente
                    try:
                        ctx['pending_virements_count'] = _c.execute("SELECT COUNT(*) FROM caisse_virements WHERE status='en_attente'").fetchone()[0]
                    except: ctx['pending_virements_count'] = 0
                    # Contrôle qualité en attente
                    try:
                        ctx['pending_cq_count'] = _c.execute("SELECT COUNT(*) FROM interventions WHERE status='travaux_termines' AND COALESCE(cq_status,'')=''").fetchone()[0]
                    except: ctx['pending_cq_count'] = 0
                    # Livraisons à programmer / en attente
                    try:
                        ctx['pending_deliv_count'] = _c.execute("""SELECT COUNT(*) FROM interventions WHERE
                            (cq_status='passed' AND COALESCE(delivery_proposed_date,'')='') OR
                            (delivery_client_status='accepted' AND status!='livre') OR
                            (delivery_client_status='proposed')""").fetchone()[0]
                    except: ctx['pending_deliv_count'] = 0
                    _c.close()
                except: pass
            # Weekly champion
            try:
                from models import get_current_champion, update_weekly_champion, get_live_champion, get_db as _gdb_cp
                update_weekly_champion()
                champ = get_current_champion()
                live = get_live_champion()
                if champ:
                    ctx['weekly_champion'] = champ
                elif live:
                    ctx['weekly_champion'] = live
                # Check visibility setting
                _sc = _gdb_cp()
                try:
                    vis = _sc.execute("SELECT value FROM app_settings WHERE key='show_champion'").fetchone()
                    ctx['show_champion'] = (vis['value'] != '0') if vis else True
                except: ctx['show_champion'] = True
                try:
                    today = datetime.now().strftime('%Y-%m-%d')
                    tenders = [dict(r) for r in _sc.execute(
                        "SELECT * FROM tender_links WHERE is_active=1 AND (deadline='' OR deadline>=?) ORDER BY deadline", (today,)).fetchall()]
                    ctx['active_tenders'] = tenders
                except: pass
                _sc.close()
            except: pass
    else:
        ctx['can_edit'] = lambda module: False
    return ctx


# ======================== AUTH ROUTES ========================


def auto_ecriture(conn, date, libelle, debit_account, credit_account, amount, reference=''):
    """Génère automatiquement une écriture comptable double (débit/crédit) SYSCOHADA."""
    if amount <= 0: return
    conn.execute("INSERT INTO ecritures_comptables (date, journal, piece, compte_debit, compte_credit, libelle, montant, created_by) VALUES (?,?,?,?,?,?,?,0)",
        (date, 'VE', reference, debit_account, credit_account, libelle, amount))

@app.route('/robots.txt')
def robots_txt():
    return "User-agent: *\nAllow: /\n", 200, {'Content-Type': 'text/plain'}

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(app.config.get('STATIC_FOLDER', 'static'), 'logo_wannygest.png', mimetype='image/png')

@app.errorhandler(500)
def internal_error(e):
    import traceback
    traceback.print_exc()
    return f"<h1>Erreur serveur</h1><p>{str(e)}</p><a href='/'>Retour</a>", 500

@app.after_request
def add_security_headers(response):
    if 'text/html' in response.content_type:
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response

@app.errorhandler(404)
def not_found(e):
    return f"<h1>Page non trouvée</h1><a href='/'>Retour à l'accueil</a>", 404

@app.route('/')
def welcome():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return render_template('welcome.html')

def check_reminder_notifications():
    """Check all reminders (client + calendar) and create notifications for upcoming ones."""
    try:
        from models import get_db as _rdb
        conn = _rdb()
        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 1. Client reminders (due today or tomorrow)
        reminders = conn.execute(
            "SELECT cr.*, c.name as client_name FROM client_reminders cr LEFT JOIN clients c ON cr.client_id=c.id WHERE cr.done=0 AND cr.date IN (?,?)", (today, tomorrow)).fetchall()
        for rem in [dict(r) for r in reminders]:
            existing = conn.execute("SELECT id FROM notifications WHERE type='rappel' AND link=? AND created_at > datetime('now','-1 day')",
                (f"/clients/{rem['client_id']}?tab=reminders",)).fetchone()
            if not existing:
                users = conn.execute("SELECT id FROM users WHERE is_active=1").fetchall()
                prefix = "📅 Aujourd'hui" if rem['date'] == today else "⏰ Demain"
                for u in users:
                    conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                        (u['id'], 'rappel', f"{prefix} — {rem['title']}",
                         f"Rappel client {rem.get('client_name','?')} : {rem['title']} ({rem['date']})",
                         f"/clients/{rem['client_id']}?tab=reminders"))
        
        # 2. Calendar events (due today or tomorrow)
        events = conn.execute("SELECT * FROM calendar_events WHERE start_date IN (?,?)", (today, tomorrow)).fetchall()
        for ev in [dict(r) for r in events]:
            existing = conn.execute("SELECT id FROM notifications WHERE type='rappel' AND title LIKE ? AND created_at > datetime('now','-1 day')",
                (f"%{ev['title']}%",)).fetchone()
            if not existing:
                users = conn.execute("SELECT id FROM users WHERE is_active=1").fetchall()
                prefix = "📅 Aujourd'hui" if ev.get('start_date','') == today else "⏰ Demain"
                time_str = f" à {ev['event_time']}" if ev.get('event_time') else ''
                for u in users:
                    conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                        (u['id'], 'rappel', f"{prefix}{time_str} — {ev['title']}",
                         f"{ev.get('description','') or ev['title']}", "/rh/calendrier"))
        
        conn.commit(); conn.close()
    except Exception as e:
        try: conn.close()
        except: pass

@app.before_request
def check_session_timeout():
    """Déconnexion automatique après 30 min d'inactivité."""
    if 'user_id' in session:
        last = session.get('last_active')
        if last:
            try:
                last_dt = datetime.fromisoformat(last)
                if datetime.now() - last_dt > timedelta(minutes=30):
                    session.clear()
                    flash("Session expirée — veuillez vous reconnecter", "info")
                    return redirect(url_for('login'))
            except:
                pass
        session['last_active'] = datetime.now().isoformat()

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        ip = request.remote_addr
        
        # Vérifier verrouillage (5 tentatives en 15 min)
        if get_failed_attempts(username) >= 5:
            flash("Compte temporairement verrouillé (trop de tentatives). Réessayez dans 15 minutes.", "error")
            return render_template('login.html')
        
        user = authenticate_user(username, request.form['password'])
        if user:
            record_login_attempt(username, True, ip)
            session['user_id'] = user['id']
            session['last_active'] = datetime.now().isoformat()
            log_activity(user['id'], user['full_name'], 'Connexion', f"Connexion réussie", ip)
            flash(f"Bienvenue {user['full_name']} !", "success")
            if user['role'] == 'client':
                return redirect('/portail/dashboard')
            return redirect(url_for('dashboard'))
        
        # Try client_users table
        conn = _gdb()
        import hashlib
        cu = conn.execute("SELECT * FROM client_users WHERE username=? AND is_active=1", (username,)).fetchone()
        if cu:
            cu = dict(cu)
            ph = hashlib.sha256((request.form['password'] + (cu.get('salt','') or '')).encode()).hexdigest()
            if ph == cu['password_hash']:
                session['client_user_id'] = cu['id']
                session['client_id'] = cu['client_id']
                session['client_name'] = cu['full_name']
                conn.execute("UPDATE client_users SET last_login=? WHERE id=?", (datetime.now().isoformat(), cu['id']))
                conn.commit(); conn.close()
                record_login_attempt(username, True, ip)
                flash(f"Bienvenue {cu['full_name']} !", "success")
                return redirect('/portail/dashboard')
        conn.close()
        
        record_login_attempt(username, False, ip)
        remaining = 5 - get_failed_attempts(username)
        flash(f"Identifiants incorrects ({remaining} tentative(s) restante(s))", "error")
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
@login_required
def register():
    # Only admin can create users
    u = get_user_by_id(session['user_id'])
    if u['role'] not in ('admin', 'dg'):
        flash("Seul l'administrateur peut créer des comptes", "error")
        return redirect('/dashboard')
    if request.method == 'POST':
        pwd = request.form['password']
        pwd2 = request.form['password2']
        if pwd != pwd2:
            flash("Les mots de passe ne correspondent pas", "error")
            return render_template('register.html')
        if len(pwd) < 6:
            flash("Le mot de passe doit faire au moins 6 caractères", "error")
            return render_template('register.html')
        role = request.form.get('role', 'technicien')
        ok, msg = create_user(
            request.form['username'], request.form['email'],
            pwd, request.form['full_name'], role
        )
        if ok:
            flash(f"Compte '{request.form['username']}' créé avec le rôle {role} !", "success")
            return redirect('/admin')
        flash(msg, "error")
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    flash("Déconnexion réussie", "success")
    return redirect(url_for('welcome'))


# ======================== DASHBOARD ========================

@app.route('/dashboard')
@login_required
def dashboard():
    # Show guide on first visit (only redirect once)
    if request.args.get('skip_guide'):
        session['guide_seen'] = True
    if not session.get('guide_seen'):
        session['guide_seen'] = True
        return redirect(url_for('guide'))
    user = get_user_by_id(session['user_id'])
    role = user['role'] if user else 'technicien'
    # Concierge users go to their dedicated dashboard
    if role == 'concierge':
        return redirect('/concierge')
    stats = get_dashboard_stats()
    inv_stats = get_invoice_stats()
    v_stats = get_visit_stats()
    d_stats = get_devis_stats()
    emp_stats = get_employee_stats()
    # Notifications for dashboard
    announcements = db_get_all('rh_announcements', order='created_at DESC', limit=5) if 'rh_announcements' in _get_tables() else []
    trainings_upcoming = db_get_all('rh_trainings', order='date ASC', limit=5) if 'rh_trainings' in _get_tables() else []
    
    # Secretaire-specific data
    sec_data = {}
    if role in ('secretaire', 'proprietaire', 'admin', 'dg'):
        conn = _gdb()
        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        try: sec_data['today_events'] = [dict(r) for r in conn.execute("SELECT * FROM calendar_events WHERE start_date=? ORDER BY event_time", (today,)).fetchall()]
        except: sec_data['today_events'] = []
        try: sec_data['tomorrow_events'] = [dict(r) for r in conn.execute("SELECT * FROM calendar_events WHERE start_date=? ORDER BY event_time", (tomorrow,)).fetchall()]
        except: sec_data['tomorrow_events'] = []
        try: sec_data['pending_reminders'] = [dict(r) for r in conn.execute("SELECT cr.*, c.name as client_name FROM client_reminders cr LEFT JOIN clients c ON cr.client_id=c.id WHERE cr.done=0 ORDER BY cr.date ASC LIMIT 10").fetchall()]
        except: sec_data['pending_reminders'] = []
        try: sec_data['recent_clients'] = [dict(r) for r in conn.execute("SELECT * FROM clients ORDER BY id DESC LIMIT 5").fetchall()]
        except: sec_data['recent_clients'] = []
        try: sec_data['total_clients'] = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
        except: sec_data['total_clients'] = 0
        try: sec_data['visiteurs_today'] = conn.execute("SELECT COUNT(*) FROM concierge_visiteurs WHERE date_visite=?", (today,)).fetchone()[0]
        except: sec_data['visiteurs_today'] = 0
        try: sec_data['courrier_today'] = conn.execute("SELECT COUNT(*) FROM concierge_courrier WHERE date_reception=?", (today,)).fetchone()[0]
        except: sec_data['courrier_today'] = 0
        try: sec_data['colis_pending'] = conn.execute("SELECT COUNT(*) FROM concierge_colis WHERE statut='en_attente' OR statut IS NULL").fetchone()[0]
        except: sec_data['colis_pending'] = 0
        conn.close()
    
    return render_template('dashboard.html', page='dashboard', stats=stats, 
                          inv_stats=inv_stats, v_stats=v_stats, d_stats=d_stats,
                          emp_stats=emp_stats, user_role=role, sec_data=sec_data,
                          today_str=datetime.now().strftime('%Y-%m-%d'),
                          announcements=announcements, trainings_upcoming=trainings_upcoming)

@app.route('/dashboard-general')
@permission_required('dashboard_general')
def dashboard_general():
    """Tableau de bord général — vue consolidée de toutes les activités."""
    conn = _gdb()
    data = {}
    
    # CRM
    data['clients'] = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    data['prospects'] = conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0] if 'prospects' in _get_tables() else 0
    data['devis'] = conn.execute("SELECT COUNT(*) FROM devis").fetchone()[0]
    data['devis_acceptes'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='accepte'").fetchone()[0]
    
    # Comptabilité
    data['invoices'] = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0] if 'invoices' in _get_tables() else 0
    data['revenue'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status='payee'").fetchone()[0] if 'invoices' in _get_tables() else 0
    data['expenses'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM pieces_caisse").fetchone()[0] if 'pieces_caisse' in _get_tables() else 0
    data['profit'] = data['revenue'] - data['expenses']
    
    # RH
    data['employees'] = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0] if 'employees' in _get_tables() else 0
    data['formations'] = conn.execute("SELECT COUNT(*) FROM rh_trainings").fetchone()[0] if 'rh_trainings' in _get_tables() else 0
    data['annonces'] = conn.execute("SELECT COUNT(*) FROM rh_announcements").fetchone()[0] if 'rh_announcements' in _get_tables() else 0
    
    # Stock & Achats
    data['stock_items'] = conn.execute("SELECT COUNT(*) FROM stock_items").fetchone()[0] if 'stock_items' in _get_tables() else 0
    data['stock_value'] = conn.execute("SELECT COALESCE(SUM(quantity*unit_price),0) FROM stock_items").fetchone()[0] if 'stock_items' in _get_tables() else 0
    data['stock_low'] = conn.execute("SELECT COUNT(*) FROM stock_items WHERE quantity<=min_stock").fetchone()[0] if 'stock_items' in _get_tables() else 0
    data['fournisseurs'] = conn.execute("SELECT COUNT(*) FROM achats_fournisseurs").fetchone()[0] if 'achats_fournisseurs' in _get_tables() else 0
    data['commandes'] = conn.execute("SELECT COUNT(*) FROM achats_commandes").fetchone()[0] if 'achats_commandes' in _get_tables() else 0
    
    # Gestion du temps
    data['rapports'] = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] if 'jobs' in _get_tables() else 0
    data['rapports_j'] = conn.execute("SELECT COUNT(*) FROM rapports_journaliers").fetchone()[0] if 'rapports_journaliers' in _get_tables() else 0
    
    # Informatique
    data['projets'] = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0] if 'projects' in _get_tables() else 0
    data['tickets'] = conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0] if 'tickets' in _get_tables() else 0
    
    # Recent activity
    data['recent_logs'] = []
    try:
        data['recent_logs'] = [dict(r) for r in conn.execute("SELECT * FROM activity_logs ORDER BY created_at DESC LIMIT 15").fetchall()]
    except: pass
    
    # Monthly revenue chart
    data['monthly_rev'] = [dict(r) for r in conn.execute("""SELECT strftime('%Y-%m', created_at) as month, SUM(amount) as total 
        FROM invoices WHERE status='payee' GROUP BY month ORDER BY month DESC LIMIT 6""").fetchall()] if 'invoices' in _get_tables() else []
    data['monthly_rev'].reverse()
    
    # Invoice stats
    data['inv_stats'] = {}
    if 'invoices' in _get_tables():
        for s in ('a_envoyer','envoyee','en_attente_paiement','payee'):
            data['inv_stats'][s] = conn.execute(f"SELECT COUNT(*) FROM invoices WHERE status='{s}'").fetchone()[0]
        data['pending_amount'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status IN ('envoyee','en_attente_paiement')").fetchone()[0]
    
    # Devis stats
    data['devis_stats'] = {}
    for s in ('brouillon','envoye','accepte','refuse'):
        data['devis_stats'][s] = conn.execute(f"SELECT COUNT(*) FROM devis WHERE status='{s}'").fetchone()[0]
    
    # Prospect stats
    data['prospect_stats'] = {}
    if 'prospects' in _get_tables():
        for s in ('nouveau','contacte','qualifie','proposition','gagne','perdu'):
            data['prospect_stats'][s] = conn.execute(f"SELECT COUNT(*) FROM prospects WHERE status='{s}'").fetchone()[0]
    
    conn.close()
    return render_template('dashboard_general.html', page='dashboard_general', data=data)

def _get_tables():
    from models import get_db
    conn = get_db()
    tables = [r['name'] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    conn.close()
    return tables

@app.route('/guide')
@login_required
def guide():
    return render_template('onboarding.html', page='guide')


# ======================== TRAITEMENT ========================

@app.route('/traitement')
@permission_required('traitement')
def traitement():
    clients = get_all_clients()
    return render_template('traitement.html', page='traitement', clients=clients)

@app.route('/traitement/preview', methods=['POST'])
@login_required
def traitement_preview():
    if 'excel_file' not in request.files:
        return jsonify({"error": "Aucun fichier"}), 400
    file = request.files['excel_file']
    if not allowed_file(file.filename):
        return jsonify({"error": "Format non supporté"}), 400
    job_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'preview')
    os.makedirs(job_dir, exist_ok=True)
    try:
        path = os.path.join(job_dir, secure_filename(file.filename))
        file.save(path)
        emps, client = extract_from_excel(path)
        from models import save_known_employees
        save_known_employees([e['name'] for e in emps])
        return jsonify({"client": client, "count": len(emps),
            "employees": [{"name": e['name'], "ref": e['ref'], "days": len(e['records'])} for e in emps]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)

@app.route('/traitement/merge', methods=['POST'])
@login_required
def traitement_merge():
    if 'enr_file' not in request.files or 'trans_file' not in request.files:
        return jsonify({"error": "Les 2 fichiers sont requis"}), 400
    enr = request.files['enr_file']
    trans = request.files['trans_file']
    if not enr.filename or not trans.filename:
        return jsonify({"error": "Les 2 fichiers sont requis"}), 400
    merge_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'merge_' + str(uuid.uuid4())[:8])
    os.makedirs(merge_dir, exist_ok=True)
    try:
        enr_path = os.path.join(merge_dir, secure_filename(enr.filename))
        trans_path = os.path.join(merge_dir, secure_filename(trans.filename))
        enr.save(enr_path); trans.save(trans_path)
        out = os.path.join(merge_dir, 'Presence_fusionnee.xlsx')
        result = generate_presence_xlsx(enr_path, trans_path, out)
        if not result:
            return jsonify({"error": "Échec de la fusion."}), 400
        merge_id = os.path.basename(merge_dir)
        # Save employee names for schedule
        if result.get('employees'):
            from models import save_known_employees
            save_known_employees(result['employees'], result.get('emp_services', {}))
        return jsonify({"success": True, "merge_id": merge_id, "client": result['client'],
            "employees": result['employees'], "emp_services": result.get('emp_services', {}),
            "rows": result['rows'], "filename": 'Presence_fusionnee.xlsx'})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/traitement/merge/download/<merge_id>')
@login_required
def traitement_merge_download(merge_id):
    safe_id = secure_filename(merge_id)
    path = os.path.join(app.config['UPLOAD_FOLDER'], safe_id, 'Presence_fusionnee.xlsx')
    if not os.path.exists(path):
        flash("Fichier expiré", "error")
        return redirect(url_for('traitement'))
    return send_file(path, as_attachment=True, download_name='Presence_fusionnee.xlsx')

@app.route('/traitement/generate', methods=['POST'])
@permission_required('traitement')
def traitement_generate():
    merge_id = request.form.get('merge_id', '').strip()
    
    if merge_id:
        merge_dir = os.path.join(app.config['UPLOAD_FOLDER'], merge_id)
        merged_file = os.path.join(merge_dir, 'Presence_fusionnee.xlsx')
        if not os.path.exists(merged_file):
            flash("Fichier fusionné expiré. Refaites la fusion.", "error")
            return redirect(url_for('traitement'))
        xlsx_source = merged_file
        filename = 'Presence_fusionnee.xlsx'
    else:
        if 'excel_file' not in request.files or not request.files['excel_file'].filename:
            flash("Aucun fichier sélectionné", "error")
            return redirect(url_for('traitement'))
        file = request.files['excel_file']
        if not allowed_file(file.filename):
            flash("Format non supporté", "error")
            return redirect(url_for('traitement'))
        xlsx_source = None
        filename = secure_filename(file.filename)
    
    provider_name = request.form.get('provider_name', '').strip() or "RAMYA TECHNOLOGIE & INNOVATION"
    provider_info = request.form.get('provider_info', '').strip() or "Tél: 2722204498 | Email: techniqueramya@gmail.com"
    client_name = request.form.get('client_name', '').strip()
    client_tel = request.form.get('client_tel', '').strip()
    client_email = request.form.get('client_email', '').strip()
    client_id_str = request.form.get('client_id', '').strip()
    hp_str = request.form.get('required_hours', '0').strip()
    hp_we_str = request.form.get('required_hours_weekend', '0').strip()
    hourly_cost_str = request.form.get('hourly_cost', '0').strip()
    employee_costs_json = request.form.get('employee_costs_json', '{}')

    
    # Auto-fill from client database
    client_id = int(client_id_str) if client_id_str else None
    if client_id:
        db_client = get_client_by_id(client_id)
        if db_client:
            if not client_name: client_name = db_client['name']
            if not client_tel: client_tel = db_client.get('tel', '')
            if not client_email: client_email = db_client.get('email', '')
    
    client_info_parts = []
    if client_tel: client_info_parts.append(f"Tél: {client_tel}")
    if client_email: client_info_parts.append(f"Email: {client_email}")
    client_info = " | ".join(client_info_parts)
    
    try:
        hp = float(hp_str) if hp_str else 0
    except:
        hp = 0
    try:
        hp_weekend = float(hp_we_str) if hp_we_str else 0
    except:
        hp_weekend = 0
    try:
        hourly_cost = float(hourly_cost_str) if hourly_cost_str else 0
    except:
        hourly_cost = 0
    try:
        employee_costs = json.loads(employee_costs_json) if employee_costs_json else {}
    except:
        employee_costs = {}
    
    # Rest days
    rest_days = request.form.getlist('rest_days')
    try:
        employee_rest_days = json.loads(request.form.get('employee_rest_days_json', '{}'))
    except:
        employee_rest_days = {}
    
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(app.config['UPLOAD_FOLDER'], job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    try:
        if xlsx_source:
            xlsx_path = os.path.join(job_dir, filename)
            shutil.copy2(xlsx_source, xlsx_path)
        else:
            file = request.files['excel_file']
            xlsx_path = os.path.join(job_dir, filename)
            file.save(xlsx_path)
        
        # Logo
        logo_path = None
        if 'logo_file' in request.files:
            lf = request.files['logo_file']
            if lf.filename:
                logo_path = os.path.join(job_dir, 'custom_logo.png')
                lf.save(logo_path)
        if not logo_path:
            for n in ['logo_ramya.png', 'logo_wannygest.png']:
                c = os.path.join(BASE_DIR, n)
                if os.path.exists(c):
                    logo_path = os.path.join(job_dir, n)
                    shutil.copy2(c, logo_path)
                    break
        
        emps, detected_client = extract_from_excel(xlsx_path)
        from models import save_known_employees
        save_known_employees([e['name'] for e in emps])
        if not emps:
            flash("Aucun employé trouvé", "error")
            return redirect(url_for('traitement'))
        
        if not client_name:
            client_name = detected_client
            # Try to find client in database
            db_client = find_client_by_name(detected_client)
            if db_client:
                client_id = db_client['id']
                if not client_tel: client_tel = db_client.get('tel', '')
                if not client_email: client_email = db_client.get('email', '')
                client_info_parts = []
                if client_tel: client_info_parts.append(f"Tél: {client_tel}")
                if client_email: client_info_parts.append(f"Email: {client_email}")
                client_info = " | ".join(client_info_parts)
        
        all_dates = [rec['date'] for emp in emps for rec in emp['records']]
        all_dates.sort()
        period = f"Période du {all_dates[0]} au {all_dates[-1]}" if all_dates else "Rapport"
        
        base = os.path.splitext(filename)[0]
        pdf_name = f"{base}_RAPPORT_DE_PRESENCE.pdf"
        output_path = os.path.join(job_dir, pdf_name)
        
        generate_full_pdf(emps, output_path, provider_name, provider_info,
                         client_name, period, logo_path, hp=hp, client_info=client_info,
                         work_dir=job_dir, hp_weekend=hp_weekend, hourly_cost=hourly_cost,
                         employee_costs=employee_costs, rest_days=rest_days,
                         employee_rest_days=employee_rest_days)
        
        if not os.path.exists(output_path):
            flash("Erreur génération PDF", "error")
            return redirect(url_for('traitement'))
        
        # Sauvegarder dans le dossier files
        files_dir = os.path.join(app.config['FILES_FOLDER'], job_id)
        os.makedirs(files_dir, exist_ok=True)
        shutil.copy2(output_path, os.path.join(files_dir, pdf_name))
        
        # Copier le xlsx fusionné si dispo
        xlsx_out = None
        if merge_id:
            merged_xlsx = os.path.join(app.config['UPLOAD_FOLDER'], merge_id, 'Presence_fusionnee.xlsx')
            if os.path.exists(merged_xlsx):
                xlsx_out = 'Presence_fusionnee.xlsx'
                shutil.copy2(merged_xlsx, os.path.join(files_dir, xlsx_out))
        
        # Enregistrer dans la BDD
        hp_text = f"{hp}h/sem" if hp > 0 else "Auto"
        if hp_weekend > 0: hp_text += f" | {hp_weekend}h/we"
        create_job(job_id, session['user_id'], client_name, provider_name,
                   filename, pdf_name, xlsx_out, len(emps), period, hp_text, client_id)
        
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Traitement', f"Rapport PDF généré — {client_name}, {len(emps)} employés",
                    request.remote_addr)
        flash(f"Rapport généré avec succès — {len(emps)} employés, {client_name}", "success")
        return send_file(output_path, as_attachment=True, download_name=pdf_name, mimetype='application/pdf')
    
    except Exception as e:
        flash(f"Erreur : {str(e)}", "error")
        return redirect(url_for('traitement'))
    finally:
        _cleanup_old(app.config['UPLOAD_FOLDER'])


# ======================== FICHIERS RH ========================

@app.route('/fichiers')
@permission_required('fichiers')
def fichiers():
    tab = request.args.get('tab', 'pending')
    pending = get_jobs_by_status('traite')
    sent = get_jobs_by_status('envoye')
    return render_template('fichiers.html', page='fichiers', tab=tab, pending=pending, sent=sent)

@app.route('/fichiers/download/<job_id>/<ftype>')
@login_required
def fichiers_download(job_id, ftype):
    safe_id = secure_filename(job_id)
    files_dir = os.path.join(app.config['FILES_FOLDER'], safe_id)
    if not os.path.isdir(files_dir):
        flash("Fichier non trouvé", "error")
        return redirect(url_for('fichiers'))
    for f in os.listdir(files_dir):
        if ftype == 'pdf' and f.endswith('.pdf'):
            return send_from_directory(files_dir, f, as_attachment=True)
        if ftype == 'xlsx' and f.endswith('.xlsx'):
            return send_from_directory(files_dir, f, as_attachment=True)
    flash("Fichier non trouvé", "error")
    return redirect(url_for('fichiers'))

@app.route('/fichiers/preview/<job_id>')
@login_required
def fichiers_preview(job_id):
    """Prévisualisation PDF dans le navigateur avant envoi."""
    safe_id = secure_filename(job_id)
    files_dir = os.path.join(app.config['FILES_FOLDER'], safe_id)
    if not os.path.isdir(files_dir):
        flash("Fichier non trouvé", "error")
        return redirect(url_for('fichiers'))
    for f in os.listdir(files_dir):
        if f.endswith('.pdf'):
            return send_from_directory(files_dir, f, as_attachment=False, mimetype='application/pdf')
    flash("PDF non trouvé", "error")
    return redirect(url_for('fichiers'))

@app.route('/fichiers/marquer/<job_id>')
@permission_required('envoyer')
def fichiers_marquer(job_id):
    mark_job_sent(job_id, session['user_id'])
    job = get_job_by_id(job_id)
    if job:
        create_invoice(job_id, job.get('client_id'), job.get('client_name', ''))
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?',
                'Envoi', f"Rapport {job_id} marqué comme envoyé", request.remote_addr)
    flash("Fichier envoyé — facture créée pour la comptabilité", "success")
    return redirect(url_for('fichiers'))


# ======================== CLIENTS ========================

@app.route('/clients')
@permission_required('clients')
def clients_page():
    clients = get_all_clients()
    return render_template('clients.html', page='clients', clients=clients)


@app.route('/clients/<int:cid>')
@permission_required('clients')
def client_profile(cid):
    conn = _gdb()
    client = conn.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone()
    if not client: flash("Client non trouvé","error"); return redirect('/clients')
    client = dict(client)
    tab = request.args.get('tab', 'profil')
    data = {'tab': tab, 'client': client}
    
    # Notes
    try: data['notes'] = [dict(r) for r in conn.execute("SELECT n.*, u.full_name as author FROM client_notes n LEFT JOIN users u ON n.created_by=u.id WHERE n.client_id=? ORDER BY n.created_at DESC", (cid,)).fetchall()]
    except: data['notes'] = []
    # Devis
    try: data['devis'] = [dict(r) for r in conn.execute("SELECT * FROM devis WHERE client_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    except: data['devis'] = []
    # Factures
    try: data['factures'] = [dict(r) for r in conn.execute("SELECT * FROM invoices WHERE client_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    except: data['factures'] = []
    # Contrats
    try: data['contrats'] = [dict(r) for r in conn.execute("SELECT * FROM achats_contrats WHERE fournisseur_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    except: data['contrats'] = []
    # Visites
    try: data['visites'] = [dict(r) for r in conn.execute("SELECT * FROM visits WHERE client_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    except: data['visites'] = []
    # Tickets
    try: data['tickets'] = [dict(r) for r in conn.execute("SELECT * FROM tickets WHERE client_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    except: data['tickets'] = []
    # Projets
    try: data['projets'] = [dict(r) for r in conn.execute("SELECT * FROM projects WHERE client_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    except: data['projets'] = []
    # Attachments
    try: data['attachments'] = [dict(r) for r in conn.execute("SELECT * FROM client_attachments WHERE client_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    except: data['attachments'] = []
    # Reminders
    try: data['reminders'] = [dict(r) for r in conn.execute("SELECT * FROM client_reminders WHERE client_id=? ORDER BY date ASC", (cid,)).fetchall()]
    except: data['reminders'] = []
    # Pieces caisse (expenses linked to client)
    try: data['depenses'] = [dict(r) for r in conn.execute("SELECT * FROM pieces_caisse WHERE supplier LIKE ? ORDER BY date DESC", (f'%{client["name"]}%',)).fetchall()]
    except: data['depenses'] = []
    
    conn.close()
    return render_template('extra_pages.html', page='client_profile', **data)

@app.route('/clients/<int:cid>/note/add', methods=['POST'])
@permission_required('clients_edit')
def client_note_add(cid):
    conn = _gdb()
    conn.execute("INSERT INTO client_notes (client_id, content, created_by) VALUES (?,?,?)",
        (cid, request.form.get('content',''), session['user_id']))
    conn.commit(); conn.close()
    flash("Note ajoutée","success"); return redirect(f'/clients/{cid}?tab=notes')

@app.route('/clients/<int:cid>/attachment/add', methods=['POST'])
@permission_required('clients_edit')
def client_attachment_add(cid):
    if 'file' in request.files and request.files['file'].filename:
        f = request.files['file']
        fname = f"client_{cid}_{secure_filename(f.filename)}"
        fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'clients')
        os.makedirs(fdir, exist_ok=True)
        f.save(os.path.join(fdir, fname))
        conn = _gdb()
        conn.execute("INSERT INTO client_attachments (client_id, filename, original_name, category, notes, created_by) VALUES (?,?,?,?,?,?)",
            (cid, fname, f.filename, request.form.get('category','general'), request.form.get('notes',''), session['user_id']))
        conn.commit(); conn.close()
        flash("Pièce jointe ajoutée","success")
    return redirect(f'/clients/{cid}?tab=attachments')


@app.route('/clients/<int:cid>/attachment/delete/<int:aid>')
@permission_required('clients_edit')
def client_attachment_delete(cid, aid):
    conn = _gdb()
    att = conn.execute("SELECT * FROM client_attachments WHERE id=? AND client_id=?", (aid, cid)).fetchone()
    if att:
        # Delete file
        try:
            fpath = os.path.join(app.config['UPLOAD_FOLDER'], 'clients', att['filename'])
            if os.path.exists(fpath): os.remove(fpath)
        except: pass
        conn.execute("DELETE FROM client_attachments WHERE id=?", (aid,))
        conn.commit()
        flash("Pièce jointe supprimée", "success")
    conn.close()
    return redirect(f'/clients/{cid}?tab=attachments')

@app.route('/clients/<int:cid>/reminder/add', methods=['POST'])
@permission_required('clients_edit')
def client_reminder_add(cid):
    conn = _gdb()
    conn.execute("INSERT INTO client_reminders (client_id, title, date, created_by) VALUES (?,?,?,?)",
        (cid, request.form.get('title',''), request.form.get('date',''), session['user_id']))
    conn.commit(); conn.close()
    flash("Rappel ajouté","success"); return redirect(f'/clients/{cid}?tab=reminders')

@app.route('/clients/<int:cid>/reminder/done/<int:rid>')
@permission_required('clients_edit')
def client_reminder_done(cid, rid):
    conn = _gdb(); conn.execute("UPDATE client_reminders SET done=1 WHERE id=?", (rid,)); conn.commit(); conn.close()
    return redirect(f'/clients/{cid}?tab=reminders')


@app.route('/clients/add', methods=['POST'])
@permission_required('clients_edit')
def clients_add():
    create_client(
        request.form['name'], request.form.get('tel', ''),
        request.form.get('email', ''), request.form.get('contact_name', ''),
        request.form.get('address', ''), request.form.get('notes', ''),
        session['user_id'],
        client_code=request.form.get('client_code', '').strip()
    )
    # Save client_status
    try:
        conn = _gdb()
        cid_new = conn.execute("SELECT id FROM clients ORDER BY id DESC LIMIT 1").fetchone()[0]
        conn.execute("UPDATE clients SET client_status=? WHERE id=?", (request.form.get('client_status','entreprise_sans_contrat'), cid_new))
        conn.commit(); conn.close()
    except: pass
    # Update enriched fields
    from models import get_db as _gdb2
    conn = _gdb2()
    cid = conn.execute("SELECT id FROM clients ORDER BY id DESC LIMIT 1").fetchone()['id']
    for field in ['sector','city','country','website','rc_number','cnps_number',
                  'contact_title','contact_tel2','contact_email2','payment_terms',
                  'source','status']:
        val = request.form.get(field, '')
        if val:
            try: conn.execute(f"UPDATE clients SET {field}=? WHERE id=?", (val, cid))
            except: pass
    if request.form.get('credit_limit'):
        conn.execute("UPDATE clients SET credit_limit=? WHERE id=?", (float(request.form['credit_limit']), cid))
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_audit(session['user_id'], user['full_name'] if user else '?', 'clients', 0, 'create', 'name', '', request.form['name'])
    flash("Client ajouté", "success")
    return redirect(url_for('clients_page'))

@app.route('/clients/delete-bulk', methods=['POST'])
@permission_required('clients_edit')
def clients_delete_bulk():
    """Supprime plusieurs clients en une seule opération."""
    ids_raw = request.form.getlist('ids') or request.form.get('ids','').split(',')
    ids = [int(x) for x in ids_raw if str(x).strip().isdigit()]
    if not ids:
        flash("Aucun client sélectionné", "warning")
        return redirect(url_for('clients_page'))
    conn = _gdb()
    deleted = 0
    # Only delete clients that have no related records (defensive)
    # Otherwise null-out the client_id on related tables to avoid FK issues
    for cid in ids:
        try:
            # Unlink in related tables (keep history)
            for tbl, col in [('invoices','client_id'),('devis','client_id'),('jobs','client_id'),
                             ('visit_reports','client_id'),('interventions','client_id'),
                             ('client_notes','client_id'),('client_attachments','client_id'),
                             ('client_reminders','client_id'),('contracts','client_id'),
                             ('client_requests','client_id')]:
                try: conn.execute(f"UPDATE {tbl} SET {col}=NULL WHERE {col}=?", (cid,))
                except: pass
            conn.execute("DELETE FROM clients WHERE id=?", (cid,))
            deleted += 1
        except Exception as e:
            pass
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Clients',
                 f"Suppression groupée de {deleted} client(s)", request.remote_addr)
    flash(f"✅ {deleted} client(s) supprimé(s)", "success")
    return redirect(url_for('clients_page'))

@app.route('/clients/edit/<int:cid>', methods=['GET', 'POST'])
@permission_required('clients_edit')
def clients_edit(cid):
    client = get_client_by_id(cid)
    if not client:
        flash("Client non trouvé", "error")
        return redirect(url_for('clients_page'))
    if request.method == 'POST':
        update_client(cid, name=request.form['name'], tel=request.form.get('tel', ''),
                      email=request.form.get('email', ''), contact_name=request.form.get('contact_name', ''),
                      address=request.form.get('address', ''), notes=request.form.get('notes', ''))
        # Update enriched fields
        from models import get_db as _gdb3
        conn = _gdb3()
        for field in ['sector','city','country','website','rc_number','cnps_number',
                      'contact_title','contact_tel2','contact_email2','payment_terms',
                      'source','status','client_status','client_code']:
            val = request.form.get(field, '')
            try: conn.execute(f"UPDATE clients SET {field}=? WHERE id=?", (val, cid))
            except: pass
        if request.form.get('credit_limit'):
            try: conn.execute("UPDATE clients SET credit_limit=? WHERE id=?", (float(request.form.get('credit_limit',0) or 0), cid))
            except: pass
        conn.commit(); conn.close()
        user = get_user_by_id(session['user_id'])
        log_audit(session['user_id'], user['full_name'] if user else '?', 'clients', cid, 'update', 'name', client['name'], request.form['name'])
        flash("Client modifié", "success")
        # Preserve return page (from hidden field or referrer)
        return_to = request.form.get('return_to') or request.referrer
        if return_to and request.host in (return_to or ''):
            return redirect(return_to)
        return redirect(url_for('clients_page'))
    return render_template('edit_pages.html', page='clients', client=client, return_to=request.args.get('return_to') or request.referrer or '')

@app.route('/clients/delete/<int:cid>')
@permission_required('clients_edit')
def clients_delete(cid):
    ok = delete_client(cid)
    if ok:
        flash("✅ Client supprimé", "success")
    else:
        flash("❌ Impossible de supprimer ce client. Il est probablement référencé par des documents (devis, factures, interventions).", "error")
    return_to = request.args.get('return_to') or request.referrer or ''
    if return_to and request.host in return_to:
        return redirect(return_to)
    return redirect(url_for('clients_page'))


@app.route('/clients/merge', methods=['POST'])
@permission_required('clients_edit')
def clients_merge():
    """Fusionne deux doublons : keep_id garde tout, drop_id est supprimé après transfert des liens."""
    from models import merge_clients
    try:
        keep_id = int(request.form.get('keep_id', 0))
        drop_id = int(request.form.get('drop_id', 0))
    except ValueError:
        flash("IDs invalides", "error")
        return redirect(request.referrer or url_for('clients_page'))
    if not keep_id or not drop_id:
        flash("Sélectionnez deux clients à fusionner", "error")
        return redirect(request.referrer or url_for('clients_page'))
    result = merge_clients(keep_id, drop_id)
    if result['success']:
        flash(f"✅ {result['message']}", "success")
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                     'Clients',
                     f"Fusion clients : conservé {result.get('keep_name','')} (id {keep_id}), supprimé {result.get('drop_name','')} (id {drop_id})",
                     request.remote_addr)
    else:
        flash(f"❌ {result['message']}", "error")
    return_to = request.form.get('return_to') or request.referrer or ''
    if return_to and request.host in return_to:
        return redirect(return_to)
    return redirect(url_for('clients_page'))


# ======================== ADMIN ========================

@app.route('/admin')
@permission_required('admin')
def admin_page():
    users = get_all_users()
    stats = get_dashboard_stats()
    role_perms = {r: get_role_permissions(r) for r in ['admin', 'dg', 'rh', 'technicien', 'commercial', 'comptable', 'moyens_generaux', 'informatique', 'resp_projet', 'proprietaire', 'concierge', 'secretaire']}
    conn = _gdb()
    try:
        tenders = [dict(r) for r in conn.execute("SELECT * FROM tender_links ORDER BY active DESC, deadline ASC").fetchall()]
    except: tenders = []
    # SMTP settings
    smtp = get_smtp_settings(session['user_id'])
    # Activity logs
    admin_logs = get_activity_logs(limit=100)
    conn.close()
    section = request.args.get('section', 'users')
    return render_template('admin.html', page='admin', users=users, stats=stats,
                          all_permissions=ALL_PERMISSIONS, role_perms=role_perms, perm_categories=PERM_CATEGORIES,
                          tenders=tenders, tab='tenders', smtp=smtp, admin_logs=admin_logs, section=section)

@app.route('/admin/add', methods=['POST'])
@permission_required('admin')
def admin_add_user():
    ok, msg = create_user(
        request.form['username'], request.form['email'],
        request.form['password'], request.form['full_name'],
        request.form.get('role', 'technicien')
    )
    if ok:
        log_activity(session['user_id'], 'Admin', 'Utilisateur', f"Compte créé: {request.form['full_name']}", request.remote_addr)
    flash(msg, "success" if ok else "error")
    return redirect(url_for('admin_page'))

@app.route('/admin/reset', methods=['POST'])
@permission_required('admin')
def admin_reset():
    target = request.form.get('target', '')
    user = get_user_by_id(session['user_id'])
    uname = user['full_name'] if user else '?'
    
    if target == 'jobs':
        reset_jobs()
        files_dir = app.config['FILES_FOLDER']
        if os.path.exists(files_dir):
            shutil.rmtree(files_dir)
            os.makedirs(files_dir, exist_ok=True)
        log_activity(session['user_id'], uname, 'Réinitialisation', 'Tous les rapports supprimés', request.remote_addr)
        flash("Tous les rapports ont été supprimés", "success")
    
    elif target == 'clients':
        reset_clients()
        log_activity(session['user_id'], uname, 'Réinitialisation', 'Tous les clients supprimés', request.remote_addr)
        flash("Tous les clients ont été supprimés", "success")
    
    elif target == 'users':
        reset_users()
        log_activity(session['user_id'], uname, 'Réinitialisation', 'Utilisateurs non-admin supprimés', request.remote_addr)
        flash("Tous les utilisateurs (sauf admin) ont été supprimés", "success")
    
    elif target == 'all':
        reset_all()
        for folder in [app.config['FILES_FOLDER'], app.config['UPLOAD_FOLDER']]:
            if os.path.exists(folder):
                shutil.rmtree(folder)
                os.makedirs(folder, exist_ok=True)
        log_activity(session['user_id'], uname, 'Réinitialisation', 'RÉINITIALISATION COMPLÈTE', request.remote_addr)
        flash("Réinitialisation complète effectuée", "success")
    
    return redirect(url_for('admin_page'))

@app.route('/admin/edit/<int:uid>', methods=['GET', 'POST'])
@permission_required('admin')
def admin_edit_user(uid):
    u = get_user_by_id(uid)
    if not u:
        flash("Utilisateur non trouvé", "error")
        return redirect(url_for('admin_page'))
    if request.method == 'POST':
        updates = {'full_name': request.form['full_name'],
                   'email': request.form['email'],
                   'role': request.form['role']}
        pwd = request.form.get('password', '').strip()
        if pwd:
            updates['password'] = pwd
        update_user(uid, **updates)
        flash("Utilisateur modifié", "success")
        return redirect(url_for('admin_page'))
    return render_template('edit_pages.html', page='admin', edit_user=u)

@app.route('/admin/toggle/<int:uid>')
@permission_required('admin')
def admin_toggle_user(uid):
    u = get_user_by_id(uid)
    if u and u['role'] != 'admin':
        update_user(uid, is_active=0 if u['is_active'] else 1)
        flash(f"Utilisateur {'désactivé' if u['is_active'] else 'activé'}", "success")
    return redirect(url_for('admin_page'))

@app.route('/admin/delete/<int:uid>')
@permission_required('admin')
def admin_delete_user(uid):
    u = get_user_by_id(uid)
    if not u:
        flash("Utilisateur non trouvé", "error")
    elif u['role'] == 'admin' and u['username'] == 'admin':
        flash("Impossible de supprimer le compte admin principal", "error")
    elif uid == session.get('user_id'):
        flash("Impossible de supprimer votre propre compte", "error")
    else:
        delete_user(uid)
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Admin', f"Utilisateur {u['full_name']} ({u['username']}) supprimé", request.remote_addr)
        flash(f"Compte '{u['username']}' supprimé définitivement", "success")
    return redirect(url_for('admin_page'))

@app.route('/admin/permissions', methods=['POST'])
@permission_required('admin')
def admin_permissions():
    for role in ['dg', 'rh', 'technicien', 'commercial', 'comptable', 'moyens_generaux', 'informatique', 'resp_projet', 'concierge', 'proprietaire', 'secretaire']:
        perms = [p for p in ALL_PERMISSIONS if request.form.get(f'{role}_{p}')]
        update_role_permissions(role, perms)
    # Admin always has all
    update_role_permissions('admin', ALL_PERMISSIONS)
    flash("Permissions mises à jour", "success")
    return redirect(url_for('admin_page'))

@app.route('/admin/toggle-champion')
@permission_required('admin')
def admin_toggle_champion():
    conn = _gdb()
    try:
        current = conn.execute("SELECT value FROM app_settings WHERE key='show_champion'").fetchone()
        new_val = '0' if (current and current['value'] == '1') else '1'
        conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('show_champion', ?)", (new_val,))
        conn.commit()
        msg = "Champion affiché" if new_val == '1' else "Champion masqué — Ticker appels d'offres activé"
        flash(msg, "success")
    except: pass
    conn.close()
    return redirect(request.referrer or '/dashboard')

@app.route('/admin/appels-offres')
@permission_required('admin')
def admin_tenders():
    # Redirect to admin page which now always shows tenders section
    return redirect(url_for('admin_page'))

@app.route('/admin/smtp', methods=['POST'])
@permission_required('admin')
def admin_smtp_save():
    smtp_host = request.form.get('smtp_host','').strip()
    smtp_port = int(request.form.get('smtp_port', 587) or 587)
    smtp_user = request.form.get('smtp_user','').strip()
    smtp_pass = request.form.get('smtp_pass','').strip()
    if smtp_host and smtp_user:
        save_smtp_settings(session['user_id'], smtp_host, smtp_port, smtp_user, smtp_pass)
        flash("Paramètres SMTP sauvegardés", "success")
    else:
        flash("Serveur et email requis", "error")
    return redirect(url_for('admin_page'))

@app.route('/admin/appels-offres/add', methods=['POST'])
@permission_required('admin')
def admin_tender_add():
    conn = _gdb()
    conn.execute("""INSERT INTO tender_links (title, url, source, deadline, category, active, created_by)
        VALUES (?,?,?,?,?,1,?)""",
        (request.form.get('title',''), request.form.get('url',''),
         request.form.get('source',''), request.form.get('deadline',''),
         request.form.get('category','securite'), session['user_id']))
    conn.commit(); conn.close()
    flash("Appel d'offres ajouté au ticker", "success")
    return redirect('/admin/appels-offres')

@app.route('/admin/appels-offres/delete/<int:tid>')
@permission_required('admin')
def admin_tender_delete(tid):
    conn = _gdb()
    conn.execute("DELETE FROM tender_links WHERE id=?", (tid,))
    conn.commit(); conn.close()
    flash("Appel d'offres supprimé", "success")
    return redirect('/admin/appels-offres')

@app.route('/admin/appels-offres/toggle/<int:tid>')
@permission_required('admin')
def admin_tender_toggle(tid):
    conn = _gdb()
    conn.execute("UPDATE tender_links SET active = CASE WHEN active=1 THEN 0 ELSE 1 END WHERE id=?", (tid,))
    conn.commit(); conn.close()
    return redirect('/admin/appels-offres')


# ======================== LOGS D'ACTIVITÉ ========================

@app.route('/logs')
@permission_required('logs')
def logs_page():
    logs = get_activity_logs(200)
    return render_template('logs.html', page='logs', logs=logs)


# ======================== DÉTAIL RAPPORT + COMMENTAIRES ========================

@app.route('/job/<job_id>')
@login_required
def job_detail(job_id):
    job = get_job_by_id(job_id)
    if not job:
        flash("Rapport non trouvé", "error")
        return redirect(url_for('dashboard'))
    comments = get_job_comments(job_id)
    return render_template('job_detail.html', page='fichiers', job=job, comments=comments)

@app.route('/job/<job_id>/comment', methods=['POST'])
@login_required
def job_add_comment(job_id):
    comment = request.form.get('comment', '').strip()
    if comment:
        user = get_user_by_id(session['user_id'])
        add_job_comment(job_id, session['user_id'], user['full_name'] if user else '?', comment)
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Commentaire', f"Commentaire ajouté sur rapport {job_id}", request.remote_addr)
    return redirect(url_for('job_detail', job_id=job_id))

@app.route('/job/<job_id>/notes', methods=['POST'])
@login_required
def job_update_notes(job_id):
    notes = request.form.get('notes', '').strip()
    update_job_notes(job_id, notes)
    flash("Notes mises à jour", "success")
    return redirect(url_for('job_detail', job_id=job_id))


# ======================== SAUVEGARDE BDD ========================

@app.route('/admin/backup')
@permission_required('admin')
def admin_backup():
    db_path = get_db_path()
    if os.path.exists(db_path):
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Sauvegarde', 'Téléchargement de la base de données', request.remote_addr)
        return send_file(db_path, as_attachment=True,
                        download_name=f"ramya_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.db")
    flash("Base de données introuvable", "error")
    return redirect(url_for('admin_page'))


# ======================== PWA MANIFEST ========================

@app.route('/manifest.json')
def pwa_manifest():
    manifest = {
        "name": "WannyGest",
        "short_name": "WannyGest",
        "start_url": "/dashboard",
        "display": "standalone",
        "background_color": "#0d2137",
        "theme_color": "#1a3a5c",
        "icons": [
            {"src": "/static/logo_wannygest.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/logo_wannygest.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return jsonify(manifest)

@app.route('/sw.js')
def service_worker():
    return app.send_static_file('sw.js') if os.path.exists(os.path.join(BASE_DIR, 'sw.js')) else ('', 204)


# ======================== CONTRATS ========================

@app.route('/contrats')
@permission_required('contrats')
def contrats_page():
    contracts = get_all_contracts()
    clients = get_all_clients()
    return render_template('contrats.html', page='contrats', contracts=contracts, clients=clients)

@app.route('/contrats/add', methods=['POST'])
@permission_required('contrats')
def contrats_add():
    create_contract(
        int(request.form['client_id']),
        request.form.get('reference', ''),
        request.form.get('start_date', ''),
        request.form.get('end_date', ''),
        float(request.form.get('monthly_rate', 0) or 0),
        request.form.get('description', ''),
        session['user_id']
    )
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?',
                'Contrat', 'Nouveau contrat ajouté', request.remote_addr)
    flash("Contrat ajouté", "success")
    return redirect(url_for('contrats_page'))

@app.route('/contrats/edit/<int:cid>', methods=['GET', 'POST'])
@permission_required('contrats')
def contrats_edit(cid):
    contract = get_contract_by_id(cid)
    if not contract:
        flash("Contrat non trouvé", "error")
        return redirect(url_for('contrats_page'))
    if request.method == 'POST':
        update_contract(cid,
            client_id=int(request.form['client_id']),
            reference=request.form.get('reference', ''),
            start_date=request.form.get('start_date', ''),
            end_date=request.form.get('end_date', ''),
            monthly_rate=float(request.form.get('monthly_rate', 0) or 0),
            description=request.form.get('description', ''),
            status=request.form.get('status', 'actif'))
        flash("Contrat modifié", "success")
        return redirect(url_for('contrats_page'))
    clients = get_all_clients()
    return render_template('edit_pages.html', page='contrats', contract=contract, clients=clients)

@app.route('/contrats/delete/<int:cid>')
@permission_required('contrats')
def contrats_delete(cid):
    delete_contract(cid)
    flash("Contrat supprimé", "success")
    ref = request.referrer or ''
    if ref and request.host in ref:
        return redirect(ref)
    return redirect(url_for('contrats_page'))


# ======================== COMPARAISON MENSUELLE ========================

@app.route('/comparaison')
@permission_required('traitement')
def comparaison_page():
    stats = get_client_monthly_stats()
    # Collect all months
    all_months = set()
    for client_data in stats.values():
        all_months.update(client_data.keys())
    months = sorted(all_months)
    return render_template('comparaison.html', page='comparaison', stats=stats, months=months)


# ======================== ALERTES ========================

@app.route('/alertes')
@permission_required('traitement')
def alertes_page():
    # Analyser les derniers rapports pour trouver les alertes
    jobs = get_all_jobs()
    alerts = []
    
    for job in jobs:
        if not job.get('job_id'):
            continue
        # Charger les données du rapport
        files_dir = os.path.join(app.config['FILES_FOLDER'], job['job_id'])
        xlsx_path = None
        if os.path.isdir(files_dir):
            for f in os.listdir(files_dir):
                if f.endswith('.xlsx'):
                    xlsx_path = os.path.join(files_dir, f)
                    break
        
        if not xlsx_path or not os.path.exists(xlsx_path):
            continue
        
        try:
            emps, _ = extract_from_excel(xlsx_path)
            from rapport_core import calc_employee_stats
            for emp in emps:
                enriched, stats = calc_employee_stats(emp)
                # Alertes retards excessifs (>5 jours)
                if stats['days_late'] >= 5:
                    alerts.append({
                        'type': 'retard',
                        'severity': 'high' if stats['days_late'] >= 10 else 'medium',
                        'employee': emp['name'],
                        'client': job['client_name'],
                        'detail': f"{stats['days_late']} jours de retard",
                        'period': job.get('period', ''),
                        'job_id': job['job_id']
                    })
                # Alertes absences (>3 jours)
                if stats['days_absent'] >= 3:
                    alerts.append({
                        'type': 'absence',
                        'severity': 'high' if stats['days_absent'] >= 5 else 'medium',
                        'employee': emp['name'],
                        'client': job['client_name'],
                        'detail': f"{stats['days_absent']} jours d'absence",
                        'period': job.get('period', ''),
                        'job_id': job['job_id']
                    })
        except:
            continue
    
    # Trier: high en premier
    alerts.sort(key=lambda a: (0 if a['severity'] == 'high' else 1, a['client']))
    return render_template('alertes.html', page='alertes', alerts=alerts)


# ======================== EMPLOI DU TEMPS & ANOMALIES ========================

@app.route('/emploi-du-temps')
@permission_required('traitement')
def schedule_page():
    conn = _gdb()
    schedules = [dict(r) for r in conn.execute("SELECT * FROM schedules ORDER BY employee_name, day_of_week").fetchall()]
    emp_schedules = {}
    for s in schedules:
        name = s['employee_name']
        if name not in emp_schedules: emp_schedules[name] = {}
        emp_schedules[name][s['day_of_week']] = s
    
    anomalies = [dict(r) for r in conn.execute("""SELECT * FROM presence_anomalies 
        ORDER BY date DESC LIMIT 100""").fetchall()]
    conn.close()
    
    # Collect employee names from DB (saved when files are processed)
    from models import get_known_employees
    known = get_known_employees()
    all_employees = {e['name']: e.get('service','') for e in known}
    for s in schedules:
        if s['employee_name'] not in all_employees:
            all_employees[s['employee_name']] = ''
    
    # Group employees by service
    by_service = {}
    for name, svc in sorted(all_employees.items()):
        svc = svc or 'Non défini'
        if svc not in by_service: by_service[svc] = []
        by_service[svc].append(name)
    
    days = {0:'Lundi', 1:'Mardi', 2:'Mercredi', 3:'Jeudi', 4:'Vendredi', 5:'Samedi', 6:'Dimanche'}
    return render_template('extra_pages.html', page='schedule', emp_schedules=emp_schedules,
        days=days, anomalies=anomalies, employees=sorted(all_employees.keys()),
        by_service=by_service)

@app.route('/emploi-du-temps/add', methods=['POST'])
@permission_required('traitement')
def schedule_add():
    names = request.form.getlist('employee_names')  # Multi-select
    if not names:
        single = request.form.get('employee_name', '').strip()
        if single: names = [single]
    
    start = request.form.get('start_time', '08:00')
    end = request.form.get('end_time', '17:00')
    bstart = request.form.get('break_start', '12:00')
    bend = request.form.get('break_end', '13:00')
    stype = request.form.get('schedule_type', 'standard')
    
    days = request.form.getlist('days')
    if not days:
        days = ['0','1','2','3','4']
    
    conn = _gdb()
    for name in names:
        name = name.strip()
        if not name: continue
        conn.execute("DELETE FROM schedules WHERE employee_name=?", (name,))
        for d in days:
            conn.execute("""INSERT INTO schedules (employee_name, day_of_week, start_time, end_time, 
                break_start, break_end, schedule_type) VALUES (?,?,?,?,?,?,?)""",
                (name, int(d), start, end, bstart, bend, stype))
    conn.commit(); conn.close()
    flash(f"Emploi du temps enregistré pour {len(names)} personne(s)", "success")
    return redirect('/emploi-du-temps')

@app.route('/emploi-du-temps/delete/<path:name>')
@permission_required('traitement')
def schedule_delete(name):
    from urllib.parse import unquote
    name = unquote(name)
    conn = _gdb()
    conn.execute("DELETE FROM schedules WHERE employee_name=?", (name,))
    conn.commit(); conn.close()
    flash(f"Emploi du temps de {name} supprimé", "success")
    return redirect('/emploi-du-temps')

@app.route('/emploi-du-temps/detect-anomalies', methods=['POST'])
@permission_required('traitement')
def schedule_detect():
    """Compare les fichiers de présence avec les emplois du temps définis."""
    
    files_dir = app.config.get('FILES_FOLDER', 'files')
    xlsx_path = None
    
    # Find latest presence file
    jobs = get_all_jobs()
    for job in reversed(jobs):
        jdir = os.path.join(files_dir, job['job_id'])
        if os.path.isdir(jdir):
            for f in os.listdir(jdir):
                if f.endswith('.xlsx'):
                    xlsx_path = os.path.join(jdir, f)
                    break
            if xlsx_path: break
    
    if not xlsx_path:
        flash("Aucun fichier de présence trouvé. Traitez d'abord un fichier.", "error")
        return redirect('/emploi-du-temps')
    
    # Load our defined schedules
    conn = _gdb()
    scheds = [dict(r) for r in conn.execute("SELECT * FROM schedules").fetchall()]
    sched_map = {}
    for s in scheds:
        name = s['employee_name']
        if name not in sched_map: sched_map[name] = {}
        sched_map[name][s['day_of_week']] = s
    
    # Parse presence file
    try:
        emps, meta = extract_from_excel(xlsx_path)
    except Exception as e:
        flash(f"Erreur lecture fichier: {e}", "error")
        conn.close()
        return redirect('/emploi-du-temps')
    
    # Clear old anomalies
    conn.execute("DELETE FROM presence_anomalies")
    
    def time_to_min(t):
        """Convertit HH:MM en minutes depuis minuit."""
        if not t or len(t) < 4: return None
        try:
            parts = t.replace('h',':').split(':')
            return int(parts[0]) * 60 + int(parts[1])
        except: return None
    
    anomaly_count = 0
    tolerance = 15  # minutes de tolérance
    
    for emp in emps:
        name = emp.get('name', '').strip()
        if name not in sched_map: continue
        
        for rec in emp.get('records', []):
            date = rec.get('date', '')
            if not date: continue
            
            try:
                from datetime import datetime as dt2
                d = dt2.strptime(date[:10], '%Y-%m-%d')
                dow = d.weekday()
            except: continue
            
            if dow not in sched_map[name]: continue
            sched = sched_map[name][dow]
            
            actual_arr = rec.get('arrival', '').strip()
            actual_dep = rec.get('departure', '').strip()
            exp_start = sched['start_time']
            exp_end = sched['end_time']
            
            exp_s = time_to_min(exp_start)
            exp_e = time_to_min(exp_end)
            act_s = time_to_min(actual_arr)
            act_e = time_to_min(actual_dep)
            
            anomalies = []
            
            if not act_s and not act_e:
                # Pas de pointage du tout
                anomalies.append('absence')
            elif act_s is not None and exp_s is not None:
                # Retard : arrivée > prévu + tolérance
                if act_s > exp_s + tolerance:
                    anomalies.append('retard')
                
                # Horaire complètement décalé : arrivée APRÈS l'heure de fin prévue
                # Ex: prévu 07:00-17:00 mais pointe à 17:00-21:00
                if exp_e is not None and act_s >= exp_e:
                    anomalies.append('horaire_decale')
                
                # Départ anticipé : départ > 30min avant la fin prévue
                if act_e is not None and exp_e is not None and act_e < exp_e - 30:
                    anomalies.append('depart_anticipe')
                
                # Arrivée très en avance (plus de 2h avant l'heure prévue)
                if act_s < exp_s - 120:
                    anomalies.append('horaire_decale')
            
            if anomalies:
                anomaly_type = '+'.join(anomalies)
                conn.execute("""INSERT INTO presence_anomalies 
                    (employee_name, date, expected_start, expected_end, actual_start, actual_end, anomaly_type)
                    VALUES (?,?,?,?,?,?,?)""",
                    (name, date, exp_start, exp_end, actual_arr or '', actual_dep or '', anomaly_type))
                anomaly_count += 1
    
    conn.commit(); conn.close()
    flash(f"{anomaly_count} anomalie(s) détectée(s) sur {len(emps)} employés", "info")
    return redirect('/emploi-du-temps')

@app.route('/emploi-du-temps/correct/<int:aid>', methods=['POST'])
@permission_required('traitement')
def schedule_correct(aid):
    conn = _gdb()
    conn.execute("""UPDATE presence_anomalies SET status='corrigee', 
        corrected_start=?, corrected_end=?, notes=? WHERE id=?""",
        (request.form.get('corrected_start',''), request.form.get('corrected_end',''),
         request.form.get('notes',''), aid))
    conn.commit(); conn.close()
    flash("Anomalie corrigée", "success")
    return redirect('/emploi-du-temps')


# ======================== CALCUL D'HEURES DPCI ========================

@app.route('/dpci')
@permission_required('traitement')
def dpci_page():
    return render_template('dpci.html', page='dpci')

@app.route('/dpci/preview', methods=['POST'])
@login_required
def dpci_preview():
    if 'excel_file' not in request.files:
        return jsonify({"error": "Fichier requis"}), 400
    f = request.files['excel_file']
    if not f.filename:
        return jsonify({"error": "Fichier vide"}), 400
    
    import tempfile
    tmp = os.path.join(tempfile.gettempdir(), f'dpci_{uuid.uuid4().hex[:8]}.xlsx')
    f.save(tmp)
    try:
        from rapport_core import parse_dpci_excel
        emps, period = parse_dpci_excel(tmp)
        
        # Save known employees
        from models import save_known_employees
        save_known_employees([e['name'] for e in emps])
        
        # Extract client from department (first part before >)
        client = ''
        if emps:
            dept = emps[0].get('department', '')
            if '>' in dept:
                client = dept.split('>')[1].strip()
            else:
                client = dept
        
        return jsonify({
            "count": len(emps),
            "period": period,
            "client": client,
            "employees": [{"name": e['name'], "id": e['id'], "dept": e['department'], "days": len(e['records'])} for e in emps]
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if os.path.exists(tmp): os.remove(tmp)

@app.route('/dpci/generate', methods=['POST'])
@permission_required('traitement')
def dpci_generate():
    if 'excel_file' not in request.files:
        flash("Fichier requis", "error")
        return redirect('/dpci')
    
    f = request.files['excel_file']
    client_name = request.form.get('client_name', '').strip() or 'DPCI'
    provider_name = request.form.get('provider_name', '').strip() or 'RAMYA TECHNOLOGIE & INNOVATION'
    default_cost = float(request.form.get('default_cost', 0) or 0)
    try:
        hp = float(request.form.get('required_hours', 0) or 0)
    except:
        hp = 0
    try:
        hp_weekend = float(request.form.get('required_hours_weekend', 0) or 0)
    except:
        hp_weekend = 0
    
    try:
        employee_costs = json.loads(request.form.get('employee_costs_json', '{}'))
    except:
        employee_costs = {}
    
    # Save file
    job_id = str(uuid.uuid4())[:8]
    job_dir = os.path.join(app.config['UPLOAD_FOLDER'], f'dpci_{job_id}')
    os.makedirs(job_dir, exist_ok=True)
    
    xlsx_path = os.path.join(job_dir, secure_filename(f.filename))
    f.save(xlsx_path)
    
    try:
        from rapport_core import parse_dpci_excel, generate_dpci_pdf
        emps, period = parse_dpci_excel(xlsx_path)
        
        if not emps:
            flash("Aucun employé trouvé dans le fichier", "error")
            return redirect('/dpci')
        
        # Filter by period
        period_mode = request.form.get('period_mode', 'all')
        period_start = request.form.get('period_start', '').strip()
        period_end = request.form.get('period_end', '').strip()
        
        if period_mode == 'day' and period_start:
            period_end = period_start
        elif period_mode == 'week' and period_start:
            from datetime import datetime as dt2, timedelta
            try:
                d = dt2.strptime(period_start, '%Y-%m-%d')
                period_end = (d + timedelta(days=6)).strftime('%Y-%m-%d')
            except: pass
        
        if period_start or period_end:
            for emp in emps:
                emp['records'] = [r for r in emp['records'] 
                    if (not period_start or r['date'] >= period_start) and 
                       (not period_end or r['date'] <= period_end)]
            emps = [e for e in emps if e['records']]
        
        if not emps:
            flash("Aucun enregistrement pour la période sélectionnée", "error")
            return redirect('/dpci')
        
        # Build period string
        all_dates = sorted([r['date'] for e in emps for r in e['records']])
        if all_dates:
            period_str = f"Débuté le {all_dates[0]} au {all_dates[-1]}"
        elif period:
            parts = period.split(' - ')
            period_str = f"Débuté le {parts[0].strip()} au {parts[-1].strip()}" if len(parts) > 1 else period
        else:
            period_str = "Rapport DPCI"
        conn = _gdb()
        scheds = [dict(r) for r in conn.execute("SELECT * FROM schedules").fetchall()]
        conn.close()
        
        schedules_map = {}
        for s in scheds:
            name = s['employee_name']
            if name not in schedules_map:
                schedules_map[name] = s  # Use first found (e.g., Monday schedule)
        
        # Generate PDF
        pdf_name = f"DPCI_{client_name.replace(' ', '_')}_{job_id}.pdf"
        output_path = os.path.join(job_dir, pdf_name)
        
        user = get_user_by_id(session['user_id'])
        treated_by = user['full_name'] if user else 'Admin'
        
        generate_dpci_pdf(emps, output_path, client_name, period_str,
                         schedules_map=schedules_map, employee_costs=employee_costs,
                         default_cost=default_cost, hp=hp, hp_weekend=hp_weekend,
                         provider_name=provider_name, treated_by=treated_by, period_mode=period_mode,
                         rest_days=request.form.getlist('rest_days'))
        
        if not os.path.exists(output_path):
            flash("Erreur de génération PDF", "error")
            return redirect('/dpci')
        
        # Save to files folder
        files_dir = os.path.join(app.config['FILES_FOLDER'], f'dpci_{job_id}')
        os.makedirs(files_dir, exist_ok=True)
        shutil.copy2(output_path, os.path.join(files_dir, pdf_name))
        shutil.copy2(xlsx_path, os.path.join(files_dir, os.path.basename(xlsx_path)))
        
        # Log
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'DPCI', f"Rapport {client_name} — {len(emps)} employés", request.remote_addr)
        
        flash(f"Rapport DPCI généré — {len(emps)} employés", "success")
        return send_file(output_path, as_attachment=True, download_name=pdf_name)
    
    except Exception as e:
        flash(f"Erreur: {e}", "error")
        return redirect('/dpci')


# ======================== ENVOI EMAIL ========================

@app.route('/fichiers/email/<job_id>', methods=['GET', 'POST'])
@permission_required('envoyer')
def fichiers_email(job_id):
    job = get_job_by_id(job_id)
    if not job:
        flash("Rapport non trouvé", "error")
        return redirect(url_for('fichiers'))
    
    # Pré-remplir avec les infos client
    client = get_client_by_id(job['client_id']) if job.get('client_id') else None
    default_email = client['email'] if client and client.get('email') else ''
    
    if request.method == 'POST':
        to_email = request.form.get('to_email', '').strip()
        subject = request.form.get('subject', '').strip()
        body = request.form.get('body', '').strip()
        smtp_host = request.form.get('smtp_host', '').strip()
        smtp_port = int(request.form.get('smtp_port', 587))
        smtp_user = request.form.get('smtp_user', '').strip()
        smtp_pass = request.form.get('smtp_pass', '').strip()
        
        # Sauvegarder les paramètres SMTP
        if smtp_host and smtp_user:
            save_smtp_settings(session['user_id'], smtp_host, smtp_port, smtp_user, smtp_pass)
        
        if not all([to_email, subject, smtp_host, smtp_user, smtp_pass]):
            flash("Tous les champs SMTP sont obligatoires", "error")
            smtp = get_smtp_settings(session['user_id'])
            return render_template('email_send.html', page='fichiers', job=job,
                                 default_email=default_email, smtp=smtp)
        
        # Préparer le fichier PDF
        files_dir = os.path.join(app.config['FILES_FOLDER'], secure_filename(job_id))
        pdf_path = None
        if os.path.isdir(files_dir):
            for f in os.listdir(files_dir):
                if f.endswith('.pdf'):
                    pdf_path = os.path.join(files_dir, f)
                    break
        
        if not pdf_path:
            flash("Fichier PDF non trouvé", "error")
            return redirect(url_for('fichiers'))
        
        try:
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            # Joindre le PDF
            with open(pdf_path, 'rb') as f:
                part = MIMEBase('application', 'pdf')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(pdf_path)}"')
                msg.attach(part)
            
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
            server.quit()
            
            # Marquer comme envoyé
            mark_job_sent(job_id, session['user_id'])
            user = get_user_by_id(session['user_id'])
            log_activity(session['user_id'], user['full_name'] if user else '?',
                        'Email', f"Rapport envoyé par email à {to_email}", request.remote_addr)
            
            flash(f"Email envoyé avec succès à {to_email}", "success")
            return redirect(url_for('fichiers'))
        
        except Exception as e:
            flash(f"Erreur d'envoi : {str(e)}", "error")
    
    smtp = get_smtp_settings(session['user_id'])
    return render_template('email_send.html', page='fichiers', job=job, default_email=default_email, smtp=smtp)


# ======================== COMPTABILITÉ ========================

@app.route('/comptabilite')
@permission_required('comptabilite')
def comptabilite_page():
    tab = request.args.get('tab', 'a_envoyer')
    inv_stats = get_invoice_stats()
    invoices = get_invoices_by_status(tab) if tab != 'all' else get_all_invoices()
    
    # Dashboard data
    conn = _gdb()
    # Monthly revenue (paid invoices)
    monthly = [dict(r) for r in conn.execute("""
        SELECT strftime('%Y-%m', created_at) as month, SUM(amount) as total 
        FROM invoices WHERE status='payee' GROUP BY month ORDER BY month DESC LIMIT 12
    """).fetchall()]
    monthly.reverse()
    
    # Monthly expenses (from pieces_caisse)
    expenses = [dict(r) for r in conn.execute("""
        SELECT strftime('%Y-%m', date) as month, SUM(amount) as total 
        FROM pieces_caisse GROUP BY month ORDER BY month DESC LIMIT 12
    """).fetchall()]
    expenses.reverse()
    
    # Totals
    total_revenue = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status='payee'").fetchone()[0]
    total_pending = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status IN ('envoyee','en_attente_paiement')").fetchone()[0]
    total_expenses = conn.execute("SELECT COALESCE(SUM(amount),0) FROM pieces_caisse").fetchone()[0]
    
    # Expense by category
    exp_cats = [dict(r) for r in conn.execute("""
        SELECT category, SUM(amount) as total FROM pieces_caisse GROUP BY category ORDER BY total DESC LIMIT 8
    """).fetchall()]
    
    # Recent devis (for comptabilite to see new devis from CRM)
    recent_devis = [dict(r) for r in conn.execute("""
        SELECT * FROM devis ORDER BY created_at DESC LIMIT 5
    """).fetchall()]
    
    # Créances (unpaid invoices)
    creances_total = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status IN ('envoyee','en_attente_paiement')").fetchone()[0]
    creances_count = conn.execute("SELECT COUNT(*) FROM invoices WHERE status IN ('envoyee','en_attente_paiement')").fetchone()[0]
    
    # Dettes fournisseurs
    try: dettes_total = conn.execute("SELECT COALESCE(SUM(amount),0) FROM pieces_caisse WHERE supplier != ''").fetchone()[0]
    except: dettes_total = 0
    
    # Trésorerie
    try: tresorerie = conn.execute("SELECT COALESCE(SUM(solde),0) FROM bank_accounts").fetchone()[0]
    except: tresorerie = 0
    
    # Ecritures count
    try: ecritures_count = conn.execute("SELECT COUNT(*) FROM ecritures_comptables WHERE strftime('%Y-%m',date)=?", (datetime.now().strftime('%Y-%m'),)).fetchone()[0]
    except: ecritures_count = 0
    
    # Paie pending
    try: paie_pending = conn.execute("SELECT COUNT(*) FROM payslips WHERE status='en_attente_compta'").fetchone()[0]
    except: paie_pending = 0
    
    conn.close()
    
    chart_data = {
        'months': [m['month'] for m in monthly],
        'revenue': [m['total'] for m in monthly],
        'exp_months': [e['month'] for e in expenses],
        'exp_totals': [e['total'] for e in expenses],
        'exp_cats': [c['category'] for c in exp_cats],
        'exp_cat_vals': [c['total'] for c in exp_cats],
        'total_revenue': total_revenue,
        'total_pending': total_pending,
        'total_expenses': total_expenses,
        'profit': total_revenue - total_expenses,
        'creances_total': creances_total, 'creances_count': creances_count,
        'dettes_total': dettes_total, 'tresorerie': tresorerie,
        'ecritures_count': ecritures_count, 'paie_pending': paie_pending,
    }
    
    return render_template('comptabilite.html', page='comptabilite', tab=tab,
                          invoices=invoices, inv_stats=inv_stats, chart=chart_data, recent_devis=recent_devis)


@app.route('/comptabilite/factures')
@permission_required('comptabilite')
def compta_factures():
    return redirect('/comptabilite?tab=factures')

@app.route('/comptabilite/creances')
@permission_required('comptabilite')
def compta_creances():
    conn = _gdb()
    creances = [dict(r) for r in conn.execute(
        "SELECT i.*, c.name as client_name, c.contact_name FROM invoices i LEFT JOIN clients c ON i.client_id=c.id WHERE i.status IN ('envoyee','en_attente_paiement') ORDER BY i.created_at DESC").fetchall()]
    total = sum(c.get('amount',0) or 0 for c in creances)
    conn.close()
    return render_template('extra_pages.html', page='creances', creances=creances, total=total)

@app.route('/comptabilite/dettes')
@permission_required('comptabilite')
def compta_dettes():
    conn = _gdb()
    dettes = []
    try:
        dettes = [dict(r) for r in conn.execute(
            "SELECT * FROM pieces_caisse WHERE category='fournisseur' OR supplier != '' ORDER BY date DESC").fetchall()]
    except: pass
    # Also get unpaid purchase orders
    commandes = []
    try:
        commandes = [dict(r) for r in conn.execute(
            "SELECT * FROM achats_commandes WHERE status NOT IN ('payee','annulee') ORDER BY created_at DESC").fetchall()]
    except: pass
    total_dettes = sum(d.get('amount',0) or 0 for d in dettes)
    conn.close()
    return render_template('extra_pages.html', page='dettes', dettes=dettes, commandes=commandes, total_dettes=total_dettes)

@app.route('/comptabilite/ecritures')
@permission_required('comptabilite')
def compta_ecritures():
    conn = _gdb()
    period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    ecritures = [dict(r) for r in conn.execute(
        "SELECT * FROM ecritures_comptables WHERE strftime('%Y-%m',date)=? ORDER BY date DESC, id DESC", (period,)).fetchall()]
    plan = [dict(r) for r in conn.execute("SELECT * FROM plan_comptable ORDER BY numero").fetchall()]
    # Build a quick lookup: numero -> libellé
    plan_map = {p['numero']: p['libelle'] for p in plan}
    for e in ecritures:
        e['debit_label'] = plan_map.get(e.get('compte_debit',''), '')
        e['credit_label'] = plan_map.get(e.get('compte_credit',''), '')
    total_debit = sum(e.get('montant',0) or 0 for e in ecritures)
    # Même montant en débit et crédit (partie double)
    total_credit = total_debit
    conn.close()
    return render_template('extra_pages.html', page='ecritures', ecritures=ecritures, plan=plan, period=period,
                          total_debit=total_debit, total_credit=total_credit)


@app.route('/comptabilite/grand-livre')
@permission_required('comptabilite')
def compta_grand_livre():
    """Grand Livre : écritures groupées par compte avec solde progressif."""
    conn = _gdb()
    year = request.args.get('year', datetime.now().strftime('%Y'))
    account_filter = request.args.get('account', '').strip()
    plan = [dict(r) for r in conn.execute("SELECT * FROM plan_comptable ORDER BY numero").fetchall()]
    plan_map = {p['numero']: p['libelle'] for p in plan}
    
    # Récupère tous les comptes utilisés dans les écritures de l'année
    used_accounts = set()
    rows = conn.execute("""SELECT DISTINCT compte_debit, compte_credit FROM ecritures_comptables
        WHERE strftime('%Y',date)=?""", (year,)).fetchall()
    for r in rows:
        if r['compte_debit']: used_accounts.add(r['compte_debit'])
        if r['compte_credit']: used_accounts.add(r['compte_credit'])
    
    # Filter if requested
    if account_filter:
        accounts = [a for a in used_accounts if a == account_filter or a.startswith(account_filter)]
    else:
        accounts = sorted(used_accounts)
    
    grand_livre = []
    for acc in accounts:
        # Tous les mouvements débit sur ce compte
        movs_debit = [dict(r) for r in conn.execute(
            "SELECT *, 'debit' as sens FROM ecritures_comptables WHERE compte_debit=? AND strftime('%Y',date)=? ORDER BY date, id",
            (acc, year)).fetchall()]
        movs_credit = [dict(r) for r in conn.execute(
            "SELECT *, 'credit' as sens FROM ecritures_comptables WHERE compte_credit=? AND strftime('%Y',date)=? ORDER BY date, id",
            (acc, year)).fetchall()]
        all_movs = sorted(movs_debit + movs_credit, key=lambda m: (m['date'], m['id']))
        
        # Calcul du solde progressif
        solde = 0.0
        for m in all_movs:
            montant = m.get('montant', 0) or 0
            if m['sens'] == 'debit':
                solde += montant
                m['debit_m'] = montant; m['credit_m'] = 0
            else:
                solde -= montant
                m['debit_m'] = 0; m['credit_m'] = montant
            m['solde_progressif'] = solde
        
        total_d = sum(m['debit_m'] for m in all_movs)
        total_c = sum(m['credit_m'] for m in all_movs)
        grand_livre.append({
            'compte': acc, 'libelle': plan_map.get(acc, '(non défini)'),
            'mouvements': all_movs,
            'total_debit': total_d, 'total_credit': total_c,
            'solde_final': total_d - total_c,
        })
    
    conn.close()
    return render_template('extra_pages.html', page='grand_livre', grand_livre=grand_livre,
                           plan=plan, year=year, account_filter=account_filter)


@app.route('/comptabilite/balance')
@permission_required('comptabilite')
def compta_balance():
    """Balance comptable : synthèse de tous les comptes avec totaux débit/crédit et solde."""
    conn = _gdb()
    year = request.args.get('year', datetime.now().strftime('%Y'))
    plan = [dict(r) for r in conn.execute("SELECT * FROM plan_comptable ORDER BY numero").fetchall()]
    plan_map = {p['numero']: p for p in plan}
    
    # Accumuler débit et crédit par compte
    balance_map = {}  # compte -> {debit, credit, libelle, classe}
    rows = conn.execute("""SELECT compte_debit, compte_credit, montant FROM ecritures_comptables
        WHERE strftime('%Y',date)=?""", (year,)).fetchall()
    for r in rows:
        amt = r['montant'] or 0
        if r['compte_debit']:
            balance_map.setdefault(r['compte_debit'], {'debit':0,'credit':0})
            balance_map[r['compte_debit']]['debit'] += amt
        if r['compte_credit']:
            balance_map.setdefault(r['compte_credit'], {'debit':0,'credit':0})
            balance_map[r['compte_credit']]['credit'] += amt
    
    balance_list = []
    for compte, totals in sorted(balance_map.items()):
        p = plan_map.get(compte, {})
        solde = totals['debit'] - totals['credit']
        balance_list.append({
            'compte': compte,
            'libelle': p.get('libelle', '(non défini)') if p else '(non défini)',
            'classe': p.get('classe', '') if p else '',
            'categorie': p.get('categorie', '') if p else '',
            'debit': totals['debit'],
            'credit': totals['credit'],
            'solde_debit': solde if solde > 0 else 0,
            'solde_credit': -solde if solde < 0 else 0,
        })
    
    total_debit = sum(b['debit'] for b in balance_list)
    total_credit = sum(b['credit'] for b in balance_list)
    total_sd = sum(b['solde_debit'] for b in balance_list)
    total_sc = sum(b['solde_credit'] for b in balance_list)
    
    conn.close()
    return render_template('extra_pages.html', page='balance', balance=balance_list,
                           year=year,
                           total_debit=total_debit, total_credit=total_credit,
                           total_sd=total_sd, total_sc=total_sc)

@app.route('/comptabilite/status/<int:inv_id>/<status>')
@permission_required('comptabilite')
def comptabilite_status(inv_id, status):
    if status in ('envoyee', 'en_attente_paiement', 'payee', 'a_envoyer'):
        # Auto-generate écriture comptable on payment
        if status == 'payee':
            try:
                conn = _gdb()
                inv2 = conn.execute("SELECT * FROM invoices WHERE id=?", (inv_id,)).fetchone()
                if inv2:
                    auto_ecriture(conn, datetime.now().strftime('%Y-%m-%d'),
                        f"Paiement facture {inv2['reference']}", '521', '411',
                        inv2['amount'] or inv2['total_ttc'] or 0, inv2['reference'])
                conn.commit(); conn.close()
            except Exception: pass
        update_invoice_status(inv_id, status, session.get('user_id'))
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Facture', f"Facture #{inv_id} → {status}", request.remote_addr)
        flash(f"Statut mis à jour : {status}", "success")
    return redirect(url_for('comptabilite_page'))

@app.route('/comptabilite/facture/new', methods=['GET', 'POST'])
@permission_required('comptabilite_edit')
def invoice_new():
    if request.method == 'POST':
        ref = f"FAC-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        amount = float(request.form.get('amount',0) or 0)
        total_ht = float(request.form.get('total_ht',0) or 0)
        tva_active = 1 if request.form.get('tva_active') in ('1','true','on','yes') else 0
        tva_rate = float(request.form.get('tva_rate', 18) or 18)
        # If TVA active, (re)compute tva amount from HT
        if tva_active and total_ht:
            tva = round(total_ht * tva_rate / 100, 0)
        else:
            tva = float(request.form.get('tva',0) or 0)
        total_ttc = float(request.form.get('total_ttc',0) or 0)
        # Fallback: if totals are empty, use amount as the TTC
        if not total_ttc and amount: total_ttc = amount
        if not total_ht and total_ttc: total_ht = total_ttc - tva
        # Rédacteur
        cur_u = get_user_by_id(session['user_id'])
        redacteur_name = (cur_u['full_name'] if cur_u else '') or 'Utilisateur'
        redacteur_dt = datetime.now().strftime('%d/%m/%Y à %H:%M')
        conn = _gdb()
        # S'assurer que la colonne created_by existe
        try: conn.execute("ALTER TABLE invoices ADD COLUMN created_by INTEGER")
        except: pass
        try: conn.execute("ALTER TABLE invoices ADD COLUMN contact_commercial TEXT")
        except: pass
        conn.execute("""INSERT INTO invoices (reference, client_name, client_id, amount, objet, 
            description, due_date, payment_method, status, total_ht, tva, total_ttc, notes,
            created_by, contact_commercial)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ref, request.form.get('client_name',''), int(request.form.get('client_id',0) or 0) or None,
             amount or total_ttc, request.form.get('objet',''),
             request.form.get('description',''), request.form.get('due_date',''),
             request.form.get('payment_method',''), 'a_envoyer',
             total_ht, tva, total_ttc, request.form.get('notes',''),
             session['user_id'], redacteur_name))
        inv_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        try:
            conn.execute("UPDATE invoices SET tva_active=?, tva_rate=?, tva_amount=?, redacteur=?, redacteur_date=? WHERE id=?",
                         (tva_active, tva_rate, tva, redacteur_name, redacteur_dt, inv_id))
        except: pass
        # Écriture auto : 411 (clients) DÉBIT / 701 (ventes) CRÉDIT HT — 443 (TVA) CRÉDIT si TVA
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            if total_ht > 0:
                auto_ecriture(conn, today, f"Facture {ref} — {request.form.get('client_name','')[:30]}",
                              '411', '701', total_ht, ref)
            if tva > 0:
                auto_ecriture(conn, today, f"TVA collectée sur {ref}", '411', '443', tva, ref)
        except: pass
        conn.commit(); conn.close()
        flash(f"Facture {ref} créée — écriture auto : 411 → 701 ({total_ht:,.0f} F HT){'  +TVA ' + f'{tva:,.0f} F' if tva > 0 else ''}", "success")
        return redirect(url_for('comptabilite_page'))
    clients = get_all_clients()
    return render_template('invoice_new.html', page='comptabilite', clients=clients)

@app.route('/devis/convert/<int:did>')
@permission_required('convertir_devis')
def devis_to_invoice(did):
    from models import db_get_by_id
    d = db_get_by_id('devis', did)
    if not d:
        flash("Devis non trouvé", "error"); return redirect('/devis')
    ref = f"FAC-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    conn = _gdb()
    conn.execute("""INSERT INTO invoices (reference, client_name, client_id, amount, objet,
        total_ht, tva, total_ttc, items_json, devis_id, status, notes)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ref, d.get('client_name',''), d.get('client_id'), d.get('total_ttc',0),
         d.get('objet',''), d.get('total_ht',0), d.get('total_ht',0)*0.18 if d.get('total_ht') else 0,
         d.get('total_ttc',0), d.get('items_json',''), did, 'a_envoyer',
         f"Convertie depuis devis {d.get('reference','')}"))
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?',
                'Facture', f"Devis {d.get('reference','')} converti en facture {ref}", request.remote_addr)
    flash(f"Devis {d.get('reference','')} converti en facture {ref}", "success")
    return redirect(url_for('comptabilite_page'))

@app.route('/comptabilite/facture/view/<int:fid>')
@permission_required('comptabilite')
def invoice_view(fid):
    conn = _gdb()
    inv = conn.execute("SELECT * FROM invoices WHERE id=?", (fid,)).fetchone()
    conn.close()
    if not inv: flash("Facture non trouvée","error"); return redirect(url_for('comptabilite_page'))
    inv = dict(inv)
    inv['items'] = json.loads(inv.get('items_json','[]') or '[]')
    return render_template('invoice_view.html', page='comptabilite', inv=inv, inv_items=inv['items'])


def _build_invoice_pdf_data(inv_row, conn):
    """Construit le dict attendu par generate_devis_pdf à partir d'une ligne invoices."""
    inv = dict(inv_row)
    # Contact commercial — le rédacteur est le créateur
    redacteur = inv.get('redacteur') or ''
    redacteur_date = inv.get('redacteur_date') or ''
    if not redacteur and inv.get('created_by'):
        u = conn.execute("SELECT full_name FROM users WHERE id=?", (inv['created_by'],)).fetchone()
        if u: redacteur = u['full_name']
    if not redacteur_date:
        ca = inv.get('created_at', '') or ''
        if ca:
            try:
                from datetime import datetime as _dt
                dt = _dt.strptime(ca[:19], '%Y-%m-%d %H:%M:%S')
                redacteur_date = dt.strftime('%d/%m/%Y à %H:%M')
            except: redacteur_date = ca[:16]
    # Client code
    client_code = inv.get('client_code') or ''
    if not client_code and inv.get('client_id'):
        c = conn.execute("SELECT client_code FROM clients WHERE id=?", (inv['client_id'],)).fetchone()
        if c: client_code = c['client_code'] or ''
    # Items
    items = []
    try: items = json.loads(inv.get('items_json','[]') or '[]')
    except: items = []
    # Si aucun item, créer un item unique à partir de l'objet + montant
    if not items and inv.get('total_ht'):
        items = [{'num': 1, 'designation': inv.get('objet','Prestation'),
                  'detail': inv.get('description','') or '',
                  'qty': 1, 'prix': inv.get('total_ht', 0), 'remise': 0}]
    # Commercial
    commercial = inv.get('contact_commercial') or ''
    # Safe numeric casts (BDD peut renvoyer str ou None)
    def _f(v):
        try: return float(v) if v not in (None, '') else 0.0
        except: return 0.0
    return {
        'doc_type': 'facture',
        'reference': inv.get('reference', ''),
        'date': (inv.get('created_at','') or '')[:10],
        'contact_commercial': commercial,
        'client_name': inv.get('client_name', ''),
        'client_code': client_code,
        'objet': inv.get('objet') or inv.get('description','') or '',
        'items_json': json.dumps(items),
        'total_ht': _f(inv.get('total_ht')),
        'petites_fournitures': 0,
        'total_ttc': _f(inv.get('total_ttc')) or _f(inv.get('amount')),
        'main_oeuvre': 0,
        'remise': 0,
        'tva_active': bool(inv.get('tva_active')) or _f(inv.get('tva')) > 0,
        'tva_rate': _f(inv.get('tva_rate')) or 18,
        'tva_amount': _f(inv.get('tva')),
        'notes': inv.get('notes', ''),
        'redacteur': redacteur,
        'redacteur_date': redacteur_date,
    }


@app.route('/comptabilite/facture/<int:fid>/pdf')
@permission_required('comptabilite')
def invoice_pdf(fid):
    """Télécharge le PDF de la facture (même format que devis)."""
    conn = _gdb()
    inv_row = conn.execute("SELECT * FROM invoices WHERE id=?", (fid,)).fetchone()
    if not inv_row:
        conn.close()
        flash("Facture introuvable", "error")
        return redirect(url_for('comptabilite_page'))
    data = _build_invoice_pdf_data(inv_row, conn)
    conn.close()
    
    from rapport_core import generate_devis_pdf
    export_dir = os.path.join(BASE_DIR, 'exports')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"{data['reference']}.pdf")
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"]
                   if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_devis_pdf(data, output, logo_path=logo_r)
    return send_file(output, as_attachment=True,
                     download_name=f"{data['reference']}.pdf")


@app.route('/comptabilite/facture/<int:fid>/preview')
@permission_required('comptabilite')
def invoice_preview(fid):
    """Prévisualise le PDF de la facture dans le navigateur (sans téléchargement)."""
    conn = _gdb()
    inv_row = conn.execute("SELECT * FROM invoices WHERE id=?", (fid,)).fetchone()
    if not inv_row:
        conn.close()
        flash("Facture introuvable", "error")
        return redirect(url_for('comptabilite_page'))
    data = _build_invoice_pdf_data(inv_row, conn)
    conn.close()
    
    from rapport_core import generate_devis_pdf
    export_dir = os.path.join(BASE_DIR, 'exports')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"{data['reference']}_preview.pdf")
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"]
                   if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_devis_pdf(data, output, logo_path=logo_r)
    # Inline display
    from flask import Response
    with open(output, 'rb') as f:
        pdf_bytes = f.read()
    return Response(pdf_bytes, mimetype='application/pdf', headers={
        'Content-Disposition': f'inline; filename="{data["reference"]}.pdf"'
    })


@app.route('/comptabilite/facture/edit/<int:fid>', methods=['GET', 'POST'])
@permission_required('comptabilite_edit')
def invoice_edit(fid):
    conn = _gdb()
    inv = conn.execute("SELECT * FROM invoices WHERE id=?", (fid,)).fetchone()
    if not inv: flash("Non trouvée","error"); return redirect(url_for('comptabilite_page'))
    if request.method == 'POST':
        conn.execute("""UPDATE invoices SET client_name=?, objet=?, amount=?, total_ht=?, tva=?, total_ttc=?,
            description=?, due_date=?, payment_method=?, notes=? WHERE id=?""",
            (request.form.get('client_name',''), request.form.get('objet',''),
             float(request.form.get('amount',0) or 0), float(request.form.get('total_ht',0) or 0),
             float(request.form.get('tva',0) or 0), float(request.form.get('total_ttc',0) or 0),
             request.form.get('description',''), request.form.get('due_date',''),
             request.form.get('payment_method',''), request.form.get('notes',''), fid))
        conn.commit(); conn.close()
        flash("Facture modifiée","success"); return redirect(f'/comptabilite/facture/view/{fid}')
    conn.close()
    clients = get_all_clients()
    return render_template('invoice_edit.html', page='comptabilite', inv=dict(inv), clients=clients)

# ======================== CAISSE (Entrées + Sorties) ========================

@app.route('/caisse-entree/add', methods=['POST'])
@permission_required('caisse_sortie')
def caisse_entree_add():
    conn = _gdb()
    ref = f"CE-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    montant = float(request.form.get('montant', 0) or 0)
    date_op = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    source = request.form.get('source', '')
    description = request.form.get('description', '')
    pay_method = request.form.get('payment_method', '')
    caisse_id_raw = request.form.get('caisse_id', '').strip()
    caisse_id = int(caisse_id_raw) if caisse_id_raw.isdigit() else None
    
    try:
        conn.execute("""INSERT INTO caisse_entrees (reference, date, source, montant, description, payment_method, caisse_id, created_by)
            VALUES (?,?,?,?,?,?,?,?)""",
            (ref, date_op, source, montant, description, pay_method, caisse_id, session['user_id']))
    except:
        # Fallback if caisse_id column not yet added
        conn.execute("""INSERT INTO caisse_entrees (reference, date, source, montant, description, payment_method, created_by)
            VALUES (?,?,?,?,?,?,?)""",
            (ref, date_op, source, montant, description, pay_method, session['user_id']))
    
    # Mettre à jour la caisse ciblée (opération + solde)
    caisse_nom = ''
    if caisse_id:
        c = conn.execute("SELECT name FROM caisses WHERE id=?", (caisse_id,)).fetchone()
        caisse_nom = c['name'] if c else ''
        try:
            conn.execute("""INSERT INTO caisse_operations (caisse_id, type, amount, description, reference, category, created_by)
                VALUES (?,?,?,?,?,?,?)""",
                (caisse_id, 'entree', montant,
                 f"Entrée {ref} — {source}" + (f" : {description[:30]}" if description else ""),
                 ref, 'entree', session['user_id']))
            conn.execute("UPDATE caisses SET solde_actuel = solde_actuel + ? WHERE id=?", (montant, caisse_id))
        except Exception: pass
    
    # Écriture comptable partie double : 571 (caisse) DÉBIT / 758 (produits divers) CRÉDIT
    libelle = f"Entrée {ref} — {source}"
    if caisse_nom: libelle += f" · Caisse {caisse_nom}"
    try:
        auto_ecriture(conn, date_op, libelle, '571', '758', montant, ref)
    except: pass
    
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Caisse',
                 f"Entrée {ref} — {montant:,.0f} F{' · Caisse ' + caisse_nom if caisse_nom else ''}", request.remote_addr)
    flash(f"Entrée {ref} enregistrée : {montant:,.0f} F{' · ' + caisse_nom if caisse_nom else ''}", "success")
    return redirect('/caisse-sortie?tab=entrees')

# ======================== BILAN COMPTABLE ========================

@app.route('/comptabilite/bilan')
@permission_required('comptabilite')
def bilan_comptable():
    tab = request.args.get('tab', 'mensuel')
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    exercice = request.args.get('exercice', datetime.now().strftime('%Y'))
    conn = _gdb()
    
    if tab == 'plan':
        comptes = [dict(r) for r in conn.execute("SELECT * FROM plan_comptable ORDER BY numero").fetchall()]
        conn.close()
        return render_template('bilan_comptable.html', page='bilan', tab=tab, comptes=comptes, exercice=exercice)
    
    if tab == 'ecritures':
        ecritures = [dict(r) for r in conn.execute("""SELECT e.*, u.full_name FROM ecritures_comptables e 
            LEFT JOIN users u ON e.created_by=u.id WHERE strftime('%Y',e.date)=? ORDER BY e.date DESC, e.id DESC""",
            (exercice,)).fetchall()]
        comptes = [dict(r) for r in conn.execute("SELECT numero, libelle FROM plan_comptable ORDER BY numero").fetchall()]
        conn.close()
        return render_template('bilan_comptable.html', page='bilan', tab=tab, ecritures=ecritures, comptes=comptes, exercice=exercice)
    
    if tab == 'bilan_officiel':
        # Compute official bilan from écritures
        rows = conn.execute("""SELECT pc.numero, pc.libelle, pc.type, pc.categorie, pc.classe,
            COALESCE(SUM(CASE WHEN ec.compte_debit=pc.numero THEN ec.montant ELSE 0 END),0) as total_debit,
            COALESCE(SUM(CASE WHEN ec.compte_credit=pc.numero THEN ec.montant ELSE 0 END),0) as total_credit
            FROM plan_comptable pc
            LEFT JOIN ecritures_comptables ec ON (ec.compte_debit=pc.numero OR ec.compte_credit=pc.numero) AND strftime('%Y',ec.date)=?
            GROUP BY pc.numero ORDER BY pc.numero""", (exercice,)).fetchall()
        
        actif_immob, actif_circ, actif_treso = [], [], []
        passif_cap, passif_dettes = [], []
        charges, produits = [], []
        
        for r in rows:
            d = dict(r)
            solde = d['total_debit'] - d['total_credit']
            d['solde'] = abs(solde)
            if d['categorie'] in ('immobilise',) and d['classe'] == '2':
                actif_immob.append(d)
            elif d['categorie'] in ('circulant',) and d['classe'] in ('3','4'):
                if d['type'] == 'actif': actif_circ.append(d)
                else: passif_dettes.append(d)
            elif d['categorie'] in ('tresorerie',):
                actif_treso.append(d)
            elif d['categorie'] in ('capitaux','dettes_financieres'):
                passif_cap.append(d)
            elif d['categorie'] in ('dettes_circulant',):
                passif_dettes.append(d)
            elif d['categorie'] == 'charges':
                charges.append(d)
            elif d['categorie'] == 'produits':
                produits.append(d)
        
        t_actif = sum(c['solde'] for c in actif_immob + actif_circ + actif_treso if c['solde'] > 0)
        t_passif = sum(c['solde'] for c in passif_cap + passif_dettes if c['solde'] > 0)
        t_charges = sum(c['solde'] for c in charges)
        t_produits = sum(c['solde'] for c in produits)
        resultat = t_produits - t_charges
        
        bilans_hist = [dict(r) for r in conn.execute("SELECT * FROM bilans WHERE exercice=? ORDER BY created_at DESC", (exercice,)).fetchall()]
        conn.close()
        
        bilan_data = {
            'actif_immob': actif_immob, 'actif_circ': actif_circ, 'actif_treso': actif_treso,
            'passif_cap': passif_cap, 'passif_dettes': passif_dettes,
            'charges': charges, 'produits': produits,
            't_actif': t_actif, 't_passif': t_passif + resultat,
            't_charges': t_charges, 't_produits': t_produits, 'resultat': resultat,
            'equilibre': abs(t_actif - (t_passif + resultat)) < 1,
            'bilans_hist': bilans_hist
        }
        return render_template('bilan_comptable.html', page='bilan', tab=tab, bilan=bilan_data, exercice=exercice)
    
    # Default: mensuel overview
    fp = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status='payee' AND strftime('%Y-%m',paid_at)=?", (month,)).fetchone()[0]
    fpen = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status IN ('envoyee','en_attente_paiement') AND strftime('%Y-%m',created_at)=?", (month,)).fetchone()[0]
    ce = conn.execute("SELECT COALESCE(SUM(montant),0) FROM caisse_entrees WHERE strftime('%Y-%m',date)=?", (month,)).fetchone()[0]
    cs = conn.execute("SELECT COALESCE(SUM(montant),0) FROM caisse_sorties WHERE status='approuve' AND strftime('%Y-%m',date)=?", (month,)).fetchone()[0]
    tr = conn.execute("SELECT COALESCE(SUM(amount),0) FROM treasury WHERE movement_type='recette' AND strftime('%Y-%m',created_at)=?", (month,)).fetchone()[0]
    td = conn.execute("SELECT COALESCE(SUM(amount),0) FROM treasury WHERE movement_type='depense' AND strftime('%Y-%m',created_at)=?", (month,)).fetchone()[0]
    dp = conn.execute("SELECT COALESCE(SUM(amount),0) FROM pieces_caisse WHERE strftime('%Y-%m',date)=?", (month,)).fetchone()[0]
    banks = [dict(r) for r in conn.execute("SELECT * FROM bank_accounts ORDER BY name").fetchall()]
    inv_list = [dict(r) for r in conn.execute("SELECT * FROM invoices WHERE strftime('%Y-%m',created_at)=? ORDER BY created_at DESC", (month,)).fetchall()]
    conn.close()
    
    te = fp + ce + tr; ts = cs + td + dp
    data = {'month': month, 'factures_payees': fp, 'factures_pending': fpen,
            'caisse_entrees': ce, 'caisse_sorties': cs, 'treso_recettes': tr, 'treso_depenses': td,
            'depenses': dp, 'total_entrees': te, 'total_sorties': ts, 'solde': te - ts,
            'banks': banks, 'inv_list': inv_list}
    return render_template('bilan_comptable.html', page='bilan', tab=tab, data=data, exercice=exercice)

@app.route('/comptabilite/ecritures/add', methods=['POST'])
@permission_required('comptabilite_edit')
def ecriture_add():
    conn = _gdb()
    conn.execute("""INSERT INTO ecritures_comptables (date, journal, piece, compte_debit, compte_credit, libelle, montant, created_by)
        VALUES (?,?,?,?,?,?,?,?)""",
        (request.form.get('date',''), request.form.get('journal','OD'), request.form.get('reference',''),
         request.form.get('compte_debit',''), request.form.get('compte_credit',''),
         request.form.get('libelle',''), float(request.form.get('montant',0) or 0), session['user_id']))
    conn.commit(); conn.close()
    flash("Écriture enregistrée","success")
    period = request.form.get('date','')[:7] or datetime.now().strftime('%Y-%m')
    return redirect(f'/comptabilite/ecritures?period={period}')

@app.route('/comptabilite/plan-comptable/add', methods=['POST'])
@permission_required('comptabilite_edit')
def plan_comptable_add():
    conn = _gdb()
    try:
        conn.execute("INSERT INTO plan_comptable (numero, libelle, type, categorie, classe) VALUES (?,?,?,?,?)",
            (request.form.get('numero',''), request.form.get('libelle',''), request.form.get('type','actif'),
             request.form.get('categorie',''), request.form.get('classe','')))
        conn.commit(); flash("Compte ajouté","success")
    except: flash("Ce numéro de compte existe déjà","error")
    conn.close()
    return redirect('/comptabilite/bilan?tab=plan')

@app.route('/comptabilite/bilan/generer', methods=['POST'])
@permission_required('comptabilite_edit')
def bilan_generer():
    exercice = request.form.get('exercice', datetime.now().strftime('%Y'))
    conn = _gdb()
    rows = conn.execute("""SELECT pc.type, pc.categorie,
        COALESCE(SUM(CASE WHEN ec.compte_debit=pc.numero THEN ec.montant ELSE 0 END),0) as td,
        COALESCE(SUM(CASE WHEN ec.compte_credit=pc.numero THEN ec.montant ELSE 0 END),0) as tc
        FROM plan_comptable pc
        LEFT JOIN ecritures_comptables ec ON (ec.compte_debit=pc.numero OR ec.compte_credit=pc.numero) AND strftime('%Y',ec.date)=?
        WHERE pc.categorie NOT IN ('charges','produits')
        GROUP BY pc.numero""", (exercice,)).fetchall()
    
    t_actif = sum(r['td'] - r['tc'] for r in rows if r['type'] == 'actif')
    t_passif = sum(r['tc'] - r['td'] for r in rows if r['type'] == 'passif')
    
    # Résultat
    ch = conn.execute("""SELECT COALESCE(SUM(CASE WHEN ec.compte_debit=pc.numero THEN ec.montant ELSE 0 END),0)
        FROM plan_comptable pc LEFT JOIN ecritures_comptables ec ON ec.compte_debit=pc.numero AND strftime('%Y',ec.date)=?
        WHERE pc.categorie='charges'""", (exercice,)).fetchone()[0]
    pr = conn.execute("""SELECT COALESCE(SUM(CASE WHEN ec.compte_credit=pc.numero THEN ec.montant ELSE 0 END),0)
        FROM plan_comptable pc LEFT JOIN ecritures_comptables ec ON ec.compte_credit=pc.numero AND strftime('%Y',ec.date)=?
        WHERE pc.categorie='produits'""", (exercice,)).fetchone()[0]
    resultat = pr - ch
    
    bilan_data = {'actif': t_actif, 'passif': t_passif, 'resultat': resultat, 'charges': ch, 'produits': pr}
    conn.execute("""INSERT INTO bilans (exercice, date_cloture, total_actif, total_passif, resultat, data_json, status, created_by)
        VALUES (?,?,?,?,?,?,?,?)""",
        (exercice, datetime.now().strftime('%Y-12-31'), t_actif, t_passif + resultat, resultat,
         json.dumps(bilan_data), 'brouillon', session['user_id']))
    conn.commit(); conn.close()
    flash(f"Bilan exercice {exercice} généré","success")
    return redirect(f'/comptabilite/bilan?tab=bilan_officiel&exercice={exercice}')

@app.route('/comptabilite/bilan/pdf')
@permission_required('comptabilite')
def bilan_pdf():
    exercice = request.args.get('exercice', datetime.now().strftime('%Y'))
    month = request.args.get('month', '')
    tab = request.args.get('tab', 'mensuel')
    
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.colors import HexColor, white
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], f'bilan_{exercice}.pdf')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    doc = SimpleDocTemplate(output, pagesize=A4, leftMargin=12*mm, rightMargin=12*mm, topMargin=10*mm, bottomMargin=10*mm)
    
    BG = HexColor('#44546A'); BL = HexColor('#4472C4'); GR = HexColor('#2e7d32'); RD = HexColor('#c53030')
    hw = ParagraphStyle('hw', fontName='Helvetica-Bold', fontSize=8, textColor=white, alignment=TA_CENTER)
    tc = ParagraphStyle('tc', fontSize=8, alignment=TA_CENTER)
    tl = ParagraphStyle('tl', fontSize=8)
    tr_s = ParagraphStyle('tr', fontSize=8, alignment=TA_RIGHT)
    tb = ParagraphStyle('tb', fontName='Helvetica-Bold', fontSize=9, alignment=TA_RIGHT)
    pw = 186*mm
    fmt = lambda x: f"{x:,.0f}"
    
    conn = _gdb()
    story = []
    
    story.append(Paragraph("RAMYA TECHNOLOGIE &amp; INNOVATION", ParagraphStyle('co', fontSize=9, textColor=HexColor('#999'), alignment=TA_CENTER, spaceAfter=3*mm)))
    story.append(Paragraph(f"<b>BILAN COMPTABLE — Exercice {exercice}</b>", ParagraphStyle('t', fontName='Helvetica-Bold', fontSize=16, textColor=BG, alignment=TA_CENTER, spaceAfter=2*mm)))
    story.append(Paragraph(f"Document conforme au SYSCOHADA révisé — Généré le {datetime.now().strftime('%d/%m/%Y')}", ParagraphStyle('sub', fontSize=7, textColor=HexColor('#999'), alignment=TA_CENTER, spaceAfter=6*mm)))
    
    # Get bilan data
    rows = conn.execute("""SELECT pc.numero, pc.libelle, pc.type, pc.categorie, pc.classe,
        COALESCE(SUM(CASE WHEN ec.compte_debit=pc.numero THEN ec.montant ELSE 0 END),0) as total_debit,
        COALESCE(SUM(CASE WHEN ec.compte_credit=pc.numero THEN ec.montant ELSE 0 END),0) as total_credit
        FROM plan_comptable pc
        LEFT JOIN ecritures_comptables ec ON (ec.compte_debit=pc.numero OR ec.compte_credit=pc.numero) AND strftime('%Y',ec.date)=?
        GROUP BY pc.numero ORDER BY pc.numero""", (exercice,)).fetchall()
    conn.close()
    
    actif_rows = [(dict(r)['numero'], dict(r)['libelle'], abs(dict(r)['total_debit'] - dict(r)['total_credit'])) 
                  for r in rows if dict(r)['type'] == 'actif' and dict(r)['categorie'] not in ('charges','produits')]
    passif_rows = [(dict(r)['numero'], dict(r)['libelle'], abs(dict(r)['total_credit'] - dict(r)['total_debit']))
                   for r in rows if dict(r)['type'] == 'passif' and dict(r)['categorie'] not in ('charges','produits')]
    
    t_actif = sum(r[2] for r in actif_rows)
    t_passif = sum(r[2] for r in passif_rows)
    
    # ACTIF table
    story.append(Paragraph("<b>ACTIF</b>", ParagraphStyle('ha', fontName='Helvetica-Bold', fontSize=11, textColor=BL, spaceAfter=2*mm)))
    ad = [[Paragraph(h, hw) for h in ['N° Compte', 'Libellé', 'Montant (FCFA)']]]
    for num, lib, val in actif_rows:
        if val > 0: ad.append([Paragraph(num, tc), Paragraph(lib, tl), Paragraph(fmt(val), tr_s)])
    ad.append([Paragraph('', tc), Paragraph('<b>TOTAL ACTIF</b>', ParagraphStyle('ta', fontName='Helvetica-Bold', fontSize=9)),
               Paragraph(f'<b>{fmt(t_actif)}</b>', tb)])
    at = Table(ad, colWidths=[20*mm, pw-50*mm, 30*mm])
    at.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),BL),('BOX',(0,0),(-1,-1),0.5,HexColor('#8EAADB')),
        ('INNERGRID',(0,0),(-1,-1),0.3,HexColor('#D6E4F0')),('BACKGROUND',(0,-1),(-1,-1),HexColor('#E2EFDA')),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    story.extend([at, Spacer(1, 5*mm)])
    
    # PASSIF table
    story.append(Paragraph("<b>PASSIF</b>", ParagraphStyle('hp', fontName='Helvetica-Bold', fontSize=11, textColor=RD, spaceAfter=2*mm)))
    pd = [[Paragraph(h, hw) for h in ['N° Compte', 'Libellé', 'Montant (FCFA)']]]
    for num, lib, val in passif_rows:
        if val > 0: pd.append([Paragraph(num, tc), Paragraph(lib, tl), Paragraph(fmt(val), tr_s)])
    pd.append([Paragraph('', tc), Paragraph('<b>TOTAL PASSIF</b>', ParagraphStyle('tp', fontName='Helvetica-Bold', fontSize=9)),
               Paragraph(f'<b>{fmt(t_passif)}</b>', tb)])
    pt = Table(pd, colWidths=[20*mm, pw-50*mm, 30*mm])
    pt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),RD),('BOX',(0,0),(-1,-1),0.5,HexColor('#EF9A9A')),
        ('INNERGRID',(0,0),(-1,-1),0.3,HexColor('#FFCDD2')),('BACKGROUND',(0,-1),(-1,-1),HexColor('#FDE8E8')),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    story.extend([pt, Spacer(1, 5*mm)])
    
    # Equilibre
    equil = "EQUILIBRE ✓" if abs(t_actif - t_passif) < 1 else f"ECART: {fmt(abs(t_actif - t_passif))}"
    story.append(Paragraph(f"<b>{equil}</b> — Total Actif: {fmt(t_actif)} F | Total Passif: {fmt(t_passif)} F",
        ParagraphStyle('eq', fontName='Helvetica-Bold', fontSize=10, textColor=BG, alignment=TA_CENTER, spaceAfter=6*mm)))
    
    story.append(Paragraph("Signature du Comptable : ___________________          Signature DG : ___________________",
        ParagraphStyle('sig', fontSize=8, textColor=HexColor('#999'), spaceAfter=4*mm)))
    story.append(Paragraph("Ce bilan est établi conformément au Plan Comptable SYSCOHADA révisé applicable en Côte d'Ivoire.",
        ParagraphStyle('legal', fontSize=7, textColor=HexColor('#bbb'), alignment=TA_CENTER)))
    
    doc.build(story)
    return send_file(output, as_attachment=True, download_name=f"Bilan_SYSCOHADA_{exercice}.pdf")

# ======================== RAPPORT DE CAISSE HEBDOMADAIRE ========================

@app.route('/comptabilite/rapport-caisse')
@permission_required('comptabilite')
def rapport_caisse():
    conn = _gdb()
    reports = [dict(r) for r in conn.execute(
        "SELECT * FROM weekly_cash_reports ORDER BY created_at DESC LIMIT 50").fetchall()]
    conn.close()
    return render_template('rapport_caisse.html', page='comptabilite', reports=reports)

@app.route('/comptabilite/rapport-caisse/new', methods=['GET', 'POST'])
@permission_required('comptabilite_edit')
def rapport_caisse_new():
    if request.method == 'POST':
        items = []
        i = 1
        while request.form.get(f'date_{i}'):
            items.append({
                'n': i, 'date': request.form.get(f'date_{i}',''),
                'description': request.form.get(f'desc_{i}',''),
                'credit': float(request.form.get(f'credit_{i}',0) or 0),
                'pc': request.form.get(f'pc_{i}',''),
                'debit': float(request.form.get(f'debit_{i}',0) or 0),
            })
            i += 1
        total_c = sum(it['credit'] for it in items)
        total_d = sum(it['debit'] for it in items)
        
        conn = _gdb()
        conn.execute("""INSERT INTO weekly_cash_reports 
            (agent_name, matricule, report_number, week_start, week_end, items_json,
             total_credit, total_debit, reste_caisse, deposit_date, created_by)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (request.form.get('agent_name',''), request.form.get('matricule',''),
             request.form.get('report_number',''), request.form.get('week_start',''),
             request.form.get('week_end',''), json.dumps(items),
             total_c, total_d, total_c - total_d,
             request.form.get('deposit_date',''), session['user_id']))
        conn.commit(); conn.close()
        flash("Rapport de caisse créé", "success")
        return redirect('/comptabilite/rapport-caisse')
    return render_template('rapport_caisse_new.html', page='comptabilite')

@app.route('/comptabilite/rapport-caisse/import', methods=['POST'])
@permission_required('comptabilite_edit')
def rapport_caisse_import():
    if 'file' not in request.files:
        flash("Fichier requis", "error"); return redirect('/comptabilite/rapport-caisse')
    f = request.files['file']
    
    import openpyxl
    wb = openpyxl.load_workbook(f, data_only=True)
    ws = wb.active
    
    agent = str(ws.cell(2,1).value or '').replace("Nom et Prénom De L'agent:", '').replace("Nom et Prénom De L\u2019agent:", '').strip().lstrip(': ')
    matricule = str(ws.cell(3,1).value or '').replace('N⁰ Matricule:', '').strip()
    report_num = str(ws.cell(4,1).value or '').replace('N⁰ Rapport:', '').strip()
    report_date = str(ws.cell(5,1).value or '').replace('Rapport Du:', '').strip()
    
    items = []
    for row in ws.iter_rows(min_row=7, max_row=ws.max_row, values_only=True):
        if not row[0] or str(row[0]).strip() == '': continue
        try: int(str(row[0]).strip())
        except: continue
        desc = str(row[2] or '').strip()
        credit = float(row[3] or 0) if row[3] and str(row[3]).replace('.','').replace('-','').isdigit() else 0
        pc = str(row[4] or '').strip()
        debit = float(row[5] or 0) if row[5] and str(row[5]).replace('.','').replace('-','').isdigit() else 0
        date_val = str(row[1] or '')[:10]
        if desc or credit > 0 or debit > 0:
            items.append({'n': len(items)+1, 'date': date_val, 'description': desc,
                         'credit': credit, 'pc': pc, 'debit': debit})
    
    total_c = sum(it['credit'] for it in items)
    total_d = sum(it['debit'] for it in items)
    
    conn = _gdb()
    conn.execute("""INSERT INTO weekly_cash_reports 
        (agent_name, matricule, report_number, week_start, items_json,
         total_credit, total_debit, reste_caisse, created_by)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (agent, matricule, report_num, report_date, json.dumps(items),
         total_c, total_d, total_c - total_d, session['user_id']))
    conn.commit(); conn.close()
    
    flash(f"Rapport importé — {agent} — {len(items)} lignes, Crédit: {total_c:,.0f}, Débit: {total_d:,.0f}", "success")
    return redirect('/comptabilite/rapport-caisse')

@app.route('/comptabilite/rapport-caisse/view/<int:rid>')
@permission_required('comptabilite')
def rapport_caisse_view(rid):
    conn = _gdb()
    r = conn.execute("SELECT * FROM weekly_cash_reports WHERE id=?", (rid,)).fetchone()
    conn.close()
    if not r: flash("Non trouvé","error"); return redirect('/comptabilite/rapport-caisse')
    report = dict(r)
    report_items = json.loads(report.get('items_json','[]') or '[]')
    return render_template('rapport_caisse_view.html', page='comptabilite', report=report, report_items=report_items)

@app.route('/comptabilite/rapport-caisse/edit/<int:rid>', methods=['GET', 'POST'])
@permission_required('comptabilite_edit')
def rapport_caisse_edit(rid):
    conn = _gdb()
    r = conn.execute("SELECT * FROM weekly_cash_reports WHERE id=?", (rid,)).fetchone()
    if not r: flash("Non trouvé","error"); return redirect('/comptabilite/rapport-caisse')
    report = dict(r)
    
    if request.method == 'POST':
        items = []
        i = 1
        while request.form.get(f'date_{i}'):
            items.append({
                'n': i, 'date': request.form.get(f'date_{i}',''),
                'description': request.form.get(f'desc_{i}',''),
                'credit': float(request.form.get(f'credit_{i}',0) or 0),
                'pc': request.form.get(f'pc_{i}',''),
                'debit': float(request.form.get(f'debit_{i}',0) or 0),
            })
            i += 1
        total_c = sum(it['credit'] for it in items)
        total_d = sum(it['debit'] for it in items)
        
        conn.execute("""UPDATE weekly_cash_reports SET agent_name=?, matricule=?, report_number=?,
            week_start=?, items_json=?, total_credit=?, total_debit=?, reste_caisse=?, deposit_date=?
            WHERE id=?""",
            (request.form.get('agent_name',''), request.form.get('matricule',''),
             request.form.get('report_number',''), request.form.get('week_start',''),
             json.dumps(items), total_c, total_d, total_c - total_d,
             request.form.get('deposit_date',''), rid))
        conn.commit(); conn.close()
        flash("Rapport modifié","success")
        return redirect(f'/comptabilite/rapport-caisse/view/{rid}')
    
    conn.close()
    report['items'] = json.loads(report.get('items_json','[]') or '[]')
    return render_template('rapport_caisse_new.html', page='comptabilite', report=report, report_items=report['items'])

@app.route('/comptabilite/rapport-caisse/pdf/<int:rid>')
@permission_required('comptabilite')
def rapport_caisse_pdf(rid):
    conn = _gdb()
    r = conn.execute("SELECT * FROM weekly_cash_reports WHERE id=?", (rid,)).fetchone()
    conn.close()
    if not r: flash("Non trouvé","error"); return redirect('/comptabilite/rapport-caisse')
    report = dict(r)
    items = json.loads(report.get('items_json','[]') or '[]')
    
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.colors import HexColor, white
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], f'rapport_caisse_{rid}.pdf')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    doc = SimpleDocTemplate(output, pagesize=A4, leftMargin=12*mm, rightMargin=12*mm, topMargin=10*mm, bottomMargin=10*mm)
    
    BG = HexColor('#44546A'); BL = HexColor('#4472C4'); WH = white; BK = HexColor('#222')
    hw = ParagraphStyle('hw', fontName='Helvetica-Bold', fontSize=8, textColor=WH, alignment=TA_CENTER)
    tc = ParagraphStyle('tc', fontSize=8, alignment=TA_CENTER, textColor=BK)
    tr = ParagraphStyle('tr', fontSize=8, alignment=TA_RIGHT, textColor=BK)
    tb = ParagraphStyle('tb', fontName='Helvetica-Bold', fontSize=8, alignment=TA_RIGHT, textColor=BK)
    
    story = []
    pw = 186*mm
    
    story.append(Paragraph("<b>RAPPORT CAISSE HEBDOMADAIRE DES DEPENSES</b>",
        ParagraphStyle('t', fontName='Helvetica-Bold', fontSize=14, textColor=BG, alignment=TA_CENTER, spaceAfter=6*mm)))
    
    info = f"Agent : <b>{report['agent_name']}</b>  |  Matricule : {report['matricule']}  |  {report['report_number']}  |  {report['week_start']}"
    story.append(Paragraph(info, ParagraphStyle('i', fontSize=9, textColor=BK, alignment=TA_LEFT, spaceAfter=4*mm)))
    
    hdrs = ['N°', 'DATE', 'DESCRIPTION', 'CREDITE', 'N° P.C', 'DEBITE']
    cw = [8*mm, 20*mm, 68*mm, 24*mm, 30*mm, 24*mm]
    td = [[Paragraph(h, hw) for h in hdrs]]
    
    for it in items:
        td.append([
            Paragraph(str(it['n']), tc), Paragraph(str(it.get('date',''))[:10], tc),
            Paragraph(it.get('description',''), ParagraphStyle('d', fontSize=7, textColor=BK)),
            Paragraph(f"{it['credit']:,.0f}" if it['credit'] else '', tr),
            Paragraph(it.get('pc',''), ParagraphStyle('pc', fontSize=6, textColor=BK, alignment=TA_CENTER)),
            Paragraph(f"{it['debit']:,.0f}" if it['debit'] else '', tr),
        ])
    
    # Totals row
    td.append([Paragraph('', tc), Paragraph('Reste En Caisse:', ParagraphStyle('rc', fontName='Helvetica-Bold', fontSize=8, textColor=BK)),
        Paragraph(f"{report['reste_caisse']:,.0f}", tb),
        Paragraph(f"{report['total_credit']:,.0f}", tb),
        Paragraph('<b>TOTAL</b>', ParagraphStyle('tt', fontName='Helvetica-Bold', fontSize=8, textColor=BK, alignment=TA_CENTER)),
        Paragraph(f"{report['total_debit']:,.0f}", tb)])
    
    t = Table(td, colWidths=cw, repeatRows=1)
    sc = [('BACKGROUND', (0,0), (-1,0), BL), ('TEXTCOLOR', (0,0), (-1,0), WH),
          ('BOX', (0,0), (-1,-1), 0.5, HexColor('#8EAADB')),
          ('INNERGRID', (0,0), (-1,-1), 0.3, HexColor('#B4C6E7')),
          ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
          ('TOPPADDING', (0,0), (-1,-1), 3), ('BOTTOMPADDING', (0,0), (-1,-1), 3),
          ('BACKGROUND', (0,-1), (-1,-1), HexColor('#E2EFDA'))]
    for i in range(2, len(td)-1, 2):
        sc.append(('BACKGROUND', (0,i), (-1,i), HexColor('#F2F2F2')))
    t.setStyle(TableStyle(sc))
    story.append(t)
    
    story.append(Spacer(1, 6*mm))
    story.append(Paragraph(f"Date de Dépôt Rapport Comptabilité : {report.get('deposit_date','')}", ParagraphStyle('ft', fontSize=8, textColor=HexColor('#888'))))
    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Signature Et Cachet Caisse :", ParagraphStyle('ft2', fontSize=8, textColor=HexColor('#888'))))
    
    doc.build(story)
    return send_file(output, as_attachment=True, download_name=f"Rapport_Caisse_{report['agent_name'].replace(' ','_')}.pdf")

@app.route('/comptabilite/rapport-caisse/excel/<int:rid>')
@permission_required('comptabilite')
def rapport_caisse_excel(rid):
    conn = _gdb()
    r = conn.execute("SELECT * FROM weekly_cash_reports WHERE id=?", (rid,)).fetchone()
    conn.close()
    if not r: flash("Non trouvé","error"); return redirect('/comptabilite/rapport-caisse')
    report = dict(r)
    items = json.loads(report.get('items_json','[]') or '[]')
    
    import openpyxl
    from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Rapport Caisse"
    
    ws.merge_cells('A1:F1'); ws['A1'] = 'RAPPORT CAISSE HEBDOMADAIRE DES DEPENSES'
    ws['A1'].font = Font(bold=True, size=14); ws['A1'].alignment = Alignment(horizontal='center')
    ws['A2'] = f"Nom et Prénom De L'agent: {report['agent_name']}"
    ws['A3'] = f"N° Matricule: {report['matricule']}"
    ws['A4'] = f"N° Rapport: {report['report_number']}"
    ws['A5'] = f"Rapport Du: {report['week_start']}"
    
    hdrs = ['N°', 'DATE', 'DESCRIPTION', 'CREDITE', 'N° P.C', 'DEBITE']
    hfill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
    hfont = Font(bold=True, color='FFFFFF')
    for i, h in enumerate(hdrs, 1):
        c = ws.cell(6, i, h); c.font = hfont; c.fill = hfill; c.alignment = Alignment(horizontal='center')
    
    for idx, it in enumerate(items):
        row = 7 + idx
        ws.cell(row, 1, it['n'])
        ws.cell(row, 2, it.get('date','')[:10])
        ws.cell(row, 3, it.get('description',''))
        if it['credit']: ws.cell(row, 4, it['credit'])
        ws.cell(row, 5, it.get('pc',''))
        if it['debit']: ws.cell(row, 6, it['debit'])
    
    tr = 7 + len(items)
    ws.cell(tr, 2, 'Reste En Caisse:').font = Font(bold=True)
    ws.cell(tr, 3, report['reste_caisse']).font = Font(bold=True)
    ws.cell(tr, 4, report['total_credit']).font = Font(bold=True)
    ws.cell(tr, 5, 'TOTAL').font = Font(bold=True)
    ws.cell(tr, 6, report['total_debit']).font = Font(bold=True)
    
    ws.column_dimensions['A'].width = 5; ws.column_dimensions['B'].width = 14
    ws.column_dimensions['C'].width = 45; ws.column_dimensions['D'].width = 14
    ws.column_dimensions['E'].width = 22; ws.column_dimensions['F'].width = 14
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], f'rapport_caisse_{rid}.xlsx')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    wb.save(output)
    return send_file(output, as_attachment=True, download_name=f"Rapport_Caisse_{report['agent_name'].replace(' ','_')}.xlsx")


# ======================== RAPPORTS DE VISITE ========================

@app.route('/visites')
@permission_required('visites')
def visites_page():
    tab = request.args.get('tab', 'en_attente')
    visits = get_visit_reports(tab if tab != 'all' else None)
    v_stats = get_visit_stats()
    return render_template('visites.html', page='visites', tab=tab, visits=visits, v_stats=v_stats)

@app.route('/visites/new', methods=['GET', 'POST'])
@permission_required('visites')
def visites_new():
    if request.method == 'POST':
        client_id = request.form.get('client_id', '')
        client_id = int(client_id) if client_id else None
        client_name = request.form.get('client_name', '').strip()
        
        if client_id and not client_name:
            c = get_client_by_id(client_id)
            if c: client_name = c['name']
        
        create_visit_report(
            client_id, client_name,
            request.form.get('site_name', ''),
            request.form.get('site_address', ''),
            request.form.get('site_location', ''),
            request.form.get('contact_name', ''),
            request.form.get('contact_tel', ''),
            request.form.get('visit_date', ''),
            request.form.get('needs', ''),
            request.form.get('observations', ''),
            request.form.get('equipment', ''),
            session['user_id']
        )
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Visite', f"Rapport de visite créé — {client_name}", request.remote_addr)
        flash("Rapport de visite créé — En attente de proforma", "success")
        return redirect(url_for('visites_page'))
    
    clients = get_all_clients()
    return render_template('visite_new.html', page='visites', clients=clients)

@app.route('/visites/<int:vid>')
@permission_required('visites')
def visite_detail(vid):
    visit = get_visit_by_id(vid)
    if not visit:
        flash("Rapport non trouvé", "error")
        return redirect(url_for('visites_page'))
    return render_template('visite_detail.html', page='visites', visit=visit)

@app.route('/visites/proforma/<int:vid>', methods=['POST'])
@permission_required('proforma')
def visites_proforma(vid):
    ref = request.form.get('proforma_ref', '').strip()
    amount = float(request.form.get('proforma_amount', 0) or 0)
    update_visit_proforma(vid, ref, amount, session['user_id'])
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?',
                'Proforma', f"Proforma {ref} envoyé pour visite #{vid}", request.remote_addr)
    flash(f"Proforma {ref} envoyé", "success")
    return redirect(url_for('visites_page'))


# ======================== LANGUE ========================

@app.route('/lang/<lang>')
def set_language(lang):
    if lang in ('fr', 'en'):
        session['lang'] = lang
    return redirect(request.referrer or url_for('dashboard'))


# ======================== EXPORT STATS ========================

@app.route('/export/stats')
@permission_required('admin')
def export_stats():
    """Exporte les statistiques en PDF avec graphiques."""
    from rapport_core import _generate_chart_image
    
    stats = get_dashboard_stats()
    monthly = get_client_monthly_stats()
    
    # Générer un rapport simple avec reportlab
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.colors import HexColor
    
    export_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'export')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, 'statistiques_ramya.pdf')
    
    doc = SimpleDocTemplate(output, pagesize=A4)
    story = []
    
    title_style = ParagraphStyle('title', fontSize=18, alignment=TA_CENTER,
                                  textColor=HexColor('#1a3a5c'), spaceAfter=20)
    h2_style = ParagraphStyle('h2', fontSize=14, textColor=HexColor('#1a3a5c'),
                               spaceAfter=10, spaceBefore=20)
    normal = ParagraphStyle('normal', fontSize=11, spaceAfter=6)
    
    story.append(Paragraph("RAMYA TECHNOLOGIE & INNOVATION", title_style))
    story.append(Paragraph("Rapport Statistique", title_style))
    story.append(Spacer(1, 10*mm))
    
    story.append(Paragraph("Vue d'ensemble", h2_style))
    story.append(Paragraph(f"Rapports traités : {stats['total_jobs']}", normal))
    story.append(Paragraph(f"En attente d'envoi : {stats['pending_jobs']}", normal))
    story.append(Paragraph(f"Envoyés : {stats['sent_jobs']}", normal))
    story.append(Paragraph(f"Clients : {stats['total_clients']}", normal))
    story.append(Paragraph(f"Utilisateurs : {stats['total_users']}", normal))
    
    if monthly:
        story.append(Paragraph("Détail par client et par mois", h2_style))
        for client, months in monthly.items():
            story.append(Paragraph(f"<b>{client}</b>", normal))
            data = [['Mois', 'Rapports', 'Employés', 'Envoyés', 'En attente']]
            for month, m_stats in sorted(months.items()):
                data.append([month, m_stats['count'], m_stats['employees'],
                           m_stats['sent'], m_stats['pending']])
            t = Table(data, colWidths=[35*mm, 25*mm, 25*mm, 25*mm, 25*mm])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), HexColor('#1a3a5c')),
                ('TEXTCOLOR', (0,0), (-1,0), HexColor('#ffffff')),
                ('FONTSIZE', (0,0), (-1,-1), 9),
                ('GRID', (0,0), (-1,-1), 0.5, HexColor('#cccccc')),
                ('TOPPADDING', (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
            ]))
            story.append(t)
            story.append(Spacer(1, 5*mm))
    
    story.append(Spacer(1, 10*mm))
    story.append(Paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}", normal))
    
    doc.build(story)
    
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?',
                'Export', 'Export statistiques PDF', request.remote_addr)
    
    return send_file(output, as_attachment=True,
                    download_name=f"stats_ramya_{datetime.now().strftime('%Y%m')}.pdf")


# ======================== DEVIS / PROFORMA ========================

@app.route('/devis')
@permission_required('proforma')
def devis_page():
    tab = request.args.get('tab', 'all')
    d_stats = get_devis_stats()
    devis_list = get_all_devis(tab if tab in ('devis', 'proforma') else None)
    return render_template('devis.html', page='devis', tab=tab, devis_list=devis_list, d_stats=d_stats)

@app.route('/devis/new', methods=['GET', 'POST'])
@permission_required('proforma_edit')
def devis_new():
    if request.method == 'POST':
        items = []
        i = 1
        while request.form.get(f'item_{i}_designation'):
            items.append({
                'num': i,
                'designation': request.form.get(f'item_{i}_designation', ''),
                'detail': request.form.get(f'item_{i}_detail', ''),
                'qty': int(request.form.get(f'item_{i}_qty', 1) or 1),
                'prix': float(request.form.get(f'item_{i}_prix', 0) or 0),
                'remise': float(request.form.get(f'item_{i}_remise', 0) or 0),
            })
            i += 1
        
        total_ht = sum(it['qty'] * it['prix'] - it['remise'] for it in items)
        main_oeuvre = float(request.form.get('main_oeuvre', 0) or 0)
        petites_fourn = float(request.form.get('petites_fournitures', 0) or 0)
        remise_glob = float(request.form.get('remise', 0) or 0)
        # TVA optionnelle
        tva_active = 1 if request.form.get('tva_active') in ('1','true','on','yes') else 0
        tva_rate = float(request.form.get('tva_rate', 18) or 18)
        tva_amount = round((total_ht - remise_glob) * tva_rate / 100, 0) if tva_active else 0
        total_ttc = total_ht + petites_fourn - remise_glob + tva_amount
        
        client_id = request.form.get('client_id', '')
        client_id = int(client_id) if client_id else None
        
        # Rédacteur = utilisateur connecté + horodatage
        cur_u = get_user_by_id(session['user_id'])
        redacteur_name = (cur_u['full_name'] if cur_u else '') or 'Utilisateur'
        redacteur_dt = datetime.now().strftime('%d/%m/%Y à %H:%M')
        
        did, ref = create_devis(
            client_id, request.form.get('client_name', ''),
            request.form.get('client_code', ''),
            request.form.get('contact_commercial', ''),
            request.form.get('objet', ''),
            json.dumps(items), total_ht, petites_fourn, total_ttc,
            main_oeuvre, remise_glob,
            request.form.get('notes', ''),
            session['user_id'],
            request.form.get('doc_type', 'devis')
        )
        # Post-update extra columns (TVA + rédacteur) — defensive (columns added by v47)
        try:
            conn = _gdb()
            conn.execute("UPDATE devis SET tva_active=?, tva_rate=?, tva_amount=?, redacteur=?, redacteur_date=? WHERE id=?",
                         (tva_active, tva_rate, tva_amount, redacteur_name, redacteur_dt, did))
            conn.commit(); conn.close()
        except Exception: pass
        
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Devis', f"{ref} créé pour {request.form.get('client_name', '')}" + (f" (TVA {tva_rate:.0f}%)" if tva_active else ""), request.remote_addr)
        flash(f"Devis {ref} créé — {total_ttc:,.0f} FCFA TTC" + (f" (dont {tva_amount:,.0f} F TVA)" if tva_active else ""), "success")
        return redirect(url_for('devis_page'))
    
    clients = get_all_clients()
    try:
        from models import db_get_all
        stock_items = db_get_all('stock_items', order='name ASC')
    except: stock_items = []
    current_user = get_user_by_id(session['user_id'])
    default_commercial = current_user['full_name'] if current_user else ''
    return render_template('devis_new.html', page='devis', clients=clients, stock_items=stock_items,
                           default_commercial=default_commercial)

@app.route('/devis/pdf/<int:did>')
@permission_required('proforma')
def devis_pdf(did):
    devis = get_devis_by_id(did)
    if not devis:
        flash("Devis non trouvé", "error")
        return redirect(url_for('devis_page'))
    
    export_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'devis')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"{devis['reference']}.pdf")
    
    devis['date'] = devis.get('created_at', '')[:10]
    # PDF options: prefer DB-stored values; fallback to sensible defaults
    devis.setdefault('tva_active', devis.get('tva_active') or False)
    devis.setdefault('tva_rate', devis.get('tva_rate') or 18)
    devis.setdefault('tva_amount', devis.get('tva_amount') or 0)
    if not devis.get('redacteur'):
        u = get_user_by_id(devis.get('created_by')) if devis.get('created_by') else None
        devis['redacteur'] = u['full_name'] if u else ''
    if not devis.get('redacteur_date'):
        ca = devis.get('created_at', '') or ''
        if ca:
            try:
                dt = datetime.strptime(ca[:19], '%Y-%m-%d %H:%M:%S')
                devis['redacteur_date'] = dt.strftime('%d/%m/%Y à %H:%M')
            except: devis['redacteur_date'] = ca[:16]
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"] if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_devis_pdf(devis, output, logo_path=logo_r)
    
    return send_file(output, as_attachment=True, download_name=f"{devis['reference']}.pdf")

@app.route('/devis/status/<int:did>/<status>')
@permission_required('proforma_edit')
def devis_status(did, status):
    if status in ('brouillon', 'envoye', 'accepte', 'refuse'):
        update_devis_status(did, status)
        # Stock deduction when devis is accepted
        if status == 'accepte':
            devis = get_devis_by_id(did)
            if devis:
                items = json.loads(devis.get('items_json', '[]')) if isinstance(devis.get('items_json'), str) else devis.get('items_json', [])
                conn = _gdb()
                for item in items:
                    designation = item.get('designation', '')
                    qty = int(item.get('qty', 0) or 0)
                    if designation and qty > 0:
                        stock_item = conn.execute("SELECT id, quantity FROM stock_items WHERE name=? OR reference=?",
                            (designation, designation)).fetchone()
                        if stock_item:
                            new_qty = max(0, stock_item['quantity'] - qty)
                            conn.execute("UPDATE stock_items SET quantity=? WHERE id=?", (new_qty, stock_item['id']))
                            conn.execute("""INSERT INTO stock_movements (item_id, movement_type, quantity, reference, notes, created_by)
                                VALUES (?, 'sortie', ?, ?, ?, ?)""",
                                (stock_item['id'], qty, devis.get('reference',''), f"Devis {devis['reference']} accepté", session.get('user_id')))
                conn.commit(); conn.close()
                flash(f"Articles déduits du stock", "info")
        flash(f"Statut mis à jour : {status}", "success")
    return redirect(url_for('devis_page'))

@app.route('/devis/edit/<int:did>', methods=['GET', 'POST'])
@permission_required('proforma_edit')
def devis_edit(did):
    devis = get_devis_by_id(did)
    if not devis: flash("Devis non trouvé", "error"); return redirect(url_for('devis_page'))
    if request.method == 'POST':
        items = []
        i = 1
        while request.form.get(f'item_{i}_designation'):
            items.append({
                'num': i, 'designation': request.form.get(f'item_{i}_designation', ''),
                'detail': request.form.get(f'item_{i}_detail', ''),
                'qty': int(request.form.get(f'item_{i}_qty', 1) or 1),
                'prix': float(request.form.get(f'item_{i}_prix', 0) or 0),
                'remise': float(request.form.get(f'item_{i}_remise', 0) or 0),
            })
            i += 1
        total_ht = sum(it['qty'] * it['prix'] - it['remise'] for it in items)
        main_oeuvre = float(request.form.get('main_oeuvre', 0) or 0)
        petites_fourn = float(request.form.get('petites_fournitures', 0) or 0)
        remise_glob = float(request.form.get('remise', 0) or 0)
        tva_active = 1 if request.form.get('tva_active') in ('1','true','on','yes') else 0
        tva_rate = float(request.form.get('tva_rate', 18) or 18)
        tva_amount = round((total_ht - remise_glob) * tva_rate / 100, 0) if tva_active else 0
        total_ttc = total_ht + petites_fourn - remise_glob + tva_amount
        
        conn = _gdb()
        # Re-link to client if client_id provided (user changed client selection)
        client_id_new = request.form.get('client_id', '')
        client_id_new = int(client_id_new) if str(client_id_new).strip().isdigit() else None
        conn.execute("""UPDATE devis SET client_id=COALESCE(?, client_id), client_name=?, client_code=?, contact_commercial=?, objet=?,
            items_json=?, total_ht=?, petites_fournitures=?, total_ttc=?, main_oeuvre=?, remise=?, notes=?
            WHERE id=?""", (client_id_new, request.form.get('client_name',''), request.form.get('client_code',''),
            request.form.get('contact_commercial',''), request.form.get('objet',''),
            json.dumps(items), total_ht, petites_fourn, total_ttc, main_oeuvre, remise_glob,
            request.form.get('notes',''), did))
        try:
            conn.execute("UPDATE devis SET tva_active=?, tva_rate=?, tva_amount=? WHERE id=?",
                         (tva_active, tva_rate, tva_amount, did))
        except: pass
        conn.commit(); conn.close()
        flash("Devis modifié" + (f" (TVA {tva_rate:.0f}% = {tva_amount:,.0f} F)" if tva_active else ""), "success"); return redirect(url_for('devis_page'))
    
    items = json.loads(devis.get('items_json', '[]')) if isinstance(devis.get('items_json'), str) else []
    clients = get_all_clients()
    from models import db_get_all
    stock_items = db_get_all('stock_items', order='name ASC')
    current_user = get_user_by_id(session['user_id'])
    default_commercial = devis.get('contact_commercial') or (current_user['full_name'] if current_user else '')
    return render_template('devis_edit.html', page='devis', devis=devis, items=items, clients=clients,
                           stock_items=stock_items, default_commercial=default_commercial)

@app.route('/devis/delete/<int:did>')
@permission_required('proforma_edit')
def devis_delete(did):
    conn = _gdb()
    conn.execute("DELETE FROM devis WHERE id=?", (did,))
    conn.commit(); conn.close()
    flash("Devis supprimé", "success"); return redirect(url_for('devis_page'))

@app.route('/devis/duplicate/<int:did>')
@permission_required('proforma_edit')
def devis_duplicate(did):
    devis = get_devis_by_id(did)
    if devis:
        from models import create_devis
        new_id, new_ref = create_devis(
            devis.get('client_id'), devis['client_name'], devis.get('client_code',''),
            devis.get('contact_commercial',''), devis.get('objet',''),
            devis.get('items_json','[]'), devis['total_ht'], devis.get('petites_fournitures',0),
            devis['total_ttc'], devis.get('main_oeuvre',0), devis.get('remise',0),
            devis.get('notes',''), session['user_id'], devis.get('doc_type','devis'))
        flash(f"Devis dupliqué → {new_ref}", "success")
    return redirect(url_for('devis_page'))

@app.route('/devis/preview/<int:did>')
@permission_required('proforma')
def devis_preview(did):
    devis = get_devis_by_id(did)
    if not devis: return "Non trouvé", 404
    export_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'devis')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"preview_{devis['reference']}.pdf")
    devis['date'] = devis.get('created_at', '')[:10]
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"] if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_devis_pdf(devis, output, logo_path=logo_r)
    return send_file(output, as_attachment=False, download_name=f"{devis['reference']}.pdf")


# ======================== RH MODULE ========================

@app.route('/rh')
@permission_required('fichiers')
def rh_dashboard():
    emp_stats = get_employee_stats()
    conn = _gdb()
    today = datetime.now().strftime('%Y-%m-%d')
    this_month = datetime.now().strftime('%Y-%m')
    # Leave requests recap
    leaves_pending = [dict(r) for r in conn.execute("""SELECT l.*, e.first_name||' '||e.last_name as employee_name
        FROM leaves l LEFT JOIN employees e ON l.employee_id=e.id
        WHERE l.status='en_attente' ORDER BY l.created_at DESC LIMIT 10""").fetchall()]
    leaves_approved = conn.execute("SELECT COUNT(*) FROM leaves WHERE status='approuve'").fetchone()[0]
    leaves_refused = conn.execute("SELECT COUNT(*) FROM leaves WHERE status='refuse'").fetchone()[0]
    leaves_total = conn.execute("SELECT COUNT(*) FROM leaves").fetchone()[0]
    # Employee status counts
    emp_total_all = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
    emp_actif = conn.execute("SELECT COUNT(*) FROM employees WHERE status='actif'").fetchone()[0]
    emp_demission = conn.execute("SELECT COUNT(*) FROM employees WHERE status='démissionné'").fetchone()[0]
    emp_renvoye = conn.execute("SELECT COUNT(*) FROM employees WHERE status='renvoyé'").fetchone()[0]
    emp_inactif = conn.execute("SELECT COUNT(*) FROM employees WHERE status='inactif'").fetchone()[0]
    new_hires = conn.execute("SELECT COUNT(*) FROM employees WHERE hire_date LIKE ?", (this_month+'%',)).fetchone()[0]
    # Contract expiry
    expired_contracts = [dict(r) for r in conn.execute("""SELECT c.*, e.first_name||' '||e.last_name as employee_name, e.department
        FROM rh_contracts c LEFT JOIN employees e ON c.employee_id=e.id
        WHERE c.end_date < ? AND c.end_date != '' AND c.status='actif' ORDER BY c.end_date""", (today,)).fetchall()]
    expiring_soon = [dict(r) for r in conn.execute("""SELECT c.*, e.first_name||' '||e.last_name as employee_name, e.department
        FROM rh_contracts c LEFT JOIN employees e ON c.employee_id=e.id
        WHERE c.end_date >= ? AND c.end_date <= date(?, '+30 days') AND c.status='actif'
        ORDER BY c.end_date""", (today, today)).fetchall()]
    for ec in expiring_soon:
        try:
            from datetime import timedelta
            end = datetime.strptime(ec['end_date'][:10], '%Y-%m-%d')
            ec['days_remaining'] = (end - datetime.now()).days + 1
        except: ec['days_remaining'] = '?'
    for ec in expired_contracts:
        try:
            end = datetime.strptime(ec['end_date'][:10], '%Y-%m-%d')
            ec['days_remaining'] = (datetime.now() - end).days
        except: ec['days_remaining'] = '?'
    # Upcoming birthdays (next 30 days)
    birthdays = []
    all_emps = [dict(r) for r in conn.execute("SELECT * FROM employees WHERE status='actif' AND birth_date IS NOT NULL AND birth_date != ''").fetchall()]
    for e in all_emps:
        try:
            bd = e.get('birth_date','')
            if not bd or len(bd) < 10: continue
            bd_this_year = f"{datetime.now().year}-{bd[5:7]}-{bd[8:10]}"
            from datetime import timedelta
            bd_date = datetime.strptime(bd_this_year, '%Y-%m-%d')
            diff = (bd_date - datetime.now()).days
            if -1 <= diff <= 30:
                e['birthday'] = bd_this_year
                e['days_until'] = diff
                e['age'] = datetime.now().year - int(bd[:4])
                birthdays.append(e)
        except: pass
    birthdays.sort(key=lambda x: x.get('days_until', 99))
    # Recent payslips
    recent_paie = [dict(r) for r in conn.execute("""SELECT p.*, e.first_name||' '||e.last_name as employee_name
        FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id
        ORDER BY p.period DESC, e.last_name LIMIT 5""").fetchall()]
    # Recent RH activities
    try:
        rh_activities = [dict(r) for r in conn.execute("""SELECT * FROM activity_logs 
            WHERE action IN ('Personnel','Paie','Absence','Email Paie','Import','Connexion','Utilisateur')
            ORDER BY created_at DESC LIMIT 10""").fetchall()]
    except: rh_activities = []
    # Concierge summary for RH
    concierge_stats = {}
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        concierge_stats['visiteurs_jour'] = conn.execute("SELECT COUNT(*) FROM concierge_visiteurs WHERE date_visite LIKE ?", (today+'%',)).fetchone()[0]
        concierge_stats['colis_attente'] = conn.execute("SELECT COUNT(*) FROM concierge_colis WHERE statut='en_attente'").fetchone()[0]
        concierge_stats['courrier_jour'] = conn.execute("SELECT COUNT(*) FROM concierge_courrier WHERE date=?", (today,)).fetchone()[0]
    except: concierge_stats = {'visiteurs_jour':0, 'colis_attente':0, 'courrier_jour':0}
    conn.close()
    return render_template('rh_dashboard.html', page='rh', emp_stats=emp_stats,
        leaves_pending=leaves_pending, leaves_approved=leaves_approved,
        leaves_refused=leaves_refused, leaves_total=leaves_total,
        emp_total_all=emp_total_all, emp_actif=emp_actif,
        emp_demission=emp_demission, emp_renvoye=emp_renvoye, emp_inactif=emp_inactif,
        new_hires=new_hires, expired_contracts=expired_contracts, expiring_soon=expiring_soon,
        birthdays=birthdays, recent_paie=recent_paie, rh_activities=rh_activities,
        concierge_stats=concierge_stats)


@app.route('/rh/calendrier')
@login_required
def rh_calendrier():
    conn = _gdb()
    today = datetime.now()
    year = int(request.args.get('year', today.year))
    month = int(request.args.get('month', today.month))
    if month > 12: month = 1; year += 1
    if month < 1: month = 12; year -= 1
    
    uid = session['user_id']
    user = get_user_by_id(uid)
    is_admin = user and user['role'] in ('admin','dg','rh')
    
    # Load events: own events + public events (admin sees all)
    try:
        if is_admin:
            all_events = [dict(r) for r in conn.execute(
                "SELECT e.*, u.full_name as author FROM calendar_events e LEFT JOIN users u ON e.created_by=u.id ORDER BY e.start_date ASC").fetchall()]
        else:
            all_events = [dict(r) for r in conn.execute(
                "SELECT e.*, u.full_name as author FROM calendar_events e LEFT JOIN users u ON e.created_by=u.id WHERE e.created_by=? OR e.is_public=1 ORDER BY e.start_date ASC",
                (uid,)).fetchall()]
    except: all_events = []
    
    events_json = []
    for ev in all_events:
        events_json.append({
            'id': ev['id'], 'title': ev.get('title',''),
            'start': ev.get('start_date',''), 'end': ev.get('end_date','') or ev.get('start_date',''),
            'color': ev.get('color','#1A7A6D'), 'type': ev.get('category','general'),
            'time': ev.get('event_time',''), 'description': ev.get('description',''),
            'public': ev.get('is_public',0), 'mine': ev.get('created_by') == uid
        })
    
    conn.close()
    import json
    return render_template('extra_pages.html', page='rh_calendrier', 
        events=json.dumps(events_json), all_events=all_events, year=year, month=month,
        is_admin=is_admin, current_uid=uid)

@app.route('/rh/calendrier/add', methods=['POST'])
@login_required
def rh_calendrier_add():
    conn = _gdb()
    colors = {'reunion':'#1A7A6D','rappel':'#e8672a','deadline':'#c53030','visite':'#2e7d32','formation':'#7b1fa2','personnel':'#ff9800','autre':'#0D6B5E'}
    cat = request.form.get('category','autre')
    is_public = 1 if request.form.get('is_public') else 0
    conn.execute("""INSERT INTO calendar_events (title, start_date, end_date, event_time, description, category, color, created_by, is_public) 
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (request.form.get('title',''), request.form.get('event_date',''), 
         request.form.get('end_date','') or request.form.get('event_date',''),
         request.form.get('event_time',''), request.form.get('description',''),
         cat, colors.get(cat,'#1A7A6D'), session['user_id'], is_public))
    conn.commit(); conn.close()
    flash("Événement ajouté", "success")
    return redirect(f"/rh/calendrier?year={request.form.get('year','')}&month={request.form.get('month','')}")

@app.route('/rh/calendrier/delete/<int:eid>')
@login_required
def rh_calendrier_delete(eid):
    conn = _gdb()
    user = get_user_by_id(session['user_id'])
    is_admin = user and user['role'] in ('admin','dg','rh')
    ev = conn.execute("SELECT * FROM calendar_events WHERE id=?", (eid,)).fetchone()
    if ev and (is_admin or ev['created_by'] == session['user_id']):
        conn.execute("DELETE FROM calendar_events WHERE id=?", (eid,))
        conn.commit()
        flash("Événement supprimé", "success")
    else:
        flash("Vous ne pouvez supprimer que vos propres événements", "error")
    conn.close()
    return redirect('/rh/calendrier')

@app.route('/rh/personnel')
@permission_required('fichiers')
def rh_personnel():
    employees = get_all_employees(status=None)
    return render_template('rh_personnel.html', page='personnel', employees=employees)

@app.route('/rh/personnel/add', methods=['GET', 'POST'])
@permission_required('fichiers')
def rh_personnel_add():
    if request.method == 'POST':
        from models import get_db
        fields = {}
        for key in ['first_name','last_name','matricule','email','tel','position','department',
                     'hire_date','contract_type','insurance','insurance_number',
                     'emergency_contact','emergency_tel','code_rh','birth_date','gender','blood_type',
                     'birth_place','birth_city','civil_status','nationality','religion',
                     'id_type','id_expiry','id_place','resident','address','education_level',
                     'work_location','bank_account','bank_name_emp','bank_holder',
                     'fiscal_code','hourly_rate','facebook','linkedin','skype',
                     'direction','email_signature','other_info','status']:
            fields[key] = request.form.get(key, '')
        fields['salary'] = float(request.form.get('salary', 0) or 0)
        try:
            create_employee(**fields)
        except Exception as e:
            flash(f"Erreur : {str(e)}", "error")
            return redirect('/rh/personnel/add')
        # Get new employee ID for photo
        conn = get_db()
        new_emp = conn.execute("SELECT id FROM employees ORDER BY id DESC LIMIT 1").fetchone()
        conn.close()
        if new_emp and 'photo' in request.files:
            f = request.files['photo']
            if f and f.filename:
                from werkzeug.utils import secure_filename
                ext = os.path.splitext(f.filename)[1].lower()
                if ext in ('.jpg','.jpeg','.png','.webp'):
                    photo_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'photos')
                    os.makedirs(photo_dir, exist_ok=True)
                    fname = f"emp_{new_emp['id']}{ext}"
                    f.save(os.path.join(photo_dir, fname))
                    update_employee(new_emp['id'], photo=fname)
        # Handle file attachments (CV, contrat, diplômes...)
        if new_emp:
            _save_employee_files(new_emp['id'], request.files.getlist('files'))
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?', 'RH',
                    f"Employé ajouté: {request.form.get('first_name','')} {request.form.get('last_name','')}", request.remote_addr)
        flash("Employé ajouté", "success")
        return redirect(url_for('rh_personnel'))
    return render_template('rh_personnel_add.html', page='personnel')

@app.route('/rh/personnel/edit/<int:eid>', methods=['GET', 'POST'])
@permission_required('fichiers')
def rh_personnel_edit(eid):
    emp = get_employee_by_id(eid)
    if not emp:
        flash("Employé non trouvé", "error")
        return redirect(url_for('rh_personnel'))
    if request.method == 'POST':
        try:
            fields = {}
            for key in ['first_name','last_name','matricule','email','tel','position','department',
                         'hire_date','contract_type','insurance','insurance_number',
                         'emergency_contact','emergency_tel','code_rh','birth_date','gender','blood_type',
                         'birth_place','birth_city','civil_status','nationality','religion',
                         'id_type','id_expiry','id_place','resident','address','education_level',
                         'work_location','bank_account','bank_name_emp','bank_holder',
                         'fiscal_code','hourly_rate','facebook','linkedin','skype',
                         'direction','email_signature','other_info','status','cnps_number']:
                fields[key] = request.form.get(key, '')
            fields['salary'] = float(request.form.get('salary', 0) or 0)
            fields['cnps_declared'] = 1 if request.form.get('cnps_declared') else 0
            # Convert empty unique fields to None
            for uf in ['matricule', 'email']:
                if not fields.get(uf): fields[uf] = None
            update_employee(eid, **fields)
            # Photo
            if 'photo' in request.files and request.files['photo'].filename:
                photo = request.files['photo']
                from werkzeug.utils import secure_filename as _sf
                fname = f"emp_{eid}_{_sf(photo.filename)}"
                photo_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'photos')
                os.makedirs(photo_dir, exist_ok=True)
                photo.save(os.path.join(photo_dir, fname))
                update_employee(eid, photo=fname)
            # Handle signature file
            if 'signature_file' in request.files and request.files['signature_file'].filename:
                sig_f = request.files['signature_file']
                from werkzeug.utils import secure_filename as _sf2
                sig_fname = f"sig_{eid}_{_sf2(sig_f.filename)}"
                sig_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'signatures')
                os.makedirs(sig_dir, exist_ok=True)
                sig_f.save(os.path.join(sig_dir, sig_fname))
                update_employee(eid, signature_file=sig_fname)
            # Handle file attachments
            _save_employee_files(eid, request.files.getlist('files'))
            flash("Employé modifié", "success")
        except Exception as e:
            flash(f"Erreur: {str(e)}", "error")
        return redirect(url_for('rh_personnel'))
    # Load attachments
    conn = _gdb()
    attachments = [dict(r) for r in conn.execute(
        "SELECT * FROM employee_attachments WHERE employee_id=? ORDER BY created_at DESC", (eid,)).fetchall()]
    conn.close()
    return render_template('rh_personnel_edit.html', page='personnel', emp=emp, attachments=attachments)

@app.route('/rh/personnel/delete/<int:eid>')
@permission_required('fichiers')
def rh_personnel_delete(eid):
    conn = _gdb()
    emp = conn.execute("SELECT first_name, last_name FROM employees WHERE id=?", (eid,)).fetchone()
    if emp:
        conn.execute("DELETE FROM employees WHERE id=?", (eid,))
        conn.commit()
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?', 'RH',
                    f"Employé supprimé: {emp['first_name']} {emp['last_name']}", request.remote_addr)
        flash("Employé supprimé", "success")
    else:
        flash("Employé non trouvé", "error")
    conn.close()
    return redirect(url_for('rh_personnel'))

def _save_employee_files(emp_id, files):
    """Save uploaded files for an employee."""
    from werkzeug.utils import secure_filename as _sf
    conn = _gdb()
    attach_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'employees', str(emp_id))
    os.makedirs(attach_dir, exist_ok=True)
    for f in files:
        if f and f.filename:
            fname = f"{int(datetime.now().timestamp())}_{_sf(f.filename)}"
            fpath = os.path.join(attach_dir, fname)
            f.save(fpath)
            ext = os.path.splitext(f.filename)[1].lower()
            cat = 'cv' if 'cv' in f.filename.lower() else 'contrat' if 'contrat' in f.filename.lower() else 'diplome' if 'diplom' in f.filename.lower() else 'cni' if 'cni' in f.filename.lower() or 'identit' in f.filename.lower() else 'autre'
            conn.execute("""INSERT INTO employee_attachments (employee_id, file_name, file_path, file_type, file_size, category, uploaded_by)
                VALUES (?,?,?,?,?,?,?)""",
                (emp_id, f.filename, fname, ext, os.path.getsize(fpath), cat, session.get('user_id')))
    conn.commit(); conn.close()

@app.route('/rh/personnel/<int:eid>/files')
@permission_required('fichiers')
def rh_personnel_files(eid):
    """View employee attachments page."""
    emp = get_employee_by_id(eid)
    if not emp: flash("Employé non trouvé","error"); return redirect('/rh/personnel')
    conn = _gdb()
    attachments = [dict(r) for r in conn.execute(
        "SELECT * FROM employee_attachments WHERE employee_id=? ORDER BY created_at DESC", (eid,)).fetchall()]
    conn.close()
    return render_template('rh_personnel_edit.html', page='personnel', emp=emp, attachments=attachments, tab='pieces')

@app.route('/rh/personnel/file/<int:fid>/download')
@permission_required('fichiers')
def rh_personnel_file_download(fid):
    conn = _gdb()
    att = conn.execute("SELECT * FROM employee_attachments WHERE id=?", (fid,)).fetchone()
    conn.close()
    if not att: flash("Fichier non trouvé","error"); return redirect('/rh/personnel')
    fpath = os.path.join(app.config['UPLOAD_FOLDER'], 'employees', str(att['employee_id']), att['file_path'])
    if os.path.exists(fpath):
        return send_file(fpath, as_attachment=True, download_name=att['file_name'])
    flash("Fichier introuvable sur le serveur","error")
    return redirect(f'/rh/personnel/edit/{att["employee_id"]}')

@app.route('/rh/personnel/file/<int:fid>/delete')
@permission_required('fichiers')
def rh_personnel_file_delete(fid):
    conn = _gdb()
    att = conn.execute("SELECT * FROM employee_attachments WHERE id=?", (fid,)).fetchone()
    if att:
        fpath = os.path.join(app.config['UPLOAD_FOLDER'], 'employees', str(att['employee_id']), att['file_path'])
        try: os.remove(fpath)
        except: pass
        conn.execute("DELETE FROM employee_attachments WHERE id=?", (fid,))
        conn.commit()
        flash("Fichier supprimé","success")
        eid = att['employee_id']
    else:
        flash("Fichier non trouvé","error"); eid = 0
    conn.close()
    return redirect(f'/rh/personnel/edit/{eid}' if eid else '/rh/personnel')

@app.route('/rh/conges')
@permission_required('fichiers')
def rh_conges():
    tab = request.args.get('tab', 'en_attente')
    leaves = get_leaves(tab if tab != 'all' else None)
    employees = get_all_employees()
    return render_template('rh_conges.html', page='conges', tab=tab, leaves=leaves, employees=employees)

@app.route('/rh/conges/add', methods=['POST'])
@permission_required('fichiers')
def rh_conges_add():
    create_leave(
        int(request.form['employee_id']),
        request.form.get('leave_type', 'conge_annuel'),
        request.form['start_date'],
        request.form['end_date'],
        int(request.form.get('days', 0) or 0),
        request.form.get('reason', '')
    )
    flash("Demande de congé créée", "success")
    return redirect(url_for('rh_conges'))

@app.route('/rh/conges/approve/<int:lid>/<status>')
@permission_required('fichiers')
def rh_conges_status(lid, status):
    if status in ('approuve', 'refuse'):
        update_leave_status(lid, status, session.get('user_id'))
        flash(f"Congé {'approuvé' if status == 'approuve' else 'refusé'}", "success")
    return redirect(url_for('rh_conges'))

# ======================== ABSENCES ========================

@app.route('/rh/absences')
@login_required
def rh_absences():
    conn = _gdb()
    user = get_user_by_id(session['user_id'])
    perms = get_role_permissions(user['role']) if user else []
    is_rh = 'fichiers' in perms or 'admin' in perms
    
    if is_rh:
        absences = [dict(r) for r in conn.execute("""SELECT a.*, e.first_name||' '||e.last_name as employee_name
            FROM absences a LEFT JOIN employees e ON a.employee_id=e.id ORDER BY a.date DESC""").fetchall()]
    else:
        # Find employee linked to this user
        emp = conn.execute("SELECT id FROM employees WHERE email=? OR first_name||' '||last_name=?",
            (user.get('email',''), user.get('full_name',''))).fetchone()
        eid = emp['id'] if emp else 0
        absences = [dict(r) for r in conn.execute("""SELECT a.*, e.first_name||' '||e.last_name as employee_name
            FROM absences a LEFT JOIN employees e ON a.employee_id=e.id WHERE a.employee_id=? ORDER BY a.date DESC""", (eid,)).fetchall()]
    
    pending_count = conn.execute("SELECT COUNT(*) FROM absences WHERE status='en_attente'").fetchone()[0]
    conn.close()
    employees = get_all_employees(status=None) if is_rh else []
    return render_template('rh_conges.html', page='absences', absences=absences, employees=employees, is_rh=is_rh, pending_count=pending_count)

@app.route('/rh/absences/add', methods=['POST'])
@login_required
def rh_absences_add():
    user = get_user_by_id(session['user_id'])
    perms = get_role_permissions(user['role']) if user else []
    is_rh = 'fichiers' in perms or 'admin' in perms
    
    conn = _gdb()
    if is_rh:
        emp_id = int(request.form['employee_id'])
        status = 'approuve'  # RH creates = auto approved
    else:
        emp = conn.execute("SELECT id FROM employees WHERE email=? OR first_name||' '||last_name=?",
            (user.get('email',''), user.get('full_name',''))).fetchone()
        emp_id = emp['id'] if emp else 0
        status = 'en_attente'
    
    conn.execute("""INSERT INTO absences (employee_id, date, type, motif, duree, justificatif, notes, created_by, status)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (emp_id, request.form['date'],
         request.form.get('type','injustifiee'), request.form.get('motif',''),
         request.form.get('duree','journee'),
         1 if request.form.get('justificatif') else 0,
         request.form.get('notes',''), session['user_id'], status))
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Absence', f"Absence déclarée le {request.form['date']}", request.remote_addr)
    flash("Demande d'absence enregistrée" if status == 'en_attente' else "Absence enregistrée", "success")
    return redirect(url_for('rh_absences'))

@app.route('/rh/absences/approve/<int:aid>/<status>')
@permission_required('fichiers')
def rh_absences_approve(aid, status):
    if status in ('approuve', 'refuse'):
        conn = _gdb()
        conn.execute("UPDATE absences SET status=?, approved_by=? WHERE id=?", (status, session['user_id'], aid))
        conn.commit(); conn.close()
        flash(f"Absence {'approuvée' if status == 'approuve' else 'refusée'}", "success")
    return redirect(url_for('rh_absences'))

@app.route('/rh/absences/delete/<int:aid>')
@permission_required('fichiers')
def rh_absences_delete(aid):
    conn = _gdb()
    conn.execute("DELETE FROM absences WHERE id=?", (aid,))
    conn.commit(); conn.close()
    flash("Absence supprimée", "success")
    return redirect(url_for('rh_absences'))

@app.route('/rh/paie')
@permission_required('fichiers')
def rh_paie():
    period = request.args.get('period', '')
    payslips = get_payslips(period if period else None)
    employees = get_all_employees()
    return render_template('rh_paie.html', page='paie', payslips=payslips, employees=employees, period=period)

@app.route('/rh/paie/add', methods=['POST'])
@permission_required('fichiers')
def rh_paie_add():
    f = lambda k: float(request.form.get(k, 0) or 0)
    base = f('base_salary')
    primes = f('prime_transport') + f('prime_anciennete') + f('prime_astreinte') + f('prime_rendement')
    brut = base + primes
    total_autres = f('prime_mission') + f('commission')
    retenues = f('cnps_employee') + f('insurance_amount') + f('its') + f('deductions') + f('autres_retenues') + f('avances')
    net = brut + total_autres - retenues
    
    create_payslip(
        employee_id=int(request.form['employee_id']), period=request.form['period'],
        base_salary=base, commission=f('commission'),
        deductions=f('deductions'),
        insurance_amount=f('insurance_amount'), tax_amount=f('its'), net_salary=net,
        prime_transport=f('prime_transport'), prime_anciennete=f('prime_anciennete'),
        prime_astreinte=f('prime_astreinte'), prime_rendement=f('prime_rendement'),
        cnps_employee=f('cnps_employee'),
        its=f('its'), autres_retenues=f('autres_retenues'), avances=f('avances'),
        jours_travailles=int(request.form.get('jours_travailles', 26) or 26),
        heures_travaillees=f('heures_travaillees'),
        conges_payes=int(request.form.get('conges_payes', 0) or 0),
        jours_absence=int(request.form.get('jours_absence', 0) or 0),
        mode_paiement=request.form.get('mode_paiement', 'virement'),
        cnps_employer=f('cnps_employer'), prime_mission=f('prime_mission'),
    )
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Paie', f"Bulletin créé — Net: {net:,.0f} FCFA", request.remote_addr)
    flash(f"Bulletin créé — Net: {net:,.0f} FCFA", "success")
    return redirect(url_for('rh_paie'))

@app.route('/rh/paie/<int:pid>/edit', methods=['GET','POST'])
@permission_required('fichiers')
def rh_paie_edit(pid):
    p = get_payslip_detail_v2(pid)
    if not p: flash("Non trouvé","error"); return redirect(url_for('rh_paie'))
    if request.method == 'POST':
        f = lambda k: float(request.form.get(k, 0) or 0)
        base = f('base_salary')
        primes = f('prime_transport') + f('prime_anciennete') + f('prime_astreinte') + f('prime_rendement')
        brut = base + primes
        total_autres = f('prime_mission') + f('commission')
        retenues = f('cnps_employee') + f('insurance_amount') + f('its') + f('deductions') + f('autres_retenues') + f('avances')
        net = brut + total_autres - retenues
        update_payslip(pid, base_salary=base, commission=f('commission'),
            deductions=f('deductions'),
            insurance_amount=f('insurance_amount'), tax_amount=f('its'), net_salary=net,
            prime_transport=f('prime_transport'), prime_anciennete=f('prime_anciennete'),
            prime_astreinte=f('prime_astreinte'), prime_rendement=f('prime_rendement'),
            cnps_employee=f('cnps_employee'),
            its=f('its'), autres_retenues=f('autres_retenues'), avances=f('avances'),
            jours_travailles=int(request.form.get('jours_travailles', 26) or 26),
            heures_travaillees=f('heures_travaillees'),
            conges_payes=int(request.form.get('conges_payes', 0) or 0),
            jours_absence=int(request.form.get('jours_absence', 0) or 0),
            mode_paiement=request.form.get('mode_paiement', 'virement'),
            cnps_employer=f('cnps_employer'), prime_mission=f('prime_mission'))
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?', 'Paie', f"Bulletin modifié — Net: {net:,.0f} FCFA", request.remote_addr)
        flash(f"Bulletin modifié — Net: {net:,.0f} FCFA", "success")
        return redirect(url_for('rh_paie_view', pid=pid))
    employees = get_all_employees()
    return render_template('rh_paie_view.html', page='paie', p=p, employees=employees, edit_mode=True)

def _generate_bulletin_pdf(pid):
    """Generate bulletin PDF and return file path."""
    p = get_payslip_detail_v2(pid)
    if not p: return None
    
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.colors import HexColor, white
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], f'bulletin_{p["period"]}_{p["employee_name"].replace(" ","_")}.pdf')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    doc = SimpleDocTemplate(output, pagesize=A4, leftMargin=12*mm, rightMargin=12*mm, topMargin=10*mm, bottomMargin=10*mm)
    
    VERT = HexColor('#1A7A6D'); VERT_F = HexColor('#0D6B5E')
    ORANGE = HexColor('#e8672a'); ORANGE_L = HexColor('#fff3e0')
    DARK = HexColor('#333'); GREY = HexColor('#888'); LIGHT = HexColor('#f8faf9')
    VERT_L = HexColor('#e0f0ee')
    
    sw = ParagraphStyle('sw', fontSize=8, textColor=white)
    swb = ParagraphStyle('swb', fontSize=8, fontName='Helvetica-Bold', textColor=white)
    sc = ParagraphStyle('sc', fontSize=8, leading=10, textColor=DARK)
    scb = ParagraphStyle('scb', fontSize=8, fontName='Helvetica-Bold', textColor=DARK)
    sr = ParagraphStyle('sr', fontSize=8, alignment=TA_RIGHT, textColor=DARK)
    srb = ParagraphStyle('srb', fontSize=8, fontName='Helvetica-Bold', alignment=TA_RIGHT, textColor=DARK)
    srw = ParagraphStyle('srw', fontSize=8, fontName='Helvetica-Bold', alignment=TA_RIGHT, textColor=white)
    fmt = lambda x: f"{(x or 0):,.0f}"
    g = lambda k: p.get(k, 0) or 0
    
    story = []
    pw = 186*mm
    
    # === HEADER WITH LOGO ===
    logo_path = None
    for n in ['logo_ramya.png', 'logo_wannygest.png']:
        lp = os.path.join(BASE_DIR, n)
        if os.path.exists(lp): logo_path = lp; break
    
    logo_el = Paragraph("<b>RAMYA</b>", ParagraphStyle('lg',fontSize=14,fontName='Helvetica-Bold',textColor=white))
    if logo_path:
        try:
            from PIL import Image as PILImage
            img = PILImage.open(logo_path)
            iw, ih = img.size
            # Keep aspect ratio, max height 14mm
            max_h = 14*mm
            ratio = iw / ih
            logo_el = Image(logo_path, width=max_h*ratio, height=max_h)
        except:
            try: logo_el = Image(logo_path, width=14*mm, height=14*mm)
            except: pass
    
    # Period format
    period_display = p['period']
    try:
        parts = p['period'].split('-')
        if len(parts) == 2:
            import calendar
            y, m = int(parts[0]), int(parts[1])
            last_day = calendar.monthrange(y, m)[1]
            mois_noms = ['','Janvier','Février','Mars','Avril','Mai','Juin','Juillet','Août','Septembre','Octobre','Novembre','Décembre']
            period_display = f"Période: 01 au {last_day} {mois_noms[m]} {y}"
    except: pass
    
    hdr = [[
        logo_el,
        Paragraph(f"<b>RAMYA TECHNOLOGIE &amp; INNOVATION</b><br/>Abidjan, Côte d'Ivoire<br/>N°RCCM : CI-ABJ-2017-A-25092", ParagraphStyle('hi',fontSize=7,textColor=white,leading=10)),
        Paragraph(f"<b>BULLETIN DE PAIE</b><br/>{period_display}<br/>{datetime.now().strftime('%d/%m/%Y')}", ParagraphStyle('hp',fontSize=9,fontName='Helvetica-Bold',textColor=white,alignment=TA_RIGHT,leading=13))
    ]]
    ht = Table(hdr, colWidths=[35*mm, pw*0.42, pw-35*mm-pw*0.42])
    ht.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),VERT),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8),('LEFTPADDING',(0,0),(-1,-1),8),('RIGHTPADDING',(0,0),(-1,-1),8)]))
    story.extend([ht, Spacer(1, 3*mm)])
    
    # === EMPLOYEE INFO ===
    cnps_display = p.get('cnps_number','') or '-'
    if not p.get('cnps_declared'):
        cnps_display = 'Non déclaré'
    assurance_display = ''
    if p.get('insurance') or p.get('insurance_number'):
        ins_name = p.get('insurance','') or ''
        ins_num = p.get('insurance_number','') or ''
        assurance_display = f"{ins_name} {ins_num}".strip() or '-'
    
    ei = [[
        Paragraph(f"<b>Employé :</b> {p['employee_name']}", scb),
        Paragraph(f"<b>Matricule :</b> {p.get('matricule','') or '-'}", sc),
        Paragraph(f"<b>N° CNPS :</b> {cnps_display}", sc),
    ],[
        Paragraph(f"<b>Poste :</b> {p.get('position','') or '-'}", sc),
        Paragraph(f"<b>Département :</b> {(p.get('department','') or '-').upper()}", sc),
        Paragraph(f"<b>Assurance :</b> {assurance_display or '-'}", sc),
    ],[
        Paragraph(f"<b>Embauche :</b> {p.get('hire_date','') or '-'}", sc),
        Paragraph(f"<b>Jours travaillés :</b> {p.get('jours_travailles',26)}", sc),
        Paragraph(f"<b>Absences :</b> {p.get('jours_absence',0)} j", sc),
    ],[
        Paragraph(f"<b>Paiement :</b> {p.get('mode_paiement','virement')}", sc),
        Paragraph('', sc),
        Paragraph('', sc),
    ]]
    et = Table(ei, colWidths=[pw*0.4, pw*0.3, pw*0.3])
    et.setStyle(TableStyle([('GRID',(0,0),(-1,-1),0.3,HexColor('#ddd')),('BACKGROUND',(0,0),(-1,-1),LIGHT),
        ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),('LEFTPADDING',(0,0),(-1,-1),6)]))
    story.extend([et, Spacer(1, 4*mm)])
    
    cw = [pw*0.45, pw*0.18, pw*0.12, pw*0.25]
    base = g('base_salary')
    brut = base + g('prime_transport') + g('prime_anciennete') + g('prime_astreinte') + g('prime_rendement')
    
    # === GAINS ===
    gains = [
        [Paragraph('<b>GAINS</b>', swb), Paragraph('<b>Base</b>', swb), Paragraph('<b>Taux</b>', swb), Paragraph('<b>Montant</b>', swb)],
        [Paragraph('Salaire de base', sc), Paragraph(fmt(base), sr), Paragraph('', sr), Paragraph(fmt(base), srb)],
        [Paragraph('Prime de transport', sc), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(g('prime_transport')), sr)],
        [Paragraph("Prime d'ancienneté", sc), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(g('prime_anciennete')), sr)],
        [Paragraph("Prime d'astreinte", sc), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(g('prime_astreinte')), sr)],
        [Paragraph('Prime de rendement', sc), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(g('prime_rendement')), sr)],
    ]
    gt = Table(gains, colWidths=cw)
    gt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),VERT),('GRID',(0,0),(-1,-1),0.3,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),('LEFTPADDING',(0,0),(-1,-1),6)]))
    story.extend([gt, Spacer(1,1*mm)])
    
    # === SALAIRE BRUT ===
    bt = Table([[Paragraph('<b>SALAIRE BRUT</b>', swb), Paragraph('', sr), Paragraph('', sr),
                 Paragraph(f'<b>{fmt(brut)} FCFA</b>', srw)]], colWidths=cw)
    bt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),VERT_F),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),('LEFTPADDING',(0,0),(-1,-1),6),('RIGHTPADDING',(0,0),(-1,-1),6)]))
    story.extend([bt, Spacer(1,3*mm)])
    
    # === AUTRES PRIMES (hors brut) ===
    pm = g('prime_mission'); com = g('commission'); tot_ap = pm + com
    ap = [
        [Paragraph('<b>AUTRES PRIMES (hors salaire brut)</b>', swb), Paragraph('', sc), Paragraph('', sc), Paragraph('<b>Montant</b>', swb)],
        [Paragraph('Prime de mission', sc), Paragraph('', sc), Paragraph('', sc), Paragraph(fmt(pm), sr)],
        [Paragraph('Commissions', sc), Paragraph('', sc), Paragraph('', sc), Paragraph(fmt(com), sr)],
        [Paragraph('<b>Total autres primes</b>', scb), Paragraph('', sc), Paragraph('', sc), Paragraph(f'<b>{fmt(tot_ap)}</b>', srb)],
    ]
    apt = Table(ap, colWidths=cw)
    apt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),ORANGE),('GRID',(0,0),(-1,-1),0.3,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),('LEFTPADDING',(0,0),(-1,-1),6),
        ('BACKGROUND',(0,3),(-1,3),ORANGE_L)]))
    story.extend([apt, Spacer(1,3*mm)])
    
    # === RETENUES ===
    has_cnps = bool(p.get('cnps_declared')) and bool((p.get('cnps_number') or '').strip())
    plafond = min(brut, 2700000)
    if has_cnps:
        cnps_sal = g('cnps_retraite_sal') or round(plafond * 0.063)
        cnps_ret_pat = round(plafond * 0.077)
        cnps_pf = round(brut * 0.0575)
        cnps_acc = round(brut * 0.03)
    else:
        cnps_sal = 0; cnps_ret_pat = 0; cnps_pf = 0; cnps_acc = 0
    tx_app = round(brut * 0.004)
    fdfp_v = round(brut * 0.012)
    total_pat = cnps_ret_pat + cnps_pf + cnps_acc + tx_app + fdfp_v
    its = g('its'); assur = g('insurance_amount'); ded = g('deductions'); av = g('avances'); ar = g('autres_retenues')
    total_sal = cnps_sal + its + assur + ded + av + ar
    
    cw5 = [pw*0.32, pw*0.14, pw*0.14, pw*0.18, pw*0.22]
    cnps_label = 'CNPS Retraite' if has_cnps else 'CNPS Retraite (non déclaré)'
    ret = [
        [Paragraph('<b>RETENUES / COTISATIONS</b>', swb), Paragraph('<b>Part patron.</b>', swb), Paragraph('<b>Part salar.</b>', swb), Paragraph('<b>Base</b>', swb), Paragraph('<b>Montant sal.</b>', swb)],
        [Paragraph(cnps_label, sc), Paragraph('7,7%' if has_cnps else '—', sr), Paragraph('6,3%' if has_cnps else '—', sr), Paragraph(f'Plaf. {fmt(plafond)}' if has_cnps else '—', ParagraphStyle('sm',fontSize=7,alignment=TA_RIGHT,textColor=GREY)), Paragraph(fmt(cnps_sal) if has_cnps else '—', srb)],
        [Paragraph('CNPS Prestations familiales', sc), Paragraph('5,75%' if has_cnps else '—', sr), Paragraph('0%' if has_cnps else '—', sr), Paragraph(fmt(brut) if has_cnps else '—', sr), Paragraph('—', sr)],
        [Paragraph('CNPS Accidents du travail', sc), Paragraph('3%' if has_cnps else '—', sr), Paragraph('0%' if has_cnps else '—', sr), Paragraph(fmt(brut) if has_cnps else '—', sr), Paragraph('—', sr)],
        [Paragraph("Taxe d'apprentissage", sc), Paragraph('0,4%', sr), Paragraph('0%', sr), Paragraph(fmt(brut), sr), Paragraph('—', sr)],
        [Paragraph('Formation prof. (FDFP)', sc), Paragraph('1,2%', sr), Paragraph('0%', sr), Paragraph(fmt(brut), sr), Paragraph('—', sr)],
        [Paragraph('ITS (Impôt sur salaire)', sc), Paragraph('', sr), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(its), sr)],
        [Paragraph('Assurance maladie', sc), Paragraph('', sr), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(assur), sr)],
        [Paragraph('Autres déductions', sc), Paragraph('', sr), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(ded), sr)],
        [Paragraph('Avances / Prêts', sc), Paragraph('', sr), Paragraph('', sr), Paragraph('', sr), Paragraph(fmt(av), sr)],
    ]
    rt = Table(ret, colWidths=cw5)
    rt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),ORANGE),('GRID',(0,0),(-1,-1),0.3,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),('LEFTPADDING',(0,0),(-1,-1),5),
        ('BACKGROUND',(0,1),(0,5),VERT_L)]))
    story.extend([rt, Spacer(1,1*mm)])
    
    # Totals
    tt = Table([
        [Paragraph('<b>Total charges patronales</b>', swb), Paragraph(f'<b>{fmt(total_pat)} FCFA</b>', srw)],
        [Paragraph('<b>Total retenues salariales</b>', swb), Paragraph(f'<b>{fmt(total_sal)} FCFA</b>', srw)],
    ], colWidths=[pw*0.65, pw*0.35])
    tt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),VERT_F),('BACKGROUND',(0,1),(-1,1),ORANGE),
        ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),('LEFTPADDING',(0,0),(-1,-1),8),('RIGHTPADDING',(0,0),(-1,-1),8)]))
    story.extend([tt, Spacer(1,4*mm)])
    
    # === NET À PAYER ===
    net = brut + tot_ap - total_sal
    nt = Table([[Paragraph('<b>NET À PAYER</b>', ParagraphStyle('n1',fontSize=13,fontName='Helvetica-Bold',textColor=white)),
                 Paragraph(f'<b>{fmt(net)} FCFA</b>', ParagraphStyle('n2',fontSize=13,fontName='Helvetica-Bold',textColor=white,alignment=TA_RIGHT))
    ]], colWidths=[pw*0.55, pw*0.45])
    nt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),VERT),('TOPPADDING',(0,0),(-1,-1),10),('BOTTOMPADDING',(0,0),(-1,-1),10),
        ('LEFTPADDING',(0,0),(-1,-1),12),('RIGHTPADDING',(0,0),(-1,-1),12)]))
    story.extend([nt, Spacer(1,8*mm)])
    
    # Signatures
    emp_sig_content = "<b>L'Employé</b><br/><br/>"
    emp_sig_elements = []
    
    # Check for signature: either digital (canvas) or imported file
    has_sig_data = p.get('signed_at') and p.get('signature_data')
    has_sig_file = p.get('signature_file') and os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], 'signatures', p.get('signature_file','')))
    
    if has_sig_data or has_sig_file:
        # Build employee column with signature image
        sig_img = None
        try:
            import base64, io as _io
            if has_sig_file and not has_sig_data:
                # Use imported signature file
                sig_path = os.path.join(app.config['UPLOAD_FOLDER'], 'signatures', p['signature_file'])
                from PIL import Image as PILImg
                pil_img = PILImg.open(sig_path)
                if pil_img.mode == 'RGBA':
                    bg = PILImg.new('RGB', pil_img.size, (255,255,255))
                    bg.paste(pil_img, mask=pil_img.split()[3])
                    pil_img = bg
                elif pil_img.mode != 'RGB':
                    pil_img = pil_img.convert('RGB')
                sig_jpgfile = os.path.join(app.config['UPLOAD_FOLDER'], f'sig_imported_{pid}.jpg')
                pil_img.save(sig_jpgfile, 'JPEG', quality=95)
                sig_img = Image(sig_jpgfile, width=45*mm, height=16*mm)
            elif has_sig_data:
                sig_b64 = p['signature_data'].split(',')[1] if ',' in p['signature_data'] else p['signature_data']
                sig_bytes = base64.b64decode(sig_b64)
            # Save to temp file to avoid BytesIO issues with reportlab
            sig_tmpfile = os.path.join(app.config['UPLOAD_FOLDER'], f'sig_tmp_{pid}.png')
            os.makedirs(os.path.dirname(sig_tmpfile), exist_ok=True)
            # Convert to RGB JPEG to avoid PNG alpha issues
            from PIL import Image as PILImg
            pil_img = PILImg.open(_io.BytesIO(sig_bytes))
            if pil_img.mode == 'RGBA':
                bg = PILImg.new('RGB', pil_img.size, (255, 255, 255))
                bg.paste(pil_img, mask=pil_img.split()[3])
                pil_img = bg
            elif pil_img.mode != 'RGB':
                pil_img = pil_img.convert('RGB')
            sig_jpgfile = sig_tmpfile.replace('.png', '.jpg')
            pil_img.save(sig_jpgfile, 'JPEG', quality=95)
            sig_img = Image(sig_jpgfile, width=45*mm, height=16*mm)
        except:
            sig_img = None
        
        sig_date = p.get('signed_at','') or ''
        if sig_img:
            emp_sig_elements = [
                Paragraph("<b>L'Employé</b>", ParagraphStyle('s2h',fontSize=8,textColor=DARK,alignment=TA_CENTER)),
                Spacer(1, 2*mm),
                sig_img,
                Spacer(1, 1*mm),
                Paragraph(f"Lu et approuvé<br/>{'<font size=7 color=#2e7d32>✅ Signé le '+sig_date+'</font><br/>' if sig_date else ''}<b>{p['employee_name']}</b>", 
                    ParagraphStyle('s2s',fontSize=8,textColor=DARK,alignment=TA_CENTER)),
            ]
        else:
            emp_sig_elements = [
                Paragraph(f"<b>L'Employé</b><br/><br/>{'<font color=#2e7d32>✅ Signé</font><br/>' if sig_date else ''}Lu et approuvé<br/><b>{p['employee_name']}</b>", 
                    ParagraphStyle('s2f',fontSize=8,textColor=DARK,alignment=TA_CENTER)),
            ]
    else:
        emp_sig_elements = [
            Paragraph(f"<b>L'Employé</b><br/><br/><br/><br/>Lu et approuvé<br/><b>{p['employee_name']}</b>", 
                ParagraphStyle('s2',fontSize=8,textColor=DARK,alignment=TA_CENTER)),
        ]
    
    # Use nested table for employee column to stack elements
    from reportlab.platypus import KeepTogether
    emp_inner = Table([[e] for e in emp_sig_elements], colWidths=[pw*0.48])
    emp_inner.setStyle(TableStyle([('ALIGN',(0,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),1),('BOTTOMPADDING',(0,0),(-1,-1),1)]))
    
    sig = [[
        Paragraph("<b>L'Employeur</b><br/><br/><br/><br/>Signature et cachet", ParagraphStyle('s1',fontSize=8,textColor=DARK,alignment=TA_CENTER)),
        emp_inner
    ]]
    st = Table(sig, colWidths=[pw*0.5, pw*0.5])
    st.setStyle(TableStyle([('GRID',(0,0),(-1,-1),0.3,HexColor('#ddd')),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
        ('VALIGN',(0,0),(-1,-1),'TOP')]))
    story.extend([st, Spacer(1,6*mm)])
    story.append(Paragraph("Ce bulletin de paie doit être conservé sans limitation de durée (Art. 32.4 du Code du Travail de Côte d'Ivoire)", ParagraphStyle('f',fontSize=7,alignment=TA_CENTER,textColor=GREY)))
    
    doc.build(story)
    return output

@app.route('/rh/paie/<int:pid>/pdf')
@permission_required('fichiers')
def rh_paie_pdf(pid):
    output = _generate_bulletin_pdf(pid)
    if not output:
        flash("Non trouvé", "error")
        return redirect(url_for('rh_paie'))
    p = get_payslip_detail_v2(pid)
    return send_file(output, as_attachment=True, download_name=f"Bulletin_{p['period']}_{p['employee_name']}.pdf")

@app.route('/rh/paie/<int:pid>/view')
@login_required
def rh_paie_view(pid):
    p = get_payslip_detail_v2(pid)
    if not p: flash("Non trouvé","error"); return redirect('/dashboard')
    # Check access: RH/admin or own payslip
    user = get_user_by_id(session['user_id'])
    perms = get_role_permissions(user['role']) if user else []
    is_rh = 'fichiers' in perms or 'admin' in perms
    if not is_rh:
        # Check if this is the employee's own payslip
        conn = _gdb()
        emp = conn.execute("SELECT id FROM employees WHERE email=? OR LOWER(first_name||' '||last_name)=LOWER(?)",
            (user.get('email',''), user.get('full_name',''))).fetchone()
        conn.close()
        if not emp or emp['id'] != p.get('employee_id'):
            flash("Accès non autorisé","error"); return redirect('/dashboard')
    return render_template('rh_paie_view.html', page='paie', p=p)

@app.route('/rh/paie/<int:pid>/status/<status>')
@login_required
def rh_paie_status(pid, status):
    valid_statuses = ('brouillon','en_attente_compta','valide_compta','envoye')
    if status not in valid_statuses:
        flash("Statut invalide","error"); return redirect(url_for('rh_paie'))
    
    p = get_payslip_detail_v2(pid)
    if not p: flash("Bulletin non trouvé","error"); return redirect(url_for('rh_paie'))
    
    user = get_user_by_id(session['user_id'])
    uname = user['full_name'] if user else '?'
    
    update_payslip(pid, status=status)
    if status == 'envoye':
        update_payslip(pid, sent_at=datetime.now().isoformat())
    
    conn = _gdb()
    
    # Notifications based on workflow
    if status == 'en_attente_compta':
        compta_users = [dict(r) for r in conn.execute("SELECT id FROM users WHERE role IN ('comptable','admin','dg')").fetchall()]
        for cu in compta_users:
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)""",
                (cu['id'], 'bulletin', f"📋 Bulletin à valider — {p['employee_name']}",
                 f"Le bulletin de paie {p['period']} de {p['employee_name']} (Net: {p.get('net_salary',0):,.0f} F) est en attente de validation comptable.",
                 f"/rh/paie/{pid}/view"))
        conn.commit(); conn.close()
        log_activity(session['user_id'], uname, 'Paie', f"Bulletin {p['period']} {p['employee_name']} → Compta", request.remote_addr)
    
    elif status == 'valide_compta':
        rh_users = [dict(r) for r in conn.execute("SELECT id FROM users WHERE role IN ('rh','admin','dg')").fetchall()]
        for ru in rh_users:
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)""",
                (ru['id'], 'bulletin', f"✅ Bulletin validé — {p['employee_name']}",
                 f"Le bulletin {p['period']} de {p['employee_name']} a été validé par la comptabilité. Prêt à envoyer.",
                 f"/rh/paie/{pid}/view"))
        conn.commit(); conn.close()
        log_activity(session['user_id'], uname, 'Paie', f"Bulletin {p['period']} {p['employee_name']} validé compta", request.remote_addr)
    else:
        conn.close()
    
    labels = {'brouillon':'Brouillon','en_attente_compta':'En attente comptabilité','valide_compta':'Validé comptabilité','envoye':'Envoyé'}
    flash(f"Statut → {labels.get(status, status)}", "success")
    return redirect(url_for('rh_paie'))


@app.route('/rh/paie/bulk-compta', methods=['POST'])
@permission_required('fichiers')
def rh_paie_bulk_compta():
    """Send all brouillon payslips to comptabilité for validation."""
    conn = _gdb()
    brouillons = conn.execute("SELECT id FROM payslips WHERE status='brouillon'").fetchall()
    count = 0
    for b in brouillons:
        conn.execute("UPDATE payslips SET status='en_attente_compta' WHERE id=?", (b['id'],))
        count += 1
    if count:
        # Notify comptables
        compta_users = [dict(r) for r in conn.execute("SELECT id FROM users WHERE role IN ('comptable','admin','dg')").fetchall()]
        for cu in compta_users:
            conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                (cu['id'], 'bulletin', f"📋 {count} bulletin(s) à valider", 
                 f"{count} bulletin(s) de paie en attente de validation comptable.",
                 '/comptabilite/validation-paie'))
        conn.commit(); conn.close()
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?', 'Paie', 
                    f"{count} bulletins envoyés à la comptabilité", request.remote_addr)
    else:
        conn.close()
    flash(f"{count} bulletin(s) envoyé(s) à la comptabilité pour validation", "success")
    return redirect(url_for('rh_paie'))

@app.route('/comptabilite/validation-paie')
@permission_required('comptabilite')
def compta_validation_paie():
    """Comptabilité: view and validate pending payslips."""
    conn = _gdb()
    pending = [dict(r) for r in conn.execute(
        """SELECT p.*, e.first_name||' '||e.last_name as employee_name, e.department, e.position
        FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id 
        WHERE p.status='en_attente_compta' ORDER BY p.period DESC, e.last_name""").fetchall()]
    validated = [dict(r) for r in conn.execute(
        """SELECT p.*, e.first_name||' '||e.last_name as employee_name, e.department
        FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id 
        WHERE p.status='valide_compta' ORDER BY p.period DESC, e.last_name LIMIT 20""").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='compta_paie', pending=pending, validated=validated)

@app.route('/comptabilite/validation-paie/bulk-validate', methods=['POST'])
@permission_required('comptabilite')
def compta_bulk_validate():
    """Validate all pending payslips at once."""
    conn = _gdb()
    pending = conn.execute("SELECT id FROM payslips WHERE status='en_attente_compta'").fetchall()
    count = 0
    for p in pending:
        conn.execute("UPDATE payslips SET status='valide_compta' WHERE id=?", (p['id'],))
        count += 1
    if count:
        rh_users = [dict(r) for r in conn.execute("SELECT id FROM users WHERE role IN ('rh','admin','dg')").fetchall()]
        for ru in rh_users:
            conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                (ru['id'], 'bulletin', f"✅ {count} bulletin(s) validé(s)",
                 f"La comptabilité a validé {count} bulletin(s). Prêts à envoyer aux employés.",
                 '/rh/paie'))
        conn.commit(); conn.close()
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?', 'Paie',
                    f"{count} bulletins validés par comptabilité", request.remote_addr)
    else:
        conn.close()
    flash(f"{count} bulletin(s) validé(s) — retournés à la RH pour envoi", "success")
    return redirect('/comptabilite/validation-paie')

@app.route('/rh/paie/<int:pid>/delete')
@permission_required('fichiers')
def rh_paie_delete(pid):
    conn = _gdb()
    p = conn.execute("SELECT p.*, e.first_name||' '||e.last_name as employee_name FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id WHERE p.id=?", (pid,)).fetchone()
    if p:
        conn.execute("DELETE FROM payslips WHERE id=?", (pid,))
        conn.commit()
        flash(f"Bulletin de paie supprimé ({p['employee_name']} — {p['period']})", "success")
    else:
        flash("Bulletin non trouvé", "error")
    conn.close()
    return redirect(url_for('rh_paie'))

@app.route('/rh/paie/<int:pid>/email', methods=['GET','POST'])
@permission_required('fichiers')
def rh_paie_email(pid):
    p = get_payslip_detail_v2(pid)
    if not p: flash("Non trouvé","error"); return redirect(url_for('rh_paie'))
    
    if request.method == 'POST':
        to_email = request.form.get('to_email','').strip()
        smtp_host = request.form.get('smtp_host','').strip()
        smtp_port = int(request.form.get('smtp_port', 587) or 587)
        smtp_user = request.form.get('smtp_user','').strip()
        smtp_pass = request.form.get('smtp_pass','').strip()
        
        if smtp_host and smtp_user:
            save_smtp_settings(session['user_id'], smtp_host, smtp_port, smtp_user, smtp_pass)
        
        if not all([to_email, smtp_host, smtp_user, smtp_pass]):
            flash("Paramètres SMTP requis","error")
            smtp = get_admin_smtp()
            return render_template('rh_paie_view.html', page='paie', p=p, send_email=True, smtp=smtp)
        
        # Generate full bulletin PDF
        try:
            pdf_path = _generate_bulletin_pdf(pid)
            if not pdf_path:
                flash("Erreur génération PDF: bulletin non trouvé", "error")
                return redirect(url_for('rh_paie'))
        except Exception as pdf_err:
            flash(f"Erreur génération PDF: {str(pdf_err)}", "error")
            return redirect(url_for('rh_paie'))
        
        if not os.path.exists(pdf_path):
            flash("Erreur génération PDF","error")
            return redirect(url_for('rh_paie'))
        
        try:
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = to_email
            msg['Subject'] = f"Bulletin de paie — {p['period']} — {p['employee_name']}"
            
            body = f"""Bonjour {p['employee_name']},

Veuillez trouver ci-joint votre bulletin de paie pour la période {p['period']}.

Net à payer : {p.get('net_salary',0):,.0f} FCFA

Cordialement,
RAMYA TECHNOLOGIE & INNOVATION
Service des Ressources Humaines"""
            
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            with open(pdf_path, 'rb') as f:
                part = MIMEBase('application', 'pdf')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="Bulletin_{p["period"]}_{p["employee_name"]}.pdf"')
                msg.attach(part)
            
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
            server.quit()
            
            # Mark as sent
            update_payslip(pid, status='envoye', sent_at=datetime.now().isoformat())
            user = get_user_by_id(session['user_id'])
            log_activity(session['user_id'], user['full_name'] if user else '?',
                        'Email Paie', f"Bulletin {p['period']} envoyé à {to_email}", request.remote_addr)
            
            # Create notification for employee
            try:
                conn_n = _gdb()
                # Find user account linked to employee
                emp_user = conn_n.execute("SELECT id FROM users WHERE email=? OR LOWER(full_name)=LOWER(?)",
                    (to_email, p['employee_name'])).fetchone()
                user_id_notif = emp_user['id'] if emp_user else None
                conn_n.execute("""INSERT INTO notifications (user_id, employee_id, type, title, message, link)
                    VALUES (?,?,?,?,?,?)""",
                    (user_id_notif, p.get('employee_id'), 'bulletin',
                     f"Bulletin de paie — {p['period']}",
                     f"Votre bulletin de paie pour la période {p['period']} est disponible. Net à payer : {p.get('net_salary',0):,.0f} FCFA",
                     f"/rh/paie/{pid}/view"))
                conn_n.commit(); conn_n.close()
            except: pass
            
            flash(f"Bulletin envoyé par email à {to_email}", "success")
            return redirect(url_for('rh_paie'))
        except Exception as e:
            flash(f"Erreur d'envoi : {str(e)}", "error")
    
    smtp = get_admin_smtp()
    return render_template('rh_paie_view.html', page='paie', p=p, send_email=True, smtp=smtp)

# ======================== CONCIERGE ========================

@app.route('/concierge')
@permission_required('concierge')
def concierge_page():
    conn = _gdb()
    tab = request.args.get('tab', 'accueil')
    data = {'tab': tab}
    try: data['visiteurs'] = [dict(r) for r in conn.execute("SELECT * FROM concierge_visiteurs ORDER BY date_visite DESC LIMIT 50").fetchall()]
    except: data['visiteurs'] = []
    try:
        data['courrier_in'] = [dict(r) for r in conn.execute("SELECT * FROM concierge_courrier WHERE type='entrant' ORDER BY date DESC LIMIT 50").fetchall()]
        data['courrier_out'] = [dict(r) for r in conn.execute("SELECT * FROM concierge_courrier WHERE type='sortant' ORDER BY date DESC LIMIT 50").fetchall()]
    except: data['courrier_in'] = []; data['courrier_out'] = []
    try: data['cles'] = [dict(r) for r in conn.execute("SELECT * FROM concierge_cles ORDER BY nom_cle").fetchall()]
    except: data['cles'] = []
    try: data['colis'] = [dict(r) for r in conn.execute("SELECT * FROM concierge_colis ORDER BY date_reception DESC LIMIT 50").fetchall()]
    except: data['colis'] = []
    try: data['salles'] = [dict(r) for r in conn.execute("SELECT * FROM concierge_salles ORDER BY nom").fetchall()]
    except: data['salles'] = []
    try: data['reservations'] = [dict(r) for r in conn.execute("SELECT r.*, s.nom as salle_nom FROM concierge_reservations r LEFT JOIN concierge_salles s ON r.salle_id=s.id ORDER BY r.date DESC LIMIT 30").fetchall()]
    except: data['reservations'] = []
    # Stats
    data['nb_visiteurs_jour'] = len([v for v in data['visiteurs'] if v.get('date_visite','')[:10] == datetime.now().strftime('%Y-%m-%d')])
    data['nb_courrier'] = len(data.get('courrier_in',[])) + len(data.get('courrier_out',[]))
    data['nb_colis_attente'] = len([c for c in data.get('colis',[]) if c.get('statut') == 'en_attente'])
    conn.close()
    return render_template('extra_pages.html', page='concierge', **data)

@app.route('/concierge/visiteur/add', methods=['POST'])
@permission_required('concierge')
def concierge_visiteur_add():
    conn = _gdb()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS concierge_visiteurs (id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT, entreprise TEXT, motif TEXT, personne_visitee TEXT, date_visite TEXT DEFAULT CURRENT_TIMESTAMP, heure_arrivee TEXT, heure_depart TEXT, badge TEXT, notes TEXT, created_by INTEGER)")
    except: pass
    conn.execute("INSERT INTO concierge_visiteurs (nom, entreprise, motif, personne_visitee, date_visite, heure_arrivee, badge, notes, created_by) VALUES (?,?,?,?,?,?,?,?,?)",
        (request.form.get('nom',''), request.form.get('entreprise',''), request.form.get('motif',''),
         request.form.get('personne_visitee',''), request.form.get('date_visite', datetime.now().strftime('%Y-%m-%d')),
         request.form.get('heure_arrivee',''), request.form.get('badge',''), request.form.get('notes',''), session['user_id']))
    conn.commit(); conn.close()
    flash("Visiteur enregistré", "success")
    return redirect('/concierge?tab=visiteurs')

@app.route('/concierge/courrier/add', methods=['POST'])
@permission_required('concierge')
def concierge_courrier_add():
    conn = _gdb()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS concierge_courrier (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, expediteur TEXT, destinataire TEXT, objet TEXT, date TEXT, reference TEXT, statut TEXT DEFAULT 'recu', notes TEXT, created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    except: pass
    conn.execute("INSERT INTO concierge_courrier (type, expediteur, destinataire, objet, date, reference, notes, created_by) VALUES (?,?,?,?,?,?,?,?)",
        (request.form.get('type','entrant'), request.form.get('expediteur',''), request.form.get('destinataire',''),
         request.form.get('objet',''), request.form.get('date', datetime.now().strftime('%Y-%m-%d')),
         request.form.get('reference',''), request.form.get('notes',''), session['user_id']))
    conn.commit(); conn.close()
    flash("Courrier enregistré", "success")
    return redirect('/concierge?tab=courrier')

@app.route('/concierge/cle/add', methods=['POST'])
@permission_required('concierge')
def concierge_cle_add():
    conn = _gdb()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS concierge_cles (id INTEGER PRIMARY KEY AUTOINCREMENT, nom_cle TEXT, emplacement TEXT, attribue_a TEXT, statut TEXT DEFAULT 'disponible', date_remise TEXT, notes TEXT, created_by INTEGER)")
    except: pass
    conn.execute("INSERT INTO concierge_cles (nom_cle, emplacement, attribue_a, statut, notes, created_by) VALUES (?,?,?,?,?,?)",
        (request.form.get('nom_cle',''), request.form.get('emplacement',''), request.form.get('attribue_a',''),
         request.form.get('statut','disponible'), request.form.get('notes',''), session['user_id']))
    conn.commit(); conn.close()
    flash("Clé enregistrée", "success")
    return redirect('/concierge?tab=cles')

@app.route('/concierge/colis/add', methods=['POST'])
@permission_required('concierge')
def concierge_colis_add():
    conn = _gdb()
    conn.execute("INSERT INTO concierge_colis (expediteur, destinataire, description, date_reception, notes, created_by) VALUES (?,?,?,?,?,?)",
        (request.form.get('expediteur',''), request.form.get('destinataire',''), request.form.get('description',''),
         request.form.get('date_reception', datetime.now().strftime('%Y-%m-%d')), request.form.get('notes',''), session['user_id']))
    conn.commit(); conn.close()
    flash("Colis enregistré", "success")
    return redirect('/concierge?tab=colis')

@app.route('/concierge/colis/retrait/<int:cid>')
@permission_required('concierge')
def concierge_colis_retrait(cid):
    conn = _gdb()
    conn.execute("UPDATE concierge_colis SET statut='retire', date_retrait=? WHERE id=?", (datetime.now().strftime('%Y-%m-%d %H:%M'), cid))
    conn.commit(); conn.close()
    flash("Colis retiré", "success")
    return redirect('/concierge?tab=colis')

@app.route('/concierge/salle/add', methods=['POST'])
@permission_required('concierge')
def concierge_salle_add():
    conn = _gdb()
    conn.execute("INSERT INTO concierge_salles (nom, capacite, equipements) VALUES (?,?,?)",
        (request.form.get('nom',''), int(request.form.get('capacite', 10) or 10), request.form.get('equipements','')))
    conn.commit(); conn.close()
    flash("Salle ajoutée", "success")
    return redirect('/concierge?tab=salles')

@app.route('/concierge/reservation/add', methods=['POST'])
@permission_required('concierge')
def concierge_reservation_add():
    conn = _gdb()
    conn.execute("INSERT INTO concierge_reservations (salle_id, date, heure_debut, heure_fin, objet, reserve_par, created_by) VALUES (?,?,?,?,?,?,?)",
        (int(request.form.get('salle_id',0)), request.form.get('date',''), request.form.get('heure_debut',''),
         request.form.get('heure_fin',''), request.form.get('objet',''), request.form.get('reserve_par',''), session['user_id']))
    conn.commit(); conn.close()
    flash("Réservation enregistrée", "success")
    return redirect('/concierge?tab=salles')

@app.route('/concierge/visiteur/delete/<int:vid>')
@permission_required('concierge')
def concierge_visiteur_delete(vid):
    conn = _gdb(); conn.execute("DELETE FROM concierge_visiteurs WHERE id=?", (vid,)); conn.commit(); conn.close()
    flash("Visiteur supprimé", "success"); return redirect('/concierge?tab=visiteurs')

@app.route('/concierge/visiteur/depart/<int:vid>')
@permission_required('concierge')
def concierge_visiteur_depart(vid):
    conn = _gdb(); conn.execute("UPDATE concierge_visiteurs SET heure_depart=? WHERE id=?", (datetime.now().strftime('%H:%M'), vid)); conn.commit(); conn.close()
    flash("Départ enregistré", "success"); return redirect('/concierge?tab=visiteurs')

@app.route('/concierge/courrier/delete/<int:cid>')
@permission_required('concierge')
def concierge_courrier_delete(cid):
    conn = _gdb(); conn.execute("DELETE FROM concierge_courrier WHERE id=?", (cid,)); conn.commit(); conn.close()
    flash("Courrier supprimé", "success"); return redirect('/concierge?tab=courrier')

@app.route('/concierge/colis/delete/<int:cid>')
@permission_required('concierge')
def concierge_colis_delete(cid):
    conn = _gdb(); conn.execute("DELETE FROM concierge_colis WHERE id=?", (cid,)); conn.commit(); conn.close()
    flash("Colis supprimé", "success"); return redirect('/concierge?tab=colis')

@app.route('/concierge/cle/delete/<int:cid>')
@permission_required('concierge')
def concierge_cle_delete(cid):
    conn = _gdb(); conn.execute("DELETE FROM concierge_cles WHERE id=?", (cid,)); conn.commit(); conn.close()
    flash("Clé supprimée", "success"); return redirect('/concierge?tab=cles')

@app.route('/concierge/salle/delete/<int:sid>')
@permission_required('concierge')
def concierge_salle_delete(sid):
    conn = _gdb(); conn.execute("DELETE FROM concierge_salles WHERE id=?", (sid,)); conn.commit(); conn.close()
    flash("Salle supprimée", "success"); return redirect('/concierge?tab=salles')

@app.route('/concierge/reservation/delete/<int:rid>')
@permission_required('concierge')
def concierge_reservation_delete(rid):
    conn = _gdb(); conn.execute("DELETE FROM concierge_reservations WHERE id=?", (rid,)); conn.commit(); conn.close()
    flash("Réservation supprimée", "success"); return redirect('/concierge?tab=salles')


@app.route('/rh/paie/<int:pid>/sign', methods=['GET','POST'])
@login_required
def rh_paie_sign(pid):
    p = get_payslip_detail_v2(pid)
    if not p: flash("Non trouvé","error"); return redirect('/notifications')
    
    if request.method == 'POST':
        sig_data = request.form.get('signature','')
        if sig_data and len(sig_data) > 50:
            user = get_user_by_id(session['user_id'])
            conn = _gdb()
            conn.execute("UPDATE payslips SET signed_at=?, signed_by=?, signature_data=? WHERE id=?",
                (datetime.now().strftime('%Y-%m-%d %H:%M'), user['full_name'] if user else '?', sig_data, pid))
            conn.commit()
            # Notify RH
            rh_users = [dict(r) for r in conn.execute("SELECT id FROM users WHERE role IN ('rh','admin','dg')").fetchall()]
            for ru in rh_users:
                conn.execute("""INSERT INTO notifications (user_id, employee_id, type, title, message, link)
                    VALUES (?,?,?,?,?,?)""",
                    (ru['id'], p.get('employee_id'), 'signature',
                     f"Bulletin signé — {p['employee_name']}",
                     f"Le bulletin {p['period']} de {p['employee_name']} a été signé numériquement.",
                     f"/rh/paie/{pid}/view"))
            conn.commit(); conn.close()
            log_activity(session['user_id'], user['full_name'] if user else '?', 'Signature', 
                        f"Bulletin {p['period']} signé par {p['employee_name']}", request.remote_addr)
            flash("Bulletin signé avec succès ! La RH a été notifiée.", "success")
            return redirect('/notifications')
        else:
            flash("Veuillez dessiner votre signature", "error")
    
    return render_template('extra_pages.html', page='paie_sign', p=p)


@app.route('/api/notif-count')
@login_required
def api_notif_count():
    user = get_user_by_id(session['user_id'])
    try:
        from models import get_db as _ndb2
        conn = _ndb2()
        # === Check reminders and create notifications ===
        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Client reminders due today/tomorrow
        for rem in [dict(r) for r in conn.execute(
            "SELECT cr.*, c.name as client_name FROM client_reminders cr LEFT JOIN clients c ON cr.client_id=c.id WHERE cr.done=0 AND cr.date IN (?,?)", (today, tomorrow)).fetchall()]:
            if not conn.execute("SELECT id FROM notifications WHERE type='rappel' AND link=? AND created_at > datetime('now','-1 day')",
                (f"/clients/{rem['client_id']}?tab=reminders",)).fetchone():
                prefix = "📅 Aujourd'hui" if rem['date'] == today else "⏰ Demain"
                for u in conn.execute("SELECT id FROM users WHERE is_active=1").fetchall():
                    conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                        (u['id'], 'rappel', f"{prefix} — {rem['title']}",
                         f"Rappel client {rem.get('client_name','?')} : {rem['title']} ({rem['date']})",
                         f"/clients/{rem['client_id']}?tab=reminders"))
        
        # Calendar events due today/tomorrow (private = only creator, public = all users)
        for ev in [dict(r) for r in conn.execute("SELECT * FROM calendar_events WHERE start_date IN (?,?)", (today, tomorrow)).fetchall()]:
            if not conn.execute("SELECT id FROM notifications WHERE type='rappel' AND title LIKE ? AND user_id=? AND created_at > datetime('now','-1 day')",
                (f"%{ev['title']}%", ev.get('created_by', 0))).fetchone():
                prefix = "📅 Aujourd'hui" if ev.get('start_date','') == today else "⏰ Demain"
                time_str = f" à {ev['event_time']}" if ev.get('event_time') else ''
                if ev.get('is_public'):
                    targets = conn.execute("SELECT id FROM users WHERE is_active=1").fetchall()
                else:
                    targets = [{'id': ev.get('created_by', 0)}] if ev.get('created_by') else []
                for u in targets:
                    uid = u['id'] if hasattr(u, '__getitem__') else u
                    conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                        (uid, 'rappel', f"{prefix}{time_str} — {ev['title']}",
                         f"{ev.get('description','') or ev['title']}", "/rh/calendrier"))
        
        conn.commit()
        # === Count unread ===
        cnt = conn.execute("SELECT COUNT(*) FROM notifications WHERE (user_id=? OR employee_id IN (SELECT id FROM employees WHERE email=?)) AND read=0",
            (user['id'], user.get('email',''))).fetchone()[0]
        conn.close()
        return jsonify({'count': cnt})
    except Exception as e:
        import sys; print(f"NOTIF_API_ERR: {e}", file=sys.stderr)
        return jsonify({'count': 0})




# ======================== PORTAIL CLIENT ========================

@app.route('/portail')
def portail_login_page():
    if 'client_user_id' in session:
        return redirect('/portail/dashboard')
    return render_template('extra_pages.html', page='portail_login')

@app.route('/portail/login', methods=['POST'])
def portail_login():
    username = request.form.get('username','')
    password = request.form.get('password','')
    conn = _gdb()
    cu = conn.execute("SELECT * FROM client_users WHERE username=? AND is_active=1", (username,)).fetchone()
    if cu:
        cu = dict(cu)
        import hashlib
        ph = hashlib.sha256((password + (cu.get('salt','') or '')).encode()).hexdigest()
        if ph == cu['password_hash']:
            # Check account approval status
            status = cu.get('account_status') or 'approved'
            if status == 'pending':
                conn.close()
                flash("⏳ Votre compte est en attente de validation par l'administrateur. Vous recevrez un email dès l'activation.", "warning")
                return redirect('/portail')
            if status == 'rejected':
                reason = cu.get('reject_reason','')
                conn.close()
                flash(f"❌ Votre demande d'inscription a été refusée. {('Motif : ' + reason) if reason else 'Contactez l''administrateur.'}", "error")
                return redirect('/portail')
            # approved → login OK
            session['client_user_id'] = cu['id']
            session['client_id'] = cu['client_id']
            session['client_name'] = cu['full_name']
            conn.execute("UPDATE client_users SET last_login=? WHERE id=?", (datetime.now().isoformat(), cu['id']))
            conn.commit(); conn.close()
            return redirect('/portail/dashboard')
    conn.close()
    flash("Identifiants incorrects", "error")
    return redirect('/portail')

@app.route('/portail/logout')
def portail_logout():
    session.pop('client_user_id', None)
    session.pop('client_id', None)
    session.pop('client_name', None)
    return redirect('/portail')

def portail_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if 'client_user_id' not in session:
            return redirect('/portail')
        return f(*args, **kwargs)
    return decorated

@app.route('/portail/dashboard')
@portail_required
def portail_dashboard():
    conn = _gdb()
    cid = session['client_id']
    today = datetime.now().strftime('%Y-%m-%d')
    data = {}
    data['requests'] = [dict(r) for r in conn.execute("SELECT * FROM client_requests WHERE client_id=? ORDER BY created_at DESC LIMIT 10", (cid,)).fetchall()]
    data['interventions'] = [dict(r) for r in conn.execute("SELECT * FROM interventions WHERE client_id=? ORDER BY scheduled_date DESC LIMIT 10", (cid,)).fetchall()]
    data['invoices'] = [dict(r) for r in conn.execute("SELECT * FROM invoices WHERE client_id=? ORDER BY created_at DESC LIMIT 10", (cid,)).fetchall()]
    data['pending'] = len([r for r in data['requests'] if r['status'] in ('soumise','en_cours')])
    data['total_interventions'] = len(data['interventions'])
    # === Planning : interventions à venir (planifiées, en cours) ===
    try:
        data['planning'] = [dict(r) for r in conn.execute(
            "SELECT * FROM interventions WHERE client_id=? AND scheduled_date >= ? AND status IN ('planifiee','en_cours','travaux_termines','controle_qualite') ORDER BY scheduled_date, scheduled_time LIMIT 10",
            (cid, today)).fetchall()]
    except: data['planning'] = []
    data['planning_count'] = len(data['planning'])
    # Equipment / systems installed
    try:
        data['equipments'] = [dict(r) for r in conn.execute(
            "SELECT * FROM tech_center WHERE client_id=? ORDER BY next_maintenance", (cid,)).fetchall()]
    except: data['equipments'] = []
    # Client info
    try:
        data['client'] = dict(conn.execute("SELECT * FROM clients WHERE id=?", (cid,)).fetchone())
    except: data['client'] = {}
    # Current user profile (with photo)
    try:
        data['me'] = dict(conn.execute("SELECT * FROM client_users WHERE id=?", (session['client_user_id'],)).fetchone())
    except: data['me'] = {}
    # === Alertes intervention (action requise + infos en temps réel) ===
    alerts = []
    try:
        # Étape 4 : dates de livraison à confirmer
        for r in conn.execute(
            """SELECT id, title, delivery_proposed_date FROM interventions
               WHERE client_id=? AND delivery_proposed_date IS NOT NULL
               AND delivery_proposed_date != ''
               AND COALESCE(delivery_client_status,'') IN ('','pending')
               AND status != 'livre'""", (cid,)).fetchall():
            alerts.append({'type':'action_required','icon':'📅',
                'title':f"Date de livraison à confirmer — {r['title']}",
                'message':f"Date proposée : {r['delivery_proposed_date']}. Validez ou proposez une alternative.",
                'link':'/portail/interventions','color':'#e8672a','bg':'#fff3e0'})
        # Étape 5 : livrées non notées
        for r in conn.execute(
            """SELECT id, title FROM interventions
               WHERE client_id=? AND status='livre'
               AND (rating_stars IS NULL OR rating_stars = 0)""", (cid,)).fetchall():
            alerts.append({'type':'rating','icon':'⭐',
                'title':f"Évaluez votre service — {r['title']}",
                'message':"Votre chantier est livré. Donnez votre avis en 5 étoiles.",
                'link':'/portail/interventions','color':'#ffb400','bg':'#fff8e1'})
        # Travaux en cours
        for r in conn.execute(
            """SELECT id, title, status, technician_name FROM interventions
               WHERE client_id=? AND status IN ('en_cours','travaux_termines','controle_qualite')
               ORDER BY updated_at DESC LIMIT 3""", (cid,)).fetchall():
            si = {'en_cours':('⚡','Travaux en cours','#1a7a6d','#e3f2fd'),
                  'travaux_termines':('🏗️','Travaux terminés — en attente CQ','#1565c0','#e3f2fd'),
                  'controle_qualite':('🔍','Contrôle qualité validé','#7b1fa2','#f3e5f5')}.get(
                  r['status'],('🔧','En cours','#1a7a6d','#e3f2fd'))
            alerts.append({'type':'info','icon':si[0],
                'title':f"{si[1]} — {r['title']}",
                'message':(f"Technicien : {r['technician_name']}" if r['technician_name'] else ""),
                'link':'/portail/interventions','color':si[2],'bg':si[3]})
    except Exception: pass
    data['alerts'] = alerts
    conn.close()
    tmrw_s = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    return render_template('extra_pages.html', page='portail_dashboard', data=data, today_str=today, tomorrow_str=tmrw_s)


@app.route('/portail/profile', methods=['GET','POST'])
@portail_required
def portail_profile():
    """Page profil client : photo + infos éditables."""
    conn = _gdb()
    cuid = session['client_user_id']
    if request.method == 'POST':
        # Photo upload → base64 data URI stocké directement en BDD (survit aux redéploiements Render)
        photo_file = request.files.get('photo')
        photo_data_uri = None
        photo_error = None
        if photo_file and photo_file.filename:
            ext = os.path.splitext(photo_file.filename)[1].lower()
            if ext not in ('.jpg','.jpeg','.png','.webp','.gif','.bmp'):
                photo_error = f"Format non supporté : {ext}. Utilisez JPG, PNG, WebP ou GIF."
            else:
                try:
                    import base64, io
                    from PIL import Image as _PIL
                    raw = photo_file.read()
                    if not raw or len(raw) < 100:
                        photo_error = "Fichier vide ou corrompu"
                    elif len(raw) > 15 * 1024 * 1024:
                        photo_error = "Fichier trop volumineux (>15 Mo)"
                    else:
                        img = _PIL.open(io.BytesIO(raw))
                        # Convertir en RGB (JPEG ne supporte pas la transparence)
                        if img.mode in ('RGBA','LA','P'):
                            bg = _PIL.new('RGB', img.size, (255,255,255))
                            if img.mode == 'RGBA':
                                bg.paste(img, mask=img.split()[-1])
                            elif img.mode == 'LA':
                                bg.paste(img.convert('RGBA'), mask=img.convert('RGBA').split()[-1])
                            else:
                                rgba = img.convert('RGBA')
                                bg.paste(rgba, mask=rgba.split()[-1])
                            img = bg
                        elif img.mode != 'RGB':
                            img = img.convert('RGB')
                        img.thumbnail((400, 400), _PIL.LANCZOS)
                        buf = io.BytesIO()
                        img.save(buf, format='JPEG', quality=80, optimize=True)
                        b64 = base64.b64encode(buf.getvalue()).decode('ascii')
                        photo_data_uri = f"data:image/jpeg;base64,{b64}"
                except Exception as e:
                    photo_error = f"Erreur de traitement : {type(e).__name__}: {str(e)[:100]}"
                    import traceback; traceback.print_exc()
        # S'assurer que la colonne photo existe (au cas où la migration n'aurait pas tourné)
        try:
            conn.execute("ALTER TABLE client_users ADD COLUMN photo TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            pass  # colonne déjà présente
        # Password change?
        new_pw = request.form.get('new_password', '').strip()
        old_pw = request.form.get('old_password', '').strip()
        pw_changed = False
        if new_pw:
            cu = dict(conn.execute("SELECT * FROM client_users WHERE id=?", (cuid,)).fetchone())
            import hashlib
            old_ph = hashlib.sha256((old_pw + (cu.get('salt','') or '')).encode()).hexdigest()
            if old_ph == cu['password_hash']:
                new_ph = hashlib.sha256((new_pw + (cu.get('salt','') or '')).encode()).hexdigest()
                conn.execute("UPDATE client_users SET password_hash=? WHERE id=?", (new_ph, cuid))
                pw_changed = True
            else:
                flash("❌ Ancien mot de passe incorrect", "error")
                conn.close()
                return redirect('/portail/profile')
        updates = {
            'full_name': request.form.get('full_name','').strip(),
            'email': request.form.get('email','').strip(),
            'tel': request.form.get('tel','').strip(),
            'address': request.form.get('address','').strip(),
        }
        # Update infos basiques séparément de la photo (pour ne pas perdre la photo si updates plantent)
        try:
            conn.execute("UPDATE client_users SET full_name=?, email=?, tel=?, address=? WHERE id=?",
                         (updates['full_name'], updates['email'], updates['tel'], updates['address'], cuid))
            conn.commit()
            session['client_name'] = updates['full_name']
        except Exception as e:
            conn.rollback()
            flash(f"Erreur infos : {type(e).__name__}", "error")
        # Update photo séparément
        photo_saved = False
        if photo_data_uri:
            try:
                conn.execute("UPDATE client_users SET photo=? WHERE id=?", (photo_data_uri, cuid))
                conn.commit()
                # Vérification : relire la photo qui vient d'être sauvée
                check = conn.execute("SELECT LENGTH(photo) as n FROM client_users WHERE id=?", (cuid,)).fetchone()
                if check and check['n'] and check['n'] > 100:
                    photo_saved = True
                else:
                    flash("⚠️ La photo n'a pas été persistée correctement (taille 0 après écriture)", "error")
            except Exception as e:
                conn.rollback()
                flash(f"❌ Erreur sauvegarde photo : {type(e).__name__}: {e}", "error")
        conn.close()
        # Construire le message final
        msg_parts = []
        if photo_saved: msg_parts.append(f"photo enregistrée ({len(photo_data_uri)//1024} Ko)")
        if pw_changed: msg_parts.append("mot de passe modifié")
        if photo_error: flash(f"❌ Photo non enregistrée : {photo_error}", "error")
        if msg_parts: flash("✅ Profil mis à jour — " + ", ".join(msg_parts), "success")
        elif not photo_error: flash("✅ Profil mis à jour", "success")
        return redirect('/portail/profile')
    # GET
    me = dict(conn.execute("SELECT * FROM client_users WHERE id=?", (cuid,)).fetchone() or {})
    client = dict(conn.execute("SELECT * FROM clients WHERE id=?", (session['client_id'],)).fetchone() or {})
    conn.close()
    return render_template('extra_pages.html', page='portail_profile', me=me, client=client)


@app.route('/portail/planning')
@portail_required
def portail_planning():
    """Page dédiée à la planification côté client : calendrier et liste des RDV à venir."""
    conn = _gdb()
    cid = session['client_id']
    today = datetime.now().strftime('%Y-%m-%d')
    # Interventions à venir (non terminées/livrées)
    upcoming = [dict(r) for r in conn.execute(
        "SELECT * FROM interventions WHERE client_id=? AND scheduled_date >= ? ORDER BY scheduled_date, scheduled_time",
        (cid, today)).fetchall()]
    # Interventions passées (10 dernières)
    past = [dict(r) for r in conn.execute(
        "SELECT * FROM interventions WHERE client_id=? AND scheduled_date < ? ORDER BY scheduled_date DESC LIMIT 10",
        (cid, today)).fetchall()]
    # Regrouper par date pour affichage chronologique
    from collections import OrderedDict
    by_date = OrderedDict()
    for i in upcoming:
        d = i.get('scheduled_date') or 'Non planifié'
        by_date.setdefault(d, []).append(i)
    conn.close()
    return render_template('extra_pages.html', page='portail_planning',
                           upcoming=upcoming, past=past, by_date=by_date, today=today)

@app.route('/portail/demande', methods=['GET','POST'])
@portail_required
def portail_demande():
    if request.method == 'POST':
        # GPS fields (may be empty if geolocation refused)
        gps_lat = request.form.get('gps_lat', '').strip()
        gps_lng = request.form.get('gps_lng', '').strip()
        gps_acc = request.form.get('gps_accuracy', '').strip()
        try: gps_lat = float(gps_lat) if gps_lat else None
        except: gps_lat = None
        try: gps_lng = float(gps_lng) if gps_lng else None
        except: gps_lng = None
        try: gps_acc = float(gps_acc) if gps_acc else None
        except: gps_acc = None
        
        conn = _gdb()
        conn.execute("""INSERT INTO client_requests
            (client_id, client_user_id, title, type, priority, description, site_address, gps_lat, gps_lng, gps_accuracy)
            VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (session['client_id'], session['client_user_id'], request.form.get('title',''),
             request.form.get('type','intervention'), request.form.get('priority','normale'),
             request.form.get('description',''), request.form.get('site_address',''),
             gps_lat, gps_lng, gps_acc))
        req_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        # Notify admins/technicians
        gps_info = ""
        if gps_lat and gps_lng:
            gps_info = f" 📍 GPS: {gps_lat:.6f}, {gps_lng:.6f}"
        users = conn.execute("SELECT id FROM users WHERE role IN ('admin','technicien','resp_projet') AND is_active=1").fetchall()
        for u in users:
            conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                (u['id'], 'client_request', f"📩 Nouvelle demande — {session.get('client_name','')}",
                 f"{request.form.get('title','')}{gps_info}", f"/admin/client-requests"))
        conn.commit(); conn.close()
        flash("Demande soumise avec succès" + (" (localisation GPS enregistrée)" if gps_lat else ""), "success")
        return redirect('/portail/dashboard')
    return render_template('extra_pages.html', page='portail_demande')

@app.route('/portail/interventions')
@portail_required
def portail_interventions():
    conn = _gdb()
    interventions = [dict(r) for r in conn.execute("SELECT * FROM interventions WHERE client_id=? ORDER BY scheduled_date DESC", (session['client_id'],)).fetchall()]
    # Load daily reports for each intervention
    for inter in interventions:
        try:
            inter['daily_reports'] = [dict(r) for r in conn.execute(
                "SELECT * FROM intervention_daily_reports WHERE intervention_id=? ORDER BY date DESC", (inter['id'],)).fetchall()]
        except: inter['daily_reports'] = []
    conn.close()
    return render_template('extra_pages.html', page='portail_interventions', interventions=interventions)

@app.route('/portail/factures')
@portail_required
def portail_factures():
    conn = _gdb()
    factures = [dict(r) for r in conn.execute("SELECT * FROM invoices WHERE client_id=? ORDER BY created_at DESC", (session['client_id'],)).fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='portail_factures', factures=factures)

@app.route('/portail/messages/<int:rid>', methods=['GET','POST'])
@portail_required
def portail_messages(rid):
    conn = _gdb()
    req = conn.execute("SELECT * FROM client_requests WHERE id=? AND client_id=?", (rid, session['client_id'])).fetchone()
    if not req: flash("Demande non trouvée","error"); conn.close(); return redirect('/portail/dashboard')
    if request.method == 'POST':
        conn.execute("INSERT INTO client_messages (request_id, client_user_id, sender_type, message) VALUES (?,?,'client',?)",
            (rid, session['client_user_id'], request.form.get('message','')))
        conn.commit()
    messages = [dict(r) for r in conn.execute("SELECT * FROM client_messages WHERE request_id=? ORDER BY created_at ASC", (rid,)).fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='portail_messages', req=dict(req), messages=messages)

@app.route('/admin/client-requests/<int:rid>/messages', methods=['GET','POST'])
@login_required
def admin_client_request_messages(rid):
    """Voir et répondre aux messages d'un client — accès commercial/admin/coordinateur/DG uniquement."""
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin','dg','commercial','coordinateur','resp_projet'):
        flash("Accès réservé aux commerciaux, admin et coordinateurs", "error")
        return redirect(url_for('dashboard'))
    conn = _gdb()
    req = conn.execute(
        "SELECT cr.*, c.name as client_name_full FROM client_requests cr LEFT JOIN clients c ON cr.client_id=c.id WHERE cr.id=?", (rid,)).fetchone()
    if not req:
        flash("Demande introuvable", "error"); conn.close()
        return redirect('/admin/client-requests')
    req = dict(req)
    if request.method == 'POST':
        msg = request.form.get('message', '').strip()
        if msg:
            conn.execute("""INSERT INTO client_messages (request_id, client_user_id, sender_type, sender_id, sender_name, message)
                VALUES (?,?,?,?,?,?)""",
                (rid, 0, 'staff', session['user_id'], user['full_name'], msg))
            # Notify the client
            try:
                cu = conn.execute("SELECT id FROM client_users WHERE client_id=? AND is_active=1", (req.get('client_id'),)).fetchall()
                for c in cu:
                    conn.execute("""INSERT INTO notifications (client_user_id, type, title, message, link)
                        VALUES (?,?,?,?,?)""",
                        (c['id'], 'client_message',
                         f"💬 Nouveau message — {req.get('title','')[:40]}",
                         f"{user['full_name']} a répondu à votre demande",
                         f"/portail/messages/{rid}"))
            except: pass
            conn.commit()
    messages = [dict(r) for r in conn.execute(
        "SELECT * FROM client_messages WHERE request_id=? ORDER BY created_at ASC", (rid,)).fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='admin_client_messages',
                           req=req, messages=messages)


# === Admin / Commercial / Coordinateur: manage client requests ===
@app.route('/admin/client-requests')
@login_required
def admin_client_requests():
    # Seuls admin, commerciaux, coordinateurs peuvent voir/répondre aux demandes client
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin','dg','commercial','coordinateur','resp_projet'):
        flash("Accès réservé aux commerciaux, admin et coordinateurs", "error")
        return redirect(url_for('dashboard'))
    conn = _gdb()
    requests_list = [dict(r) for r in conn.execute(
        "SELECT cr.*, c.name as client_name FROM client_requests cr LEFT JOIN clients c ON cr.client_id=c.id ORDER BY cr.created_at DESC").fetchall()]
    technicians = [dict(r) for r in conn.execute("SELECT id, full_name FROM users WHERE role IN ('technicien','admin') AND is_active=1").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='client_requests', requests_list=requests_list, technicians=technicians)

@app.route('/admin/client-requests/<int:rid>/convert', methods=['POST'])
@permission_required('admin')
def admin_convert_request(rid):
    conn = _gdb()
    req = conn.execute("SELECT * FROM client_requests WHERE id=?", (rid,)).fetchone()
    if not req: flash("Demande non trouvée","error"); conn.close(); return redirect('/admin/client-requests')
    req = dict(req)
    tech_id = int(request.form.get('technician_id',0) or 0)
    tech_name = ''
    if tech_id:
        tu = conn.execute("SELECT full_name FROM users WHERE id=?", (tech_id,)).fetchone()
        tech_name = tu['full_name'] if tu else ''
    client = conn.execute("SELECT name FROM clients WHERE id=?", (req['client_id'],)).fetchone()
    client_name = client['name'] if client else ''
    ref = f"INT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    conn.execute("""INSERT INTO interventions (reference, title, type, client_id, client_name, site_address,
        technician_id, technician_name, scheduled_date, priority, description, is_billable, created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,1,?)""",
        (ref, req['title'], req.get('type','intervention'), req['client_id'], client_name,
         req.get('site_address',''), tech_id, tech_name,
         request.form.get('scheduled_date', datetime.now().strftime('%Y-%m-%d')),
         req.get('priority','normale'), req.get('description',''), session['user_id']))
    int_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    # Transférer les coordonnées GPS s'il y en a
    if req.get('gps_lat') and req.get('gps_lng'):
        try:
            conn.execute("UPDATE interventions SET gps_lat=?, gps_lng=? WHERE id=?",
                         (req['gps_lat'], req['gps_lng'], int_id))
        except: pass
    conn.execute("UPDATE client_requests SET status='convertie', intervention_id=?, assigned_to=? WHERE id=?", (int_id, tech_id, rid))
    # Notify technician — include GPS info if available
    if tech_id:
        msg = req['title']
        if req.get('gps_lat') and req.get('gps_lng'):
            msg += f" 📍 GPS : {req['gps_lat']:.5f}, {req['gps_lng']:.5f}"
        conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
            (tech_id, 'intervention', f"🔧 Intervention {ref} — {client_name}", msg, '/interventions/tech'))
    conn.commit(); conn.close()
    flash(f"Demande convertie en intervention {ref}" + (" (coordonnées GPS transférées)" if req.get('gps_lat') else ""), "success")
    return redirect('/admin/client-requests')

@app.route('/admin/client-users')
@permission_required('admin')
def admin_client_users():
    conn = _gdb()
    users_list = [dict(r) for r in conn.execute(
        "SELECT cu.*, c.name as client_name FROM client_users cu LEFT JOIN clients c ON cu.client_id=c.id ORDER BY cu.created_at DESC").fetchall()]
    # Compte les pending pour le badge
    pending = [u for u in users_list if (u.get('account_status') or 'approved') == 'pending']
    clients = [dict(r) for r in conn.execute("SELECT id, name FROM clients ORDER BY name").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='client_users', users_list=users_list,
                           clients=clients, pending_count=len(pending))

@app.route('/admin/client-users/pending')
@permission_required('admin')
def admin_client_users_pending():
    """Liste des comptes clients en attente de validation."""
    conn = _gdb()
    pending = [dict(r) for r in conn.execute(
        """SELECT cu.*, c.name as client_name FROM client_users cu
           LEFT JOIN clients c ON cu.client_id=c.id
           WHERE cu.account_status='pending' ORDER BY cu.created_at DESC""").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='client_users_pending', pending=pending)

@app.route('/admin/client-users/approve/<int:cuid>', methods=['POST','GET'])
@permission_required('admin')
def admin_client_user_approve(cuid):
    """Approuve un compte client (l'active) et notifie."""
    conn = _gdb()
    cu = conn.execute("SELECT cu.*, c.name as client_name FROM client_users cu LEFT JOIN clients c ON cu.client_id=c.id WHERE cu.id=?", (cuid,)).fetchone()
    if not cu:
        flash("Compte introuvable", "error"); conn.close()
        return redirect('/admin/client-users/pending')
    cu = dict(cu)
    now_iso = datetime.now().isoformat()
    try:
        conn.execute("""UPDATE client_users SET account_status='approved',
                        approved_by=?, approved_at=?, reject_reason='' WHERE id=?""",
                     (session['user_id'], now_iso, cuid))
    except: pass
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Admin',
                 f"Compte client approuvé : {cu['username']} ({cu['client_name']})", request.remote_addr)
    flash(f"✅ Compte de {cu['full_name']} ({cu['username']}) approuvé. Le client peut maintenant se connecter.", "success")
    return redirect('/admin/client-users/pending')

@app.route('/admin/client-users/reject/<int:cuid>', methods=['POST'])
@permission_required('admin')
def admin_client_user_reject(cuid):
    reason = request.form.get('reason', '').strip()
    conn = _gdb()
    cu = conn.execute("SELECT username, full_name, client_id FROM client_users WHERE id=?", (cuid,)).fetchone()
    if not cu: flash("Compte introuvable", "error"); conn.close(); return redirect('/admin/client-users/pending')
    try:
        conn.execute("UPDATE client_users SET account_status='rejected', reject_reason=? WHERE id=?",
                     (reason, cuid))
    except: pass
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Admin',
                 f"Compte client refusé : {cu['username']} (motif: {reason[:40]})", request.remote_addr)
    flash(f"❌ Compte de {cu['full_name']} refusé.", "warning")
    return redirect('/admin/client-users/pending')

@app.route('/admin/client-users/add', methods=['POST'])
@permission_required('admin')
def admin_client_user_add():
    import hashlib, secrets
    salt = secrets.token_hex(16)
    pw = request.form.get('password','client2026')
    ph = hashlib.sha256((pw + salt).encode()).hexdigest()
    conn = _gdb()
    try:
        # Comptes créés par l'admin directement sont déjà approuvés
        try:
            conn.execute("""INSERT INTO client_users
                (client_id, username, password_hash, salt, full_name, email, tel, account_status)
                VALUES (?,?,?,?,?,?,?, 'approved')""",
                (int(request.form.get('client_id',0)), request.form.get('username',''),
                 ph, salt, request.form.get('full_name',''),
                 request.form.get('email',''), request.form.get('tel','')))
        except:
            conn.execute("""INSERT INTO client_users
                (client_id, username, password_hash, salt, full_name, email, tel)
                VALUES (?,?,?,?,?,?,?)""",
                (int(request.form.get('client_id',0)), request.form.get('username',''),
                 ph, salt, request.form.get('full_name',''),
                 request.form.get('email',''), request.form.get('tel','')))
        conn.commit(); flash("Compte client créé (déjà approuvé)", "success")
    except: flash("Ce nom d'utilisateur existe déjà","error")
    conn.close()
    return redirect('/admin/client-users')



# ======================== MODULE INITIALIZATION ========================

@app.route('/admin/modules')
@permission_required('admin')
def admin_modules():
    conn = _gdb()
    modules = [dict(r) for r in conn.execute("SELECT * FROM module_settings ORDER BY id").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='admin_modules', modules=modules)

@app.route('/admin/modules/toggle/<module>')
@permission_required('admin')
def admin_module_toggle(module):
    conn = _gdb()
    m = conn.execute("SELECT is_active FROM module_settings WHERE module=?", (module,)).fetchone()
    if m:
        new_val = 0 if m['is_active'] else 1
        conn.execute("UPDATE module_settings SET is_active=? WHERE module=?", (new_val, module))
        conn.commit()
        flash(f"Module {'activé' if new_val else 'désactivé'}", "success")
    conn.close()
    return redirect('/admin/modules')

# ======================== CLIENT SELF-REGISTRATION ========================

@app.route('/portail/register', methods=['GET','POST'])
def portail_register():
    if request.method == 'POST':
        import hashlib, secrets
        username = request.form.get('username','').strip()
        password = request.form.get('password','')
        email = request.form.get('email','').strip()
        full_name = request.form.get('full_name','').strip()
        company = request.form.get('company','').strip()
        tel = request.form.get('tel','').strip()
        
        if not username or not password or not company:
            flash("Tous les champs obligatoires doivent être remplis", "error")
            return redirect('/portail/register')
        
        conn = _gdb()
        # Check username not taken
        existing = conn.execute("SELECT id FROM client_users WHERE username=?", (username,)).fetchone()
        if existing:
            flash("Ce nom d'utilisateur est déjà pris", "error")
            conn.close(); return redirect('/portail/register')
        
        # Create or find client
        client = conn.execute("SELECT id FROM clients WHERE name=? OR email=?", (company, email)).fetchone()
        if client:
            client_id = client['id']
        else:
            conn.execute("INSERT INTO clients (name, tel, email, contact_name, address, notes, created_by) VALUES (?,?,?,?,?,?,0)",
                (company, tel, email, full_name, '', 'Inscription portail'))
            client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        # Create client_user with status 'pending' (requires admin approval)
        salt = secrets.token_hex(16)
        ph = hashlib.sha256((password + salt).encode()).hexdigest()
        try:
            # Try with new v47 columns
            try:
                conn.execute("""INSERT INTO client_users
                    (client_id, username, password_hash, salt, full_name, email, tel, account_status)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (client_id, username, ph, salt, full_name, email, tel, 'pending'))
            except:
                # Fallback if columns don't yet exist
                conn.execute("""INSERT INTO client_users
                    (client_id, username, password_hash, salt, full_name, email, tel)
                    VALUES (?,?,?,?,?,?,?)""",
                    (client_id, username, ph, salt, full_name, email, tel))
            new_cu_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            # Notify admins about pending registration
            try:
                for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','dg','comptable') AND is_active=1").fetchall():
                    conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                        VALUES (?,?,?,?,?)""",
                        (u['id'], 'client_register',
                         f"⏳ Nouveau compte client à valider",
                         f"{company} — {full_name} ({username})", "/admin/client-users/pending"))
            except: pass
            conn.commit()
            conn.close()
            # NO auto-login anymore — admin must approve first
            flash("✅ Votre inscription a été enregistrée avec succès. Un administrateur va valider votre compte sous peu. Vous serez notifié par email dès l'activation.", "success")
            return redirect('/portail')
        except Exception as e:
            flash(f"Erreur lors de la création du compte", "error")
            conn.close()
            return redirect('/portail/register')
    
    return render_template('extra_pages.html', page='portail_register')

# ======================== FICHE D'INTERVENTION ========================

@app.route('/interventions/<int:iid>/fiche')
@login_required
def intervention_fiche(iid):
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=?", (iid,)).fetchone()
    if not inter: flash("Intervention non trouvée","error"); conn.close(); return redirect('/interventions')
    inter = dict(inter)
    # Client info
    if inter.get('client_id'):
        client = conn.execute("SELECT * FROM clients WHERE id=?", (inter['client_id'],)).fetchone()
        inter['client_info'] = dict(client) if client else {}
    # Technician info
    if inter.get('technician_id'):
        tech = conn.execute("SELECT * FROM users WHERE id=?", (inter['technician_id'],)).fetchone()
        inter['tech_info'] = dict(tech) if tech else {}
    # Project info
    if inter.get('project_id'):
        proj = conn.execute("SELECT name FROM projects WHERE id=?", (inter['project_id'],)).fetchone()
        inter['project_name'] = proj['name'] if proj else ''
    # Equipment
    equips = []
    if inter.get('client_id'):
        equips = [dict(r) for r in conn.execute("SELECT * FROM tech_center WHERE client_id=?", (inter['client_id'],)).fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='fiche_intervention', inter=inter, equips=equips, now_str=datetime.now().strftime('%d/%m/%Y %H:%M'))



# ======================== PROGRAMME DU JOUR ========================

@app.route('/interventions/programme')
@login_required
def interventions_programme():
    conn = _gdb()
    today = datetime.now().strftime('%Y-%m-%d')
    date = request.args.get('date', today)
    user = get_user_by_id(session['user_id'])
    is_admin = user and user['role'] in ('admin','dg','resp_projet')
    
    if is_admin:
        interventions = [dict(r) for r in conn.execute(
            "SELECT * FROM interventions WHERE scheduled_date=? ORDER BY scheduled_time, id", (date,)).fetchall()]
    else:
        interventions = [dict(r) for r in conn.execute(
            "SELECT * FROM interventions WHERE scheduled_date=? AND (technician_id=? OR created_by=?) ORDER BY scheduled_time",
            (date, session['user_id'], session['user_id'])).fetchall()]
    
    # Enrich each intervention with client info (zone = city or site_address, code = client_code)
    for inter in interventions:
        try:
            inter['daily_reports'] = [dict(r) for r in conn.execute(
                "SELECT * FROM intervention_daily_reports WHERE intervention_id=? ORDER BY date DESC", (inter['id'],)).fetchall()]
        except: inter['daily_reports'] = []
        # Pull client code + zone from clients table
        inter['client_code'] = ''
        inter['zone'] = ''
        if inter.get('client_id'):
            try:
                c = conn.execute("SELECT client_code, city, address FROM clients WHERE id=?", (inter['client_id'],)).fetchone()
                if c:
                    inter['client_code'] = c['client_code'] or ''
                    inter['zone'] = c['city'] or c['address'] or ''
            except: pass
        # Fallback: use site_address as zone if no client city
        if not inter.get('zone') and inter.get('site_address'):
            inter['zone'] = inter['site_address']
    
    # Group by type for the 3 tables (entretien / depannage / installation = projet neuf)
    by_type = {'entretien': [], 'depannage': [], 'projet_neuf': []}
    for inter in interventions:
        t = (inter.get('type') or '').lower().strip()
        if t in ('entretien','maintenance'):
            by_type['entretien'].append(inter)
        elif t in ('depannage','depanage','panne','dépannage'):
            by_type['depannage'].append(inter)
        else:
            # installation, projet, autre => projet neuf
            by_type['projet_neuf'].append(inter)
    
    # Format date for display (DD/MM/YYYY)
    try:
        y,m,d = date.split('-')
        date_display = f"{d}/{m}/{y}"
    except: date_display = date
    
    conn.close()
    return render_template('extra_pages.html', page='programme_jour', interventions=interventions,
                          by_type=by_type, date=date, date_display=date_display, today=today, is_admin=is_admin)

@app.route('/interventions/<int:iid>/daily-report', methods=['POST'])
@login_required
def intervention_daily_report(iid):
    conn = _gdb()
    # Save images
    img_names = []
    files = request.files.getlist('photos')
    for f in files:
        if f and f.filename:
            ext = os.path.splitext(f.filename)[1].lower()
            if ext in ('.jpg','.jpeg','.png','.webp'):
                fname = f"int_{iid}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(img_names)}{ext}"
                fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'interventions')
                os.makedirs(fdir, exist_ok=True)
                f.save(os.path.join(fdir, fname))
                img_names.append(fname)
    
    conn.execute("""INSERT INTO intervention_daily_reports 
        (intervention_id, date, report, progress, technician_id, images) VALUES (?,?,?,?,?,?)""",
        (iid, datetime.now().strftime('%Y-%m-%d'), request.form.get('report',''),
         int(request.form.get('progress',0) or 0), session['user_id'],
         ','.join(img_names)))
    conn.commit(); conn.close()
    flash("Rapport enregistré avec photos", "success")
    return redirect(request.referrer or '/interventions/tech')

@app.route('/uploads/interventions/<path:filename>')
def intervention_file(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'interventions'), filename)

@app.route('/interventions/<int:iid>/quality', methods=['POST'])
@permission_required('controle_qualite')
def intervention_quality(iid):
    """ÉTAPE 2 : Coordinateur fait le contrôle qualité."""
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=?", (iid,)).fetchone()
    if not inter: flash("Intervention introuvable","error"); conn.close(); return redirect('/interventions')
    inter = dict(inter)
    
    # Save quality photos
    img_names = []
    files = request.files.getlist('photos')
    for f in files:
        if f and f.filename:
            ext = os.path.splitext(f.filename)[1].lower()
            if ext in ('.jpg','.jpeg','.png','.webp'):
                fname = f"qc_{iid}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(img_names)}{ext}"
                fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'interventions')
                os.makedirs(fdir, exist_ok=True)
                f.save(os.path.join(fdir, fname))
                img_names.append(fname)
    
    cq_status = request.form.get('cq_status', 'passed')  # passed | failed
    cq_comments = request.form.get('quality_report', '')
    user = get_user_by_id(session['user_id'])
    actor_name = user['full_name'] if user else '?'
    actor_role = user['role'] if user else ''
    now_iso = datetime.now().isoformat()
    
    conn.execute("""UPDATE interventions SET 
        quality_report=?, quality_date=?, quality_by=?,
        cq_status=?, cq_at=?, cq_by=?, cq_by_name=?, cq_comments=?, cq_photos=?,
        status=? WHERE id=?""",
        (cq_comments + ('\n📸 ' + ','.join(img_names) if img_names else ''),
         datetime.now().strftime('%Y-%m-%d'), session['user_id'],
         cq_status, now_iso, session['user_id'], actor_name, cq_comments, json.dumps(img_names),
         'controle_qualite' if cq_status == 'passed' else 'en_cours',  # si CQ failed, retour à en_cours
         iid))
    
    _timeline_add(conn, iid, 'cq', f"🔍 Contrôle qualité : {cq_status.upper()}",
        cq_comments[:200], session['user_id'], actor_name, actor_role)
    
    if cq_status == 'passed':
        _notify_client(conn, iid, f"🔍 Contrôle qualité validé — {inter['reference']}",
            f"Le contrôle qualité de « {inter['title']} » est validé. Nous allons vous proposer une date de livraison.")
        # Notifier techs pour info
        if inter.get('technician_id'):
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?,?,?,?,?)""",
                (inter['technician_id'], 'intervention', f"✅ CQ validé — {inter['reference']}",
                 f"Le coordinateur a validé le contrôle qualité. Prochaine étape : proposition de date de livraison.",
                 f"/interventions/{iid}/fiche"))
    else:
        # CQ failed → retour au technicien
        if inter.get('technician_id'):
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?,?,?,?,?)""",
                (inter['technician_id'], 'intervention', f"⚠️ CQ refusé — reprise nécessaire — {inter['reference']}",
                 f"Le coordinateur a refusé le contrôle qualité. Motif : {cq_comments[:100]}",
                 f"/interventions/{iid}/fiche"))
    
    conn.commit(); conn.close()
    flash(f"Contrôle qualité {'validé ✅' if cq_status=='passed' else 'refusé ❌ — intervention remise en cours'}", "success" if cq_status == 'passed' else "warning")
    return redirect(request.referrer or f'/interventions/{iid}/fiche')


@app.route('/interventions/<int:iid>/propose-delivery', methods=['POST'])
@permission_required('controle_qualite')
def intervention_propose_delivery(iid):
    """ÉTAPE 3 : Coordinateur/Admin propose une date de livraison au client."""
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=?", (iid,)).fetchone()
    if not inter: flash("Intervention introuvable","error"); conn.close(); return redirect('/interventions')
    inter = dict(inter)
    
    proposed = request.form.get('delivery_date', '')
    if not proposed:
        flash("Date invalide","error"); conn.close(); return redirect(request.referrer or '/interventions')
    
    user = get_user_by_id(session['user_id'])
    actor_name = user['full_name'] if user else '?'
    actor_role = user['role'] if user else ''
    now_iso = datetime.now().isoformat()
    
    conn.execute("""UPDATE interventions SET
        delivery_proposed_date=?, delivery_proposed_at=?, delivery_proposed_by=?,
        delivery_client_status='pending' WHERE id=?""",
        (proposed, now_iso, session['user_id'], iid))
    
    _timeline_add(conn, iid, 'propose_delivery',
        f"📅 Date de livraison proposée : {proposed}",
        f"Proposée par {actor_name}", session['user_id'], actor_name, actor_role)
    
    _notify_client(conn, iid, f"📅 Date de livraison proposée — {inter['reference']}",
        f"Nous proposons la date du {proposed} pour la livraison de « {inter['title']} ». Merci de valider ou proposer une autre date depuis votre portail.")
    
    conn.commit(); conn.close()
    flash(f"Date de livraison proposée au client : {proposed}", "success")
    return redirect(request.referrer or f'/interventions/{iid}/fiche')


@app.route('/portail/intervention/<int:iid>/delivery-response', methods=['POST'])
@portail_required
def portail_delivery_response(iid):
    """ÉTAPE 4 : Client valide ou propose une autre date."""
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=? AND client_id=?",
                         (iid, session['client_id'])).fetchone()
    if not inter: flash("Intervention introuvable","error"); conn.close(); return redirect('/portail/interventions')
    inter = dict(inter)
    
    response = request.form.get('response', '')  # 'accept' | 'propose'
    comment = request.form.get('comment', '')
    proposed_date = request.form.get('proposed_date', '')
    now_iso = datetime.now().isoformat()
    client_name = session.get('client_name', 'Client')
    
    if response == 'accept':
        conn.execute("""UPDATE interventions SET
            delivery_client_status='accepted',
            delivery_client_comment=?,
            delivery_client_answered_at=? WHERE id=?""",
            (comment, now_iso, iid))
        _timeline_add(conn, iid, 'client_accept_delivery',
            "✅ Client a accepté la date de livraison",
            f"{client_name} a confirmé la date du {inter.get('delivery_proposed_date','')}. {comment[:150]}")
        # Notifier coordinateurs
        for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','dg','resp_projet','coordinateur') AND is_active=1").fetchall():
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?,?,?,?,?)""",
                (u['id'], 'intervention',
                 f"✅ Client a validé la date — {inter['reference']}",
                 f"{client_name} accepte la livraison le {inter.get('delivery_proposed_date','')}",
                 f"/interventions/{iid}/fiche"))
        flash("✅ Date de livraison validée. Nos équipes vous contacteront avant la date convenue.", "success")
    else:
        # Propose une autre date
        if not proposed_date:
            flash("Veuillez indiquer une date alternative","error"); conn.close()
            return redirect('/portail/interventions')
        conn.execute("""UPDATE interventions SET
            delivery_client_status='proposed',
            delivery_client_proposed_date=?,
            delivery_client_comment=?,
            delivery_client_answered_at=? WHERE id=?""",
            (proposed_date, comment, now_iso, iid))
        _timeline_add(conn, iid, 'client_propose_delivery',
            f"📅 Client propose une autre date : {proposed_date}",
            f"{client_name} : {comment[:150]}")
        for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','dg','resp_projet','coordinateur') AND is_active=1").fetchall():
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?,?,?,?,?)""",
                (u['id'], 'intervention',
                 f"📅 Client propose une autre date — {inter['reference']}",
                 f"{client_name} propose le {proposed_date} au lieu du {inter.get('delivery_proposed_date','')}. {comment[:80]}",
                 f"/interventions/{iid}/fiche"))
        flash(f"Votre proposition de date ({proposed_date}) a été transmise à nos équipes.", "success")
    
    conn.commit(); conn.close()
    return redirect('/portail/interventions')


@app.route('/interventions/<int:iid>/deliver', methods=['POST'])
@permission_required('livraison_intervention')
def intervention_deliver(iid):
    """ÉTAPE 5 : Livraison du site avec 3 signatures (coordinateur, client, tech)."""
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=?", (iid,)).fetchone()
    if not inter: flash("Intervention introuvable","error"); conn.close(); return redirect('/interventions')
    inter = dict(inter)
    
    now_iso = datetime.now().isoformat()
    bon_ref = f"BL-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    user = get_user_by_id(session['user_id'])
    actor_name = user['full_name'] if user else '?'
    actor_role = user['role'] if user else ''
    
    conn.execute("""UPDATE interventions SET status='livre',
        delivery_date=?, delivery_note=?, end_date=?,
        delivered_at=?, delivered_by=?,
        delivery_bon_ref=?,
        delivery_signed_client=?,
        delivery_signed_coordinator=?,
        delivery_signed_technicien=?
        WHERE id=?""",
        (datetime.now().strftime('%Y-%m-%d'), request.form.get('delivery_note',''),
         datetime.now().strftime('%Y-%m-%d'),
         now_iso, session['user_id'],
         bon_ref,
         request.form.get('sig_client', ''),
         request.form.get('sig_coordinator', ''),
         request.form.get('sig_technicien', ''),
         iid))
    
    _timeline_add(conn, iid, 'delivered',
        f"📦 Site livré au client — Bon {bon_ref}",
        f"Livré par {actor_name} en présence du client et du responsable technique",
        session['user_id'], actor_name, actor_role)
    
    # Auto-billing
    if inter.get('is_billable') and (inter.get('total_cost') or 0) > 0:
        try:
            auto_ecriture(conn, datetime.now().strftime('%Y-%m-%d'),
                f"Livraison {inter['reference']} — {inter['client_name']}",
                '411', '706', inter['total_cost'] or 0, inter['reference'])
        except: pass
    if inter.get('task_id'):
        try: conn.execute("UPDATE tasks SET status='terminee' WHERE id=?", (inter['task_id'],))
        except: pass
    
    # Notifier le client — lui demander d'évaluer
    _notify_client(conn, iid, f"📦 Chantier livré — {inter['reference']}",
        f"Votre chantier « {inter['title']} » est livré. Bon de livraison {bon_ref}. Merci de nous laisser une note et un commentaire depuis votre portail !")
    
    # Notifier la COMPTABILITÉ pour émission et envoi de la facture définitive de recouvrement
    try:
        amount_str = ""
        if inter.get('total_cost'):
            amount_str = f" · Montant : {float(inter['total_cost']):,.0f} F"
        for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','dg','comptable') AND is_active=1").fetchall():
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?, ?, ?, ?, ?)""",
                (u['id'], 'facture_a_emettre',
                 f"💰 Facture définitive à émettre — {inter['reference']}",
                 f"Chantier livré pour {inter.get('client_name','-')} (BL {bon_ref}){amount_str}. "
                 f"Émettez et envoyez la facture définitive par email pour recouvrement.",
                 f"/comptabilite/facture/new?intervention_id={iid}"))
    except Exception:
        pass
    
    conn.commit(); conn.close()
    flash(f"Site livré — Bon de livraison {bon_ref} · la comptabilité a été notifiée pour émettre la facture définitive", "success")
    return redirect(request.referrer or f'/interventions/{iid}/fiche')


@app.route('/interventions/<int:iid>/bon-livraison.pdf')
@login_required
def intervention_bon_livraison_pdf(iid):
    """Génère et télécharge le PDF du bon de livraison."""
    conn = _gdb()
    inter = conn.execute(
        """SELECT i.*, c.client_code as client_code_full,
                  u.full_name as delivered_by_name
           FROM interventions i
           LEFT JOIN clients c ON i.client_id = c.id
           LEFT JOIN users u ON i.delivered_by = u.id
           WHERE i.id=?""", (iid,)).fetchone()
    conn.close()
    if not inter:
        flash("Intervention introuvable", "error")
        return redirect('/interventions')
    inter = dict(inter)
    if inter.get('status') != 'livre' or not inter.get('delivery_bon_ref'):
        flash("Le bon de livraison n'est disponible qu'après livraison du chantier", "error")
        return redirect(f'/interventions/{iid}/fiche')
    inter.setdefault('client_code', inter.get('client_code_full'))
    
    from rapport_core import generate_bon_livraison_pdf
    export_dir = os.path.join(BASE_DIR, 'exports')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"BL-{inter['delivery_bon_ref']}.pdf")
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"]
                   if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_bon_livraison_pdf(inter, output, logo_path=logo_r)
    return send_file(output, as_attachment=True,
                     download_name=f"BonLivraison-{inter['delivery_bon_ref']}.pdf")


@app.route('/interventions/<int:iid>/attestation.pdf')
@login_required
def intervention_attestation_pdf(iid):
    """Génère et télécharge l'attestation de bonne exécution (à soumettre au client)."""
    conn = _gdb()
    inter = conn.execute(
        """SELECT i.*, c.client_code as client_code_full
           FROM interventions i
           LEFT JOIN clients c ON i.client_id = c.id
           WHERE i.id=?""", (iid,)).fetchone()
    conn.close()
    if not inter:
        flash("Intervention introuvable", "error"); return redirect('/interventions')
    inter = dict(inter)
    if inter.get('status') != 'livre':
        flash("L'attestation n'est disponible qu'après livraison du chantier", "error")
        return redirect(f'/interventions/{iid}/fiche')
    inter.setdefault('client_code', inter.get('client_code_full'))
    
    from rapport_core import generate_attestation_pdf
    export_dir = os.path.join(BASE_DIR, 'exports')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"Attestation-{inter.get('delivery_bon_ref') or inter['reference']}.pdf")
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"]
                   if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_attestation_pdf(inter, output, logo_path=logo_r)
    return send_file(output, as_attachment=True,
                     download_name=f"Attestation-{inter['reference']}.pdf")


# ===== Portail client : téléchargement des documents de livraison =====
@app.route('/portail/intervention/<int:iid>/bon-livraison.pdf')
@portail_required
def portail_bon_livraison(iid):
    conn = _gdb()
    inter = conn.execute(
        """SELECT i.*, c.client_code as client_code_full,
                  u.full_name as delivered_by_name
           FROM interventions i
           LEFT JOIN clients c ON i.client_id = c.id
           LEFT JOIN users u ON i.delivered_by = u.id
           WHERE i.id=? AND i.client_id=?""", (iid, session['client_id'])).fetchone()
    conn.close()
    if not inter:
        flash("Intervention introuvable", "error"); return redirect('/portail/interventions')
    inter = dict(inter)
    if inter.get('status') != 'livre' or not inter.get('delivery_bon_ref'):
        flash("Le bon de livraison n'est pas encore disponible", "error")
        return redirect('/portail/interventions')
    inter.setdefault('client_code', inter.get('client_code_full'))
    from rapport_core import generate_bon_livraison_pdf
    export_dir = os.path.join(BASE_DIR, 'exports')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"BL-{inter['delivery_bon_ref']}.pdf")
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"]
                   if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_bon_livraison_pdf(inter, output, logo_path=logo_r)
    return send_file(output, as_attachment=True,
                     download_name=f"BonLivraison-{inter['delivery_bon_ref']}.pdf")


@app.route('/portail/intervention/<int:iid>/attestation.pdf')
@portail_required
def portail_attestation(iid):
    conn = _gdb()
    inter = conn.execute(
        """SELECT i.*, c.client_code as client_code_full
           FROM interventions i
           LEFT JOIN clients c ON i.client_id = c.id
           WHERE i.id=? AND i.client_id=?""", (iid, session['client_id'])).fetchone()
    conn.close()
    if not inter:
        flash("Intervention introuvable", "error"); return redirect('/portail/interventions')
    inter = dict(inter)
    if inter.get('status') != 'livre':
        flash("L'attestation n'est pas encore disponible", "error")
        return redirect('/portail/interventions')
    inter.setdefault('client_code', inter.get('client_code_full'))
    from rapport_core import generate_attestation_pdf
    export_dir = os.path.join(BASE_DIR, 'exports')
    os.makedirs(export_dir, exist_ok=True)
    output = os.path.join(export_dir, f"Attestation-{inter.get('delivery_bon_ref') or inter['reference']}.pdf")
    logo_r = next((os.path.join(BASE_DIR, n) for n in ["logo_ramya.png","logo_wannygest.png"]
                   if os.path.exists(os.path.join(BASE_DIR, n))), None)
    generate_attestation_pdf(inter, output, logo_path=logo_r)
    return send_file(output, as_attachment=True,
                     download_name=f"Attestation-{inter['reference']}.pdf")


@app.route('/portail/intervention/<int:iid>/rate', methods=['POST'])
@portail_required
def portail_rate_intervention(iid):
    """Le client donne sa note (1-5 étoiles) + commentaire."""
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=? AND client_id=?",
                         (iid, session['client_id'])).fetchone()
    if not inter: flash("Intervention introuvable","error"); conn.close(); return redirect('/portail/interventions')
    inter = dict(inter)
    if inter.get('status') != 'livre':
        flash("Vous ne pouvez évaluer qu'une intervention livrée","error"); conn.close()
        return redirect('/portail/interventions')
    
    stars = int(request.form.get('stars', 5) or 5)
    stars = max(1, min(5, stars))
    comment = request.form.get('comment', '')
    conn.execute("""UPDATE interventions SET rating_stars=?, rating_comment=?, rating_at=? WHERE id=?""",
        (stars, comment, datetime.now().isoformat(), iid))
    _timeline_add(conn, iid, 'rating',
        f"⭐ Évaluation client : {stars}/5",
        comment[:200])
    # Notifier equipes
    for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','dg','resp_projet','coordinateur','commercial') AND is_active=1").fetchall():
        conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
            VALUES (?,?,?,?,?)""",
            (u['id'], 'rating',
             f"⭐ Note client : {stars}/5 — {inter['reference']}",
             f"{session.get('client_name','')} a évalué le chantier. {comment[:80]}",
             f"/interventions/{iid}/fiche"))
    conn.commit(); conn.close()
    flash(f"Merci pour votre évaluation ({stars}/5) !", "success")
    return redirect('/portail/interventions')


@app.route('/portail/intervention/<int:iid>/validate', methods=['POST'])
@portail_required
def portail_validate_intervention(iid):
    """Endpoint historique — garde pour compat."""
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=? AND client_id=?", (iid, session['client_id'])).fetchone()
    if inter and inter['status'] == 'travaux_termines':
        conn.execute("UPDATE interventions SET client_validated=1, client_validation_date=? WHERE id=?",
            (datetime.now().strftime('%Y-%m-%d'), iid))
        conn.commit()
        flash("Travaux validés. Le coordinateur va procéder au contrôle qualité.", "success")
    conn.close()
    return redirect('/portail/interventions')

# ======================== MODULE RESET ========================

@app.route('/admin/modules/reset', methods=['POST'])
@permission_required('admin')
def admin_modules_reset():
    conn = _gdb()
    modules_to_reset = request.form.getlist('modules')
    
    table_map = {
        'comptabilite': ['invoices','pieces_caisse','caisse_entrees','caisse_sorties','ecritures_comptables',
                         'bilans','bank_accounts','bank_transfers','caisses','caisse_operations','weekly_cash_reports','caisse_virements'],
        'rh': ['employees','payslips','leaves','absences','rh_contracts','rh_trainings','rh_announcements'],
        'projets': ['projects','tasks','task_comments'],
        'centre_technique': ['interventions','intervention_daily_reports','tech_center','visit_reports'],
        'crm': ['clients','prospects','prospect_notes','prospect_tasks','prospect_offers','prospect_reminders',
                'client_notes','client_attachments','client_reminders','devis','devis_templates'],
        'concierge': ['concierge_visiteurs','concierge_courrier','concierge_colis','concierge_cles',
                      'concierge_salles','concierge_reservations'],
        'tracking': ['tracking_vehicles','tracking_history','tracking_alerts','tracking_geofences'],
        'stock': ['stock_items','stock_categories','stock_movements','achats_fournisseurs',
                  'achats_commandes','achats_demandes','achats_devis','achats_contrats'],
        'it': ['it_equipment','it_tickets','it_logs'],
        'portail_client': ['client_users','client_requests','client_messages'],
    }
    
    count = 0
    for mod in modules_to_reset:
        tables = table_map.get(mod, [])
        for tbl in tables:
            try:
                conn.execute(f"DELETE FROM {tbl}")
                count += 1
            except: pass
    
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Admin', 
                f"Reset modules: {', '.join(modules_to_reset)} ({count} tables vidées)", request.remote_addr)
    flash(f"Modules réinitialisés : {', '.join(modules_to_reset)} ({count} tables vidées)", "success")
    return redirect('/admin/modules')


# ======================== MULTI-CAISSES ========================

@app.route('/comptabilite/caisses')
@permission_required('comptabilite')
def compta_caisses():
    conn = _gdb()
    caisses = [dict(r) for r in conn.execute("SELECT * FROM caisses ORDER BY name").fetchall()]
    # Recalculate solde for each caisse
    for ca in caisses:
        entrees = conn.execute("SELECT COALESCE(SUM(amount),0) FROM caisse_operations WHERE caisse_id=? AND type='entree'", (ca['id'],)).fetchone()[0]
        sorties = conn.execute("SELECT COALESCE(SUM(amount),0) FROM caisse_operations WHERE caisse_id=? AND type='sortie'", (ca['id'],)).fetchone()[0]
        trans_in = conn.execute("SELECT COALESCE(SUM(amount),0) FROM caisse_operations WHERE dest_caisse_id=? AND type='transfert'", (ca['id'],)).fetchone()[0]
        trans_out = conn.execute("SELECT COALESCE(SUM(amount),0) FROM caisse_operations WHERE source_caisse_id=? AND type='transfert'", (ca['id'],)).fetchone()[0]
        ca['solde_calcule'] = ca['solde_initial'] + entrees + trans_in - sorties - trans_out
        ca['nb_ops'] = conn.execute("SELECT COUNT(*) FROM caisse_operations WHERE caisse_id=?", (ca['id'],)).fetchone()[0]
    users = [dict(r) for r in conn.execute("SELECT id, full_name FROM users WHERE is_active=1").fetchall()]
    total = sum(ca['solde_calcule'] for ca in caisses)
    conn.close()
    return render_template('extra_pages.html', page='caisses', caisses=caisses, users=users, total=total)

@app.route('/comptabilite/caisses/add', methods=['POST'])
@permission_required('comptabilite_edit')
def compta_caisse_add():
    conn = _gdb()
    resp_id = int(request.form.get('responsible_id',0) or 0)
    resp_name = ''
    if resp_id:
        u = conn.execute("SELECT full_name FROM users WHERE id=?", (resp_id,)).fetchone()
        resp_name = u['full_name'] if u else ''
    solde = float(request.form.get('solde_initial',0) or 0)
    conn.execute("INSERT INTO caisses (name, description, responsible_id, responsible_name, solde_initial, solde_actuel, created_by) VALUES (?,?,?,?,?,?,?)",
        (request.form.get('name',''), request.form.get('description',''), resp_id, resp_name, solde, solde, session['user_id']))
    conn.commit(); conn.close()
    flash("Caisse créée", "success")
    return redirect('/comptabilite/caisses')


@app.route('/comptabilite/caisses/<int:cid>/delete', methods=['POST','GET'])
@permission_required('comptabilite_edit')
def compta_caisse_delete(cid):
    """Supprime une caisse — soft delete si des opérations existent, sinon DELETE."""
    conn = _gdb()
    ca = conn.execute("SELECT * FROM caisses WHERE id=?", (cid,)).fetchone()
    if not ca:
        flash("Caisse non trouvée", "error"); conn.close()
        return redirect('/comptabilite/caisses')
    ca = dict(ca)
    # Check solde
    if abs(ca.get('solde_actuel') or 0) > 0.01 and not request.args.get('force'):
        flash(f"⚠️ Impossible de supprimer « {ca['name']} » : solde actuel = {ca['solde_actuel']:,.0f} F. Videz la caisse d'abord (transfert vers autre caisse) ou utilisez 'Forcer la suppression'.", "error")
        conn.close()
        return redirect('/comptabilite/caisses')
    # Check opérations
    nb_ops = conn.execute("SELECT COUNT(*) FROM caisse_operations WHERE caisse_id=?", (cid,)).fetchone()[0]
    if nb_ops > 0 and not request.args.get('force'):
        # Soft delete : désactive la caisse
        conn.execute("UPDATE caisses SET is_active=0 WHERE id=?", (cid,))
        conn.commit(); conn.close()
        flash(f"✅ Caisse « {ca['name']} » désactivée ({nb_ops} opération(s) conservées en historique).", "success")
        return redirect('/comptabilite/caisses')
    # Hard delete : pas d'opérations ou force=1
    try:
        if request.args.get('force'):
            conn.execute("DELETE FROM caisse_operations WHERE caisse_id=?", (cid,))
        conn.execute("DELETE FROM caisses WHERE id=?", (cid,))
        conn.commit()
        flash(f"✅ Caisse « {ca['name']} » supprimée définitivement.", "success")
    except Exception as e:
        flash(f"Erreur: {e}", "error")
    conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Caisse',
                 f"Suppression caisse {ca['name']} (force={bool(request.args.get('force'))})", request.remote_addr)
    return redirect('/comptabilite/caisses')


# ======================== VIREMENTS BANQUE → CAISSE (avec validation DG) ========================

@app.route('/comptabilite/virements')
@permission_required('comptabilite')
def compta_virements_list():
    """Liste de tous les virements banque → caisse avec leur statut."""
    conn = _gdb()
    virements = [dict(r) for r in conn.execute("SELECT * FROM caisse_virements ORDER BY created_at DESC LIMIT 100").fetchall()]
    banks = [dict(r) for r in conn.execute("SELECT id, name FROM bank_accounts").fetchall()]
    caisses = [dict(r) for r in conn.execute("SELECT id, name FROM caisses WHERE is_active=1 ORDER BY name").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='virements_list',
                           virements=virements, banks=banks, caisses=caisses,
                           now=datetime.now().strftime('%Y-%m-%d'))

@app.route('/comptabilite/virement/new', methods=['POST'])
@permission_required('comptabilite')
def compta_virement_new():
    """Crée une demande de virement banque → caisse (en attente validation DG)."""
    conn = _gdb()
    ref = f"VIR-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    amount = float(request.form.get('amount', 0) or 0)
    if amount <= 0:
        flash("Montant invalide", "error"); conn.close()
        return redirect('/comptabilite/virements')
    bank_id = int(request.form.get('bank_account_id', 0) or 0) or None
    caisse_id = int(request.form.get('caisse_id', 0) or 0)
    if not caisse_id:
        flash("Choisissez une caisse de destination", "error"); conn.close()
        return redirect('/comptabilite/virements')
    # Libellés
    bank_name = ''
    if bank_id:
        b = conn.execute("SELECT name FROM bank_accounts WHERE id=?", (bank_id,)).fetchone()
        bank_name = b['name'] if b else ''
    c = conn.execute("SELECT name FROM caisses WHERE id=?", (caisse_id,)).fetchone()
    caisse_name = c['name'] if c else ''
    user = get_user_by_id(session['user_id'])
    date_op = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    motif = request.form.get('motif', '')
    try:
        conn.execute("""INSERT INTO caisse_virements
            (reference, date, bank_account_id, bank_name, caisse_id, caisse_name, amount, motif,
             status, requested_by, requested_by_name)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (ref, date_op, bank_id, bank_name, caisse_id, caisse_name, amount, motif,
             'en_attente', session['user_id'], user['full_name'] if user else '?'))
        # Notifier tous les DG/admin
        for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','dg','directeur') AND is_active=1").fetchall():
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?,?,?,?,?)""",
                (u['id'], 'virement',
                 f"🏦 Virement banque→caisse à valider — {amount:,.0f} F",
                 f"{ref} · {bank_name or 'Banque'} → {caisse_name} · {motif[:50]}",
                 "/comptabilite/virements"))
        conn.commit()
    except Exception as e:
        flash(f"Erreur : {e}", "error")
    conn.close()
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Virement',
                 f"Demande virement {ref} : {bank_name} → {caisse_name} · {amount:,.0f} F", request.remote_addr)
    flash(f"🏦 Virement {ref} demandé ({amount:,.0f} F). En attente de validation du DG.", "success")
    return redirect('/comptabilite/virements')

@app.route('/comptabilite/virement/<int:vid>/valider', methods=['POST','GET'])
@login_required
def compta_virement_valider(vid):
    """DG valide un virement : décrémente banque, crédite caisse, écritures comptables."""
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin', 'dg', 'directeur'):
        flash("Seul le DG peut valider les virements banque→caisse", "error")
        return redirect('/comptabilite/virements')
    conn = _gdb()
    v = conn.execute("SELECT * FROM caisse_virements WHERE id=?", (vid,)).fetchone()
    if not v:
        flash("Virement introuvable", "error"); conn.close()
        return redirect('/comptabilite/virements')
    v = dict(v)
    if v['status'] != 'en_attente':
        flash(f"Ce virement est déjà {v['status']}", "error"); conn.close()
        return redirect('/comptabilite/virements')
    now_iso = datetime.now().isoformat()
    try:
        conn.execute("""UPDATE caisse_virements SET status='valide',
            validated_by=?, validated_by_name=?, validated_at=? WHERE id=?""",
            (session['user_id'], user['full_name'], now_iso, vid))
        # Mouvements sur la caisse (entrée)
        conn.execute("""INSERT INTO caisse_operations (caisse_id, type, amount, description, reference, category, bank_account_id, created_by)
            VALUES (?,?,?,?,?,?,?,?)""",
            (v['caisse_id'], 'entree', v['amount'],
             f"Virement {v['reference']} depuis {v['bank_name'] or 'Banque'}",
             v['reference'], 'virement', v['bank_account_id'], session['user_id']))
        conn.execute("UPDATE caisses SET solde_actuel = solde_actuel + ? WHERE id=?", (v['amount'], v['caisse_id']))
        # Décrémenter le compte bancaire si possible
        if v['bank_account_id']:
            try: conn.execute("UPDATE bank_accounts SET balance = COALESCE(balance,0) - ? WHERE id=?", (v['amount'], v['bank_account_id']))
            except: pass
        # Écriture partie double : 571 (caisse) DÉBIT / 521 (banque) CRÉDIT
        try:
            auto_ecriture(conn, v['date'] or datetime.now().strftime('%Y-%m-%d'),
                f"Virement {v['reference']} : {v['bank_name']} → {v['caisse_name']}",
                '571', '521', v['amount'], v['reference'])
        except: pass
        # Notifier le demandeur
        if v.get('requested_by'):
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?,?,?,?,?)""",
                (v['requested_by'], 'virement',
                 f"✅ Virement {v['reference']} validé",
                 f"{v['amount']:,.0f} F transférés de {v['bank_name']} vers {v['caisse_name']}",
                 "/comptabilite/virements"))
        conn.commit()
    except Exception as e:
        flash(f"Erreur: {e}", "error")
    conn.close()
    log_activity(session['user_id'], user['full_name'], 'Virement',
                 f"Validation virement {v['reference']} : {v['amount']:,.0f} F", request.remote_addr)
    flash(f"✅ Virement {v['reference']} validé — {v['amount']:,.0f} F crédités sur {v['caisse_name']}", "success")
    return redirect('/comptabilite/virements')

@app.route('/comptabilite/virement/<int:vid>/refuser', methods=['POST','GET'])
@login_required
def compta_virement_refuser(vid):
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin', 'dg', 'directeur'):
        flash("Seul le DG peut refuser les virements", "error")
        return redirect('/comptabilite/virements')
    reason = request.form.get('reason', 'Refusé sans motif précisé')
    conn = _gdb()
    v = conn.execute("SELECT * FROM caisse_virements WHERE id=?", (vid,)).fetchone()
    if not v: flash("Virement introuvable", "error"); conn.close(); return redirect('/comptabilite/virements')
    v = dict(v)
    try:
        conn.execute("""UPDATE caisse_virements SET status='refuse',
            validated_by=?, validated_by_name=?, validated_at=?, reject_reason=? WHERE id=? AND status='en_attente'""",
            (session['user_id'], user['full_name'], datetime.now().isoformat(), reason, vid))
        if v.get('requested_by'):
            conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                VALUES (?,?,?,?,?)""",
                (v['requested_by'], 'virement',
                 f"❌ Virement {v['reference']} refusé",
                 f"Motif : {reason[:60]}",
                 "/comptabilite/virements"))
        conn.commit()
    except: pass
    conn.close()
    flash(f"Virement {v['reference']} refusé.", "warning")
    return redirect('/comptabilite/virements')

@app.route('/comptabilite/caisses/<int:cid>')
@permission_required('comptabilite')
def compta_caisse_detail(cid):
    conn = _gdb()
    caisse = conn.execute("SELECT * FROM caisses WHERE id=?", (cid,)).fetchone()
    if not caisse: flash("Caisse non trouvée","error"); conn.close(); return redirect('/comptabilite/caisses')
    caisse = dict(caisse)
    period = request.args.get('period', datetime.now().strftime('%Y-%m'))
    operations = [dict(r) for r in conn.execute(
        "SELECT * FROM caisse_operations WHERE caisse_id=? AND strftime('%Y-%m',created_at)=? ORDER BY created_at DESC", (cid, period)).fetchall()]
    all_ops = [dict(r) for r in conn.execute("SELECT * FROM caisse_operations WHERE caisse_id=? ORDER BY created_at DESC", (cid,)).fetchall()]
    entrees = sum(o['amount'] for o in all_ops if o['type']=='entree')
    sorties = sum(o['amount'] for o in all_ops if o['type']=='sortie')
    trans_in = conn.execute("SELECT COALESCE(SUM(amount),0) FROM caisse_operations WHERE dest_caisse_id=? AND type='transfert'", (cid,)).fetchone()[0]
    trans_out = conn.execute("SELECT COALESCE(SUM(amount),0) FROM caisse_operations WHERE source_caisse_id=? AND type='transfert'", (cid,)).fetchone()[0]
    caisse['solde_calcule'] = caisse['solde_initial'] + entrees + trans_in - sorties - trans_out
    other_caisses = [dict(r) for r in conn.execute("SELECT id, name FROM caisses WHERE id!=? AND is_active=1", (cid,)).fetchall()]
    banks = [dict(r) for r in conn.execute("SELECT id, name FROM bank_accounts").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='caisse_detail', caisse=caisse, operations=operations,
        period=period, other_caisses=other_caisses, banks=banks)

@app.route('/comptabilite/caisses/<int:cid>/operation', methods=['POST'])
@permission_required('comptabilite_edit')
def compta_caisse_operation(cid):
    conn = _gdb()
    op_type = request.form.get('type','entree')
    amount = float(request.form.get('amount',0) or 0)
    if amount <= 0: flash("Montant invalide","error"); conn.close(); return redirect(f'/comptabilite/caisses/{cid}')
    
    ref = f"OP-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    if op_type == 'transfert_caisse':
        dest_id = int(request.form.get('dest_caisse_id',0) or 0)
        conn.execute("INSERT INTO caisse_operations (caisse_id, type, amount, description, reference, category, source_caisse_id, dest_caisse_id, created_by) VALUES (?,'transfert',?,?,?,?,?,?,?)",
            (cid, amount, request.form.get('description','Transfert inter-caisses'), ref, 'transfert', cid, dest_id, session['user_id']))
        auto_ecriture(conn, datetime.now().strftime('%Y-%m-%d'), f"Transfert caisse {ref}", '580', '580', amount, ref)
    elif op_type == 'transfert_banque':
        bank_id = int(request.form.get('bank_account_id',0) or 0)
        conn.execute("INSERT INTO caisse_operations (caisse_id, type, amount, description, reference, category, bank_account_id, created_by) VALUES (?,'sortie',?,?,?,?,?,?)",
            (cid, amount, request.form.get('description','Versement banque'), ref, 'banque', bank_id, session['user_id']))
        auto_ecriture(conn, datetime.now().strftime('%Y-%m-%d'), f"Versement banque {ref}", '521', '571', amount, ref)
    else:
        conn.execute("INSERT INTO caisse_operations (caisse_id, type, amount, description, reference, category, intervention_id, project_id, created_by) VALUES (?,?,?,?,?,?,?,?,?)",
            (cid, op_type, amount, request.form.get('description',''), ref, request.form.get('category','divers'),
             int(request.form.get('intervention_id',0) or 0) or None,
             int(request.form.get('project_id',0) or 0) or None, session['user_id']))
        if op_type == 'entree':
            auto_ecriture(conn, datetime.now().strftime('%Y-%m-%d'), f"Entrée caisse {ref}", '571', '758', amount, ref)
        else:
            auto_ecriture(conn, datetime.now().strftime('%Y-%m-%d'), f"Sortie caisse {ref}", '658', '571', amount, ref)
    
    conn.commit(); conn.close()
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Caisse', f"{op_type} {ref} — {amount:,.0f} F", request.remote_addr)
    flash(f"Opération {ref} enregistrée — {amount:,.0f} F", "success")
    return redirect(f'/comptabilite/caisses/{cid}')


# ======================== INTERVENTIONS ========================

@app.route('/interventions')
@login_required
def interventions_list():
    conn = _gdb()
    interventions = [dict(r) for r in conn.execute(
        "SELECT * FROM interventions ORDER BY scheduled_date DESC, id DESC").fetchall()]
    projects = [dict(r) for r in conn.execute("SELECT id, name FROM projects ORDER BY name").fetchall()]
    clients = [dict(r) for r in conn.execute("SELECT id, name FROM clients ORDER BY name").fetchall()]
    technicians = [dict(r) for r in conn.execute("SELECT id, full_name FROM users WHERE role IN ('technicien','admin') ORDER BY full_name").fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='interventions', interventions=interventions,
                          projects=projects, clients=clients, technicians=technicians)

@app.route('/interventions/tech')
@login_required
def interventions_tech():
    conn = _gdb()
    user = get_user_by_id(session['user_id'])
    is_admin = user and user['role'] in ('admin','dg','resp_projet')
    if is_admin:
        interventions = [dict(r) for r in conn.execute("SELECT * FROM interventions ORDER BY scheduled_date DESC").fetchall()]
    else:
        interventions = [dict(r) for r in conn.execute(
            "SELECT * FROM interventions WHERE technician_id=? OR created_by=? ORDER BY scheduled_date DESC",
            (session['user_id'], session['user_id'])).fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='interventions_tech', interventions=interventions, is_admin=is_admin)

@app.route('/interventions/add', methods=['POST'])
@login_required
def intervention_add():
    conn = _gdb()
    ref = f"INT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    tech_id = int(request.form.get('technician_id',0) or 0)
    tech_name = ''
    if tech_id:
        tu = conn.execute("SELECT full_name FROM users WHERE id=?", (tech_id,)).fetchone()
        tech_name = tu['full_name'] if tu else ''
    
    client_id = int(request.form.get('client_id',0) or 0)
    client_name = ''
    if client_id:
        cl = conn.execute("SELECT name FROM clients WHERE id=?", (client_id,)).fetchone()
        client_name = cl['name'] if cl else ''
    
    project_id = int(request.form.get('project_id',0) or 0) or None
    task_id = int(request.form.get('task_id',0) or 0) or None
    
    conn.execute("""INSERT INTO interventions (reference, title, type, project_id, task_id, client_id, client_name,
        site_address, technician_id, technician_name, scheduled_date, scheduled_time, priority, description, 
        is_billable, created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ref, request.form.get('title',''), request.form.get('type','maintenance'),
         project_id, task_id, client_id, client_name,
         request.form.get('site_address',''), tech_id, tech_name,
         request.form.get('scheduled_date',''), request.form.get('scheduled_time',''),
         request.form.get('priority','normale'), request.form.get('description',''),
         1 if request.form.get('is_billable') else 0, session['user_id']))
    conn.commit()
    
    # Notify technician
    if tech_id and tech_id != session['user_id']:
        conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
            (tech_id, 'intervention', f"🔧 Nouvelle intervention — {client_name}",
             f"Intervention planifiée le {request.form.get('scheduled_date','')}: {request.form.get('title','')}",
             '/interventions/tech'))
        conn.commit()
    conn.close()
    
    # Link to task if specified
    if task_id:
        try:
            conn2 = _gdb()
            int_id = conn2.execute("SELECT id FROM interventions WHERE reference=?", (ref,)).fetchone()
            if int_id:
                conn2.execute("UPDATE tasks SET intervention_id=?, status='en_cours' WHERE id=?", (int_id['id'], task_id))
                conn2.commit()
            conn2.close()
        except: pass
    
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Intervention', f"Intervention {ref} planifiée — {client_name}", request.remote_addr)
    flash(f"Intervention {ref} créée", "success")
    return redirect(request.form.get('redirect', '/interventions'))

def _notify_client(conn, iid, title, message):
    """Envoie une notification au client propriétaire de l'intervention via son client_user."""
    inter = conn.execute("SELECT client_id FROM interventions WHERE id=?", (iid,)).fetchone()
    if not inter or not inter['client_id']: return
    cid = inter['client_id']
    # Trouver tous les client_users rattachés
    cus = conn.execute("SELECT id FROM client_users WHERE client_id=? AND is_active=1 AND account_status='approved'", (cid,)).fetchall()
    for cu in cus:
        try:
            conn.execute("""INSERT INTO notifications (client_user_id, type, title, message, link)
                            VALUES (?,?,?,?,?)""",
                         (cu['id'], 'intervention', title, message, f"/portail/interventions"))
        except:
            # Fallback if notifications has no client_user_id column, silently skip
            pass

def _timeline_add(conn, iid, step, title, description, actor_id=None, actor_name='', actor_role=''):
    try:
        conn.execute("""INSERT INTO intervention_timeline
            (intervention_id, step, title, description, actor_id, actor_name, actor_role)
            VALUES (?,?,?,?,?,?,?)""",
            (iid, step, title, description, actor_id, actor_name, actor_role))
    except: pass

@app.route('/interventions/<int:iid>/status/<status>')
@login_required
def intervention_status(iid, status):
    valid = ('planifiee','en_cours','travaux_termines','controle_qualite','livre','annulee')
    if status not in valid: flash("Statut invalide","error"); return redirect('/interventions')
    conn = _gdb()
    inter = conn.execute("SELECT * FROM interventions WHERE id=?", (iid,)).fetchone()
    if inter:
        inter = dict(inter)
        user = get_user_by_id(session['user_id'])
        actor_name = user['full_name'] if user else '?'
        actor_role = user['role'] if user else ''
        updates = {'status': status}
        if status == 'en_cours':
            updates['start_date'] = datetime.now().strftime('%Y-%m-%d')
            _timeline_add(conn, iid, 'start', "🔧 Intervention démarrée",
                f"Le technicien {actor_name} a commencé les travaux", session['user_id'], actor_name, actor_role)
            _notify_client(conn, iid, f"🔧 Intervention en cours — {inter['reference']}",
                f"Nos techniciens ont commencé les travaux de « {inter['title']} ».")
        
        if status == 'travaux_termines':
            # ÉTAPE 1 : technicien termine
            updates['end_work_at'] = datetime.now().isoformat()
            updates['end_work_by'] = session['user_id']
            _timeline_add(conn, iid, 'end_work', "🏗️ Travaux terminés par le technicien",
                f"{actor_name} a déclaré les travaux terminés — en attente de contrôle qualité",
                session['user_id'], actor_name, actor_role)
            # Notifier coordinateurs + resp_projet + admin
            for u in conn.execute(
                "SELECT id, full_name FROM users WHERE role IN ('admin','dg','resp_projet','coordinateur') AND is_active=1").fetchall():
                conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                    VALUES (?,?,?,?,?)""",
                    (u['id'], 'intervention', f"🔍 Contrôle qualité à réaliser — {inter['reference']}",
                     f"Le technicien {actor_name} a terminé {inter['title']} chez {inter['client_name']}. Effectuer le CQ.",
                     f"/gestion-projets/controle-qualite"))
            _notify_client(conn, iid, f"🏗️ Travaux terminés — {inter['reference']}",
                f"Les travaux « {inter['title']} » sont terminés. Nos équipes procèdent au contrôle qualité avant la livraison.")
        
        if status == 'livre':
            updates['end_date'] = datetime.now().strftime('%Y-%m-%d')
            if inter.get('is_billable') and (inter.get('total_cost') or 0) > 0:
                try:
                    auto_ecriture(conn, datetime.now().strftime('%Y-%m-%d'),
                        f"Livraison {inter['reference']} — {inter['client_name']}",
                        '411', '706', inter['total_cost'] or 0, inter['reference'])
                except: pass
            if inter.get('task_id'):
                try: conn.execute("UPDATE tasks SET status='terminee' WHERE id=?", (inter['task_id'],))
                except: pass
        
        set_clause = ', '.join(f"{k}=?" for k in updates)
        conn.execute(f"UPDATE interventions SET {set_clause} WHERE id=?", (*updates.values(), iid))
        conn.commit()
        labels = {'en_cours':'En cours','travaux_termines':'Travaux terminés','controle_qualite':'Contrôle qualité','livre':'Livré','annulee':'Annulée'}
        flash(f"Statut → {labels.get(status, status)}", "success")
    conn.close()
    return redirect(request.referrer or '/interventions')

@app.route('/interventions/<int:iid>/rapport', methods=['POST'])
@login_required
def intervention_rapport(iid):
    """Rapport final du technicien — met les coûts mais NE TERMINE PAS. Statut → travaux_termines."""
    conn = _gdb()
    material_cost = float(request.form.get('material_cost',0) or 0)
    labor_cost = float(request.form.get('labor_cost',0) or 0)
    conn.execute("""UPDATE interventions SET rapport=?, material_used=?, material_cost=?, 
        labor_cost=?, total_cost=?, duration_hours=?, status='travaux_termines' WHERE id=?""",
        (request.form.get('rapport',''), request.form.get('material_used',''),
         material_cost, labor_cost, material_cost + labor_cost,
         float(request.form.get('duration_hours',0) or 0), iid))
    
    # Notify coordinator + client
    inter = conn.execute("SELECT * FROM interventions WHERE id=?", (iid,)).fetchone()
    if inter:
        for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','resp_projet') AND is_active=1").fetchall():
            conn.execute("INSERT INTO notifications (user_id, type, title, message, link) VALUES (?,?,?,?,?)",
                (u['id'], 'intervention', f"🏗️ Travaux terminés — {inter['client_name']}",
                 f"{inter['reference']}: En attente validation client puis contrôle qualité", f"/interventions/{iid}/fiche"))
    
    conn.commit(); conn.close()
    flash("Rapport final soumis — En attente de validation client", "success")
    return redirect('/interventions/tech')


# ======================== NOTIFICATIONS ========================

@app.route('/notifications')
@login_required
def notifications_page():
    conn = _gdb()
    user = get_user_by_id(session['user_id'])
    notifs = [dict(r) for r in conn.execute("""SELECT * FROM notifications 
        WHERE user_id=? OR employee_id IN (SELECT id FROM employees WHERE email=?)
        ORDER BY created_at DESC LIMIT 50""",
        (session['user_id'], user.get('email',''))).fetchall()]
    # Mark all as read
    conn.execute("""UPDATE notifications SET read=1 
        WHERE (user_id=? OR employee_id IN (SELECT id FROM employees WHERE email=?)) AND read=0""",
        (session['user_id'], user.get('email','')))
    conn.commit(); conn.close()
    return render_template('rh_conges.html', page='notifications', notifs=notifs)

@app.route('/notifications/read/<int:nid>')
@login_required
def notification_read(nid):
    conn = _gdb()
    n = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
    if n:
        conn.execute("UPDATE notifications SET read=1 WHERE id=?", (nid,))
        conn.commit()
        link = n['link'] or '/dashboard'
    else:
        link = '/dashboard'
    conn.close()
    return redirect(link)

# ======================== EMPLOYEE PHOTO ========================

@app.route('/rh/personnel/photo/<int:eid>', methods=['POST'])
@permission_required('fichiers')
def rh_personnel_photo(eid):
    if 'photo' in request.files:
        f = request.files['photo']
        if f and f.filename:
            from werkzeug.utils import secure_filename
            ext = os.path.splitext(f.filename)[1].lower()
            if ext in ('.jpg','.jpeg','.png','.webp'):
                photo_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'photos')
                os.makedirs(photo_dir, exist_ok=True)
                fname = f"emp_{eid}{ext}"
                f.save(os.path.join(photo_dir, fname))
                update_employee(eid, photo=fname)
                flash("Photo mise à jour", "success")
    return redirect(url_for('rh_personnel'))

@app.route('/uploads/photos/<path:filename>')
def employee_photo(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'photos'), filename)

@app.route('/uploads/employees/<path:filename>')
def employee_file_serve(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'employees'), filename)

@app.route('/uploads/chat_files/<path:filename>')
def chat_file_serve(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'chat_files'), filename)

@app.route('/uploads/client_photos/<path:filename>')
def client_photo_serve(filename):
    """Serve client portal user photos."""
    fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'client_photos')
    return send_from_directory(fdir, filename)

# ======================== IMPORT CLIENTS EXCEL ========================

@app.route('/clients/import', methods=['POST'])
@permission_required('clients')
def clients_import():
    if 'file' not in request.files:
        flash("Aucun fichier", "error"); return redirect(url_for('clients_page'))
    f = request.files['file']
    if f.filename.endswith(('.xlsx','.xls')):
        import openpyxl
        tmp = os.path.join(app.config['UPLOAD_FOLDER'], 'import_tmp.xlsx')
        f.save(tmp)
        wb = openpyxl.load_workbook(tmp, data_only=True)
        ws = wb.active
        count = 0
        for row in ws.iter_rows(min_row=2, values_only=True):
            if row[0]:
                try:
                    create_client(str(row[0]), str(row[1] or ''), str(row[2] or ''), str(row[3] or ''))
                    count += 1
                except: pass
        flash(f"{count} clients importés depuis Excel", "success")
        os.remove(tmp)
    else:
        flash("Format non supporté (xlsx uniquement)", "error")
    return redirect(url_for('clients_page'))

# ======================== CENTRE TECHNIQUE ========================

@app.route('/centre-technique')
@permission_required('centre_technique')
def tech_center():
    from models import db_get_all, db_insert, db_update
    systems = db_get_all('tech_center', order='next_maintenance ASC')
    due = get_maintenance_due()
    clients = get_all_clients()
    return render_template('tech_center.html', page='tech_center', systems=systems, due=due, clients=clients)

@app.route('/centre-technique/add', methods=['POST'])
@login_required
def tech_center_add():
    from models import db_insert
    db_insert('tech_center', client_name=request.form.get('client_name',''),
        client_id=int(request.form['client_id']) if request.form.get('client_id') else None,
        system_type=request.form.get('system_type',''),
        installation_date=request.form.get('installation_date',''),
        next_maintenance=request.form.get('next_maintenance',''),
        maintenance_interval=int(request.form.get('maintenance_interval', 90) or 90),
        notes=request.form.get('notes',''), created_by=session['user_id'])
    flash("Système ajouté au centre technique", "success")
    return redirect(url_for('tech_center'))

@app.route('/centre-technique/view/<int:sid>')
@permission_required('centre_technique')
def tech_center_view(sid):
    from models import db_get_by_id
    system = db_get_by_id('tech_center', sid)
    if not system: flash("Non trouvé","error"); return redirect(url_for('tech_center'))
    conn = _gdb()
    devis_list = [dict(r) for r in conn.execute("SELECT * FROM devis WHERE client_name=? ORDER BY created_at DESC LIMIT 10",
        (system.get('client_name',''),)).fetchall()]
    conn.close()
    return render_template('tech_center_view.html', page='tech_center', system=system, devis_list=devis_list)

@app.route('/centre-technique/edit/<int:sid>', methods=['POST'])
@login_required
def tech_center_edit(sid):
    from models import db_update
    db_update('tech_center', sid, client_name=request.form.get('client_name',''),
        code=request.form.get('code',''), contact_name=request.form.get('contact_name',''),
        tel=request.form.get('tel',''), email=request.form.get('email',''),
        system_type=request.form.get('system_type',''), category=request.form.get('category',''),
        next_maintenance=request.form.get('next_maintenance',''),
        maintenance_interval=int(request.form.get('maintenance_interval',90) or 90),
        notes=request.form.get('notes',''), status=request.form.get('status','actif'))
    flash("Modifié","success"); return redirect(f'/centre-technique/view/{sid}')

@app.route('/centre-technique/delete/<int:sid>')
@login_required
def tech_center_delete(sid):
    conn = _gdb(); conn.execute("DELETE FROM tech_center WHERE id=?", (sid,)); conn.commit(); conn.close()
    flash("Supprimé","success"); return redirect(url_for('tech_center'))

@app.route('/prospects/view/<int:pid>')
@login_required
def prospect_view(pid):
    from models import db_get_by_id
    p = db_get_by_id('prospects', pid)
    if not p: flash("Non trouvé","error"); return redirect('/prospects')
    conn = _gdb()
    notes = [dict(r) for r in conn.execute("SELECT * FROM prospect_notes WHERE prospect_id=? ORDER BY created_at DESC", (pid,)).fetchall()]
    tasks = [dict(r) for r in conn.execute("SELECT * FROM prospect_tasks WHERE prospect_id=? ORDER BY created_at DESC", (pid,)).fetchall()]
    offers = [dict(r) for r in conn.execute("SELECT * FROM prospect_offers WHERE prospect_id=? ORDER BY created_at DESC", (pid,)).fetchall()]
    reminders = [dict(r) for r in conn.execute("SELECT * FROM prospect_reminders WHERE prospect_id=? ORDER BY reminder_date ASC", (pid,)).fetchall()]
    files = [dict(r) for r in conn.execute("SELECT * FROM prospect_files WHERE prospect_id=? ORDER BY created_at DESC", (pid,)).fetchall()]
    conn.close()
    tab = request.args.get('tab', 'profil')
    from datetime import datetime as dt2
    now_str = dt2.now().strftime('%Y-%m-%d')
    return render_template('prospect_view.html', page='prospects', prospect=p, tab=tab,
        notes=notes, tasks=tasks, offers=offers, reminders=reminders, files=files, now_str=now_str)

@app.route('/prospects/view/<int:pid>/note/add', methods=['POST'])
@login_required
def prospect_note_add(pid):
    from models import db_insert
    db_insert('prospect_notes', prospect_id=pid, content=request.form.get('content',''), created_by=session['user_id'])
    flash("Note ajoutée","success"); return redirect(f'/prospects/view/{pid}?tab=notes')

@app.route('/prospects/view/<int:pid>/task/add', methods=['POST'])
@login_required
def prospect_task_add(pid):
    from models import db_insert
    db_insert('prospect_tasks', prospect_id=pid, title=request.form.get('title',''),
        status=request.form.get('status','a_faire'), priority=request.form.get('priority','normale'),
        due_date=request.form.get('due_date',''), assigned_to=request.form.get('assigned_to',''),
        created_by=session['user_id'])
    flash("Tâche ajoutée","success"); return redirect(f'/prospects/view/{pid}?tab=taches')

@app.route('/prospects/view/<int:pid>/offer/add', methods=['POST'])
@login_required
def prospect_offer_add(pid):
    from models import db_insert
    db_insert('prospect_offers', prospect_id=pid, title=request.form.get('title',''),
        amount=float(request.form.get('amount',0) or 0), status=request.form.get('status','brouillon'),
        description=request.form.get('description',''), created_by=session['user_id'])
    flash("Offre ajoutée","success"); return redirect(f'/prospects/view/{pid}?tab=offres')

@app.route('/prospects/view/<int:pid>/reminder/add', methods=['POST'])
@login_required
def prospect_reminder_add(pid):
    from models import db_insert
    db_insert('prospect_reminders', prospect_id=pid, title=request.form.get('title',''),
        reminder_date=request.form.get('reminder_date',''), notes=request.form.get('notes',''),
        created_by=session['user_id'])
    flash("Rappel ajouté","success"); return redirect(f'/prospects/view/{pid}?tab=rappels')

@app.route('/prospects/view/<int:pid>/file/add', methods=['POST'])
@login_required
def prospect_file_add(pid):
    from models import db_insert
    if 'file' in request.files and request.files['file'].filename:
        f = request.files['file']
        fname = f"prospect_{pid}_{secure_filename(f.filename)}"
        fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'prospect_files')
        os.makedirs(fdir, exist_ok=True)
        f.save(os.path.join(fdir, fname))
        db_insert('prospect_files', prospect_id=pid, filename=fname,
            original_name=f.filename, created_by=session['user_id'])
        flash("Fichier ajouté","success")
    return redirect(f'/prospects/view/{pid}?tab=pieces')

@app.route('/prospects/view/<int:pid>/delete/<table>/<int:item_id>')
@login_required
def prospect_item_delete(pid, table, item_id):
    allowed = {'note': 'prospect_notes', 'task': 'prospect_tasks', 'offer': 'prospect_offers',
               'reminder': 'prospect_reminders', 'file': 'prospect_files'}
    if table in allowed:
        conn = _gdb(); conn.execute(f"DELETE FROM {allowed[table]} WHERE id=? AND prospect_id=?", (item_id, pid))
        conn.commit(); conn.close()
        flash("Supprimé","success")
    return redirect(f'/prospects/view/{pid}')

# ======================== RESPONSABLE PROJET ========================

@app.route('/gestion-projets/controle-qualite')
@permission_required('controle_qualite')
def gestion_controle_qualite():
    """Liste des interventions à contrôler (après fin des travaux)."""
    conn = _gdb()
    # À contrôler : travaux_termines (+ cq non encore passé)
    to_check = [dict(r) for r in conn.execute("""
        SELECT i.*, c.name as client_name_full, c.client_code,
               u.full_name as tech_name_full
        FROM interventions i
        LEFT JOIN clients c ON i.client_id = c.id
        LEFT JOIN users u ON i.technician_id = u.id
        WHERE i.status = 'travaux_termines' AND COALESCE(i.cq_status,'') = ''
        ORDER BY i.end_work_at DESC
    """).fetchall()]
    # Validées récemment (historique)
    done = [dict(r) for r in conn.execute("""
        SELECT i.*, c.name as client_name_full
        FROM interventions i
        LEFT JOIN clients c ON i.client_id = c.id
        WHERE i.cq_status IN ('passed','failed')
        ORDER BY i.cq_at DESC LIMIT 20
    """).fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='controle_qualite',
                           to_check=to_check, done=done)


@app.route('/gestion-projets/livraisons')
@permission_required('livraison_intervention')
def gestion_livraisons():
    """Liste des interventions en cours de livraison (après CQ validé)."""
    conn = _gdb()
    # Prêts pour proposition date
    ready = [dict(r) for r in conn.execute("""
        SELECT i.*, c.name as client_name_full, c.client_code
        FROM interventions i
        LEFT JOIN clients c ON i.client_id = c.id
        WHERE i.cq_status='passed' AND COALESCE(i.delivery_proposed_date,'')=''
        ORDER BY i.cq_at DESC
    """).fetchall()]
    # En attente client (date proposée, pas encore de réponse)
    waiting = [dict(r) for r in conn.execute("""
        SELECT i.*, c.name as client_name_full
        FROM interventions i
        LEFT JOIN clients c ON i.client_id = c.id
        WHERE COALESCE(i.delivery_proposed_date,'') != ''
          AND COALESCE(i.delivery_client_status,'') IN ('','pending','proposed')
          AND i.status != 'livre'
        ORDER BY i.delivery_proposed_at DESC
    """).fetchall()]
    # Prêts pour livraison (client a accepté)
    ready_deliver = [dict(r) for r in conn.execute("""
        SELECT i.*, c.name as client_name_full
        FROM interventions i
        LEFT JOIN clients c ON i.client_id = c.id
        WHERE i.delivery_client_status='accepted' AND i.status != 'livre'
        ORDER BY i.delivery_client_answered_at DESC
    """).fetchall()]
    # Livrés récemment
    delivered = [dict(r) for r in conn.execute("""
        SELECT i.*, c.name as client_name_full
        FROM interventions i
        LEFT JOIN clients c ON i.client_id = c.id
        WHERE i.status='livre'
        ORDER BY i.delivered_at DESC LIMIT 20
    """).fetchall()]
    conn.close()
    return render_template('extra_pages.html', page='gestion_livraisons',
                           ready=ready, waiting=waiting, ready_deliver=ready_deliver, delivered=delivered)


@app.route('/resp-projet')
@permission_required('resp_projet')
def resp_projet_dashboard():
    conn = _gdb()
    projects = [dict(r) for r in conn.execute("SELECT * FROM projects ORDER BY created_at DESC").fetchall()]
    tasks = [dict(r) for r in conn.execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()]
    
    # KPIs
    total_p = len(projects)
    en_cours = len([p for p in projects if p['status'] == 'en_cours'])
    termines = len([p for p in projects if p['status'] == 'termine'])
    en_retard = len([p for p in projects if p.get('end_date') and p['end_date'] < datetime.now().strftime('%Y-%m-%d') and p['status'] != 'termine'])
    
    tasks_total = len(tasks)
    tasks_done = len([t for t in tasks if t['status'] == 'termine'])
    tasks_pending = len([t for t in tasks if t['status'] == 'a_faire'])
    tasks_progress = len([t for t in tasks if t['status'] == 'en_cours'])
    
    budget_total = sum(float(p.get('budget',0) or 0) for p in projects)
    budget_consumed = sum(float(p.get('budget_consumed',0) or 0) for p in projects)
    
    # Recent tasks
    recent = [dict(r) for r in conn.execute("""SELECT t.*, p.name as project_name, u.full_name as assignee 
        FROM tasks t LEFT JOIN projects p ON t.project_id=p.id LEFT JOIN users u ON t.assigned_to=u.id
        ORDER BY t.created_at DESC LIMIT 10""").fetchall()]
    
    # Deadlines this week
    from datetime import timedelta
    today = datetime.now().date()
    week_end = (today + timedelta(days=6)).strftime('%Y-%m-%d')
    deadlines = [dict(r) for r in conn.execute("""SELECT t.*, p.name as project_name 
        FROM tasks t LEFT JOIN projects p ON t.project_id=p.id 
        WHERE t.due_date <= ? AND t.status != 'termine' ORDER BY t.due_date""",
        (week_end,)).fetchall()]
    
    # === RÉSUMÉ INTERVENTIONS WORKFLOW 5 ÉTAPES ===
    interv_summary = {'counts': {}}
    try:
        interv_summary['to_check'] = [dict(r) for r in conn.execute(
            """SELECT * FROM interventions WHERE status='travaux_termines' AND COALESCE(cq_status,'')=''
               ORDER BY end_work_at DESC LIMIT 10""").fetchall()]
        interv_summary['cq_done_no_date'] = [dict(r) for r in conn.execute(
            """SELECT * FROM interventions WHERE cq_status='passed' AND status!='livre'
               AND COALESCE(delivery_proposed_date,'')='' ORDER BY cq_at DESC LIMIT 10""").fetchall()]
        interv_summary['waiting_client'] = [dict(r) for r in conn.execute(
            """SELECT * FROM interventions WHERE COALESCE(delivery_proposed_date,'')!=''
               AND COALESCE(delivery_client_status,'pending') IN ('','pending') AND status!='livre'
               ORDER BY delivery_proposed_at DESC LIMIT 10""").fetchall()]
        interv_summary['client_proposed'] = [dict(r) for r in conn.execute(
            """SELECT * FROM interventions WHERE delivery_client_status='proposed' AND status!='livre'
               ORDER BY delivery_client_answered_at DESC LIMIT 10""").fetchall()]
        interv_summary['ready_deliver'] = [dict(r) for r in conn.execute(
            """SELECT * FROM interventions WHERE delivery_client_status='accepted' AND status!='livre'
               ORDER BY delivery_proposed_date LIMIT 10""").fetchall()]
        month_start = datetime.now().strftime('%Y-%m-01')
        interv_summary['delivered_month'] = [dict(r) for r in conn.execute(
            """SELECT * FROM interventions WHERE status='livre' AND delivered_at >= ?
               ORDER BY delivered_at DESC LIMIT 10""", (month_start,)).fetchall()]
        interv_summary['counts'] = {
            'to_check': len(interv_summary['to_check']),
            'cq_done_no_date': len(interv_summary['cq_done_no_date']),
            'waiting_client': len(interv_summary['waiting_client']),
            'client_proposed': len(interv_summary['client_proposed']),
            'ready_deliver': len(interv_summary['ready_deliver']),
            'delivered_month': len(interv_summary['delivered_month']),
        }
    except Exception:
        pass
    
    conn.close()
    return render_template('resp_projet.html', page='resp_projet',
        projects=projects, total_p=total_p, en_cours=en_cours, termines=termines, en_retard=en_retard,
        tasks_total=tasks_total, tasks_done=tasks_done, tasks_pending=tasks_pending, tasks_progress=tasks_progress,
        budget_total=budget_total, budget_consumed=budget_consumed,
        recent=recent, deadlines=deadlines, today=datetime.now().strftime('%Y-%m-%d'),
        interv_summary=interv_summary)

@app.route('/resp-projet/projets')
@permission_required('resp_projet')
def resp_projet_list():
    conn = _gdb()
    projects = [dict(r) for r in conn.execute("""SELECT p.*, u.full_name as manager_name,
        (SELECT COUNT(*) FROM tasks WHERE project_id=p.id) as task_count,
        (SELECT COUNT(*) FROM tasks WHERE project_id=p.id AND status='termine') as task_done
        FROM projects p LEFT JOIN users u ON p.manager_id=u.id ORDER BY p.created_at DESC""").fetchall()]
    users = get_all_users()
    clients = get_all_clients()
    conn.close()
    return render_template('resp_projet_list.html', page='resp_projets', projects=projects, users=users, clients=clients)

@app.route('/resp-projet/projet/add', methods=['POST'])
@permission_required('resp_projet')
def resp_projet_add():
    from models import db_insert
    db_insert('projects', name=request.form.get('name',''), description=request.form.get('description',''),
        objectives=request.form.get('objectives',''), client=request.form.get('client',''),
        status=request.form.get('status','non_commence'), priority=request.form.get('priority','moyenne'),
        start_date=request.form.get('start_date',''), end_date=request.form.get('end_date',''),
        budget=float(request.form.get('budget',0) or 0),
        manager_id=int(request.form.get('manager_id',0) or 0) or None,
        client_id=int(request.form.get('client_id',0) or 0) or None,
        created_by=session['user_id'])
    flash("Projet créé","success"); return redirect('/resp-projet/projets')

@app.route('/resp-projet/projet/<int:pid>')
@permission_required('resp_projet')
def resp_projet_view(pid):
    from models import db_get_by_id
    project = db_get_by_id('projects', pid)
    if not project: flash("Non trouvé","error"); return redirect('/resp-projet/projets')
    conn = _gdb()
    tasks = [dict(r) for r in conn.execute("""SELECT t.*, u.full_name as assignee FROM tasks t 
        LEFT JOIN users u ON t.assigned_to=u.id WHERE t.project_id=? ORDER BY t.priority DESC, t.due_date""",
        (pid,)).fetchall()]
    comments = [dict(r) for r in conn.execute("""SELECT tc.*, u.full_name FROM task_comments tc
        LEFT JOIN users u ON tc.user_id=u.id WHERE tc.task_id IN (SELECT id FROM tasks WHERE project_id=?)
        ORDER BY tc.created_at DESC LIMIT 20""", (pid,)).fetchall()]
    users = get_all_users()
    conn.close()
    
    done = len([t for t in tasks if t['status'] == 'termine'])
    progress = round(done / max(len(tasks), 1) * 100)
    
    return render_template('resp_projet_view.html', page='resp_projets', project=project,
        tasks=tasks, comments=comments, users=users, progress=progress, today=datetime.now().strftime('%Y-%m-%d'))

@app.route('/resp-projet/projet/<int:pid>/task/add', methods=['POST'])
@permission_required('resp_projet')
def resp_projet_task_add(pid):
    from models import db_insert
    db_insert('tasks', project_id=pid, title=request.form.get('title',''),
        description=request.form.get('description',''), priority=request.form.get('priority','moyenne'),
        status='a_faire', due_date=request.form.get('due_date',''),
        assigned_to=int(request.form.get('assigned_to',0) or 0) or None,
        created_by=session['user_id'])
    flash("Tâche ajoutée","success"); return redirect(f'/resp-projet/projet/{pid}')

@app.route('/resp-projet/task/<int:tid>/status/<status>')
@permission_required('resp_projet')
def resp_projet_task_status(tid, status):
    from models import db_get_by_id as _gbi, db_update
    if status in ('a_faire','en_cours','termine'):
        db_update('tasks', tid, status=status)
        flash("Statut mis à jour","success")
    t = _gbi('tasks', tid)
    return redirect(f'/resp-projet/projet/{t["project_id"]}' if t else '/resp-projet')

@app.route('/resp-projet/task/<int:tid>/comment', methods=['POST'])
@permission_required('resp_projet')
def resp_projet_comment(tid):
    from models import db_insert, db_get_by_id as _gbi2
    db_insert('task_comments', task_id=tid, user_id=session['user_id'], content=request.form.get('content',''))
    t = _gbi2('tasks', tid)
    flash("Commentaire ajouté","success")
    return redirect(f'/resp-projet/projet/{t["project_id"]}' if t else '/resp-projet')

@app.route('/resp-projet/planning')
@permission_required('resp_projet')
def resp_projet_planning():
    conn = _gdb()
    import json as _json
    month_param = request.args.get('month', datetime.now().strftime('%Y-%m'))
    try:
        year = int(month_param[:4])
        month = int(month_param[5:7])
    except:
        year = datetime.now().year; month = datetime.now().month
    
    month_names = ['','Janvier','Février','Mars','Avril','Mai','Juin','Juillet','Août','Septembre','Octobre','Novembre','Décembre']
    prev_m = month - 1; prev_y = year
    if prev_m < 1: prev_m = 12; prev_y -= 1
    next_m = month + 1; next_y = year
    if next_m > 12: next_m = 1; next_y += 1
    
    projects = [dict(r) for r in conn.execute("SELECT * FROM projects WHERE start_date IS NOT NULL AND start_date != '' ORDER BY start_date").fetchall()]
    tasks = [dict(r) for r in conn.execute("""SELECT t.*, p.name as project_name, u.full_name as assigned_name 
        FROM tasks t LEFT JOIN projects p ON t.project_id=p.id LEFT JOIN users u ON t.assigned_to=u.id
        WHERE t.due_date IS NOT NULL AND t.due_date != '' ORDER BY t.due_date""").fetchall()]
    conn.close()
    
    tasks_json = _json.dumps([{'title':t['title'],'due':t.get('due_date',''),'status':t.get('status',''),'project':t.get('project_name','')} for t in tasks])
    projects_json = _json.dumps([{'name':p['name'],'start':p.get('start_date',''),'end':p.get('end_date','')} for p in projects])
    
    return render_template('resp_projet_planning.html', page='resp_planning', 
        projects=projects, tasks=tasks, today=datetime.now().strftime('%Y-%m-%d'),
        year=year, month=month, month_name=month_names[month],
        prev_month=f"{prev_y}-{prev_m:02d}", next_month=f"{next_y}-{next_m:02d}",
        tasks_json=tasks_json, projects_json=projects_json)

# ======================== CONTRATS RH ========================

@app.route('/rh/contrats-rh')
@permission_required('fichiers')
def rh_contracts():
    from models import db_get_all
    contracts = db_get_all('rh_contracts', order='end_date ASC')
    # Enrich with employee names
    employees = get_all_employees(status=None)
    emp_map = {e['id']: f"{e['first_name']} {e['last_name']}" for e in employees}
    for c in contracts:
        c['employee_name'] = emp_map.get(c.get('employee_id'), '-')
    return render_template('rh_contrats.html', page='contrats_rh', contracts=contracts, employees=employees,
        now_str=datetime.now().strftime('%Y-%m-%d'))

@app.route('/rh/contrats-rh/add', methods=['POST'])
@permission_required('fichiers')
def rh_contracts_add():
    from models import db_insert
    db_insert('rh_contracts', code=request.form.get('code',''),
        employee_id=int(request.form['employee_id']),
        contract_type=request.form.get('contract_type','CDI'),
        start_date=request.form.get('start_date',''),
        end_date=request.form.get('end_date',''),
        salary=float(request.form.get('salary',0) or 0),
        department=request.form.get('department',''),
        signature_date=request.form.get('signature_date',''),
        notes=request.form.get('notes',''))
    flash("Contrat RH ajouté", "success")
    return redirect(url_for('rh_contracts'))

@app.route('/rh/contrats-rh/edit/<int:cid>', methods=['GET','POST'])
@permission_required('fichiers')
def rh_contracts_edit(cid):
    from models import db_get_by_id, db_update
    contract = db_get_by_id('rh_contracts', cid)
    if not contract: flash("Contrat non trouvé","error"); return redirect(url_for('rh_contracts'))
    if request.method == 'POST':
        db_update('rh_contracts', cid,
            code=request.form.get('code',''),
            employee_id=int(request.form.get('employee_id', contract['employee_id'])),
            contract_type=request.form.get('contract_type','CDI'),
            start_date=request.form.get('start_date',''),
            end_date=request.form.get('end_date',''),
            salary=float(request.form.get('salary',0) or 0),
            department=request.form.get('department',''),
            signature_date=request.form.get('signature_date',''),
            status=request.form.get('status','actif'),
            notes=request.form.get('notes',''))
        flash("Contrat modifié","success")
        return redirect(url_for('rh_contracts'))
    employees = get_all_employees(status=None)
    return render_template('rh_contrats.html', page='contrats_rh', edit_contract=dict(contract), employees=employees, contracts=[])

@app.route('/rh/contrats-rh/status/<int:cid>/<status>')
@permission_required('fichiers')
def rh_contracts_status(cid, status):
    from models import db_update
    if status in ('actif','expiré','résilié','terminé'):
        db_update('rh_contracts', cid, status=status)
        flash(f"Contrat → {status}","success")
    return redirect(url_for('rh_contracts'))

@app.route('/rh/contrats-rh/delete/<int:cid>')
@permission_required('fichiers')
def rh_contracts_delete(cid):
    conn = _gdb()
    conn.execute("DELETE FROM rh_contracts WHERE id=?", (cid,))
    conn.commit(); conn.close()
    flash("Contrat supprimé","success")
    return redirect(url_for('rh_contracts'))


# ======================== ORGANIGRAMME ========================

@app.route('/rh/organigramme')
@permission_required('fichiers')
def rh_organigramme():
    employees = get_all_employees()
    depts = {}
    for e in employees:
        d = e.get('department') or 'Non assigné'
        if d not in depts: depts[d] = []
        depts[d].append(e)
    
    # Build hierarchy levels based on position keywords
    hierarchy = {0: [], 1: [], 2: [], 3: []}
    level_names = {0: 'Direction Générale', 1: 'Directeurs & Chefs', 2: 'Responsables', 3: 'Équipes'}
    for e in employees:
        pos = (e.get('position') or '').lower()
        if any(k in pos for k in ['dg', 'directeur général', 'pdg', 'ceo', 'gérant']):
            hierarchy[0].append(e)
        elif any(k in pos for k in ['directeur', 'director', 'chef de département', 'daf', 'drh', 'dsi']):
            hierarchy[1].append(e)
        elif any(k in pos for k in ['responsable', 'chef', 'superviseur', 'manager', 'coordinat']):
            hierarchy[2].append(e)
        else:
            hierarchy[3].append(e)
    
    return render_template('rh_organigramme.html', page='organigramme', depts=depts,
                          hierarchy=hierarchy, level_names=level_names)


# ======================== CHAT ========================

@app.route('/chat')
@permission_required('chat')
def chat_page():
    channel = request.args.get('channel', 'general')
    dm_user = request.args.get('dm')
    users = get_all_users()
    conn = _gdb()
    channels = [dict(r) for r in conn.execute("SELECT * FROM chat_channels ORDER BY name").fetchall()]
    
    # Recent DM contacts with last message
    dm_contacts = [dict(r) for r in conn.execute("""
        SELECT DISTINCT CASE WHEN m.sender_id=? THEN m.receiver_id ELSE m.sender_id END as uid,
            u.full_name, MAX(m.created_at) as last_msg,
            (SELECT content FROM messages WHERE 
                ((sender_id=? AND receiver_id=u.id) OR (sender_id=u.id AND receiver_id=?))
                ORDER BY created_at DESC LIMIT 1) as last_content,
            (SELECT COUNT(*) FROM messages WHERE sender_id=u.id AND receiver_id=? AND read=0) as unread
        FROM messages m
        JOIN users u ON u.id = CASE WHEN m.sender_id=? THEN m.receiver_id ELSE m.sender_id END
        WHERE (m.sender_id=? OR m.receiver_id=?) AND m.channel='direct'
        GROUP BY uid ORDER BY last_msg DESC
    """, (session['user_id'],)*7).fetchall()]
    
    if dm_user:
        msgs = get_direct_messages(session['user_id'], int(dm_user))
        target = get_user_by_id(int(dm_user))
        mark_chat_read(session['user_id'], '_dm_all')
        # Mark messages as read
        conn.execute("UPDATE messages SET status='read' WHERE sender_id=? AND receiver_id=? AND status!='read'",
            (int(dm_user), session['user_id']))
        conn.commit()
    else:
        msgs = get_messages(channel)
        target = None
        mark_chat_read(session['user_id'], channel)
    
    conn.close()
    return render_template('chat.html', page='chat', messages=msgs, users=users,
        channel=channel, dm_target=target, channels=channels, dm_contacts=dm_contacts,
        now_date=datetime.now().strftime('%Y-%m-%d'),
        yesterday_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))

@app.route('/chat/send', methods=['POST'])
@permission_required('chat')
def chat_send():
    content = request.form.get('content', '').strip()
    channel = request.form.get('channel', 'general')
    msg_type = request.form.get('message_type', 'text')
    dm_id = None
    if channel.startswith('dm_'):
        dm_id = int(channel.split('_')[1])
    
    # Handle file upload
    file_url = ''
    file_name = ''
    if 'file' in request.files and request.files['file'].filename:
        f = request.files['file']
        from werkzeug.utils import secure_filename as _sf
        fname = f"{int(datetime.now().timestamp())}_{_sf(f.filename)}"
        fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'chat_files')
        os.makedirs(fdir, exist_ok=True)
        f.save(os.path.join(fdir, fname))
        file_url = f'/uploads/chat_files/{fname}'
        file_name = f.filename
        msg_type = 'file'
        if not content: content = f'📎 {f.filename}'
    
    if content or file_url:
        conn = _gdb()
        if dm_id:
            conn.execute("""INSERT INTO messages (sender_id, receiver_id, channel, content, message_type, file_url, file_name, status)
                VALUES (?,?,'direct',?,?,?,?,'sent')""",
                (session['user_id'], dm_id, content, msg_type, file_url, file_name))
        else:
            conn.execute("""INSERT INTO messages (sender_id, channel, content, message_type, file_url, file_name, status)
                VALUES (?,?,?,?,?,?,'sent')""",
                (session['user_id'], channel, content, msg_type, file_url, file_name))
        conn.commit(); conn.close()
    
    if dm_id: return redirect(f'/chat?dm={dm_id}')
    return redirect(f'/chat?channel={channel}')

@app.route('/chat/api')
@login_required
def chat_api():
    channel = request.args.get('channel', 'general')
    dm_user = request.args.get('dm')
    after_id = int(request.args.get('after', 0) or 0)
    conn = _gdb()
    if dm_user:
        uid = int(dm_user)
        msgs = [dict(r) for r in conn.execute("""SELECT m.*, u.full_name as sender_name FROM messages m
            LEFT JOIN users u ON m.sender_id=u.id
            WHERE m.channel='direct' AND ((m.sender_id=? AND m.receiver_id=?) OR (m.sender_id=? AND m.receiver_id=?))
            AND m.id > ? ORDER BY m.created_at""",
            (session['user_id'], uid, uid, session['user_id'], after_id)).fetchall()]
        # Mark as read
        conn.execute("UPDATE messages SET status='read' WHERE sender_id=? AND receiver_id=? AND status!='read'",
            (uid, session['user_id']))
        conn.commit()
    else:
        msgs = [dict(r) for r in conn.execute("""SELECT m.*, u.full_name as sender_name FROM messages m
            LEFT JOIN users u ON m.sender_id=u.id
            WHERE m.channel=? AND m.id > ? ORDER BY m.created_at""",
            (channel, after_id)).fetchall()]
    
    # Typing indicators
    typing_users = [dict(r) for r in conn.execute("""SELECT u.full_name FROM chat_typing ct
        JOIN users u ON ct.user_id=u.id
        WHERE ct.channel=? AND ct.user_id!=? AND ct.updated_at > datetime('now', '-5 seconds')""",
        (channel if not dm_user else f'dm_{dm_user}', session['user_id'])).fetchall()]
    
    conn.close()
    return jsonify({
        'messages': [{'id': m['id'], 'sender': m.get('sender_name','?'), 'sender_id': m['sender_id'],
                      'content': m['content'], 'time': (m['created_at'] or '')[-8:-3],
                      'type': m.get('message_type','text'), 'file_url': m.get('file_url',''),
                      'file_name': m.get('file_name',''), 'status': m.get('status','sent')} for m in msgs],
        'typing': [t['full_name'] for t in typing_users]
    })

@app.route('/chat/typing', methods=['POST'])
@login_required
def chat_typing():
    channel = request.form.get('channel', 'general')
    conn = _gdb()
    conn.execute("DELETE FROM chat_typing WHERE user_id=? AND channel=?", (session['user_id'], channel))
    conn.execute("INSERT INTO chat_typing (user_id, channel, updated_at) VALUES (?,?,datetime('now'))",
        (session['user_id'], channel))
    conn.commit(); conn.close()
    return jsonify({'ok': True})

@app.route('/chat/unread')
@login_required
def chat_unread_api():
    count = get_unread_count(session['user_id'])
    return jsonify({'unread': count})

@app.route('/chat/ticket', methods=['POST'])
@permission_required('chat')
def chat_create_ticket():
    msg_id = int(request.form.get('message_id', 0) or 0)
    title = request.form.get('title', 'Ticket depuis chat')
    conn = _gdb()
    conn.execute("INSERT INTO chat_tickets (message_id, title, created_by, status, priority) VALUES (?,?,?,'ouvert','normal')",
        (msg_id, title, session['user_id']))
    conn.commit(); conn.close()
    flash("Ticket créé depuis le chat", "success")
    return redirect('/chat')

@app.route('/chat/supervision')
@permission_required('admin')
def chat_supervision():
    conn = _gdb()
    stats = {
        'total_messages': conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
        'today': conn.execute("SELECT COUNT(*) FROM messages WHERE date(created_at)=date('now')").fetchone()[0],
        'active_users': conn.execute("SELECT COUNT(DISTINCT sender_id) FROM messages WHERE created_at > datetime('now','-24 hours')").fetchone()[0],
        'unread_total': conn.execute("SELECT COUNT(*) FROM messages WHERE read=0").fetchone()[0],
    }
    recent = [dict(r) for r in conn.execute("""SELECT m.*, u.full_name as sender_name FROM messages m
        LEFT JOIN users u ON m.sender_id=u.id ORDER BY m.created_at DESC LIMIT 30""").fetchall()]
    user_stats = [dict(r) for r in conn.execute("""SELECT u.full_name, COUNT(m.id) as msg_count,
        MAX(m.created_at) as last_active FROM messages m LEFT JOIN users u ON m.sender_id=u.id
        GROUP BY m.sender_id ORDER BY msg_count DESC""").fetchall()]
    tickets = [dict(r) for r in conn.execute("""SELECT t.*, u.full_name as creator FROM chat_tickets t
        LEFT JOIN users u ON t.created_by=u.id ORDER BY t.created_at DESC LIMIT 20""").fetchall()]
    conn.close()
    return render_template('chat_supervision.html', page='chat', stats=stats,
        recent=recent, user_stats=user_stats, tickets=tickets)


# ======================== APPELS AUDIO/VIDEO ========================

@app.route('/call/start', methods=['POST'])
@login_required
def call_start():
    """Initier un appel."""
    target_id = int(request.form.get('target_id', 0))
    call_type = request.form.get('type', 'audio')
    room = request.form.get('room', '') or f"wannygest-{session['user_id']}-{target_id}-{int(datetime.now().timestamp())}"
    
    conn = _gdb()
    # Clear old ringing calls from this caller
    conn.execute("DELETE FROM calls WHERE caller_id=? AND status='ringing'", (session['user_id'],))
    
    if target_id > 0:
        # Direct call to specific user
        conn.execute("""INSERT INTO calls (caller_id, callee_id, room, call_type, status)
            VALUES (?, ?, ?, ?, 'ringing')""",
            (session['user_id'], target_id, room, call_type))
    else:
        # Channel call — ring all other users
        users = conn.execute("SELECT id FROM users WHERE id!=?", (session['user_id'],)).fetchall()
        for u in users:
            conn.execute("""INSERT INTO calls (caller_id, callee_id, room, call_type, status)
                VALUES (?, ?, ?, ?, 'ringing')""",
                (session['user_id'], u['id'], room, call_type))
    
    conn.commit(); conn.close()
    return jsonify({'room': room, 'url': f'https://meet.jit.si/{room}'})

@app.route('/call/check')
@login_required
def call_check():
    """Vérifie si un appel entrant est en cours (polling)."""
    conn = _gdb()
    # Auto-expire ringing calls older than 60s
    conn.execute("""UPDATE calls SET status='missed' 
        WHERE status='ringing' AND created_at < datetime('now', '-60 seconds')""")
    conn.commit()
    
    call = conn.execute("""SELECT c.*, u.full_name as caller_name FROM calls c
        JOIN users u ON c.caller_id=u.id
        WHERE c.callee_id=? AND c.status='ringing'
        ORDER BY c.created_at DESC LIMIT 1""", (session['user_id'],)).fetchone()
    conn.close()
    if call:
        return jsonify({
            'incoming': True,
            'call_id': call['id'],
            'caller': call['caller_name'],
            'type': call['call_type'],
            'room': call['room'],
            'url': f"https://meet.jit.si/{call['room']}"
        })
    return jsonify({'incoming': False})

@app.route('/call/<int:cid>/accept')
@login_required
def call_accept(cid):
    conn = _gdb()
    call = conn.execute("SELECT * FROM calls WHERE id=? AND callee_id=?", (cid, session['user_id'])).fetchone()
    if call:
        conn.execute("UPDATE calls SET status='active' WHERE id=?", (cid,))
        conn.commit()
        conn.close()
        return jsonify({'url': f"https://meet.jit.si/{call['room']}", 'room': call['room']})
    conn.close()
    return jsonify({'error': 'Appel non trouvé'}), 404

@app.route('/call/<int:cid>/reject')
@login_required
def call_reject(cid):
    conn = _gdb()
    conn.execute("UPDATE calls SET status='rejected' WHERE id=? AND callee_id=?", (cid, session['user_id']))
    conn.commit(); conn.close()
    return jsonify({'ok': True})

@app.route('/call/<int:cid>/end')
@login_required
def call_end(cid):
    conn = _gdb()
    conn.execute("UPDATE calls SET status='ended', ended_at=CURRENT_TIMESTAMP WHERE id=?", (cid,))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


# ======================== RH EXPANDED ========================

@app.route('/rh/postes')
@permission_required('fichiers')
def rh_postes():
    from models import db_get_all
    postes = db_get_all('rh_job_descriptions', order='title ASC')
    return render_template('rh_postes.html', page='postes', postes=postes)

@app.route('/rh/postes/add', methods=['POST'])
@permission_required('fichiers')
def rh_postes_add():
    from models import db_insert
    db_insert('rh_job_descriptions', title=request.form['title'],
        department=request.form.get('department',''), description=request.form.get('description',''),
        requirements=request.form.get('requirements',''), responsibilities=request.form.get('responsibilities',''),
        salary_range=request.form.get('salary_range',''))
    flash("Poste ajouté", "success"); return redirect(url_for('rh_postes'))

@app.route('/rh/formations')
@permission_required('fichiers')
def rh_formations():
    from models import db_get_all
    u = dict(get_user_by_id(session["user_id"]) or {})
    all_trainings = db_get_all('rh_trainings', order='date DESC')
    # Filter: show only trainings for user's department or "tous"
    if u['role'] in ('admin', 'dg', 'rh'):
        trainings = all_trainings  # Admin/DG/RH see all
    else:
        dept = u.get('department', '') or u.get('role', '')
        trainings = [t for t in all_trainings if not t.get('target') or t.get('target') == 'tous' or t.get('target','').lower() == dept.lower()]
    return render_template('rh_formations.html', page='formations', trainings=trainings)

@app.route('/rh/formations/add', methods=['POST'])
@permission_required('fichiers')
def rh_formations_add():
    from models import db_insert
    target = request.form.get('target', 'tous')
    db_insert('rh_trainings', title=request.form['title'], description=request.form.get('description',''),
        trainer=request.form.get('trainer',''), date=request.form.get('date',''),
        duration=request.form.get('duration',''),
        department=request.form.get('department',''),
        cost=request.form.get('cost','0'),
        target=target)
    flash("Formation ajoutée", "success"); return redirect(url_for('rh_formations'))

@app.route('/rh/formations/edit/<int:fid>', methods=['POST'])
@permission_required('fichiers')
def rh_formations_edit(fid):
    from models import db_update
    db_update('rh_trainings', fid, title=request.form['title'], description=request.form.get('description',''),
        trainer=request.form.get('trainer',''), date=request.form.get('date',''),
        duration=request.form.get('duration',''), department=request.form.get('department',''),
        cost=request.form.get('cost','0'), target=request.form.get('target','tous'),
        status=request.form.get('status','planifie'))
    flash("Formation modifiée", "success"); return redirect(url_for('rh_formations'))

@app.route('/rh/formations/delete/<int:fid>')
@permission_required('fichiers')
def rh_formations_delete(fid):
    conn = _gdb()
    conn.execute("DELETE FROM rh_trainings WHERE id=?", (fid,))
    conn.commit(); conn.close()
    flash("Formation supprimée", "success"); return redirect(url_for('rh_formations'))

@app.route('/rh/annonces')
@permission_required('fichiers')
def rh_annonces():
    from models import db_get_all
    annonces = db_get_all('rh_announcements', order='created_at DESC')
    return render_template('rh_annonces.html', page='annonces', annonces=annonces)

@app.route('/rh/annonces/add', methods=['POST'])
@permission_required('fichiers')
def rh_annonces_add():
    from models import db_insert
    db_insert('rh_announcements', title=request.form['title'], content=request.form.get('content',''),
        priority=request.form.get('priority','normale'), created_by=session['user_id'])
    flash("Annonce publiée", "success"); return redirect(url_for('rh_annonces'))

@app.route('/rh/annonces/edit/<int:aid>', methods=['POST'])
@permission_required('fichiers')
def rh_annonces_edit(aid):
    conn = _gdb()
    conn.execute("UPDATE rh_announcements SET title=?, content=?, priority=? WHERE id=?",
        (request.form['title'], request.form.get('content',''), request.form.get('priority','normale'), aid))
    conn.commit(); conn.close()
    flash("Annonce modifiée", "success"); return redirect(url_for('rh_annonces'))

@app.route('/rh/annonces/delete/<int:aid>')
@permission_required('fichiers')
def rh_annonces_delete(aid):
    conn = _gdb()
    conn.execute("DELETE FROM rh_announcements WHERE id=?", (aid,))
    conn.commit(); conn.close()
    flash("Annonce supprimée", "success"); return redirect(url_for('rh_annonces'))


# ======================== MODULE TRACKING GPS ========================

@app.route('/tracking')
@permission_required('tracking')
def tracking_dashboard():
    conn = _gdb()
    vehicles = [dict(r) for r in conn.execute("SELECT * FROM tracking_vehicles ORDER BY status, immatriculation").fetchall()]
    actifs = len([v for v in vehicles if v['status'] == 'actif'])
    en_mouvement = len([v for v in vehicles if v.get('last_speed',0) and float(v.get('last_speed',0) or 0) > 2])
    alerts_new = conn.execute("SELECT COUNT(*) FROM tracking_alerts WHERE acknowledged=0").fetchone()[0]
    recent_alerts = [dict(r) for r in conn.execute("""SELECT a.*, v.immatriculation, v.marque 
        FROM tracking_alerts a LEFT JOIN tracking_vehicles v ON a.vehicle_id=v.id
        ORDER BY a.created_at DESC LIMIT 10""").fetchall()]
    conn.close()
    return render_template('tracking_dashboard.html', page='tracking', vehicles=vehicles,
        actifs=actifs, en_mouvement=en_mouvement, alerts_new=alerts_new, recent_alerts=recent_alerts)

@app.route('/tracking/vehicules')
@permission_required('tracking')
def tracking_vehicules():
    conn = _gdb()
    vehicles = [dict(r) for r in conn.execute("""SELECT v.*, u.full_name as tech_name 
        FROM tracking_vehicles v LEFT JOIN users u ON v.created_by=u.id
        ORDER BY v.created_at DESC""").fetchall()]
    conn.close()
    return render_template('tracking_vehicules.html', page='tracking_vehicules', vehicles=vehicles)

@app.route('/tracking/vehicules/add', methods=['POST'])
@permission_required('tracking')
def tracking_vehicule_add():
    conn = _gdb()
    conn.execute("""INSERT INTO tracking_vehicles (immatriculation, marque, modele, type, couleur, annee,
        proprietaire, tel_proprietaire, gps_device_id, gps_brand, gps_model, gps_sim, gps_imei,
        installation_date, installation_tech, notes, created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (request.form.get('immatriculation',''), request.form.get('marque',''),
         request.form.get('modele',''), request.form.get('type','voiture'),
         request.form.get('couleur',''), request.form.get('annee',''),
         request.form.get('proprietaire',''), request.form.get('tel_proprietaire',''),
         request.form.get('gps_device_id',''), request.form.get('gps_brand','Concox'),
         request.form.get('gps_model',''), request.form.get('gps_sim',''),
         request.form.get('gps_imei',''), request.form.get('installation_date',''),
         request.form.get('installation_tech',''), request.form.get('notes',''),
         session['user_id']))
    conn.commit(); conn.close()
    flash("Véhicule ajouté","success"); return redirect('/tracking/vehicules')

@app.route('/tracking/vehicules/edit/<int:vid>', methods=['GET','POST'])
@permission_required('tracking')
def tracking_vehicule_edit(vid):
    conn = _gdb()
    v = conn.execute("SELECT * FROM tracking_vehicles WHERE id=?", (vid,)).fetchone()
    if not v: flash("Non trouvé","error"); return redirect('/tracking/vehicules')
    if request.method == 'POST':
        for col in ['immatriculation','marque','modele','type','couleur','annee',
                     'proprietaire','tel_proprietaire','gps_device_id','gps_brand',
                     'gps_model','gps_sim','gps_imei','installation_date',
                     'installation_tech','status','notes']:
            val = request.form.get(col, '')
            conn.execute(f"UPDATE tracking_vehicles SET {col}=? WHERE id=?", (val, vid))
        conn.commit(); conn.close()
        flash("Véhicule modifié","success"); return redirect('/tracking/vehicules')
    conn.close()
    return render_template('tracking_vehicule_view.html', page='tracking_vehicules', vehicle=dict(v))

@app.route('/tracking/vehicules/delete/<int:vid>')
@permission_required('tracking')
def tracking_vehicule_delete(vid):
    conn = _gdb()
    v = conn.execute("SELECT immatriculation FROM tracking_vehicles WHERE id=?", (vid,)).fetchone()
    if v:
        conn.execute("DELETE FROM tracking_history WHERE vehicle_id=?", (vid,))
        conn.execute("DELETE FROM tracking_alerts WHERE vehicle_id=?", (vid,))
        conn.execute("DELETE FROM tracking_vehicles WHERE id=?", (vid,))
        conn.commit()
        flash(f"Véhicule {v['immatriculation']} supprimé", "success")
    conn.close()
    return redirect('/tracking/vehicules')

@app.route('/tracking/vehicules/view/<int:vid>')
@permission_required('tracking')
def tracking_vehicule_view(vid):
    conn = _gdb()
    v = conn.execute("SELECT * FROM tracking_vehicles WHERE id=?", (vid,)).fetchone()
    if not v: flash("Non trouvé","error"); return redirect('/tracking/vehicules')
    history = [dict(r) for r in conn.execute(
        "SELECT * FROM tracking_history WHERE vehicle_id=? ORDER BY created_at DESC LIMIT 50", (vid,)).fetchall()]
    alerts = [dict(r) for r in conn.execute(
        "SELECT * FROM tracking_alerts WHERE vehicle_id=? ORDER BY created_at DESC LIMIT 20", (vid,)).fetchall()]
    conn.close()
    return render_template('tracking_vehicule_view.html', page='tracking_vehicules', vehicle=dict(v), history=history, alerts=alerts)

@app.route('/tracking/carte')
@permission_required('tracking')
def tracking_carte():
    conn = _gdb()
    vehicles = [dict(r) for r in conn.execute(
        "SELECT * FROM tracking_vehicles WHERE status='actif' AND last_lat IS NOT NULL ORDER BY immatriculation").fetchall()]
    conn.close()
    return render_template('tracking_carte.html', page='tracking_carte', vehicles=vehicles)

@app.route('/tracking/alertes')
@permission_required('tracking')
def tracking_alertes():
    conn = _gdb()
    alerts = [dict(r) for r in conn.execute("""SELECT a.*, v.immatriculation, v.marque, v.modele
        FROM tracking_alerts a LEFT JOIN tracking_vehicles v ON a.vehicle_id=v.id
        ORDER BY a.created_at DESC LIMIT 100""").fetchall()]
    conn.close()
    return render_template('tracking_alertes.html', page='tracking_alertes', alerts=alerts)

@app.route('/tracking/alertes/<int:aid>/ack')
@permission_required('tracking')
def tracking_alert_ack(aid):
    conn = _gdb()
    conn.execute("UPDATE tracking_alerts SET acknowledged=1 WHERE id=?", (aid,))
    conn.commit(); conn.close()
    flash("Alerte acquittée","success"); return redirect('/tracking/alertes')

@app.route('/tracking/position/update', methods=['POST'])
def tracking_position_update():
    """API endpoint pour recevoir les positions GPS (depuis les balises ou un serveur intermédiaire)."""
    data = request.get_json(silent=True) or {}
    device_id = data.get('device_id','')
    if not device_id: return jsonify({'error': 'device_id requis'}), 400
    conn = _gdb()
    v = conn.execute("SELECT id FROM tracking_vehicles WHERE gps_device_id=?", (device_id,)).fetchone()
    if not v: conn.close(); return jsonify({'error': 'device inconnu'}), 404
    lat = float(data.get('lat',0)); lng = float(data.get('lng',0)); speed = float(data.get('speed',0))
    addr = data.get('address','')
    conn.execute("UPDATE tracking_vehicles SET last_lat=?, last_lng=?, last_speed=?, last_address=?, last_update=? WHERE id=?",
        (lat, lng, speed, addr, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), v['id']))
    conn.execute("INSERT INTO tracking_history (vehicle_id, lat, lng, speed, address) VALUES (?,?,?,?,?)",
        (v['id'], lat, lng, speed, addr))
    # Alert: excès de vitesse
    if speed > 120:
        conn.execute("INSERT INTO tracking_alerts (vehicle_id, alert_type, message, lat, lng) VALUES (?,?,?,?,?)",
            (v['id'], 'vitesse', f'Excès de vitesse: {speed:.0f} km/h', lat, lng))
    conn.commit(); conn.close()
    return jsonify({'ok': True})

# ======================== MODULE IT ========================

@app.route('/it')
@permission_required('informatique')
def it_dashboard():
    conn = _gdb()
    equip_total = conn.execute("SELECT COUNT(*) FROM it_equipment").fetchone()[0]
    equip_actif = conn.execute("SELECT COUNT(*) FROM it_equipment WHERE status='actif'").fetchone()[0]
    tickets_open = conn.execute("SELECT COUNT(*) FROM it_tickets WHERE status IN ('ouvert','en_cours')").fetchone()[0]
    tickets_resolved = conn.execute("SELECT COUNT(*) FROM it_tickets WHERE status='resolu'").fetchone()[0]
    tickets_recent = [dict(r) for r in conn.execute("""SELECT t.*, u.full_name as requester, u2.full_name as assignee
        FROM it_tickets t LEFT JOIN users u ON t.requester_id=u.id LEFT JOIN users u2 ON t.assigned_to=u2.id
        ORDER BY t.created_at DESC LIMIT 10""").fetchall()]
    logs_recent = [dict(r) for r in conn.execute("SELECT * FROM it_logs ORDER BY created_at DESC LIMIT 10").fetchall()]
    conn.close()
    return render_template('it_dashboard.html', page='it', equip_total=equip_total, equip_actif=equip_actif,
        tickets_open=tickets_open, tickets_resolved=tickets_resolved, tickets_recent=tickets_recent, logs_recent=logs_recent)

@app.route('/it/parc')
@permission_required('informatique')
def it_parc():
    conn = _gdb()
    equipment = [dict(r) for r in conn.execute("""SELECT e.*, u.full_name as user_name FROM it_equipment e
        LEFT JOIN users u ON e.assigned_to=u.id ORDER BY e.name""").fetchall()]
    users = get_all_users()
    conn.close()
    return render_template('it_parc.html', page='it_parc', equipment=equipment, users=users)

@app.route('/it/parc/add', methods=['POST'])
@permission_required('informatique')
def it_parc_add():
    conn = _gdb()
    conn.execute("""INSERT INTO it_equipment (name, type, brand, model, serial_number, assigned_to, location,
        status, purchase_date, purchase_price, warranty_end, notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (request.form.get('name',''), request.form.get('type',''), request.form.get('brand',''),
         request.form.get('model',''), request.form.get('serial_number',''),
         int(request.form.get('assigned_to',0) or 0) or None, request.form.get('location',''),
         'actif', request.form.get('purchase_date',''),
         float(request.form.get('purchase_price',0) or 0), request.form.get('warranty_end',''),
         request.form.get('notes','')))
    conn.commit(); conn.close()
    flash("Équipement ajouté","success"); return redirect('/it/parc')

@app.route('/it/tickets')
@permission_required('informatique')
def it_tickets():
    tab = request.args.get('tab', 'ouvert')
    conn = _gdb()
    if tab == 'all':
        tickets = [dict(r) for r in conn.execute("""SELECT t.*, u.full_name as requester, u2.full_name as assignee
            FROM it_tickets t LEFT JOIN users u ON t.requester_id=u.id LEFT JOIN users u2 ON t.assigned_to=u2.id
            ORDER BY t.created_at DESC""").fetchall()]
    else:
        tickets = [dict(r) for r in conn.execute("""SELECT t.*, u.full_name as requester, u2.full_name as assignee
            FROM it_tickets t LEFT JOIN users u ON t.requester_id=u.id LEFT JOIN users u2 ON t.assigned_to=u2.id
            WHERE t.status=? ORDER BY t.created_at DESC""", (tab,)).fetchall()]
    stats = {
        'ouvert': conn.execute("SELECT COUNT(*) FROM it_tickets WHERE status='ouvert'").fetchone()[0],
        'en_cours': conn.execute("SELECT COUNT(*) FROM it_tickets WHERE status='en_cours'").fetchone()[0],
        'resolu': conn.execute("SELECT COUNT(*) FROM it_tickets WHERE status='resolu'").fetchone()[0],
    }
    users = get_all_users()
    equipment = [dict(r) for r in conn.execute("SELECT id, name FROM it_equipment ORDER BY name").fetchall()]
    conn.close()
    return render_template('it_tickets.html', page='it_tickets', tickets=tickets, tab=tab, stats=stats, users=users, equipment=equipment)

@app.route('/it/tickets/add', methods=['POST'])
@permission_required('informatique')
def it_ticket_add():
    conn = _gdb()
    conn.execute("""INSERT INTO it_tickets (title, description, category, priority, status, requester_id, assigned_to, equipment_id)
        VALUES (?,?,?,?,?,?,?,?)""",
        (request.form.get('title',''), request.form.get('description',''),
         request.form.get('category','incident'), request.form.get('priority','normal'),
         'ouvert', session['user_id'],
         int(request.form.get('assigned_to',0) or 0) or None,
         int(request.form.get('equipment_id',0) or 0) or None))
    conn.commit(); conn.close()
    flash("Ticket créé","success"); return redirect('/it/tickets')

@app.route('/it/tickets/<int:tid>/status/<status>')
@permission_required('informatique')
def it_ticket_status(tid, status):
    conn = _gdb()
    if status in ('ouvert','en_cours','resolu','ferme'):
        updates = {'status': status}
        if status == 'resolu': updates['resolved_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
        conn.execute(f"UPDATE it_tickets SET status=?, resolved_at=? WHERE id=?",
            (status, updates.get('resolved_at',''), tid))
        conn.commit()
    conn.close()
    flash("Ticket mis à jour","success"); return redirect('/it/tickets')

@app.route('/it/securite')
@permission_required('informatique')
def it_securite():
    conn = _gdb()
    logs = [dict(r) for r in conn.execute("SELECT * FROM it_logs ORDER BY created_at DESC LIMIT 50").fetchall()]
    login_logs = [dict(r) for r in conn.execute("SELECT * FROM activity_logs WHERE action LIKE '%Connexion%' ORDER BY created_at DESC LIMIT 20").fetchall()]
    conn.close()
    return render_template('it_securite.html', page='it_securite', logs=logs, login_logs=login_logs)

# ======================== KANBAN ========================

@app.route('/kanban')
@permission_required('resp_projet')
def kanban():
    from models import db_get_all, get_all_users, get_db
    user = get_user_by_id(session['user_id'])
    if user and user['role'] == 'admin':
        tasks = db_get_all('tasks')
    else:
        tasks = db_get_all('tasks', where={'assigned_to': session['user_id']})
    # Enrich with project names
    conn = get_db()
    projects = {}
    try:
        for p in conn.execute("SELECT id, name FROM projects").fetchall():
            projects[p['id']] = p['name']
    except: pass
    conn.close()
    for t in tasks:
        t['project_name'] = projects.get(t.get('project_id'), '')
    by_status = {'a_faire': [], 'en_cours': [], 'en_revue': [], 'termine': []}
    for t in tasks:
        s = t.get('status', 'a_faire')
        if s in by_status: by_status[s].append(t)
        else: by_status['a_faire'].append(t)
    users = get_all_users()
    user_map = {u['id']: u['full_name'] for u in users}
    return render_template('kanban.html', page='kanban', by_status=by_status, users=users, user_map=user_map)

@app.route('/taches')
@login_required
def taches_list():
    return redirect(url_for('kanban'))

@app.route('/taches/new')
@login_required
def taches_new():
    return redirect('/resp-projet/projets')

@app.route('/kanban/move/<int:tid>/<status>')
@login_required
def kanban_move(tid, status):
    from models import db_update
    if status in ('a_faire','en_cours','en_revue','termine'):
        db_update('tasks', tid, status=status)
        user = get_user_by_id(session['user_id'])
        log_audit(session['user_id'], user['full_name'] if user else '?', 'tasks', tid, 'status_change', 'status', '', status)
    return redirect(url_for('kanban'))


# ======================== HISTORIQUE DES MODIFICATIONS ========================

@app.route('/historique')
@permission_required('admin')
def historique():
    logs = get_activity_logs(limit=200)
    conn = _gdb()
    try:
        trail = [dict(r) for r in conn.execute("SELECT * FROM audit_trail ORDER BY created_at DESC LIMIT 100").fetchall()]
    except: trail = []
    conn.close()
    return render_template('historique.html', page='historique', trail=trail, logs=logs, filter_table=request.args.get('table',''))


# ======================== TABLEAU DE BORD EXÉCUTIF ========================

@app.route('/executif')
@permission_required('admin')
def executif():
    stats = get_executive_stats()
    return render_template('extra_pages.html', page='executif', s=stats)

@app.route('/executif/pdf')
@permission_required('admin')
def executif_pdf():
    """Génère le rapport PDF mensuel exécutif."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.colors import HexColor
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT

    s = get_executive_stats()
    output = os.path.join(app.config['UPLOAD_FOLDER'], 'rapport_executif.pdf')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    doc = SimpleDocTemplate(output, pagesize=A4, leftMargin=20*mm, rightMargin=20*mm, topMargin=15*mm, bottomMargin=15*mm)

    NAVY = HexColor('#1a3a5c'); ORANGE = HexColor('#e8672a')
    s_t = ParagraphStyle('t', fontSize=20, fontName='Helvetica-Bold', textColor=NAVY, alignment=TA_CENTER)
    s_sub = ParagraphStyle('sub', fontSize=11, alignment=TA_CENTER, textColor=HexColor('#888'))
    s_h = ParagraphStyle('h', fontSize=14, fontName='Helvetica-Bold', textColor=NAVY, spaceBefore=12, spaceAfter=6)
    s_n = ParagraphStyle('n', fontSize=10)
    s_r = ParagraphStyle('r', fontSize=10, alignment=TA_RIGHT)
    s_hd = ParagraphStyle('hd', fontSize=9, fontName='Helvetica-Bold', textColor=HexColor('#fff'))
    s_c = ParagraphStyle('c', fontSize=9)
    fmt = lambda x: f"{x:,.0f}"

    story = []
    story.append(Paragraph("RAPPORT EXÉCUTIF MENSUEL", s_t))
    from datetime import datetime
    story.append(Paragraph(f"WannyGest — {datetime.now().strftime('%B %Y')}", s_sub))
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width="100%", thickness=2, color=NAVY))
    story.append(Spacer(1, 6*mm))

    # KPIs table
    story.append(Paragraph("Indicateurs Clés", s_h))
    kpis = [
        [Paragraph(h, s_hd) for h in ['Indicateur', 'Valeur']],
        [Paragraph('Clients actifs', s_c), Paragraph(str(s['clients']), s_r)],
        [Paragraph('Employés', s_c), Paragraph(str(s['employes']), s_r)],
        [Paragraph('Rapports traités', s_c), Paragraph(str(s['rapports']), s_r)],
        [Paragraph('Prospects totaux', s_c), Paragraph(str(s['prospects']), s_r)],
        [Paragraph('Prospects gagnés', s_c), Paragraph(str(s['prospects_gagnes']), s_r)],
    ]
    t = Table(kpis, colWidths=[100*mm, 60*mm])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),NAVY),('GRID',(0,0),(-1,-1),0.5,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story.append(t)
    story.append(Spacer(1, 6*mm))

    # Finance
    story.append(Paragraph("Finance", s_h))
    fin = [
        [Paragraph(h, s_hd) for h in ['Rubrique', 'Montant (FCFA)']],
        [Paragraph('CA facturé', s_c), Paragraph(fmt(s['montant_facture']), s_r)],
        [Paragraph('CA encaissé', s_c), Paragraph(fmt(s['montant_paye']), s_r)],
        [Paragraph('Impayé', s_c), Paragraph(fmt(s['montant_impaye']), s_r)],
        [Paragraph('CA devis acceptés', s_c), Paragraph(fmt(s['ca_devis']), s_r)],
        [Paragraph('Recettes trésorerie', s_c), Paragraph(fmt(s['recettes']), s_r)],
        [Paragraph('Dépenses', s_c), Paragraph(fmt(s['depenses']), s_r)],
        [Paragraph('<b>Solde</b>', s_c), Paragraph(f"<b>{fmt(s['solde'])}</b>", s_r)],
    ]
    t2 = Table(fin, colWidths=[100*mm, 60*mm])
    t2.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),ORANGE),('GRID',(0,0),(-1,-1),0.5,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('BACKGROUND',(0,7),(-1,7),HexColor('#e8f0f5'))]))
    story.append(t2)
    story.append(Spacer(1, 6*mm))

    # Commercial
    story.append(Paragraph("Commercial", s_h))
    com = [
        [Paragraph(h, s_hd) for h in ['Métrique', 'Valeur']],
        [Paragraph('Devis émis', s_c), Paragraph(str(s['devis_total']), s_r)],
        [Paragraph('Devis acceptés', s_c), Paragraph(str(s['devis_acceptes']), s_r)],
        [Paragraph('Taux conversion', s_c), Paragraph(f"{s['devis_acceptes']*100//max(s['devis_total'],1)}%", s_r)],
        [Paragraph('Factures émises', s_c), Paragraph(str(s['factures_total']), s_r)],
        [Paragraph('Factures payées', s_c), Paragraph(str(s['factures_payees']), s_r)],
    ]
    t3 = Table(com, colWidths=[100*mm, 60*mm])
    t3.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),HexColor('#2e7d32')),('GRID',(0,0),(-1,-1),0.5,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story.append(t3)
    story.append(Spacer(1, 6*mm))

    # RH
    story.append(Paragraph("Ressources Humaines", s_h))
    rh = [
        [Paragraph(h, s_hd) for h in ['Indicateur', 'Valeur']],
        [Paragraph('Effectif actif', s_c), Paragraph(str(s['employes']), s_r)],
        [Paragraph('Masse salariale mensuelle', s_c), Paragraph(fmt(s['masse_salariale']), s_r)],
        [Paragraph('Congés en attente', s_c), Paragraph(str(s['conges_pending']), s_r)],
        [Paragraph('Formations planifiées', s_c), Paragraph(str(s['formations']), s_r)],
    ]
    t4 = Table(rh, colWidths=[100*mm, 60*mm])
    t4.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),DARK_TEAL),('GRID',(0,0),(-1,-1),0.5,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story.append(t4)
    story.append(Spacer(1, 6*mm))

    # KPIs performance
    story.append(Paragraph("Indicateurs de Performance", s_h))
    taux_conv = s['devis_acceptes']*100//max(s['devis_total'],1)
    taux_recouv = s['factures_payees']*100//max(s['factures_total'],1)
    ca_client = s['montant_facture'] // max(s['clients'],1)
    perf = [
        [Paragraph(h, s_hd) for h in ['KPI', 'Valeur', 'Cible']],
        [Paragraph('Taux conversion devis', s_c), Paragraph(f"{taux_conv}%", s_r), Paragraph('> 20%', s_r)],
        [Paragraph('Taux recouvrement', s_c), Paragraph(f"{taux_recouv}%", s_r), Paragraph('> 80%', s_r)],
        [Paragraph('CA moyen / client', s_c), Paragraph(f"{fmt(ca_client)} F", s_r), Paragraph('> 500 000 F', s_r)],
    ]
    t5 = Table(perf, colWidths=[80*mm, 40*mm, 40*mm])
    t5.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),NAVY),('GRID',(0,0),(-1,-1),0.5,HexColor('#ddd')),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story.append(t5)

    story.append(Spacer(1, 15*mm))
    story.append(Paragraph(f"Document généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')} — WannyGest", ParagraphStyle('f', fontSize=8, alignment=TA_CENTER, textColor=HexColor('#999'))))

    doc.build(story)
    return send_file(output, as_attachment=True, download_name=f"Rapport_Executif_{datetime.now().strftime('%Y-%m')}.pdf")


# ======================== MODÈLES DE DEVIS ========================

@app.route('/devis/templates')
@permission_required('proforma')
def devis_templates_page():
    templates = get_devis_templates()
    return render_template('devis_templates.html', page='devis', templates=templates)

@app.route('/devis/templates/add', methods=['POST'])
@permission_required('proforma_edit')
def devis_templates_add():
    from models import db_insert
    items = []
    designations = request.form.getlist('item_designation[]')
    quantities = request.form.getlist('item_qty[]')
    prices = request.form.getlist('item_price[]')
    for d, q, p in zip(designations, quantities, prices):
        if d.strip():
            items.append({'designation': d, 'quantity': float(q or 1), 'unit_price': float(p or 0)})
    db_insert('devis_templates', name=request.form['name'],
        category=request.form.get('category', ''),
        description=request.form.get('description', ''),
        items_json=json.dumps(items),
        notes=request.form.get('notes', ''),
        created_by=session['user_id'])
    flash("Modèle de devis créé", "success")
    return redirect(url_for('devis_templates_page'))

@app.route('/devis/from-template/<int:tid>')
@permission_required('proforma_edit')
def devis_from_template(tid):
    tpl = get_devis_template(tid)
    if not tpl:
        flash("Modèle non trouvé", "error"); return redirect(url_for('devis_templates_page'))
    clients = get_all_clients()
    return render_template('devis_from_template.html', page='devis', tpl=tpl, clients=clients,
                          items=json.loads(tpl['items_json']) if tpl.get('items_json') else [])


# ======================== PIÈCE DE CAISSE SORTIE ========================

@app.route('/caisse-sortie')
@login_required
def caisse_sortie():
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    tab = request.args.get('tab', 'sorties')
    sorties = get_caisse_sorties(month=month)
    stats = get_caisse_stats(month=month)
    conn = _gdb()
    entrees = [dict(r) for r in conn.execute("SELECT * FROM caisse_entrees WHERE strftime('%Y-%m',date)=? ORDER BY date DESC", (month,)).fetchall()]
    total_entrees = conn.execute("SELECT COALESCE(SUM(montant),0) FROM caisse_entrees WHERE strftime('%Y-%m',date)=?", (month,)).fetchone()[0]
    caisses = [dict(r) for r in conn.execute("SELECT * FROM caisses WHERE is_active=1 ORDER BY name").fetchall()]
    caisses_map = {r['id']: r['name'] for r in caisses}
    # Enrichir les entrées/sorties avec le nom de la caisse
    for e in entrees:
        e['caisse_name'] = caisses_map.get(e.get('caisse_id'), '')
    for s in sorties:
        s['caisse_name'] = caisses_map.get(s.get('caisse_id'), '') if hasattr(s, 'get') else ''
    conn.close()
    return render_template('caisse_sortie.html', page='caisse_sortie', sorties=sorties, stats=stats, month=month,
        tab=tab, entrees=entrees, total_entrees=total_entrees, caisses=caisses)

@app.route('/caisse-sortie/demande', methods=['GET','POST'])
@login_required
def caisse_demande():
    """Tout le personnel peut faire une demande."""
    if request.method == 'POST':
        user = get_user_by_id(session['user_id'])
        ref = gen_caisse_ref()
        from models import get_db
        conn = get_db()
        montant = float(request.form.get('montant', 0) or 0)
        caisse_id_raw = request.form.get('caisse_id', '').strip()
        caisse_id = int(caisse_id_raw) if caisse_id_raw.isdigit() else None
        try:
            conn.execute("""INSERT INTO caisse_sorties (reference, date, beneficiaire, type_beneficiaire,
                montant, nature, motif, demandeur_id, demandeur_name, sig_beneficiaire, caisse_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (ref, request.form.get('date', datetime.now().strftime('%Y-%m-%d')),
                 request.form['beneficiaire'], request.form.get('type_beneficiaire', 'particulier'),
                 montant, request.form.get('nature', 'espece'), request.form.get('motif', ''),
                 session['user_id'], user['full_name'] if user else '?',
                 request.form.get('sig_beneficiaire', ''), caisse_id))
        except:
            # Fallback sans caisse_id
            conn.execute("""INSERT INTO caisse_sorties (reference, date, beneficiaire, type_beneficiaire,
                montant, nature, motif, demandeur_id, demandeur_name, sig_beneficiaire) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (ref, request.form.get('date', datetime.now().strftime('%Y-%m-%d')),
                 request.form['beneficiaire'], request.form.get('type_beneficiaire', 'particulier'),
                 montant, request.form.get('nature', 'espece'), request.form.get('motif', ''),
                 session['user_id'], user['full_name'] if user else '?',
                 request.form.get('sig_beneficiaire', '')))
        # Notifier DG et admin
        try:
            for u in conn.execute("SELECT id FROM users WHERE role IN ('admin','dg','directeur') AND is_active=1").fetchall():
                conn.execute("""INSERT INTO notifications (user_id, type, title, message, link)
                    VALUES (?,?,?,?,?)""",
                    (u['id'], 'caisse_demande',
                     f"💰 Demande sortie caisse — {montant:,.0f} F",
                     f"{ref} · {request.form['beneficiaire']} · {request.form.get('motif','')[:50]}",
                     "/caisse-sortie"))
        except: pass
        conn.commit(); conn.close()
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Caisse', f"Demande sortie {ref} — {montant:,.0f} F", request.remote_addr)
        flash(f"Demande de sortie de caisse {ref} envoyée au DG pour validation", "success")
        return redirect(url_for('caisse_sortie'))
    # GET : passer la liste des caisses au template
    conn = _gdb()
    caisses = [dict(r) for r in conn.execute("SELECT * FROM caisses WHERE is_active=1 ORDER BY name").fetchall()]
    conn.close()
    return render_template('caisse_demande.html', page='caisse_sortie', caisses=caisses)

@app.route('/caisse-sortie/<int:sid>/valider')
@login_required
def caisse_valider(sid):
    from models import get_db
    """DG ou admin valide la demande."""
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin', 'dg', 'directeur'):
        flash("Seul le DG peut valider les sorties de caisse", "error")
        return redirect(url_for('caisse_sortie'))
    conn = get_db()
    conn.execute("UPDATE caisse_sorties SET status='valide', valideur_id=?, valideur_name=?, validated_at=? WHERE id=? AND status='en_attente'",
                 (session['user_id'], user['full_name'], datetime.now().isoformat(), sid))
    conn.commit()
    s = conn.execute("SELECT * FROM caisse_sorties WHERE id=?", (sid,)).fetchone()
    conn.close()
    if s:
        log_activity(session['user_id'], user['full_name'], 'Caisse',
                    f"Sortie {s['reference']} validée — {s['montant']:,.0f} F", request.remote_addr)
    flash("Sortie de caisse validée → transmise à la comptabilité", "success")
    return redirect(url_for('caisse_sortie'))

@app.route('/caisse-sortie/<int:sid>/refuser')
@login_required
def caisse_refuser(sid):
    from models import get_db
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin', 'dg', 'directeur'):
        flash("Seul le DG peut refuser", "error")
        return redirect(url_for('caisse_sortie'))
    conn = get_db()
    conn.execute("UPDATE caisse_sorties SET status='refuse', valideur_id=?, valideur_name=?, validated_at=? WHERE id=? AND status='en_attente'",
                 (session['user_id'], user['full_name'], datetime.now().isoformat(), sid))
    conn.commit(); conn.close()
    flash("Demande refusée", "info")
    return redirect(url_for('caisse_sortie'))

@app.route('/caisse-sortie/<int:sid>/comptabiliser')
@login_required
def caisse_comptabiliser(sid):
    from models import get_db
    """La comptabilité enregistre le décaissement."""
    conn = get_db()
    conn.execute("UPDATE caisse_sorties SET comptabilise=1, comptabilise_at=? WHERE id=? AND status='valide'",
                 (datetime.now().isoformat(), sid))
    s = conn.execute("SELECT * FROM caisse_sorties WHERE id=?", (sid,)).fetchone()
    if s:
        s = dict(s)
        # Ajouter dans la trésorerie
        try:
            conn.execute("INSERT INTO treasury (type, amount, description, category, created_by, created_at) VALUES (?,?,?,?,?,?)",
                         ('depense', s['montant'], f"Sortie caisse {s['reference']} — {s['beneficiaire']} — {s['motif']}",
                          'sortie_caisse', session.get('user_id'), datetime.now().isoformat()))
        except: pass
        # Si une caisse est rattachée : enregistrer l'opération + mettre à jour son solde
        cid = s.get('caisse_id')
        if cid:
            try:
                conn.execute("""INSERT INTO caisse_operations (caisse_id, type, amount, description, reference, category, created_by)
                    VALUES (?,?,?,?,?,?,?)""",
                    (cid, 'sortie', s['montant'],
                     f"Sortie {s['reference']} — {s['beneficiaire']}",
                     s['reference'], 'sortie_caisse', session.get('user_id')))
                conn.execute("UPDATE caisses SET solde_actuel = solde_actuel - ? WHERE id=?", (s['montant'], cid))
            except: pass
        # Écriture comptable partie double : 658 (charges diverses) DÉBIT / 571 (caisse) CRÉDIT
        try:
            auto_ecriture(conn, s.get('date') or datetime.now().strftime('%Y-%m-%d'),
                f"Sortie {s['reference']} — {s['beneficiaire']}", '658', '571', s['montant'], s['reference'])
        except: pass
    conn.commit(); conn.close()
    flash("Décaissement comptabilisé" + (" (solde caisse mis à jour)" if s and s.get('caisse_id') else ""), "success")
    return redirect(url_for('caisse_sortie'))

@app.route('/caisse-sortie/<int:sid>/edit', methods=['GET','POST'])
@login_required
def caisse_edit(sid):
    from models import get_db
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin', 'dg', 'directeur'):
        flash("Seul l'admin ou le DG peut modifier", "error"); return redirect(url_for('caisse_sortie'))
    conn = get_db()
    s = conn.execute("SELECT * FROM caisse_sorties WHERE id=?", (sid,)).fetchone()
    conn.close()
    if not s: flash("Non trouvé","error"); return redirect(url_for('caisse_sortie'))
    if request.method == 'POST':
        conn = get_db()
        conn.execute("""UPDATE caisse_sorties SET beneficiaire=?, type_beneficiaire=?, montant=?,
            nature=?, motif=?, date=? WHERE id=?""",
            (request.form['beneficiaire'], request.form.get('type_beneficiaire','particulier'),
             float(request.form.get('montant',0) or 0), request.form.get('nature','espece'),
             request.form.get('motif',''), request.form.get('date',''), sid))
        conn.commit(); conn.close()
        flash("Sortie de caisse modifiée", "success")
        return redirect(url_for('caisse_sortie'))
    return render_template('caisse_preview.html', page='caisse_sortie', s=dict(s))

@app.route('/caisse-sortie/<int:sid>/preview')
@login_required
def caisse_preview(sid):
    from models import get_db
    conn = get_db()
    s = conn.execute("SELECT * FROM caisse_sorties WHERE id=?", (sid,)).fetchone()
    conn.close()
    if not s: flash("Non trouvé","error"); return redirect(url_for('caisse_sortie'))
    return render_template('caisse_preview.html', page='caisse_sortie', s=dict(s))

@app.route('/caisse-sortie/<int:sid>/signer', methods=['POST'])
@login_required
def caisse_signer(sid):
    """Enregistre la signature (base64 canvas) dans une colonne dédiée."""
    from models import get_db
    sig_type = request.form.get('type', 'beneficiaire')
    sig_data = request.form.get('signature', '')
    if sig_data and sig_type in ('beneficiaire', 'caisse', 'autorisation'):
        conn = get_db()
        conn.execute(f"UPDATE caisse_sorties SET sig_{sig_type}=? WHERE id=?", (sig_data, sid))
        conn.commit(); conn.close()
        flash(f"Signature {sig_type} enregistrée ✓", "success")
    return redirect(f'/caisse-sortie/{sid}/preview')

@app.route('/caisse-sortie/<int:sid>/delete')
@login_required
def caisse_delete(sid):
    """Admin supprime une demande de caisse."""
    user = get_user_by_id(session['user_id'])
    if not user or user['role'] not in ('admin', 'dg', 'directeur'):
        flash("Non autorisé", "error"); return redirect(url_for('caisse_sortie'))
    delete_caisse(sid)
    log_activity(session['user_id'], user['full_name'], 'Caisse', f"Sortie #{sid} supprimée", request.remote_addr)
    flash("Demande supprimée", "success")
    return redirect(url_for('caisse_sortie'))

@app.route('/caisse-sortie/<int:sid>/pdf')
@login_required
def caisse_pdf(sid):
    from models import get_db
    """Génère le PDF de la pièce de caisse sortie."""
    conn = get_db()
    s = conn.execute("SELECT * FROM caisse_sorties WHERE id=?", (sid,)).fetchone()
    conn.close()
    if not s: flash("Non trouvé","error"); return redirect(url_for('caisse_sortie'))
    s = dict(s)
    
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.colors import HexColor
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], f'caisse_{s["reference"]}.pdf')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    doc = SimpleDocTemplate(output, pagesize=A4, leftMargin=20*mm, rightMargin=20*mm, topMargin=15*mm, bottomMargin=15*mm)
    
    NAVY = HexColor('#1a3a5c'); RED = HexColor('#c53030')
    s_t = ParagraphStyle('t', fontSize=18, fontName='Helvetica-Bold', textColor=NAVY, alignment=TA_CENTER)
    s_ref = ParagraphStyle('ref', fontSize=12, fontName='Helvetica-Bold', alignment=TA_RIGHT, textColor=RED)
    s_n = ParagraphStyle('n', fontSize=11, leading=14)
    s_b = ParagraphStyle('b', fontSize=11, fontName='Helvetica-Bold')
    s_c = ParagraphStyle('c', fontSize=10)
    s_r = ParagraphStyle('r', fontSize=10, alignment=TA_RIGHT)
    s_h = ParagraphStyle('h', fontSize=10, fontName='Helvetica-Bold', textColor=HexColor('#fff'))
    
    story = []
    # Header
    story.append(Paragraph("PIÈCE DE CAISSE SORTIE", s_t))
    story.append(Paragraph(f"N° {s['reference']}", s_ref))
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width="100%", thickness=2, color=NAVY))
    story.append(Spacer(1, 5*mm))
    
    # Date + lieu
    story.append(Paragraph(f"ABIDJAN le {s.get('date','') or ''}", ParagraphStyle('d', fontSize=11, alignment=TA_CENTER)))
    story.append(Spacer(1, 6*mm))
    
    # Info table
    typ = '☑' if s['type_beneficiaire']=='entreprise' else '☐'
    typ2 = '☑' if s['type_beneficiaire']=='particulier' else '☐'
    typ3 = '☑' if s['type_beneficiaire'] not in ('entreprise','particulier') else '☐'
    
    info = [
        [Paragraph("<b>A l'ordre de :</b>", s_b), Paragraph(f"<b>{s['beneficiaire']}</b>", s_b)],
        [Paragraph(f"Entreprise: {typ}     Particulier: {typ2}     Autres: {typ3}", s_c), Paragraph('', s_c)],
    ]
    it = Table(info, colWidths=[85*mm, 85*mm])
    it.setStyle(TableStyle([('GRID',(0,0),(-1,-1),0.5,HexColor('#ccc')),
        ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),('LEFTPADDING',(0,0),(-1,-1),8)]))
    story.append(it)
    story.append(Spacer(1, 4*mm))
    
    # Montant + Nature + Motif
    nature = s.get('nature','espece')
    ch = '...........' if nature!='cheque' else f" N°{s.get('notes','')}"
    vi = '...........' if nature!='virement' else f" ✓"
    es = '...........' if nature!='espece' else f" ✓"
    
    details = [
        [Paragraph(h, s_h) for h in ['Rubrique', 'Détail']],
        [Paragraph('<b>MONTANT</b>', s_c), Paragraph(f"<b>{s['montant']:,.0f} FCFA</b>", ParagraphStyle('m', fontSize=14, fontName='Helvetica-Bold', textColor=RED))],
        [Paragraph('<b>NATURE</b>', s_c), Paragraph(f"Chèque {ch}     Virement {vi}     Espèce {es}", s_c)],
        [Paragraph('<b>MOTIF</b>', s_c), Paragraph(s.get('motif','') or '-', s_n)],
    ]
    dt = Table(details, colWidths=[40*mm, 130*mm])
    dt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),NAVY),('GRID',(0,0),(-1,-1),0.5,HexColor('#ccc')),
        ('TOPPADDING',(0,0),(-1,-1),8),('BOTTOMPADDING',(0,0),(-1,-1),8),('LEFTPADDING',(0,0),(-1,-1),8)]))
    story.append(dt)
    story.append(Spacer(1, 8*mm))
    
    # Status
    if s['status'] == 'valide':
        story.append(Paragraph(f"✅ Validé par {s.get('valideur_name','')} le {(s.get('validated_at','') or '')[:10]}", ParagraphStyle('st', fontSize=10, textColor=HexColor('#2e7d32'))))
    elif s['status'] == 'refuse':
        story.append(Paragraph(f"❌ Refusé par {s.get('valideur_name','')}", ParagraphStyle('st', fontSize=10, textColor=RED)))
    story.append(Spacer(1, 10*mm))
    
    # Signatures
    sig = [[Paragraph("<b>Bénéficiaire</b>", ParagraphStyle('s1', fontSize=10, alignment=TA_CENTER)),
            Paragraph("<b>Caisse</b>", ParagraphStyle('s2', fontSize=10, alignment=TA_CENTER)),
            Paragraph("<b>Autorisation</b>", ParagraphStyle('s3', fontSize=10, alignment=TA_CENTER))]]
    st = Table(sig, colWidths=[57*mm, 57*mm, 57*mm])
    st.setStyle(TableStyle([('LINEABOVE',(0,0),(-1,0),1,HexColor('#000')),
        ('TOPPADDING',(0,0),(-1,-1),6)]))
    story.append(st)
    story.append(Spacer(1, 20*mm))
    story.append(Paragraph("WannyGest — Pièce de caisse générée automatiquement", ParagraphStyle('f', fontSize=7, alignment=TA_CENTER, textColor=HexColor('#999'))))
    
    doc.build(story)
    return send_file(output, as_attachment=True, download_name=f"Caisse_{s['reference']}.pdf")

@app.route('/caisse-sortie/rapport')
@login_required
def caisse_rapport():
    """Rapport mensuel Excel + PDF."""
    month = request.args.get('month', datetime.now().strftime('%Y-%m'))
    fmt = request.args.get('format', 'excel')
    sorties = get_caisse_sorties(status='valide', month=month)
    stats = get_caisse_stats(month=month)
    
    if fmt == 'pdf':
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib.colors import HexColor
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, HRFlowable
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT
        
        output = os.path.join(app.config['UPLOAD_FOLDER'], f'rapport_caisse_{month}.pdf')
        os.makedirs(os.path.dirname(output), exist_ok=True)
        doc = SimpleDocTemplate(output, pagesize=A4, leftMargin=15*mm, rightMargin=15*mm, topMargin=12*mm, bottomMargin=12*mm)
        NAVY = HexColor('#1a3a5c')
        s_t = ParagraphStyle('t', fontSize=16, fontName='Helvetica-Bold', textColor=NAVY, alignment=TA_CENTER)
        s_s = ParagraphStyle('s', fontSize=10, alignment=TA_CENTER, textColor=HexColor('#888'))
        s_h = ParagraphStyle('h', fontSize=8, fontName='Helvetica-Bold', textColor=HexColor('#fff'))
        s_c = ParagraphStyle('c', fontSize=8)
        s_r = ParagraphStyle('r', fontSize=8, alignment=TA_RIGHT)
        s_rb = ParagraphStyle('rb', fontSize=9, fontName='Helvetica-Bold', alignment=TA_RIGHT)
        fmt_n = lambda x: f"{x:,.0f}"
        
        story = []
        story.append(Paragraph(f"RAPPORT DES SORTIES DE CAISSE", s_t))
        story.append(Paragraph(f"Période : {month}", s_s))
        story.append(Spacer(1, 3*mm))
        story.append(HRFlowable(width="100%", thickness=2, color=NAVY))
        story.append(Spacer(1, 5*mm))
        
        # Summary
        summary = [
            [Paragraph(h, s_h) for h in ['Total validé', 'Espèces', 'Chèques', 'Virements', 'Nb opérations']],
            [Paragraph(f"<b>{fmt_n(stats['montant_total'])}</b>", s_rb),
             Paragraph(fmt_n(stats['montant_espece']), s_r),
             Paragraph(fmt_n(stats['montant_cheque']), s_r),
             Paragraph(fmt_n(stats['montant_virement']), s_r),
             Paragraph(str(stats['valide']), s_r)],
        ]
        st = Table(summary, colWidths=[36*mm]*5)
        st.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),NAVY),('GRID',(0,0),(-1,-1),0.5,HexColor('#ddd')),
            ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
        story.append(st)
        story.append(Spacer(1, 6*mm))
        
        # Detail
        rows = [[Paragraph(h, s_h) for h in ['N°', 'Date', 'Bénéficiaire', 'Motif', 'Nature', 'Montant']]]
        for s_item in sorties:
            rows.append([Paragraph(s_item['reference'], s_c), Paragraph(s_item.get('date','')[:10], s_c),
                Paragraph(s_item['beneficiaire'], s_c), Paragraph((s_item.get('motif','') or '')[:30], s_c),
                Paragraph(s_item.get('nature',''), s_c), Paragraph(fmt_n(s_item['montant']), s_r)])
        rows.append([Paragraph('', s_c)]*4 + [Paragraph('<b>TOTAL</b>', s_c), Paragraph(f"<b>{fmt_n(stats['montant_total'])} FCFA</b>", s_rb)])
        
        dt = Table(rows, colWidths=[28*mm, 20*mm, 40*mm, 35*mm, 20*mm, 27*mm])
        dt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),NAVY),('GRID',(0,0),(-1,-1),0.5,HexColor('#ddd')),
            ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('BACKGROUND',(0,-1),(-1,-1),HexColor('#f0f4f8'))]))
        story.append(dt)
        
        story.append(Spacer(1, 15*mm))
        story.append(Paragraph(f"Généré le {datetime.now().strftime('%d/%m/%Y')} — WannyGest", ParagraphStyle('f', fontSize=7, alignment=TA_CENTER, textColor=HexColor('#999'))))
        doc.build(story)
        return send_file(output, as_attachment=True, download_name=f"Rapport_Caisse_{month}.pdf")
    
    else:  # Excel
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = f"Sorties Caisse {month}"
        
        # Header
        ws.merge_cells('A1:F1')
        ws['A1'] = f"RAPPORT DES SORTIES DE CAISSE — {month}"
        ws['A1'].font = Font(bold=True, size=14, color="1A3A5C")
        ws['A1'].alignment = Alignment(horizontal='center')
        
        # Summary
        ws['A3'] = 'Total validé'; ws['B3'] = stats['montant_total']
        ws['C3'] = 'Espèces'; ws['D3'] = stats['montant_espece']
        ws['E3'] = 'Opérations'; ws['F3'] = stats['valide']
        for cell in ws[3]:
            cell.font = Font(bold=True)
        
        # Headers
        headers = ['Référence', 'Date', 'Bénéficiaire', 'Motif', 'Nature', 'Montant (FCFA)']
        for i, h in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=h)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="1A3A5C", end_color="1A3A5C", patternType="solid")
        
        # Data
        for row_idx, s_item in enumerate(sorties, 6):
            ws.cell(row=row_idx, column=1, value=s_item['reference'])
            ws.cell(row=row_idx, column=2, value=s_item.get('date',''))
            ws.cell(row=row_idx, column=3, value=s_item['beneficiaire'])
            ws.cell(row=row_idx, column=4, value=s_item.get('motif',''))
            ws.cell(row=row_idx, column=5, value=s_item.get('nature',''))
            ws.cell(row=row_idx, column=6, value=s_item['montant'])
        
        # Total row
        total_row = len(sorties) + 6
        ws.cell(row=total_row, column=5, value='TOTAL').font = Font(bold=True)
        ws.cell(row=total_row, column=6, value=stats['montant_total']).font = Font(bold=True, color="C53030")
        
        for col in range(1, 7):
            ws.column_dimensions[chr(64+col)].width = 18
        
        output = os.path.join(app.config['UPLOAD_FOLDER'], f'rapport_caisse_{month}.xlsx')
        os.makedirs(os.path.dirname(output), exist_ok=True)
        wb.save(output)
        return send_file(output, as_attachment=True, download_name=f"Rapport_Caisse_{month}.xlsx")


# ======================== UTILS ========================

def _cleanup_old(upload_dir, max_age_hours=2):
    import time
    now = time.time()
    try:
        for name in os.listdir(upload_dir):
            path = os.path.join(upload_dir, name)
            if os.path.isdir(path) and (now - os.path.getmtime(path)) > max_age_hours * 3600:
                shutil.rmtree(path, ignore_errors=True)
    except:
        pass


# ======================== RAPPORTS JOURNALIERS ========================
from models import get_db as _gdb, db_insert as _dbi

@app.route('/rapports-journaliers')
@permission_required('rapports_j')
def rapports_journaliers():
    u = dict(get_user_by_id(session['user_id']))
    conn = _gdb()
    if u['role'] in ('admin', 'dg'):
        rapports = [dict(r) for r in conn.execute("""SELECT rj.*, u.full_name FROM rapports_journaliers rj 
            LEFT JOIN users u ON rj.user_id=u.id ORDER BY rj.date DESC, rj.created_at DESC LIMIT 100""").fetchall()]
    else:
        rapports = [dict(r) for r in conn.execute("""SELECT rj.*, u.full_name FROM rapports_journaliers rj 
            LEFT JOIN users u ON rj.user_id=u.id WHERE rj.user_id=? ORDER BY rj.date DESC LIMIT 50""",
            (session['user_id'],)).fetchall()]
    
    # My counter this week
    from datetime import timedelta
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    my_week = conn.execute("SELECT COUNT(DISTINCT date) FROM rapports_journaliers WHERE user_id=? AND date>=?",
        (session['user_id'], week_start.strftime('%Y-%m-%d'))).fetchone()[0]
    my_total = conn.execute("SELECT COUNT(*) FROM rapports_journaliers WHERE user_id=?",
        (session['user_id'],)).fetchone()[0]
    conn.close()
    return render_template('rapports_journaliers.html', page='rapports_j', rapports=rapports, user=u,
        my_week=my_week, my_total=my_total)

@app.route('/rapports-journaliers/add', methods=['POST'])
@login_required
def rapports_journaliers_add():
    u = dict(get_user_by_id(session['user_id']))
    _dbi('rapports_journaliers', user_id=session['user_id'],
        date=request.form.get('date', datetime.now().strftime('%Y-%m-%d')),
        tasks_done=request.form.get('tasks_done', ''),
        tasks_planned=request.form.get('tasks_planned', ''),
        issues=request.form.get('issues', ''),
        achievements=request.form.get('achievements', ''),
        completion_pct=int(request.form.get('completion_pct', 0) or 0),
        department=u.get('department', '') or u.get('role', ''))
    flash("Rapport journalier soumis", "success")
    return redirect('/rapports-journaliers')

@app.route('/rapports-journaliers/<int:rid>/validate', methods=['POST'])
@login_required
def rapports_journaliers_validate(rid):
    u = dict(get_user_by_id(session['user_id']))
    if u['role'] not in ('admin', 'dg'):
        flash("Seuls les responsables peuvent valider", "error")
        return redirect('/rapports-journaliers')
    conn = _gdb()
    conn.execute("UPDATE rapports_journaliers SET status=?, validated_by=?, comments=? WHERE id=?",
        (request.form.get('status', 'valide'), session['user_id'], request.form.get('comments', ''), rid))
    conn.commit(); conn.close()
    flash("Rapport validé", "success")
    return redirect('/rapports-journaliers')

@app.route('/rapports-journaliers/assiduite')
@permission_required('rapports_j')
def rapports_assiduite():
    """Tableau d'assiduité — suivi des dépôts de rapports."""
    from datetime import datetime as dt2, timedelta
    conn = _gdb()
    u = dict(get_user_by_id(session['user_id']))
    
    # Current week boundaries (Mon→Sun)
    today = dt2.now().date()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    
    # Previous weeks for history
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end = week_start - timedelta(days=1)
    
    # Period filter
    period = request.args.get('period', 'semaine')
    if period == 'mois':
        p_start = today.replace(day=1).strftime('%Y-%m-%d')
        p_end = today.strftime('%Y-%m-%d')
        p_label = f"Mois de {today.strftime('%B %Y')}"
        expected_days = sum(1 for d in range((today - today.replace(day=1)).days + 1)
                          if (today.replace(day=1) + timedelta(days=d)).weekday() < 6)
    elif period == 'semaine_prec':
        p_start = prev_week_start.strftime('%Y-%m-%d')
        p_end = prev_week_end.strftime('%Y-%m-%d')
        p_label = f"Semaine du {prev_week_start.strftime('%d/%m')} au {prev_week_end.strftime('%d/%m/%Y')}"
        expected_days = 6
    else:
        p_start = week_start.strftime('%Y-%m-%d')
        p_end = week_end.strftime('%Y-%m-%d')
        p_label = f"Semaine du {week_start.strftime('%d/%m')} au {week_end.strftime('%d/%m/%Y')}"
        expected_days = min(6, (today - week_start).days + 1)  # Only count past days
    
    # Get all reports in period grouped by user
    rows = conn.execute("""
        SELECT rj.user_id, u.full_name, u.role, COUNT(DISTINCT rj.date) as nb_rapports,
               AVG(rj.completion_pct) as avg_completion,
               GROUP_CONCAT(DISTINCT rj.date) as dates
        FROM rapports_journaliers rj
        LEFT JOIN users u ON rj.user_id=u.id
        WHERE rj.date >= ? AND rj.date <= ?
        GROUP BY rj.user_id
        ORDER BY nb_rapports DESC, avg_completion DESC
    """, (p_start, p_end)).fetchall()
    
    ranking = []
    for r in rows:
        d = dict(r)
        d['expected'] = expected_days
        d['taux'] = round((d['nb_rapports'] / max(expected_days, 1)) * 100)
        d['avg_completion'] = round(d['avg_completion'] or 0)
        # Badges
        if d['taux'] >= 100:
            d['badge'] = '🏆 Assidu'
            d['badge_color'] = '#B8860B'
        elif d['taux'] >= 80:
            d['badge'] = '⭐ Régulier'
            d['badge_color'] = '#2e7d32'
        elif d['taux'] >= 60:
            d['badge'] = '👍 Correct'
            d['badge_color'] = '#1565c0'
        elif d['taux'] >= 40:
            d['badge'] = '⚠️ À améliorer'
            d['badge_color'] = '#e8672a'
        else:
            d['badge'] = '🔴 Insuffisant'
            d['badge_color'] = '#c53030'
        ranking.append(d)
    
    # All users for comparison
    all_users = [dict(r) for r in conn.execute("SELECT id, full_name, role FROM users WHERE is_active=1").fetchall()]
    active_ids = {r['user_id'] for r in ranking}
    for usr in all_users:
        if usr['id'] not in active_ids:
            ranking.append({
                'user_id': usr['id'], 'full_name': usr['full_name'], 'role': usr['role'],
                'nb_rapports': 0, 'expected': expected_days, 'taux': 0, 'avg_completion': 0,
                'dates': '', 'badge': '❌ Aucun rapport', 'badge_color': '#c53030'
            })
    
    # Global stats
    total_reports = sum(r['nb_rapports'] for r in ranking)
    total_users = len([r for r in ranking if r['nb_rapports'] > 0])
    assidus = len([r for r in ranking if r['taux'] >= 100])
    
    # Best employee overall
    best = ranking[0] if ranking and ranking[0]['nb_rapports'] > 0 else None
    
    # Split ranking by category
    tech_roles = ['technicien', 'tech_reseau', 'tech_maintenance', 'installateur', 'stagiaire']
    ranking_tech = sorted([r for r in ranking if r.get('role','') in tech_roles], key=lambda x: -x['nb_rapports'])
    ranking_admin = sorted([r for r in ranking if r.get('role','') not in tech_roles], key=lambda x: -x['nb_rapports'])
    
    best_tech = ranking_tech[0] if ranking_tech and ranking_tech[0]['nb_rapports'] > 0 else None
    best_admin = ranking_admin[0] if ranking_admin and ranking_admin[0]['nb_rapports'] > 0 else None
    
    # All stored champions for attestation download
    champions_list = [dict(r) for r in conn.execute("SELECT * FROM weekly_champion ORDER BY week_end DESC LIMIT 10").fetchall()]
    
    conn.close()
    return render_template('rapports_assiduite.html', page='rapports_j',
        ranking=ranking, ranking_tech=ranking_tech, ranking_admin=ranking_admin,
        period=period, p_label=p_label, expected_days=expected_days,
        total_reports=total_reports, total_users=total_users, assidus=assidus,
        best=best, best_tech=best_tech, best_admin=best_admin,
        user=u, today=today.strftime('%Y-%m-%d'), champions_list=champions_list)

@app.route('/rapports-journaliers/attestation/<int:champ_id>')
@permission_required('rapports_j')
def rapports_attestation(champ_id):
    """Génère l'attestation d'encouragement PDF."""
    from models import get_db as _gdb2
    conn = _gdb2()
    champ = conn.execute("SELECT * FROM weekly_champion WHERE id=?", (champ_id,)).fetchone()
    conn.close()
    if not champ:
        flash("Champion non trouvé", "error")
        return redirect('/rapports-journaliers/assiduite')
    champ = dict(champ)
    
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.units import mm, cm
    from reportlab.lib.colors import HexColor, white, black
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, Frame, PageTemplate
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.graphics.shapes import Drawing, Rect, Line, String
    from reportlab.graphics import renderPDF
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], f'attestation_{champ_id}.pdf')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    
    pw, ph = landscape(A4)
    doc = SimpleDocTemplate(output, pagesize=landscape(A4),
        leftMargin=30*mm, rightMargin=30*mm, topMargin=15*mm, bottomMargin=15*mm)
    
    GOLD = HexColor('#B8860B')
    GOLD_LIGHT = HexColor('#DAA520')
    NAVY = HexColor('#1a1a2e')
    GREY = HexColor('#555555')
    LGREY = HexColor('#999999')
    
    usable_w = pw - 60*mm
    
    story = []
    
    # ===== TOP LINE =====
    top_line = Table([['']], colWidths=[usable_w], rowHeights=[1*mm])
    top_line.setStyle(TableStyle([('LINEABOVE', (0,0), (-1,0), 3, GOLD)]))
    story.append(top_line)
    story.append(Spacer(1, 8*mm))
    
    # ===== COMPANY =====
    story.append(Paragraph("RAMYA TECHNOLOGIE &amp; INNOVATION",
        ParagraphStyle('company', fontName='Helvetica', fontSize=9, textColor=LGREY, 
                       alignment=TA_CENTER, spaceAfter=8*mm)))
    
    # ===== TITLE =====
    story.append(Paragraph("ATTESTATION D'ENCOURAGEMENT",
        ParagraphStyle('title', fontName='Helvetica-Bold', fontSize=24, textColor=NAVY, 
                       alignment=TA_CENTER, spaceAfter=12*mm)))
    
    # ===== LINE =====
    story.append(HRFlowable(width="35%", thickness=2, color=GOLD, spaceAfter=12*mm))
    
    # ===== SUBTITLE =====
    story.append(Paragraph("Employ\u00e9 le plus assidu de la semaine",
        ParagraphStyle('subtitle', fontName='Helvetica-Bold', fontSize=12, textColor=GOLD, 
                       alignment=TA_CENTER, spaceAfter=10*mm)))
    
    # ===== INTRO =====
    story.append(Paragraph("Nous avons l'honneur de d\u00e9cerner cette attestation \u00e0 :",
        ParagraphStyle('intro', fontName='Helvetica', fontSize=10, textColor=GREY, 
                       alignment=TA_CENTER, spaceAfter=8*mm)))
    
    # ===== NAME =====
    story.append(Paragraph(f"<b>{champ['full_name']}</b>",
        ParagraphStyle('fullname', fontName='Helvetica-Bold', fontSize=28, textColor=NAVY, 
                       alignment=TA_CENTER, spaceAfter=10*mm)))
    
    # ===== ROLE =====
    role_dept = champ['role']
    if champ.get('department'):
        role_dept += f" - {champ['department']}"
    story.append(Paragraph(role_dept,
        ParagraphStyle('roledept', fontName='Helvetica', fontSize=10, textColor=LGREY, 
                       alignment=TA_CENTER, spaceAfter=12*mm)))
    
    # ===== LINE =====
    story.append(HRFlowable(width="35%", thickness=1, color=GOLD_LIGHT, spaceAfter=8*mm))
    
    # ===== REASON =====
    story.append(Paragraph("Pour son assiduit\u00e9 exemplaire dans le d\u00e9p\u00f4t des rapports journaliers",
        ParagraphStyle('reason', fontName='Helvetica', fontSize=11, textColor=HexColor('#333333'), 
                       alignment=TA_CENTER, spaceAfter=4*mm)))
    
    story.append(Paragraph(f"Semaine du <b>{champ['week_start']}</b> au <b>{champ['week_end']}</b>",
        ParagraphStyle('period', fontName='Helvetica', fontSize=10, textColor=GREY, 
                       alignment=TA_CENTER, spaceAfter=8*mm)))
    
    # ===== STATS =====
    s_label = ParagraphStyle('sl', fontName='Helvetica', fontSize=8, textColor=LGREY, alignment=TA_CENTER)
    stats_data = [[
        Paragraph(f"<b>{champ['nb_rapports']}</b>", ParagraphStyle('sn', fontName='Helvetica-Bold', fontSize=20, textColor=GOLD, alignment=TA_CENTER)),
        Paragraph(f"<b>{int(champ['avg_completion'])}%</b>", ParagraphStyle('sc', fontName='Helvetica-Bold', fontSize=20, textColor=HexColor('#2e7d32'), alignment=TA_CENTER)),
        Paragraph("<b>ASSIDU</b>", ParagraphStyle('sb', fontName='Helvetica-Bold', fontSize=14, textColor=GOLD, alignment=TA_CENTER)),
    ], [
        Paragraph("rapports d\u00e9pos\u00e9s", s_label),
        Paragraph("r\u00e9alisation moyenne", s_label),
        Paragraph("badge d'assiduit\u00e9", s_label),
    ]]
    cw = usable_w / 3
    st = Table(stats_data, colWidths=[cw, cw, cw], rowHeights=[12*mm, 5*mm])
    st.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('LINEABOVE', (0,0), (-1,0), 0.5, HexColor('#eeeeee')),
        ('LINEBELOW', (0,-1), (-1,-1), 0.5, HexColor('#eeeeee')),
        ('INNERGRID', (0,0), (-1,-1), 0.3, HexColor('#eeeeee')),
    ]))
    story.append(st)
    story.append(Spacer(1, 5*mm))
    
    # ===== FOOTER =====
    now_str = datetime.now().strftime("%d/%m/%Y")
    story.append(Paragraph(f"Fait le {now_str} - RAMYA TECHNOLOGIE &amp; INNOVATION",
        ParagraphStyle('date', fontName='Helvetica', fontSize=8, textColor=LGREY, alignment=TA_CENTER, spaceAfter=3*mm)))
    
    # ===== BOTTOM LINE =====
    bot_line = Table([['']], colWidths=[usable_w], rowHeights=[1*mm])
    bot_line.setStyle(TableStyle([('LINEBELOW', (0,0), (-1,0), 3, GOLD)]))
    story.append(bot_line)
    
    doc.build(story)
    return send_file(output, as_attachment=True, download_name=f"Attestation_{champ['full_name'].replace(' ','_')}.pdf")


# ======================== PIÈCES DE CAISSE / DÉPENSES ========================

@app.route('/comptabilite/pieces')
@permission_required('comptabilite')
def pieces_caisse():
    conn = _gdb()
    pieces = [dict(r) for r in conn.execute("SELECT * FROM pieces_caisse ORDER BY date DESC").fetchall()]
    # Enrich with caisse names
    caisses_map = {r['id']: r['name'] for r in conn.execute("SELECT id, name FROM caisses").fetchall()}
    for p in pieces:
        p['caisse_nom'] = caisses_map.get(p.get('caisse_id'), '—')
    total = conn.execute("SELECT COALESCE(SUM(amount),0) FROM pieces_caisse").fetchone()[0]
    by_cat = [dict(r) for r in conn.execute("SELECT category, SUM(amount) as total, COUNT(*) as count FROM pieces_caisse GROUP BY category ORDER BY total DESC").fetchall()]
    caisses = [dict(r) for r in conn.execute("SELECT * FROM caisses WHERE is_active=1 ORDER BY name").fetchall()]
    conn.close()
    return render_template('pieces_caisse.html', page='pieces', pieces=pieces, total=total, by_cat=by_cat, caisses=caisses)

@app.route('/comptabilite/pieces/add', methods=['POST'])
@permission_required('comptabilite')
def pieces_caisse_add():
    ref = f"PC-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    file_path = ''
    if 'file' in request.files and request.files['file'].filename:
        f = request.files['file']
        ext = os.path.splitext(f.filename)[1].lower()
        if ext in ('.jpg', '.jpeg', '.png', '.pdf', '.webp'):
            fname = f"piece_{ref}{ext}"
            fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'pieces')
            os.makedirs(fdir, exist_ok=True)
            f.save(os.path.join(fdir, fname))
            file_path = fname
    
    amount = float(request.form.get('amount', 0) or 0)
    category = request.form.get('category', 'divers')
    supplier = request.form.get('supplier', '')
    description = request.form.get('description', '')
    date_op = request.form.get('date', datetime.now().strftime('%Y-%m-%d'))
    # Caisse de rattachement
    caisse_id_raw = request.form.get('caisse_id', '').strip()
    caisse_id = int(caisse_id_raw) if caisse_id_raw.isdigit() else None
    
    _dbi('pieces_caisse', reference=ref,
        date=date_op,
        description=description,
        amount=amount, category=category,
        supplier=supplier,
        caisse_id=caisse_id,
        file_path=file_path, created_by=session['user_id'])
    
    # Enregistrer aussi un mouvement de sortie sur la caisse choisie
    caisse_nom = ''
    if caisse_id:
        conn_op = _gdb()
        c_row = conn_op.execute("SELECT name FROM caisses WHERE id=?", (caisse_id,)).fetchone()
        caisse_nom = c_row['name'] if c_row else ''
        try:
            conn_op.execute("""INSERT INTO caisse_operations (caisse_id, type, amount, description, reference, category, created_by)
                VALUES (?,?,?,?,?,?,?)""",
                (caisse_id, 'sortie', amount,
                 f"Dépense {ref} — {description[:40]}" + (f" ({supplier})" if supplier else ""),
                 ref, category, session['user_id']))
            # Update caisse balance
            conn_op.execute("UPDATE caisses SET solde_actuel = solde_actuel - ? WHERE id=?", (amount, caisse_id))
        except Exception: pass
        conn_op.commit(); conn_op.close()
    
    # ==== AUTO-ÉCRITURE COMPTABLE (SYSCOHADA partie double) ====
    cat_to_account = {
        'achat':'601', 'achats':'601',
        'fournisseur':'401', 'fournisseurs':'401',
        'transport':'611', 'carburant':'611',
        'services':'624', 'entretien':'624', 'maintenance':'624',
        'loyer':'622',
        'fourniture':'605', 'fournitures':'605',
        'electricite':'606', 'eau':'606', 'telephone':'626', 'internet':'626',
        'publicite':'627', 'marketing':'627',
        'impots':'641', 'taxes':'641',
        'personnel':'661', 'salaire':'661', 'salaires':'661',
        'banque':'627', 'frais_bancaire':'627',
        'divers':'658', 'autre':'658', 'autres':'658',
    }
    debit_account = cat_to_account.get(category.lower().replace(' ','_'), '658')
    # Si un fournisseur est renseigné, on crédite 401 (dette fournisseur) sinon 571 (caisse directe)
    credit_account = '401' if supplier else '571'
    try:
        conn2 = _gdb()
        libelle = f"Dépense {ref} — {description[:40]}"
        if supplier: libelle += f" ({supplier})"
        if caisse_nom: libelle += f" · Caisse {caisse_nom}"
        auto_ecriture(conn2, date_op, libelle, debit_account, credit_account, amount, ref)
        conn2.commit(); conn2.close()
    except Exception as e:
        pass
    
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?', 'Caisse',
                 f"Pièce {ref} — {amount:,.0f} F (compte {debit_account}){' - Caisse '+caisse_nom if caisse_nom else ''}", request.remote_addr)
    flash(f"Pièce {ref} — {amount:,.0f} F enregistrée (écriture : {debit_account} → {credit_account}){' · caisse ' + caisse_nom if caisse_nom else ''}", "success")
    return redirect('/comptabilite/pieces')

@app.route('/comptabilite/pieces/edit/<int:pid>', methods=['GET','POST'])
@permission_required('comptabilite')
def pieces_caisse_edit(pid):
    conn = _gdb()
    piece = conn.execute("SELECT * FROM pieces_caisse WHERE id=?", (pid,)).fetchone()
    if not piece:
        conn.close(); flash("Pièce non trouvée","error"); return redirect('/comptabilite/pieces')
    if request.method == 'POST':
        # Handle file upload
        file_path = piece['file_path'] if piece.get('file_path') else ''
        if 'file' in request.files and request.files['file'].filename:
            f = request.files['file']
            ext = os.path.splitext(f.filename)[1].lower()
            if ext in ('.jpg', '.jpeg', '.png', '.pdf', '.webp'):
                fname = f"piece_{piece['reference']}{ext}"
                fdir = os.path.join(app.config['UPLOAD_FOLDER'], 'pieces')
                os.makedirs(fdir, exist_ok=True)
                f.save(os.path.join(fdir, fname))
                file_path = fname
        
        conn.execute("""UPDATE pieces_caisse SET date=?, description=?, amount=?, category=?, supplier=?, file_path=?
            WHERE id=?""", (
            request.form.get('date', piece['date']),
            request.form.get('description', piece['description']),
            float(request.form.get('amount', piece['amount']) or 0),
            request.form.get('category', piece['category']),
            request.form.get('supplier', piece.get('supplier','')),
            file_path, pid))
        conn.commit(); conn.close()
        flash("Pièce de caisse modifiée","success")
        return redirect('/comptabilite/pieces')
    conn.close()
    return render_template('pieces_caisse.html', page='pieces', edit_piece=dict(piece), pieces=[], total=0, by_cat=[])

@app.route('/comptabilite/pieces/delete/<int:pid>')
@permission_required('comptabilite')
def pieces_caisse_delete(pid):
    conn = _gdb()
    conn.execute("DELETE FROM pieces_caisse WHERE id=?", (pid,))
    conn.commit(); conn.close()
    flash("Pièce supprimée","success")
    return redirect('/comptabilite/pieces')

@app.route('/uploads/pieces/<path:filename>')
def piece_file(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'pieces'), filename)


@app.route('/uploads/signatures/<path:filename>')
def signature_file(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'signatures'), filename)

@app.route('/uploads/clients/<path:filename>')
def client_file(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'clients'), filename)

@app.route('/uploads/stock/<path:filename>')
def stock_image(filename):
    return send_from_directory(os.path.join(app.config['UPLOAD_FOLDER'], 'stock'), filename)

@app.route('/stock')
@login_required
def stock_redirect():
    return redirect('/achats?tab=stock')


# ======================== MODULE ACHATS ========================

@app.route('/achats')
@permission_required('comptabilite')
def achats_page():
    tab = request.args.get('tab', 'fournisseurs')
    conn = _gdb()
    from models import db_get_all
    
    data = {'tab': tab}
    data['stock_items'] = db_get_all('stock_items', order='name ASC')
    data['stock_total'] = sum((s.get('quantity',0) or 0) * (s.get('unit_price',0) or 0) for s in data['stock_items'])
    try: data['stock_categories'] = [dict(r) for r in conn.execute("SELECT * FROM stock_categories ORDER BY name").fetchall()]
    except: data['stock_categories'] = []
    data['fournisseurs'] = [dict(r) for r in conn.execute("SELECT * FROM achats_fournisseurs ORDER BY name").fetchall()]
    data['demandes'] = [dict(r) for r in conn.execute("""SELECT ad.*, u.full_name as requester FROM achats_demandes ad 
        LEFT JOIN users u ON ad.requested_by=u.id ORDER BY ad.created_at DESC LIMIT 50""").fetchall()]
    data['devis_achats'] = [dict(r) for r in conn.execute("""SELECT ad.*, f.name as fournisseur_name FROM achats_devis ad
        LEFT JOIN achats_fournisseurs f ON ad.fournisseur_id=f.id ORDER BY ad.created_at DESC LIMIT 50""").fetchall()]
    data['commandes'] = [dict(r) for r in conn.execute("""SELECT ac.*, f.name as fournisseur_name FROM achats_commandes ac
        LEFT JOIN achats_fournisseurs f ON ac.fournisseur_id=f.id ORDER BY ac.created_at DESC LIMIT 50""").fetchall()]
    data['contrats'] = [dict(r) for r in conn.execute("""SELECT ac.*, f.name as fournisseur_name FROM achats_contrats ac
        LEFT JOIN achats_fournisseurs f ON ac.fournisseur_id=f.id ORDER BY ac.created_at DESC""").fetchall()]
    
    # Stats
    data['total_commandes'] = conn.execute("SELECT COALESCE(SUM(total),0) FROM achats_commandes").fetchone()[0]
    data['pending_demandes'] = conn.execute("SELECT COUNT(*) FROM achats_demandes WHERE status='en_attente'").fetchone()[0]
    conn.close()
    return render_template('achats.html', page='achats', **data)

@app.route('/achats/fournisseur/add', methods=['POST'])
@permission_required('comptabilite_edit')
def achats_fournisseur_add():
    _dbi('achats_fournisseurs', name=request.form['name'], contact_name=request.form.get('contact_name',''),
        tel=request.form.get('tel',''), email=request.form.get('email',''),
        address=request.form.get('address',''), city=request.form.get('city',''),
        sector=request.form.get('sector',''), payment_terms=request.form.get('payment_terms',''))
    flash("Fournisseur ajouté", "success"); return redirect('/achats?tab=fournisseurs')

@app.route('/achats/fournisseur/edit/<int:fid>', methods=['POST'])
@permission_required('comptabilite_edit')
def achats_fournisseur_edit(fid):
    conn = _gdb()
    conn.execute("""UPDATE achats_fournisseurs SET name=?,contact_name=?,tel=?,email=?,address=?,city=?,sector=?,payment_terms=?,notes=? WHERE id=?""",
        (request.form['name'],request.form.get('contact_name',''),request.form.get('tel',''),request.form.get('email',''),
         request.form.get('address',''),request.form.get('city',''),request.form.get('sector',''),
         request.form.get('payment_terms',''),request.form.get('notes',''),fid))
    conn.commit(); conn.close()
    flash("Fournisseur modifié", "success"); return redirect('/achats?tab=fournisseurs')

@app.route('/achats/fournisseur/delete/<int:fid>')
@permission_required('comptabilite_edit')
def achats_fournisseur_delete(fid):
    conn = _gdb(); conn.execute("DELETE FROM achats_fournisseurs WHERE id=?", (fid,)); conn.commit(); conn.close()
    flash("Fournisseur supprimé", "success"); return redirect('/achats?tab=fournisseurs')

@app.route('/achats/demande/add', methods=['POST'])
@permission_required('comptabilite_edit')
def achats_demande_add():
    ref = f"DA-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    _dbi('achats_demandes', reference=ref, date=request.form.get('date', datetime.now().strftime('%Y-%m-%d')),
        department=request.form.get('department',''), requested_by=session['user_id'],
        description=request.form.get('description',''), urgency=request.form.get('urgency','normale'))
    flash(f"Demande {ref} créée", "success"); return redirect('/achats?tab=demandes')

@app.route('/achats/demande/<int:did>/approve')
@permission_required('comptabilite_edit')
def achats_demande_approve(did):
    conn = _gdb()
    conn.execute("UPDATE achats_demandes SET status='approuvee', approved_by=?, approved_at=CURRENT_TIMESTAMP WHERE id=?",
        (session['user_id'], did))
    conn.commit(); conn.close()
    flash("Demande approuvée", "success"); return redirect('/achats?tab=demandes')

@app.route('/achats/demande/<int:did>/reject')
@permission_required('comptabilite_edit')
def achats_demande_reject(did):
    conn = _gdb()
    conn.execute("UPDATE achats_demandes SET status='refusee' WHERE id=?", (did,))
    conn.commit(); conn.close()
    flash("Demande refusée", "success"); return redirect('/achats?tab=demandes')

@app.route('/achats/devis/add', methods=['POST'])
@permission_required('proforma_edit')
def achats_devis_add():
    ref = f"DAC-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    total_ht = float(request.form.get('total_ht', 0) or 0)
    tva = total_ht * 0.18
    _dbi('achats_devis', reference=ref, fournisseur_id=int(request.form.get('fournisseur_id',0) or 0),
        date=request.form.get('date', datetime.now().strftime('%Y-%m-%d')),
        items_json=request.form.get('items_description',''), total_ht=total_ht, tva=tva,
        total_ttc=total_ht + tva, status='en_attente', notes=request.form.get('notes',''),
        created_by=session['user_id'])
    flash(f"Devis fournisseur {ref} enregistré", "success"); return redirect('/achats?tab=devis')

@app.route('/achats/devis/<int:did>/status/<status>')
@permission_required('proforma_edit')
def achats_devis_status(did, status):
    if status in ('en_attente', 'accepte', 'refuse'):
        conn = _gdb(); conn.execute("UPDATE achats_devis SET status=? WHERE id=?", (status, did)); conn.commit(); conn.close()
    flash("Statut mis à jour", "success"); return redirect('/achats?tab=devis')

@app.route('/achats/commande/add', methods=['POST'])
@permission_required('comptabilite_edit')
def achats_commande_add():
    ref = f"BC-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    _dbi('achats_commandes', reference=ref, fournisseur_id=int(request.form.get('fournisseur_id',0) or 0),
        date=request.form.get('date', datetime.now().strftime('%Y-%m-%d')),
        items_json=request.form.get('items_description',''),
        total=float(request.form.get('total',0) or 0),
        delivery_date=request.form.get('delivery_date',''),
        notes=request.form.get('notes',''), created_by=session['user_id'])
    flash(f"Bon de commande {ref} créé", "success"); return redirect('/achats?tab=commandes')

@app.route('/achats/commande/<int:cid>/status/<status>')
@permission_required('comptabilite_edit')
def achats_commande_status(cid, status):
    if status in ('en_cours', 'livree', 'annulee'):
        conn = _gdb(); conn.execute("UPDATE achats_commandes SET status=? WHERE id=?", (status, cid)); conn.commit(); conn.close()
    flash("Statut mis à jour", "success"); return redirect('/achats?tab=commandes')

@app.route('/achats/contrat/add', methods=['POST'])
@permission_required('comptabilite_edit')
def achats_contrat_add():
    ref = f"CTR-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    _dbi('achats_contrats', reference=ref, fournisseur_id=int(request.form.get('fournisseur_id',0) or 0),
        title=request.form.get('title',''), description=request.form.get('description',''),
        start_date=request.form.get('start_date',''), end_date=request.form.get('end_date',''),
        amount=float(request.form.get('amount',0) or 0), created_by=session['user_id'])
    flash("Contrat ajouté", "success"); return redirect('/achats?tab=contrats')


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

# ======================== IMPORT/EXPORT STOCK & FOURNISSEURS ========================

@app.route('/achats/fournisseurs/import', methods=['POST'])
@permission_required('comptabilite_edit')
def import_fournisseurs():
    if 'file' not in request.files or not request.files['file'].filename:
        flash("Aucun fichier sélectionné", "error"); return redirect('/achats?tab=fournisseurs')
    f = request.files['file']
    try:
        import openpyxl
        fpath = os.path.join(app.config['UPLOAD_FOLDER'], 'import_fournisseurs.xlsx')
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        f.save(fpath)
        wb = openpyxl.load_workbook(fpath)
        ws = wb.active
        conn = _gdb()
        count = 0
        for row in ws.iter_rows(min_row=2, values_only=True):
            # Skip header rows
            if not row or not row[1] or str(row[1]).startswith('#') or str(row[1]).startswith('nom'):
                continue
            name = str(row[1] or '').strip()
            if not name: continue
            contact = str(row[2] or '').strip() if len(row) > 2 else ''
            email = str(row[3] or '').strip() if len(row) > 3 else ''
            tel = str(row[4] or '').strip() if len(row) > 4 else ''
            active = str(row[5] or '').strip().lower() if len(row) > 5 else 'oui'
            status = 'actif' if active in ('oui', 'yes', '1', 'actif', '') else 'inactif'
            # Check if exists
            existing = conn.execute("SELECT id FROM achats_fournisseurs WHERE name=?", (name,)).fetchone()
            if existing:
                conn.execute("UPDATE achats_fournisseurs SET contact_name=?, email=?, tel=?, status=? WHERE id=?",
                    (contact, email, tel, status, existing['id']))
            else:
                conn.execute("INSERT INTO achats_fournisseurs (name, contact_name, email, tel, status) VALUES (?,?,?,?,?)",
                    (name, contact, email, tel, status))
            count += 1
        conn.commit(); conn.close()
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Import', f"{count} fournisseurs importés", request.remote_addr)
        flash(f"{count} fournisseurs importés avec succès", "success")
    except Exception as e:
        flash(f"Erreur import : {str(e)}", "error")
    return redirect('/achats?tab=fournisseurs')

@app.route('/achats/fournisseurs/export')
@permission_required('comptabilite')
def export_fournisseurs():
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Fournisseurs"
    ws.append(['#', 'Nom', 'Contact principal', 'Email', 'Téléphone', 'Statut', 'Date création'])
    conn = _gdb()
    fournisseurs = conn.execute("SELECT * FROM achats_fournisseurs ORDER BY name").fetchall()
    conn.close()
    for i, f in enumerate(fournisseurs, 1):
        ws.append([i, f['name'], f['contact_name'] or '', f['email'] or '', f['tel'] or '',
                   f['status'] or 'actif', f['created_at'][:10] if f['created_at'] else ''])
    # Style header
    from openpyxl.styles import Font, PatternFill
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="1A7A6D", end_color="1A7A6D", fill_type="solid")
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 20
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], 'export_fournisseurs.xlsx')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    wb.save(output)
    return send_file(output, as_attachment=True, download_name='Fournisseurs_RAMYA.xlsx')

@app.route('/achats/stock/import', methods=['POST'])
@permission_required('comptabilite_edit')
def import_stock():
    if 'file' not in request.files or not request.files['file'].filename:
        flash("Aucun fichier sélectionné", "error"); return redirect('/achats?tab=stock')
    f = request.files['file']
    try:
        import openpyxl
        fpath = os.path.join(app.config['UPLOAD_FOLDER'], 'import_stock.xlsx')
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        f.save(fpath)
        wb = openpyxl.load_workbook(fpath)
        ws = wb.active
        conn = _gdb()
        count = 0
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row or not row[1] and not row[2]:
                continue
            # Skip header rows
            ref_val = str(row[1] or '').strip() if len(row) > 1 else ''
            name_val = str(row[2] or '').strip() if len(row) > 2 else ''
            if not name_val or name_val.startswith('Nom') or ref_val.startswith('Code'):
                continue
            category = str(row[3] or '').strip() if len(row) > 3 else ''
            location = str(row[4] or '').strip() if len(row) > 4 else ''
            qty = int(row[5] or 0) if len(row) > 5 and row[5] else 0
            unit = str(row[6] or '').strip() if len(row) > 6 else ''
            price = float(row[8] or 0) if len(row) > 8 and row[8] else 0
            # Check if exists by reference
            existing = conn.execute("SELECT id FROM stock_items WHERE reference=? OR name=?", (ref_val, name_val)).fetchone()
            if existing:
                conn.execute("UPDATE stock_items SET name=?, category=?, quantity=?, unit_price=?, location=? WHERE id=?",
                    (name_val, category, qty, price, location, existing['id']))
            else:
                conn.execute("INSERT INTO stock_items (name, reference, category, quantity, unit_price, location, notes) VALUES (?,?,?,?,?,?,?)",
                    (name_val, ref_val, category, qty, price, location, unit))
            count += 1
        conn.commit(); conn.close()
        user = get_user_by_id(session['user_id'])
        log_activity(session['user_id'], user['full_name'] if user else '?',
                    'Import', f"{count} articles de stock importés", request.remote_addr)
        flash(f"{count} articles importés avec succès", "success")
    except Exception as e:
        flash(f"Erreur import : {str(e)}", "error")
    return redirect('/achats?tab=stock')

@app.route('/achats/stock/export')
@permission_required('comptabilite')
def export_stock():
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Stock"
    ws.append(['#', 'Code', 'Nom article', 'Catégorie', 'Emplacement', 'Quantité', 'Unité', 'Prix achat', 'Valeur stock'])
    conn = _gdb()
    items = [dict(r) for r in conn.execute("SELECT * FROM stock_items ORDER BY name").fetchall()]
    conn.close()
    for i, it in enumerate(items, 1):
        qty = it.get('quantity', 0) or 0
        price = it.get('unit_price', 0) or 0
        ws.append([i, it.get('reference','') or '', it.get('name',''), it.get('category','') or '', it.get('location','') or '',
                   qty, it.get('notes','') or '', price, qty * price])
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill(start_color="E8672A", end_color="E8672A", fill_type="solid")
    for col in ws.columns:
        ws.column_dimensions[col[0].column_letter].width = 22
    
    output = os.path.join(app.config['UPLOAD_FOLDER'], 'export_stock.xlsx')
    os.makedirs(os.path.dirname(output), exist_ok=True)
    wb.save(output)
    return send_file(output, as_attachment=True, download_name='Stock_RAMYA.xlsx')

@app.route('/achats/stock/category/add', methods=['POST'])
@permission_required('comptabilite_edit')
def stock_category_add():
    name = request.form.get('name','').strip()
    if name:
        conn = _gdb()
        try: conn.execute("INSERT INTO stock_categories (name, description) VALUES (?,?)", (name, request.form.get('description','')))
        except: flash("Catégorie existe déjà","error")
        conn.commit(); conn.close()
        flash(f"Catégorie '{name}' créée","success")
    return redirect('/achats?tab=stock')

@app.route('/achats/stock/category/delete/<int:cid>')
@permission_required('comptabilite_edit')
def stock_category_delete(cid):
    conn = _gdb()
    conn.execute("DELETE FROM stock_categories WHERE id=?", (cid,))
    conn.commit(); conn.close()
    flash("Catégorie supprimée","success")
    return redirect('/achats?tab=stock')
