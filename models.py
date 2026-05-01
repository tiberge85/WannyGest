#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Modèles de base de données - RAMYA Rapport de Pointage
SQLite avec Flask-Login
"""

import sqlite3
import os
import hashlib
import secrets
from datetime import datetime

PERSISTENT_DIR = os.environ.get('PERSISTENT_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
DB_PATH = os.path.join(PERSISTENT_DIR, 'ramya.db')


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Crée les tables si elles n'existent pas."""
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'technicien',
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_login TEXT
        );
        
        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            contact_name TEXT,
            tel TEXT,
            email TEXT,
            address TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER,
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT UNIQUE NOT NULL,
            user_id INTEGER NOT NULL,
            client_id INTEGER,
            client_name TEXT,
            provider_name TEXT,
            filename_source TEXT,
            filename_pdf TEXT,
            filename_xlsx TEXT,
            employee_count INTEGER,
            period TEXT,
            hp TEXT,
            status TEXT DEFAULT 'traite',
            sent_at TEXT,
            sent_by INTEGER,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (sent_by) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role TEXT NOT NULL,
            permission TEXT NOT NULL,
            UNIQUE(role, permission)
        );
        
        CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            user_name TEXT,
            action TEXT NOT NULL,
            detail TEXT,
            ip_address TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS job_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            user_name TEXT,
            comment TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS contracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            reference TEXT,
            start_date TEXT,
            end_date TEXT,
            monthly_rate REAL DEFAULT 0,
            description TEXT,
            status TEXT DEFAULT 'actif',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            created_by INTEGER,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS smtp_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            smtp_host TEXT DEFAULT 'smtp.gmail.com',
            smtp_port INTEGER DEFAULT 587,
            smtp_user TEXT,
            smtp_pass TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            client_id INTEGER,
            client_name TEXT,
            reference TEXT,
            amount REAL DEFAULT 0,
            status TEXT DEFAULT 'a_envoyer',
            sent_at TEXT,
            sent_by INTEGER,
            paid_at TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (sent_by) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS devis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE,
            doc_type TEXT DEFAULT 'devis',
            client_id INTEGER,
            client_name TEXT,
            client_code TEXT,
            contact_commercial TEXT,
            objet TEXT,
            items_json TEXT,
            total_ht REAL DEFAULT 0,
            petites_fournitures REAL DEFAULT 0,
            total_ttc REAL DEFAULT 0,
            main_oeuvre REAL DEFAULT 0,
            remise REAL DEFAULT 0,
            status TEXT DEFAULT 'brouillon',
            notes TEXT,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS visit_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER,
            client_name TEXT,
            site_name TEXT,
            site_address TEXT,
            site_location TEXT,
            contact_name TEXT,
            contact_tel TEXT,
            visit_date TEXT,
            needs TEXT,
            observations TEXT,
            equipment TEXT,
            status TEXT DEFAULT 'en_attente',
            proforma_ref TEXT,
            proforma_amount REAL DEFAULT 0,
            proforma_sent_at TEXT,
            proforma_sent_by INTEGER,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
    ''')
    
    # Permissions par défaut — tous les rôles
    default_perms = {
        'admin': ['traitement', 'fichiers', 'clients', 'clients_edit', 'admin', 'dashboard', 'dashboard_general', 'envoyer', 'logs', 'contrats', 'comptabilite', 'comptabilite_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit', 'moyens_generaux', 'moyens_generaux_edit', 'informatique', 'projets', 'caisse_sortie', 'rapports_j', 'convertir_devis', 'resp_projet', 'resp_projet_edit', 'centre_technique', 'centre_technique_edit', 'chat', 'tracking', 'grand_livre', 'balance', 'client_users_approve', 'caisse_multi', 'gps_itineraire', 'virement_demande', 'virement_valide', 'client_requests_view'],
        'dg': ['traitement', 'fichiers', 'clients', 'clients_edit', 'admin', 'dashboard', 'dashboard_general', 'envoyer', 'logs', 'contrats', 'comptabilite', 'comptabilite_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit', 'moyens_generaux', 'moyens_generaux_edit', 'informatique', 'projets', 'caisse_sortie', 'rapports_j', 'convertir_devis', 'resp_projet', 'resp_projet_edit', 'centre_technique', 'centre_technique_edit', 'chat', 'tracking', 'grand_livre', 'balance', 'caisse_multi', 'gps_itineraire', 'virement_valide', 'client_users_approve', 'client_requests_view'],
        'rh': ['fichiers', 'clients', 'dashboard', 'envoyer', 'contrats', 'rapports_j', 'chat'],
        'technicien': ['traitement', 'dashboard', 'visites', 'rapports_j', 'centre_technique', 'chat', 'gps_itineraire'],
        'commercial': ['dashboard', 'clients', 'clients_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit', 'contrats', 'rapports_j', 'chat', 'client_requests_view'],
        'comptable': ['dashboard', 'comptabilite', 'comptabilite_edit', 'clients', 'caisse_sortie', 'rapports_j', 'convertir_devis', 'chat', 'grand_livre', 'balance', 'caisse_multi', 'virement_demande'],
        'moyens_generaux': ['dashboard', 'moyens_generaux', 'moyens_generaux_edit', 'clients', 'rapports_j', 'chat'],
        'informatique': ['dashboard', 'informatique', 'traitement', 'visites', 'projets', 'rapports_j', 'centre_technique', 'chat', 'gps_itineraire'],
        'resp_projet': ['dashboard', 'resp_projet', 'resp_projet_edit', 'clients', 'rapports_j', 'proforma', 'chat', 'gps_itineraire', 'client_requests_view', 'controle_qualite', 'livraison_intervention'],
        'gestionnaire_projet': ['dashboard', 'resp_projet', 'resp_projet_edit', 'clients', 'clients_edit', 'rapports_j', 'proforma', 'proforma_edit', 'visites', 'centre_technique', 'chat', 'gps_itineraire'],
        'coordinateur': ['dashboard', 'resp_projet', 'resp_projet_edit', 'clients', 'rapports_j', 'chat', 'gps_itineraire', 'client_requests_view', 'controle_qualite', 'livraison_intervention', 'centre_technique', 'visites'],
        'proprietaire': ['dashboard', 'dashboard_general', 'clients', 'comptabilite', 'grand_livre', 'balance', 'rapports_j', 'logs', 'chat', 'tracking', 'admin'],
    }
    for role, perms in default_perms.items():
        for perm in perms:
            try:
                conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except:
                pass
    
    # Créer admin par défaut si aucun utilisateur
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM users")
    if cursor.fetchone()['cnt'] == 0:
        salt = secrets.token_hex(16)
        pwd_hash = hash_password('admin2026', salt)
        conn.execute("""
            INSERT INTO users (username, email, password_hash, salt, full_name, role)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ('admin', 'admin@ramya.ci', pwd_hash, salt, 'Administrateur', 'admin'))
    
    conn.commit()
    conn.close()


def hash_password(password, salt):
    return hashlib.sha256((password + salt).encode()).hexdigest()


def verify_password(password, salt, password_hash):
    return hash_password(password, salt) == password_hash


# ======================== USER OPERATIONS ========================

def create_user(username, email, password, full_name, role='technicien'):
    conn = get_db()
    salt = secrets.token_hex(16)
    pwd_hash = hash_password(password, salt)
    try:
        conn.execute("""
            INSERT INTO users (username, email, password_hash, salt, full_name, role)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (username, email, pwd_hash, salt, full_name, role))
        conn.commit()
        return True, "Compte créé avec succès"
    except sqlite3.IntegrityError as e:
        if 'username' in str(e):
            return False, "Ce nom d'utilisateur existe déjà"
        if 'email' in str(e):
            return False, "Cet email est déjà utilisé"
        return False, str(e)
    finally:
        conn.close()


def authenticate_user(username, password):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE username = ? AND is_active = 1", (username,)).fetchone()
    if user and verify_password(password, user['salt'], user['password_hash']):
        conn.execute("UPDATE users SET last_login = ? WHERE id = ?", (datetime.now().isoformat(), user['id']))
        conn.commit()
        conn.close()
        return dict(user)
    conn.close()
    return None


def get_user_by_id(user_id):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    return dict(user) if user else None


def get_all_users():
    conn = get_db()
    users = conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(u) for u in users]


def update_user(user_id, **kwargs):
    conn = get_db()
    for key, val in kwargs.items():
        if key == 'password':
            salt = secrets.token_hex(16)
            pwd_hash = hash_password(val, salt)
            conn.execute("UPDATE users SET password_hash=?, salt=? WHERE id=?", (pwd_hash, salt, user_id))
        elif key in ('role', 'is_active', 'full_name', 'email'):
            conn.execute(f"UPDATE users SET {key}=? WHERE id=?", (val, user_id))
    conn.commit()
    conn.close()


def delete_user(user_id):
    conn = get_db()
    conn.execute("DELETE FROM users WHERE id = ? AND role != 'admin'", (user_id,))
    conn.commit()
    conn.close()


# ======================== CLIENT OPERATIONS ========================

def create_client(name, tel='', email='', contact_name='', address='', notes='', created_by=None, client_code=''):
    conn = get_db()
    # Auto-generate code if not provided
    if not client_code:
        max_num = 0
        for r in conn.execute("SELECT client_code FROM clients WHERE client_code LIKE 'C %'").fetchall():
            try:
                n = int(str(r['client_code']).replace('C ','').strip())
                if n > max_num: max_num = n
            except: pass
        client_code = f"C {(max_num + 1):03d}"
    # Ensure column exists (defensive)
    try: conn.execute("ALTER TABLE clients ADD COLUMN client_code TEXT DEFAULT ''")
    except: pass
    try:
        conn.execute("""
            INSERT INTO clients (name, tel, email, contact_name, address, notes, client_code, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, tel, email, contact_name, address, notes, client_code, created_by))
    except Exception:
        # Fallback without client_code in case the column still doesn't exist
        conn.execute("""
            INSERT INTO clients (name, tel, email, contact_name, address, notes, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (name, tel, email, contact_name, address, notes, created_by))
    conn.commit()
    client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    # Backfill client_code in case INSERT used the fallback
    try: conn.execute("UPDATE clients SET client_code=? WHERE id=? AND (client_code IS NULL OR client_code='')",
                      (client_code, client_id))
    except: pass
    conn.commit()
    conn.close()
    return client_id


def get_all_clients():
    conn = get_db()
    clients = conn.execute("SELECT * FROM clients ORDER BY name").fetchall()
    conn.close()
    return [dict(c) for c in clients]


def get_client_by_id(client_id):
    conn = get_db()
    client = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
    conn.close()
    return dict(client) if client else None


def find_client_by_name(name):
    """Cherche un client par nom (recherche partielle)."""
    conn = get_db()
    client = conn.execute("SELECT * FROM clients WHERE LOWER(name) LIKE ?", (f'%{name.lower()}%',)).fetchone()
    conn.close()
    return dict(client) if client else None


def update_client(client_id, **kwargs):
    conn = get_db()
    for key, val in kwargs.items():
        if key in ('name', 'tel', 'email', 'contact_name', 'address', 'notes'):
            conn.execute(f"UPDATE clients SET {key}=? WHERE id=?", (val, client_id))
    conn.commit()
    conn.close()


def delete_client(client_id):
    """Supprime un client en délinkant d'abord toutes les références FK."""
    conn = get_db()
    try:
        tables_to_unlink = [
            'devis', 'invoices', 'contrats', 'interventions',
            'visits', 'tech_center', 'client_messages', 'client_requests',
            'client_users', 'client_attachments', 'client_reminders',
            'treasury', 'caisse_sorties', 'caisse_entrees', 'bilans', 'prospects'
        ]
        for tbl in tables_to_unlink:
            try: conn.execute(f"UPDATE {tbl} SET client_id = NULL WHERE client_id = ?", (client_id,))
            except: pass
        conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()


def merge_clients(keep_id, drop_id):
    """Fusionne deux doublons : réattribue TOUTES les références FK du drop_id
    vers le keep_id puis supprime le drop_id.
    
    Returns dict {success: bool, moved: {table: count}, message: str}
    """
    if keep_id == drop_id:
        return {'success': False, 'moved': {}, 'message': 'Impossible : même ID'}
    conn = get_db()
    moved = {}
    try:
        # Vérifier que les 2 clients existent
        k = conn.execute("SELECT id, name FROM clients WHERE id=?", (keep_id,)).fetchone()
        d = conn.execute("SELECT id, name FROM clients WHERE id=?", (drop_id,)).fetchone()
        if not k or not d:
            return {'success': False, 'moved': {}, 'message': 'Un des clients est introuvable'}
        
        # Tables avec FK client_id → tout réattribuer vers keep_id
        tables_to_merge = [
            'devis', 'invoices', 'contrats', 'interventions',
            'visits', 'tech_center', 'client_messages', 'client_requests',
            'client_users', 'client_attachments', 'client_reminders',
            'treasury', 'caisse_sorties', 'caisse_entrees', 'bilans', 'prospects'
        ]
        for tbl in tables_to_merge:
            try:
                cur = conn.execute(f"UPDATE {tbl} SET client_id = ? WHERE client_id = ?", (keep_id, drop_id))
                if cur.rowcount > 0:
                    moved[tbl] = cur.rowcount
            except Exception:
                pass  # table inexistante ou colonne absente
        
        # Supprimer le doublon
        conn.execute("DELETE FROM clients WHERE id = ?", (drop_id,))
        conn.commit()
        total_moved = sum(moved.values())
        return {
            'success': True, 'moved': moved,
            'keep_name': k['name'], 'drop_name': d['name'],
            'message': f"Fusion réussie : {total_moved} lien(s) transférés de « {d['name']} » vers « {k['name']} »"
        }
    except Exception as e:
        conn.rollback()
        return {'success': False, 'moved': moved, 'message': f'Erreur : {e}'}
    finally:
        conn.close()


# ======================== JOB OPERATIONS ========================

def create_job(job_id, user_id, client_name, provider_name, filename_source,
               filename_pdf, filename_xlsx, employee_count, period, hp, client_id=None):
    conn = get_db()
    conn.execute("""
        INSERT INTO jobs (job_id, user_id, client_id, client_name, provider_name,
            filename_source, filename_pdf, filename_xlsx, employee_count, period, hp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (job_id, user_id, client_id, client_name, provider_name,
          filename_source, filename_pdf, filename_xlsx, employee_count, period, hp))
    conn.commit()
    conn.close()


def get_jobs_by_status(status='traite'):
    conn = get_db()
    jobs = conn.execute("""
        SELECT j.*, u.full_name as user_name, 
               su.full_name as sent_by_name
        FROM jobs j 
        LEFT JOIN users u ON j.user_id = u.id
        LEFT JOIN users su ON j.sent_by = su.id
        WHERE j.status = ?
        ORDER BY j.created_at DESC
    """, (status,)).fetchall()
    conn.close()
    return [dict(j) for j in jobs]


def get_all_jobs():
    conn = get_db()
    jobs = conn.execute("""
        SELECT j.*, u.full_name as user_name,
               su.full_name as sent_by_name
        FROM jobs j 
        LEFT JOIN users u ON j.user_id = u.id
        LEFT JOIN users su ON j.sent_by = su.id
        ORDER BY j.created_at DESC
    """).fetchall()
    conn.close()
    return [dict(j) for j in jobs]


def get_user_jobs(user_id):
    conn = get_db()
    jobs = conn.execute("""
        SELECT j.*, u.full_name as user_name
        FROM jobs j LEFT JOIN users u ON j.user_id = u.id
        WHERE j.user_id = ?
        ORDER BY j.created_at DESC
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(j) for j in jobs]


def mark_job_sent(job_id, sent_by):
    conn = get_db()
    conn.execute("""
        UPDATE jobs SET status='envoye', sent_at=?, sent_by=? WHERE job_id=?
    """, (datetime.now().isoformat(), sent_by, job_id))
    conn.commit()
    conn.close()


def get_dashboard_stats():
    conn = get_db()
    stats = {}
    stats['total_jobs'] = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    stats['pending_jobs'] = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='traite'").fetchone()[0]
    stats['sent_jobs'] = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='envoye'").fetchone()[0]
    stats['total_clients'] = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    stats['total_users'] = conn.execute("SELECT COUNT(*) FROM users WHERE is_active=1").fetchone()[0]
    
    # Derniers traitements
    stats['recent_jobs'] = [dict(r) for r in conn.execute("""
        SELECT j.*, u.full_name as user_name
        FROM jobs j LEFT JOIN users u ON j.user_id = u.id
        ORDER BY j.created_at DESC LIMIT 10
    """).fetchall()]
    
    conn.close()
    return stats


def has_permission(role, permission):
    conn = get_db()
    result = conn.execute(
        "SELECT COUNT(*) FROM permissions WHERE role=? AND permission=?", 
        (role, permission)
    ).fetchone()[0]
    conn.close()
    return result > 0


def get_role_permissions(role):
    conn = get_db()
    perms = conn.execute("SELECT permission FROM permissions WHERE role=?", (role,)).fetchall()
    conn.close()
    return [p['permission'] for p in perms]


def update_role_permissions(role, permissions):
    conn = get_db()
    conn.execute("DELETE FROM permissions WHERE role=?", (role,))
    for perm in permissions:
        conn.execute("INSERT INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
    conn.commit()
    conn.close()


# ======================== RESET OPERATIONS ========================

def reset_jobs():
    """Supprime tous les rapports traités."""
    conn = get_db()
    conn.execute("DELETE FROM jobs")
    conn.commit()
    conn.close()

def reset_clients():
    """Supprime tous les clients."""
    conn = get_db()
    conn.execute("DELETE FROM clients")
    conn.commit()
    conn.close()

def reset_users():
    """Supprime tous les utilisateurs sauf les admins."""
    conn = get_db()
    conn.execute("DELETE FROM users WHERE role != 'admin'")
    conn.commit()
    conn.close()

def reset_all():
    """Réinitialisation complète : jobs, clients, utilisateurs non-admin."""
    conn = get_db()
    conn.execute("DELETE FROM jobs")
    conn.execute("DELETE FROM clients")
    conn.execute("DELETE FROM users WHERE role != 'admin'")
    conn.execute("DELETE FROM activity_logs")
    conn.execute("DELETE FROM job_comments")
    conn.commit()
    conn.close()


# ======================== ACTIVITY LOGS ========================

def log_activity(user_id, user_name, action, detail='', ip_address=''):
    conn = get_db()
    conn.execute("""
        INSERT INTO activity_logs (user_id, user_name, action, detail, ip_address)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, user_name, action, detail, ip_address))
    conn.commit()
    conn.close()

def get_activity_logs(limit=100):
    conn = get_db()
    logs = conn.execute("""
        SELECT * FROM activity_logs ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(l) for l in logs]

def get_user_activity(user_id, limit=50):
    conn = get_db()
    logs = conn.execute("""
        SELECT * FROM activity_logs WHERE user_id=? ORDER BY created_at DESC LIMIT ?
    """, (user_id, limit)).fetchall()
    conn.close()
    return [dict(l) for l in logs]


# ======================== JOB COMMENTS ========================

def add_job_comment(job_id, user_id, user_name, comment):
    conn = get_db()
    conn.execute("""
        INSERT INTO job_comments (job_id, user_id, user_name, comment)
        VALUES (?, ?, ?, ?)
    """, (job_id, user_id, user_name, comment))
    conn.commit()
    conn.close()

def get_job_comments(job_id):
    conn = get_db()
    comments = conn.execute("""
        SELECT * FROM job_comments WHERE job_id=? ORDER BY created_at ASC
    """, (job_id,)).fetchall()
    conn.close()
    return [dict(c) for c in comments]

def update_job_notes(job_id, notes):
    conn = get_db()
    conn.execute("UPDATE jobs SET notes=? WHERE job_id=?", (notes, job_id))
    conn.commit()
    conn.close()

def get_job_by_id(job_id):
    conn = get_db()
    job = conn.execute("""
        SELECT j.*, u.full_name as user_name, su.full_name as sent_by_name
        FROM jobs j 
        LEFT JOIN users u ON j.user_id = u.id
        LEFT JOIN users su ON j.sent_by = su.id
        WHERE j.job_id = ?
    """, (job_id,)).fetchone()
    conn.close()
    return dict(job) if job else None


# ======================== BACKUP ========================

def get_db_path():
    return DB_PATH


# ======================== SMTP SETTINGS ========================

def save_smtp_settings(user_id, smtp_host, smtp_port, smtp_user, smtp_pass):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO smtp_settings (user_id, smtp_host, smtp_port, smtp_user, smtp_pass) VALUES (?,?,?,?,?)",
                 (user_id, smtp_host, smtp_port, smtp_user, smtp_pass))
    conn.commit()
    conn.close()

def get_smtp_settings(user_id):
    conn = get_db()
    s = conn.execute("SELECT * FROM smtp_settings WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return dict(s) if s else {'smtp_host': 'smtp.gmail.com', 'smtp_port': 587, 'smtp_user': '', 'smtp_pass': ''}


# ======================== INVOICES ========================

def create_invoice(job_id, client_id, client_name, reference='', amount=0, notes=''):
    conn = get_db()
    conn.execute("INSERT INTO invoices (job_id, client_id, client_name, reference, amount, notes) VALUES (?,?,?,?,?,?)",
                 (job_id, client_id, client_name, reference, amount, notes))
    conn.commit()
    conn.close()

def get_invoices_by_status(status):
    conn = get_db()
    invoices = conn.execute("SELECT i.*, su.full_name as sent_by_name FROM invoices i LEFT JOIN users su ON i.sent_by=su.id WHERE i.status=? ORDER BY i.created_at DESC", (status,)).fetchall()
    conn.close()
    return [dict(i) for i in invoices]

def get_all_invoices():
    conn = get_db()
    invoices = conn.execute("SELECT i.*, su.full_name as sent_by_name FROM invoices i LEFT JOIN users su ON i.sent_by=su.id ORDER BY i.created_at DESC").fetchall()
    conn.close()
    return [dict(i) for i in invoices]

def update_invoice_status(invoice_id, status, user_id=None):
    conn = get_db()
    now = datetime.now().isoformat()
    if status == 'envoyee':
        conn.execute("UPDATE invoices SET status=?, sent_at=?, sent_by=? WHERE id=?", (status, now, user_id, invoice_id))
    elif status == 'en_attente_paiement':
        conn.execute("UPDATE invoices SET status=? WHERE id=?", (status, invoice_id))
    elif status == 'payee':
        conn.execute("UPDATE invoices SET status=?, paid_at=? WHERE id=?", (status, now, invoice_id))
    else:
        conn.execute("UPDATE invoices SET status=? WHERE id=?", (status, invoice_id))
    conn.commit()
    conn.close()

def get_invoice_stats():
    conn = get_db()
    stats = {}
    stats['a_envoyer'] = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='a_envoyer'").fetchone()[0]
    stats['envoyee'] = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='envoyee'").fetchone()[0]
    stats['en_attente_paiement'] = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='en_attente_paiement'").fetchone()[0]
    stats['payee'] = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='payee'").fetchone()[0]
    stats['total_amount'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status='payee'").fetchone()[0]
    conn.close()
    return stats


# ======================== VISIT REPORTS ========================

def create_visit_report(client_id, client_name, site_name, site_address, site_location,
                        contact_name, contact_tel, visit_date, needs, observations, equipment, created_by):
    conn = get_db()
    conn.execute("""INSERT INTO visit_reports (client_id, client_name, site_name, site_address, site_location,
        contact_name, contact_tel, visit_date, needs, observations, equipment, created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (client_id, client_name, site_name, site_address, site_location,
         contact_name, contact_tel, visit_date, needs, observations, equipment, created_by))
    conn.commit()
    conn.close()

def get_visit_reports(status=None):
    conn = get_db()
    if status:
        visits = conn.execute("""SELECT v.*, u.full_name as created_by_name, su.full_name as proforma_sent_by_name
            FROM visit_reports v LEFT JOIN users u ON v.created_by=u.id LEFT JOIN users su ON v.proforma_sent_by=su.id
            WHERE v.status=? ORDER BY v.created_at DESC""", (status,)).fetchall()
    else:
        visits = conn.execute("""SELECT v.*, u.full_name as created_by_name, su.full_name as proforma_sent_by_name
            FROM visit_reports v LEFT JOIN users u ON v.created_by=u.id LEFT JOIN users su ON v.proforma_sent_by=su.id
            ORDER BY v.created_at DESC""").fetchall()
    conn.close()
    return [dict(v) for v in visits]

def get_visit_by_id(visit_id):
    conn = get_db()
    v = conn.execute("""SELECT v.*, u.full_name as created_by_name
        FROM visit_reports v LEFT JOIN users u ON v.created_by=u.id WHERE v.id=?""", (visit_id,)).fetchone()
    conn.close()
    return dict(v) if v else None

def update_visit_proforma(visit_id, proforma_ref, proforma_amount, sent_by):
    conn = get_db()
    conn.execute("""UPDATE visit_reports SET status='proforma_envoye', proforma_ref=?, proforma_amount=?,
        proforma_sent_at=?, proforma_sent_by=? WHERE id=?""",
        (proforma_ref, proforma_amount, datetime.now().isoformat(), sent_by, visit_id))
    conn.commit()
    conn.close()

def get_visit_stats():
    conn = get_db()
    stats = {}
    stats['en_attente'] = conn.execute("SELECT COUNT(*) FROM visit_reports WHERE status='en_attente'").fetchone()[0]
    stats['proforma_envoye'] = conn.execute("SELECT COUNT(*) FROM visit_reports WHERE status='proforma_envoye'").fetchone()[0]
    stats['total'] = conn.execute("SELECT COUNT(*) FROM visit_reports").fetchone()[0]
    conn.close()
    return stats


# ======================== CONTRACTS ========================

def create_contract(client_id, reference='', start_date='', end_date='', monthly_rate=0, description='', created_by=None):
    conn = get_db()
    conn.execute("""
        INSERT INTO contracts (client_id, reference, start_date, end_date, monthly_rate, description, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (client_id, reference, start_date, end_date, monthly_rate, description, created_by))
    conn.commit()
    conn.close()

def get_client_contracts(client_id):
    conn = get_db()
    contracts = conn.execute("""
        SELECT c.*, cl.name as client_name FROM contracts c
        LEFT JOIN clients cl ON c.client_id = cl.id
        WHERE c.client_id = ? ORDER BY c.created_at DESC
    """, (client_id,)).fetchall()
    conn.close()
    return [dict(c) for c in contracts]

def get_all_contracts():
    conn = get_db()
    contracts = conn.execute("""
        SELECT c.*, cl.name as client_name FROM contracts c
        LEFT JOIN clients cl ON c.client_id = cl.id
        ORDER BY c.status, c.end_date
    """).fetchall()
    conn.close()
    return [dict(c) for c in contracts]

def get_contract_by_id(contract_id):
    conn = get_db()
    c = conn.execute("SELECT * FROM contracts WHERE id = ?", (contract_id,)).fetchone()
    conn.close()
    return dict(c) if c else None

def update_contract(contract_id, **kwargs):
    conn = get_db()
    for key, val in kwargs.items():
        if key in ('reference', 'start_date', 'end_date', 'monthly_rate', 'description', 'status', 'client_id'):
            conn.execute(f"UPDATE contracts SET {key}=? WHERE id=?", (val, contract_id))
    conn.commit()
    conn.close()

def delete_contract(contract_id):
    conn = get_db()
    conn.execute("DELETE FROM contracts WHERE id = ?", (contract_id,))
    conn.commit()
    conn.close()


# ======================== COMPARISON STATS ========================

def get_client_monthly_stats():
    """Retourne les stats par client et par mois pour comparaison."""
    conn = get_db()
    jobs = conn.execute("""
        SELECT job_id, client_name, employee_count, period, hp, status, created_at
        FROM jobs ORDER BY created_at
    """).fetchall()
    conn.close()
    
    stats = {}
    for j in jobs:
        j = dict(j)
        client = j['client_name'] or 'Inconnu'
        # Extract month from created_at
        month = j['created_at'][:7] if j['created_at'] else 'N/A'
        
        if client not in stats:
            stats[client] = {}
        if month not in stats[client]:
            stats[client][month] = {'count': 0, 'employees': 0, 'sent': 0, 'pending': 0}
        
        stats[client][month]['count'] += 1
        stats[client][month]['employees'] += j['employee_count'] or 0
        if j['status'] == 'envoye':
            stats[client][month]['sent'] += 1
        else:
            stats[client][month]['pending'] += 1
    
    return stats


# ======================== RH - PERSONNEL ========================

def init_rh_tables():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            matricule TEXT UNIQUE,
            email TEXT,
            tel TEXT,
            position TEXT,
            department TEXT,
            hire_date TEXT,
            contract_type TEXT DEFAULT 'CDI',
            salary REAL DEFAULT 0,
            insurance TEXT,
            insurance_number TEXT,
            emergency_contact TEXT,
            emergency_tel TEXT,
            status TEXT DEFAULT 'actif',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS leaves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            leave_type TEXT DEFAULT 'conge_annuel',
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            days INTEGER DEFAULT 0,
            reason TEXT,
            status TEXT DEFAULT 'en_attente',
            approved_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES employees(id)
        );
        
        CREATE TABLE IF NOT EXISTS payslips (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            period TEXT NOT NULL,
            base_salary REAL DEFAULT 0,
            worked_hours REAL DEFAULT 0,
            overtime_hours REAL DEFAULT 0,
            overtime_amount REAL DEFAULT 0,
            bonus REAL DEFAULT 0,
            commission REAL DEFAULT 0,
            deductions REAL DEFAULT 0,
            insurance_amount REAL DEFAULT 0,
            tax_amount REAL DEFAULT 0,
            net_salary REAL DEFAULT 0,
            status TEXT DEFAULT 'brouillon',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES employees(id)
        );
    ''')
    conn.commit()
    conn.close()


def get_all_employees(status='actif'):
    conn = get_db()
    if status:
        emps = conn.execute("SELECT * FROM employees WHERE status=? ORDER BY last_name", (status,)).fetchall()
    else:
        emps = conn.execute("SELECT * FROM employees ORDER BY last_name").fetchall()
    conn.close()
    return [dict(e) for e in emps]

def get_employee_by_id(eid):
    conn = get_db()
    e = conn.execute("SELECT * FROM employees WHERE id=?", (eid,)).fetchone()
    conn.close()
    return dict(e) if e else None

def create_employee(**kwargs):
    # Convert empty unique fields to None to avoid UNIQUE constraint on empty strings
    for unique_field in ['matricule', 'email']:
        if unique_field in kwargs and not kwargs[unique_field]:
            kwargs[unique_field] = None
    conn = get_db()
    # Filter kwargs to only include columns that exist in the table
    existing_cols = set(r['name'] for r in conn.execute("PRAGMA table_info(employees)").fetchall())
    filtered = {k: v for k, v in kwargs.items() if k in existing_cols}
    if filtered:
        cols = ', '.join(filtered.keys())
        placeholders = ', '.join(['?' for _ in filtered])
        conn.execute(f"INSERT INTO employees ({cols}) VALUES ({placeholders})", list(filtered.values()))
        conn.commit()
    conn.close()

def update_employee(eid, **kwargs):
    conn = get_db()
    existing_cols = set(r['name'] for r in conn.execute("PRAGMA table_info(employees)").fetchall())
    for k, v in kwargs.items():
        if k in existing_cols:
            conn.execute(f"UPDATE employees SET {k}=? WHERE id=?", (v, eid))
    conn.commit()
    conn.close()

def get_employee_stats():
    conn = get_db()
    s = {}
    s['total'] = conn.execute("SELECT COUNT(*) FROM employees WHERE status='actif'").fetchone()[0]
    s['cdi'] = conn.execute("SELECT COUNT(*) FROM employees WHERE contract_type='CDI' AND status='actif'").fetchone()[0]
    s['cdd'] = conn.execute("SELECT COUNT(*) FROM employees WHERE contract_type='CDD' AND status='actif'").fetchone()[0]
    s['pending_leaves'] = conn.execute("SELECT COUNT(*) FROM leaves WHERE status='en_attente'").fetchone()[0]
    conn.close()
    return s

def get_leaves(status=None):
    conn = get_db()
    if status:
        leaves = conn.execute("""SELECT l.*, e.first_name||' '||e.last_name as employee_name
            FROM leaves l LEFT JOIN employees e ON l.employee_id=e.id WHERE l.status=? ORDER BY l.created_at DESC""", (status,)).fetchall()
    else:
        leaves = conn.execute("""SELECT l.*, e.first_name||' '||e.last_name as employee_name
            FROM leaves l LEFT JOIN employees e ON l.employee_id=e.id ORDER BY l.created_at DESC""").fetchall()
    conn.close()
    return [dict(l) for l in leaves]

def create_leave(employee_id, leave_type, start_date, end_date, days, reason):
    conn = get_db()
    conn.execute("INSERT INTO leaves (employee_id, leave_type, start_date, end_date, days, reason) VALUES (?,?,?,?,?,?)",
                 (employee_id, leave_type, start_date, end_date, days, reason))
    conn.commit()
    conn.close()

def update_leave_status(leave_id, status, approved_by=None):
    conn = get_db()
    conn.execute("UPDATE leaves SET status=?, approved_by=? WHERE id=?", (status, approved_by, leave_id))
    conn.commit()
    conn.close()

def get_payslips(period=None):
    conn = get_db()
    if period:
        slips = conn.execute("""SELECT p.*, e.first_name||' '||e.last_name as employee_name, e.matricule
            FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id WHERE p.period=? ORDER BY e.last_name""", (period,)).fetchall()
    else:
        slips = conn.execute("""SELECT p.*, e.first_name||' '||e.last_name as employee_name, e.matricule
            FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id ORDER BY p.period DESC, e.last_name""").fetchall()
    conn.close()
    return [dict(s) for s in slips]

def create_payslip(**kwargs):
    conn = get_db()
    cols = ', '.join(kwargs.keys())
    placeholders = ', '.join(['?' for _ in kwargs])
    conn.execute(f"INSERT INTO payslips ({cols}) VALUES ({placeholders})", list(kwargs.values()))
    conn.commit()
    conn.close()

def update_payslip(pid, **kwargs):
    conn = get_db()
    for k, v in kwargs.items():
        conn.execute(f"UPDATE payslips SET {k}=? WHERE id=?", (v, pid))
    conn.commit()
    conn.close()


# ======================== DEVIS / PROFORMA ========================

def init_devis_tables():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS devis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE,
            doc_type TEXT DEFAULT 'DEVIS',
            client_id INTEGER,
            client_name TEXT,
            client_contact TEXT,
            client_code TEXT,
            objet TEXT,
            commercial TEXT,
            items TEXT,
            total_pieces REAL DEFAULT 0,
            main_oeuvre REAL DEFAULT 0,
            total_ht REAL DEFAULT 0,
            remise REAL DEFAULT 0,
            petites_fournitures REAL DEFAULT 0,
            total_ttc REAL DEFAULT 0,
            notes TEXT,
            status TEXT DEFAULT 'brouillon',
            sent_at TEXT,
            sent_by INTEGER,
            accepted_at TEXT,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
    ''')
    conn.commit()
    conn.close()

def create_devis_simple(**kwargs):
    conn = get_db()
    cols = ', '.join(kwargs.keys())
    placeholders = ', '.join(['?' for _ in kwargs])
    conn.execute(f"INSERT INTO devis ({cols}) VALUES ({placeholders})", list(kwargs.values()))
    conn.commit()
    did = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return did

def get_all_devis(status=None):
    conn = get_db()
    if status:
        rows = conn.execute("SELECT d.*, u.full_name as created_by_name FROM devis d LEFT JOIN users u ON d.created_by=u.id WHERE d.status=? ORDER BY d.created_at DESC", (status,)).fetchall()
    else:
        rows = conn.execute("SELECT d.*, u.full_name as created_by_name FROM devis d LEFT JOIN users u ON d.created_by=u.id ORDER BY d.created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_devis_by_id(did):
    conn = get_db()
    d = conn.execute("SELECT d.*, u.full_name as created_by_name FROM devis d LEFT JOIN users u ON d.created_by=u.id WHERE d.id=?", (did,)).fetchone()
    conn.close()
    return dict(d) if d else None

def update_devis_status(did, status, user_id=None):
    conn = get_db()
    now = datetime.now().isoformat()
    if status == 'envoye':
        conn.execute("UPDATE devis SET status=?, sent_at=?, sent_by=? WHERE id=?", (status, now, user_id, did))
    elif status == 'accepte':
        conn.execute("UPDATE devis SET status=?, accepted_at=? WHERE id=?", (status, now, did))
    else:
        conn.execute("UPDATE devis SET status=? WHERE id=?", (status, did))
    conn.commit()
    conn.close()

def get_devis_stats():
    conn = get_db()
    s = {}
    s['brouillon'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='brouillon'").fetchone()[0]
    s['envoye'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='envoye'").fetchone()[0]
    s['accepte'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='accepte'").fetchone()[0]
    s['decline'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='decline'").fetchone()[0]
    s['total'] = conn.execute("SELECT COUNT(*) FROM devis").fetchone()[0]
    s['total_amount'] = conn.execute("SELECT COALESCE(SUM(total_ttc),0) FROM devis WHERE status='accepte'").fetchone()[0]
    conn.close()
    return s

def get_next_devis_ref(doc_type='DEV'):
    conn = get_db()
    year = datetime.now().strftime('%y')
    prefix = f"{doc_type}-"
    count = conn.execute("SELECT COUNT(*) FROM devis WHERE reference LIKE ?", (f'{prefix}%{year}',)).fetchone()[0]
    conn.close()
    return f"{prefix}{str(count+1).zfill(6)}-{year}"


# ======================== DEVIS / PROFORMA ========================

def create_devis(client_id, client_name, client_code, contact_commercial,
                 objet, items_json, total_ht, petites_fournitures, total_ttc,
                 main_oeuvre, remise, notes, created_by, doc_type='devis'):
    conn = get_db()
    # Auto-generate reference
    year = datetime.now().strftime('%y')
    count = conn.execute("SELECT COUNT(*) FROM devis WHERE doc_type=?", (doc_type,)).fetchone()[0] + 1
    prefix = 'DEV' if doc_type == 'devis' else 'PRO'
    reference = f"{prefix}-{count:06d}-{year}"
    
    conn.execute("""INSERT INTO devis (reference, doc_type, client_id, client_name, client_code,
        contact_commercial, objet, items_json, total_ht, petites_fournitures, total_ttc,
        main_oeuvre, remise, notes, created_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (reference, doc_type, client_id, client_name, client_code,
         contact_commercial, objet, items_json, total_ht, petites_fournitures, total_ttc,
         main_oeuvre, remise, notes, created_by))
    conn.commit()
    did = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return did, reference

def get_all_devis(doc_type=None):
    conn = get_db()
    if doc_type:
        rows = conn.execute("""SELECT d.*, u.full_name as created_by_name FROM devis d
            LEFT JOIN users u ON d.created_by=u.id WHERE d.doc_type=? ORDER BY d.created_at DESC""", (doc_type,)).fetchall()
    else:
        rows = conn.execute("""SELECT d.*, u.full_name as created_by_name FROM devis d
            LEFT JOIN users u ON d.created_by=u.id ORDER BY d.created_at DESC""").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_devis_by_id(did):
    conn = get_db()
    d = conn.execute("""SELECT d.*, u.full_name as created_by_name FROM devis d
        LEFT JOIN users u ON d.created_by=u.id WHERE d.id=?""", (did,)).fetchone()
    conn.close()
    return dict(d) if d else None

def update_devis_status(did, status):
    conn = get_db()
    conn.execute("UPDATE devis SET status=? WHERE id=?", (status, did))
    conn.commit()
    conn.close()

def get_devis_stats():
    conn = get_db()
    s = {}
    s['brouillon'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='brouillon'").fetchone()[0]
    s['envoye'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='envoye'").fetchone()[0]
    s['accepte'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='accepte'").fetchone()[0]
    s['refuse'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='refuse'").fetchone()[0]
    s['total'] = conn.execute("SELECT COUNT(*) FROM devis").fetchone()[0]
    s['montant_total'] = conn.execute("SELECT COALESCE(SUM(total_ttc),0) FROM devis WHERE status='accepte'").fetchone()[0]
    conn.close()
    return s


# ======================== SECURITY ========================

def record_login_attempt(username, success, ip=''):
    conn = get_db()
    conn.execute("CREATE TABLE IF NOT EXISTS login_attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, success INTEGER, ip TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("INSERT INTO login_attempts (username, success, ip) VALUES (?,?,?)", (username, 1 if success else 0, ip))
    conn.commit()
    conn.close()

def get_failed_attempts(username, minutes=15):
    conn = get_db()
    conn.execute("CREATE TABLE IF NOT EXISTS login_attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, success INTEGER, ip TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    count = conn.execute("SELECT COUNT(*) FROM login_attempts WHERE username=? AND success=0 AND created_at > datetime('now', ?)", (username, f'-{minutes} minutes')).fetchone()[0]
    conn.close()
    return count

def save_otp(user_id, code):
    conn = get_db()
    conn.execute("CREATE TABLE IF NOT EXISTS otp_codes (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, code TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("DELETE FROM otp_codes WHERE user_id=?", (user_id,))
    conn.execute("INSERT INTO otp_codes (user_id, code) VALUES (?,?)", (user_id, code))
    conn.commit()
    conn.close()

def verify_otp(user_id, code):
    conn = get_db()
    conn.execute("CREATE TABLE IF NOT EXISTS otp_codes (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, code TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    row = conn.execute("SELECT * FROM otp_codes WHERE user_id=? AND code=? AND created_at > datetime('now', '-10 minutes')", (user_id, code)).fetchone()
    if row:
        conn.execute("DELETE FROM otp_codes WHERE user_id=?", (user_id,))
        conn.commit()
    conn.close()
    return row is not None


# ======================== PROJECTS ========================

def init_extra_tables():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, client_id INTEGER, description TEXT,
            status TEXT DEFAULT 'non_commence', priority TEXT DEFAULT 'moyenne',
            start_date TEXT, end_date TEXT, budget REAL DEFAULT 0,
            manager_id INTEGER, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER, title TEXT NOT NULL, description TEXT,
            assigned_to INTEGER, priority TEXT DEFAULT 'moyenne',
            status TEXT DEFAULT 'a_faire', due_date TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects(id),
            FOREIGN KEY (assigned_to) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS prospects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company TEXT NOT NULL, contact_name TEXT, tel TEXT, email TEXT,
            source TEXT, status TEXT DEFAULT 'nouveau',
            estimated_value REAL DEFAULT 0, notes TEXT,
            assigned_to INTEGER, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS stock_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, reference TEXT, category TEXT,
            quantity INTEGER DEFAULT 0, unit_price REAL DEFAULT 0,
            min_stock INTEGER DEFAULT 0, location TEXT,
            notes TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS stock_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL, movement_type TEXT NOT NULL,
            quantity INTEGER NOT NULL, unit_price REAL DEFAULT 0,
            reference TEXT, notes TEXT, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (item_id) REFERENCES stock_items(id)
        );
        CREATE TABLE IF NOT EXISTS treasury (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            movement_type TEXT NOT NULL, category TEXT,
            amount REAL NOT NULL, description TEXT,
            reference TEXT, payment_method TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS calendar_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, description TEXT,
            start_date TEXT, end_date TEXT,
            all_day INTEGER DEFAULT 0, color TEXT DEFAULT '#1a3a5c',
            user_id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT NOT NULL, description TEXT,
            client_id INTEGER, priority TEXT DEFAULT 'normale',
            status TEXT DEFAULT 'ouvert', assigned_to INTEGER,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT, amount REAL NOT NULL,
            description TEXT, date TEXT, receipt_ref TEXT,
            status TEXT DEFAULT 'en_attente',
            approved_by INTEGER, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS user_todos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL, title TEXT NOT NULL,
            done INTEGER DEFAULT 0, priority TEXT DEFAULT 'normale',
            due_date TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    ''')
    conn.commit()
    conn.close()


# ======================== GENERIC CRUD HELPERS ========================

def db_insert(table, **kwargs):
    conn = get_db()
    cols = ', '.join(kwargs.keys())
    vals = ', '.join(['?' for _ in kwargs])
    conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({vals})", list(kwargs.values()))
    conn.commit()
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return rid

def db_get_all(table, where=None, order='created_at DESC', limit=200):
    conn = get_db()
    q = f"SELECT * FROM {table}"
    params = []
    if where:
        conditions = ' AND '.join([f"{k}=?" for k in where.keys()])
        q += f" WHERE {conditions}"
        params = list(where.values())
    q += f" ORDER BY {order} LIMIT {limit}"
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def db_get_by_id(table, rid):
    conn = get_db()
    row = conn.execute(f"SELECT * FROM {table} WHERE id=?", (rid,)).fetchone()
    conn.close()
    return dict(row) if row else None

def db_update(table, rid, **kwargs):
    conn = get_db()
    sets = ', '.join([f"{k}=?" for k in kwargs.keys()])
    conn.execute(f"UPDATE {table} SET {sets} WHERE id=?", list(kwargs.values()) + [rid])
    conn.commit()
    conn.close()

def db_delete(table, rid):
    conn = get_db()
    conn.execute(f"DELETE FROM {table} WHERE id=?", (rid,))
    conn.commit()
    conn.close()

def db_count(table, where=None):
    conn = get_db()
    q = f"SELECT COUNT(*) FROM {table}"
    params = []
    if where:
        conditions = ' AND '.join([f"{k}=?" for k in where.keys()])
        q += f" WHERE {conditions}"
        params = list(where.values())
    count = conn.execute(q, params).fetchone()[0]
    conn.close()
    return count

def db_sum(table, col, where=None):
    conn = get_db()
    q = f"SELECT COALESCE(SUM({col}),0) FROM {table}"
    params = []
    if where:
        conditions = ' AND '.join([f"{k}=?" for k in where.keys()])
        q += f" WHERE {conditions}"
        params = list(where.values())
    total = conn.execute(q, params).fetchone()[0]
    conn.close()
    return total


def init_mg_tables():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS mg_vehicules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            immatriculation TEXT, marque TEXT, modele TEXT,
            affectation TEXT, km INTEGER DEFAULT 0,
            assurance_exp TEXT, visite_exp TEXT,
            status TEXT DEFAULT 'disponible',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS mg_fournitures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, category TEXT,
            quantity INTEGER DEFAULT 0, unit TEXT,
            min_stock INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS mg_maintenance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            equipment TEXT NOT NULL, description TEXT,
            priority TEXT DEFAULT 'normale', status TEXT DEFAULT 'en_attente',
            requested_by INTEGER, date_requested TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    conn.close()


# ======================== CHAT / MESSAGING ========================

def init_chat_tables():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER NOT NULL,
            receiver_id INTEGER,
            channel TEXT DEFAULT 'general',
            content TEXT NOT NULL,
            read INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sender_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS rh_job_descriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, department TEXT,
            description TEXT, requirements TEXT, responsibilities TEXT,
            salary_range TEXT, status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS rh_trainings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, description TEXT,
            trainer TEXT, date TEXT, duration TEXT,
            employees_json TEXT, status TEXT DEFAULT 'planifie',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS rh_announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, content TEXT,
            priority TEXT DEFAULT 'normale',
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
    ''')
    conn.commit(); conn.close()

def get_messages(channel='general', limit=50):
    conn = get_db()
    msgs = conn.execute("""SELECT m.*, u.full_name as sender_name FROM messages m
        LEFT JOIN users u ON m.sender_id=u.id WHERE m.channel=? ORDER BY m.created_at DESC LIMIT ?""",
        (channel, limit)).fetchall()
    conn.close()
    return [dict(m) for m in reversed(msgs)]

def get_direct_messages(user1, user2, limit=50):
    conn = get_db()
    msgs = conn.execute("""SELECT m.*, u.full_name as sender_name FROM messages m
        LEFT JOIN users u ON m.sender_id=u.id
        WHERE (m.sender_id=? AND m.receiver_id=?) OR (m.sender_id=? AND m.receiver_id=?)
        ORDER BY m.created_at DESC LIMIT ?""", (user1, user2, user2, user1, limit)).fetchall()
    conn.close()
    return [dict(m) for m in reversed(msgs)]

def send_message(sender_id, content, channel='general', receiver_id=None):
    conn = get_db()
    conn.execute("INSERT INTO messages (sender_id, receiver_id, channel, content) VALUES (?,?,?,?)",
                 (sender_id, receiver_id, channel, content))
    conn.commit(); conn.close()

def get_unread_count(user_id):
    """Compte les messages non lus : DMs + canaux depuis la dernière lecture."""
    conn = get_db()
    # Ensure chat_last_read table exists
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS chat_last_read (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, channel TEXT,
            last_read_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, channel))""")
        conn.commit()
    except: pass
    
    total = 0
    # Unread DMs (messages sent TO this user, not by this user, after last read)
    last_dm = conn.execute("SELECT last_read_at FROM chat_last_read WHERE user_id=? AND channel='_dm_all'",
        (user_id,)).fetchone()
    if last_dm:
        total += conn.execute("""SELECT COUNT(*) FROM messages WHERE receiver_id=? AND sender_id!=? 
            AND created_at>?""", (user_id, user_id, last_dm['last_read_at'])).fetchone()[0]
    else:
        total += conn.execute("SELECT COUNT(*) FROM messages WHERE receiver_id=? AND sender_id!=?",
            (user_id, user_id)).fetchone()[0]
    
    # Unread channel messages (not sent by this user, after last read)
    for ch in ['general', 'technique', 'commercial']:
        last_ch = conn.execute("SELECT last_read_at FROM chat_last_read WHERE user_id=? AND channel=?",
            (user_id, ch)).fetchone()
        if last_ch:
            total += conn.execute("""SELECT COUNT(*) FROM messages WHERE channel=? AND sender_id!=? 
                AND receiver_id IS NULL AND created_at>?""",
                (ch, user_id, last_ch['last_read_at'])).fetchone()[0]
        else:
            total += conn.execute("""SELECT COUNT(*) FROM messages WHERE channel=? AND sender_id!=? 
                AND receiver_id IS NULL""", (ch, user_id)).fetchone()[0]
    
    conn.close()
    return total

def mark_chat_read(user_id, channel):
    """Marque un canal ou les DMs comme lus pour cet utilisateur."""
    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS chat_last_read (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, channel TEXT,
            last_read_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, channel))""")
        conn.commit()
    except: pass
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        conn.execute("INSERT OR REPLACE INTO chat_last_read (user_id, channel, last_read_at) VALUES (?, ?, ?)",
            (user_id, channel, now))
        conn.commit()
    except: pass
    conn.close()


# ======================== MIGRATIONS V4 ========================

def migrate_v4():
    conn = get_db()
    # Employee photo + files
    for col in ['photo', 'files', 'code_rh', 'birth_date', 'gender', 'blood_type']:
        try: conn.execute(f"ALTER TABLE employees ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    # Payslip status actions
    try: conn.execute("ALTER TABLE payslips ADD COLUMN sent_at TEXT")
    except: pass
    # RH Contracts
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS rh_contracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT, employee_id INTEGER, contract_type TEXT DEFAULT 'CDI',
            start_date TEXT, end_date TEXT, status TEXT DEFAULT 'actif',
            salary REAL DEFAULT 0, notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES employees(id)
        );
        CREATE TABLE IF NOT EXISTS tech_center (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER, client_name TEXT, system_type TEXT,
            installation_date TEXT, next_maintenance TEXT,
            maintenance_interval INTEGER DEFAULT 90,
            last_maintenance TEXT, status TEXT DEFAULT 'actif',
            notes TEXT, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
    ''')
    # Trainings enriched
    for col in ['department', 'cost', 'files']:
        try: conn.execute(f"ALTER TABLE rh_trainings ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()


def get_payslip_detail(pid):
    conn = get_db()
    p = conn.execute("""SELECT p.*, e.first_name||' '||e.last_name as employee_name,
        e.matricule, e.position, e.department, e.insurance, e.insurance_number
        FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id WHERE p.id=?""", (pid,)).fetchone()
    conn.close()
    return dict(p) if p else None


def get_maintenance_due():
    """Retourne les systèmes dont la maintenance est due."""
    conn = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    rows = conn.execute("""SELECT * FROM tech_center WHERE status='actif'
        AND (next_maintenance <= ? OR next_maintenance IS NULL) ORDER BY next_maintenance ASC""", (today,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ======================== MIGRATIONS V5 ========================

def migrate_v5():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS audit_trail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, user_name TEXT,
            table_name TEXT, record_id INTEGER,
            action TEXT, field_name TEXT,
            old_value TEXT, new_value TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS devis_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, category TEXT,
            description TEXT, items_json TEXT,
            notes TEXT, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
    ''')
    # Task kanban columns
    for col in ['kanban_order', 'color']:
        try: conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()


def log_audit(user_id, user_name, table_name, record_id, action, field_name='', old_value='', new_value=''):
    conn = get_db()
    conn.execute("""INSERT INTO audit_trail (user_id, user_name, table_name, record_id, action, field_name, old_value, new_value)
        VALUES (?,?,?,?,?,?,?,?)""", (user_id, user_name, table_name, record_id, action, field_name, str(old_value)[:500], str(new_value)[:500]))
    conn.commit(); conn.close()


def get_audit_trail(table_name=None, record_id=None, limit=50):
    conn = get_db()
    if table_name and record_id:
        rows = conn.execute("SELECT * FROM audit_trail WHERE table_name=? AND record_id=? ORDER BY created_at DESC LIMIT ?",
            (table_name, record_id, limit)).fetchall()
    elif table_name:
        rows = conn.execute("SELECT * FROM audit_trail WHERE table_name=? ORDER BY created_at DESC LIMIT ?",
            (table_name, limit)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM audit_trail ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_executive_stats():
    """Statistiques pour le tableau de bord exécutif."""
    conn = get_db()
    s = {}
    # Factures
    s['factures_total'] = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    s['factures_payees'] = conn.execute("SELECT COUNT(*) FROM invoices WHERE status='payee'").fetchone()[0]
    s['montant_facture'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices").fetchone()[0]
    s['montant_paye'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM invoices WHERE status='payee'").fetchone()[0]
    s['montant_impaye'] = s['montant_facture'] - s['montant_paye']
    # Devis
    s['devis_total'] = conn.execute("SELECT COUNT(*) FROM devis").fetchone()[0]
    s['devis_acceptes'] = conn.execute("SELECT COUNT(*) FROM devis WHERE status='accepte'").fetchone()[0]
    s['ca_devis'] = conn.execute("SELECT COALESCE(SUM(total_ttc),0) FROM devis WHERE status='accepte'").fetchone()[0]
    # Clients
    s['clients'] = conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0]
    # Employés
    try: s['employes'] = conn.execute("SELECT COUNT(*) FROM employees WHERE status='actif'").fetchone()[0]
    except: s['employes'] = 0
    # Prospects
    s['prospects'] = conn.execute("SELECT COUNT(*) FROM prospects").fetchone()[0]
    s['prospects_gagnes'] = conn.execute("SELECT COUNT(*) FROM prospects WHERE status='gagne'").fetchone()[0]
    # Jobs
    s['rapports'] = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    # Trésorerie
    try:
        s['recettes'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM treasury WHERE type='recette'").fetchone()[0]
        s['depenses'] = conn.execute("SELECT COALESCE(SUM(amount),0) FROM treasury WHERE type='depense'").fetchone()[0]
    except:
        s['recettes'] = 0; s['depenses'] = 0
    s['solde'] = s['recettes'] - s['depenses']
    # RH extended
    try: s['masse_salariale'] = conn.execute("SELECT COALESCE(SUM(salary),0) FROM employees WHERE status='actif'").fetchone()[0]
    except: s['masse_salariale'] = 0
    try: s['conges_pending'] = conn.execute("SELECT COUNT(*) FROM leaves WHERE status='en_attente'").fetchone()[0]
    except: s['conges_pending'] = 0
    try: s['formations'] = conn.execute("SELECT COUNT(*) FROM rh_trainings WHERE status='planifie'").fetchone()[0]
    except: s['formations'] = 0
    conn.close()
    return s


def get_devis_templates():
    conn = get_db()
    rows = conn.execute("SELECT * FROM devis_templates ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_devis_template(tid):
    conn = get_db()
    t = conn.execute("SELECT * FROM devis_templates WHERE id=?", (tid,)).fetchone()
    conn.close()
    return dict(t) if t else None


# ======================== PAYSLIP V2 (CI FORMAT) ========================

def migrate_payslip_v2():
    conn = get_db()
    new_cols = [
        ('prime_transport', 'REAL DEFAULT 0'),
        ('prime_anciennete', 'REAL DEFAULT 0'),
        ('prime_logement', 'REAL DEFAULT 0'),
        ('prime_rendement', 'REAL DEFAULT 0'),
        ('avantages_nature', 'REAL DEFAULT 0'),
        ('cnps_employee', 'REAL DEFAULT 0'),
        ('its', 'REAL DEFAULT 0'),
        ('autres_retenues', 'REAL DEFAULT 0'),
        ('avances', 'REAL DEFAULT 0'),
        ('jours_travailles', 'INTEGER DEFAULT 26'),
        ('heures_travaillees', 'REAL DEFAULT 0'),
        ('conges_payes', 'INTEGER DEFAULT 0'),
        ('jours_absence', 'INTEGER DEFAULT 0'),
        ('cumul_annuel', 'REAL DEFAULT 0'),
        ('mode_paiement', "TEXT DEFAULT 'virement'"),
        ('cnps_employer', 'REAL DEFAULT 0'),
    ]
    for col, typ in new_cols:
        try: conn.execute(f"ALTER TABLE payslips ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def get_payslip_detail_v2(pid):
    conn = get_db()
    p = conn.execute("""SELECT p.*, e.first_name, e.last_name, e.matricule, e.position, 
        e.department, e.insurance, e.insurance_number, e.hire_date, e.email, e.tel,
        e.code_rh, e.gender, e.cnps_number, e.cnps_declared
        FROM payslips p LEFT JOIN employees e ON p.employee_id=e.id WHERE p.id=?""", (pid,)).fetchone()
    conn.close()
    if not p: return None
    d = dict(p)
    d['employee_name'] = f"{d.get('first_name','')} {d.get('last_name','')}".strip()
    # Calculate totals
    d['total_primes'] = (d.get('bonus',0) or 0) + (d.get('prime_transport',0) or 0) + (d.get('prime_anciennete',0) or 0) + (d.get('prime_logement',0) or 0) + (d.get('prime_rendement',0) or 0) + (d.get('avantages_nature',0) or 0)
    d['salaire_brut'] = (d.get('base_salary',0) or 0) + (d.get('overtime_amount',0) or 0) + d['total_primes']
    d['total_retenues'] = (d.get('cnps_employee',0) or 0) + (d.get('insurance_amount',0) or 0) + (d.get('its',0) or 0) + (d.get('deductions',0) or 0) + (d.get('autres_retenues',0) or 0) + (d.get('avances',0) or 0)
    return d


# ======================== PIÈCE DE CAISSE SORTIE ========================

def migrate_caisse():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS caisse_sorties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE,
            date TEXT,
            beneficiaire TEXT NOT NULL,
            type_beneficiaire TEXT DEFAULT 'particulier',
            montant REAL NOT NULL,
            nature TEXT DEFAULT 'espece',
            motif TEXT,
            status TEXT DEFAULT 'en_attente',
            demandeur_id INTEGER,
            demandeur_name TEXT,
            valideur_id INTEGER,
            valideur_name TEXT,
            validated_at TEXT,
            comptabilise INTEGER DEFAULT 0,
            comptabilise_at TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (demandeur_id) REFERENCES users(id)
        );
    ''')
    conn.commit(); conn.close()

def gen_caisse_ref():
    """Génère une référence unique : S + AAAAMM + numéro séquentiel."""
    conn = get_db()
    now = datetime.now()
    prefix = f"S{now.strftime('%Y%m')}"
    last = conn.execute("SELECT reference FROM caisse_sorties WHERE reference LIKE ? ORDER BY id DESC LIMIT 1",
                        (f"{prefix}%",)).fetchone()
    if last:
        num = int(last['reference'][-4:]) + 1
    else:
        num = 1
    conn.close()
    return f"{prefix}{num:04d}"

def get_caisse_sorties(status=None, month=None):
    conn = get_db()
    q = "SELECT * FROM caisse_sorties WHERE 1=1"
    params = []
    if status:
        q += " AND status=?"; params.append(status)
    if month:
        q += " AND strftime('%Y-%m', date)=?"; params.append(month)
    q += " ORDER BY created_at DESC"
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_caisse_stats(month=None):
    conn = get_db()
    s = {}
    where = ""
    params = []
    if month:
        where = " AND strftime('%Y-%m', date)=?"; params = [month]
    s['total'] = conn.execute(f"SELECT COUNT(*) FROM caisse_sorties WHERE 1=1{where}", params).fetchone()[0]
    s['en_attente'] = conn.execute(f"SELECT COUNT(*) FROM caisse_sorties WHERE status='en_attente'{where}", params).fetchone()[0]
    s['valide'] = conn.execute(f"SELECT COUNT(*) FROM caisse_sorties WHERE status='valide'{where}", params).fetchone()[0]
    s['refuse'] = conn.execute(f"SELECT COUNT(*) FROM caisse_sorties WHERE status='refuse'{where}", params).fetchone()[0]
    s['montant_total'] = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM caisse_sorties WHERE status='valide'{where}", params).fetchone()[0]
    s['montant_espece'] = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM caisse_sorties WHERE status='valide' AND nature='espece'{where}", params).fetchone()[0]
    s['montant_cheque'] = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM caisse_sorties WHERE status='valide' AND nature='cheque'{where}", params).fetchone()[0]
    s['montant_virement'] = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM caisse_sorties WHERE status='valide' AND nature='virement'{where}", params).fetchone()[0]
    conn.close()
    return s


# ======================== CAISSE SIGNATURES ========================

def migrate_caisse_v2():
    conn = get_db()
    for col in ['sig_beneficiaire', 'sig_caisse', 'sig_autorisation']:
        try: conn.execute(f"ALTER TABLE caisse_sorties ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()

def delete_caisse(sid):
    conn = get_db()
    conn.execute("DELETE FROM caisse_sorties WHERE id=?", (sid,))
    conn.commit(); conn.close()


# ======================== MIGRATION V6 — RAPPORTS JOURNALIERS + CLIENTS ENRICHIS + COMPTA ========================

def migrate_v6():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS rapports_journaliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, date TEXT,
            tasks_done TEXT, tasks_planned TEXT,
            issues TEXT, achievements TEXT,
            completion_pct INTEGER DEFAULT 0,
            department TEXT, status TEXT DEFAULT 'soumis',
            validated_by INTEGER, comments TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS pieces_caisse (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT, date TEXT,
            description TEXT, amount REAL DEFAULT 0,
            category TEXT DEFAULT 'divers',
            supplier TEXT, file_path TEXT,
            comptabilise INTEGER DEFAULT 0,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            caller_id INTEGER, callee_id INTEGER,
            room TEXT, call_type TEXT DEFAULT 'audio',
            status TEXT DEFAULT 'ringing',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            ended_at TEXT
        );
    ''')
    # Enrich clients table
    new_cols = [
        ('sector', 'TEXT'), ('city', 'TEXT'), ('country', 'TEXT DEFAULT \'Côte d\\\'Ivoire\''),
        ('website', 'TEXT'), ('rc_number', 'TEXT'), ('cnps_number', 'TEXT'),
        ('contact_title', 'TEXT'), ('contact_tel2', 'TEXT'), ('contact_email2', 'TEXT'),
        ('payment_terms', 'TEXT'), ('credit_limit', 'REAL DEFAULT 0'),
        ('source', 'TEXT'), ('status', 'TEXT DEFAULT \'actif\''),
        ('annual_revenue', 'REAL DEFAULT 0'),
    ]
    for col, typ in new_cols:
        try: conn.execute(f"ALTER TABLE clients ADD COLUMN {col} {typ}")
        except: pass
    # Formation: add target_department
    try: conn.execute("ALTER TABLE rh_trainings ADD COLUMN target TEXT DEFAULT 'tous'")
    except: pass
    # Prospects: add more fields for better conversion
    for col in ['address', 'city', 'sector', 'contact_tel2']:
        try: conn.execute(f"ALTER TABLE prospects ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()


# ======================== MIGRATION V7 — ACHATS MODULE + STOCK IMAGE ========================

def migrate_v7():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS achats_fournisseurs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, contact_name TEXT, tel TEXT, email TEXT,
            address TEXT, city TEXT, sector TEXT, website TEXT,
            payment_terms TEXT, notes TEXT, status TEXT DEFAULT 'actif',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS achats_demandes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE, date TEXT, department TEXT,
            requested_by INTEGER, description TEXT,
            urgency TEXT DEFAULT 'normale', status TEXT DEFAULT 'en_attente',
            approved_by INTEGER, approved_at TEXT,
            notes TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS achats_demande_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            demande_id INTEGER, designation TEXT, quantity INTEGER DEFAULT 1,
            estimated_price REAL DEFAULT 0, notes TEXT,
            FOREIGN KEY (demande_id) REFERENCES achats_demandes(id)
        );
        CREATE TABLE IF NOT EXISTS achats_devis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE, fournisseur_id INTEGER,
            demande_id INTEGER, date TEXT,
            items_json TEXT, total_ht REAL DEFAULT 0, tva REAL DEFAULT 0,
            total_ttc REAL DEFAULT 0, status TEXT DEFAULT 'en_attente',
            notes TEXT, file_path TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS achats_commandes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE, fournisseur_id INTEGER,
            devis_achat_id INTEGER, date TEXT,
            items_json TEXT, total REAL DEFAULT 0,
            status TEXT DEFAULT 'en_cours', delivery_date TEXT,
            notes TEXT, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS achats_contrats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT, fournisseur_id INTEGER,
            title TEXT, description TEXT,
            start_date TEXT, end_date TEXT,
            amount REAL DEFAULT 0, status TEXT DEFAULT 'actif',
            file_path TEXT, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    # Stock image
    try: conn.execute("ALTER TABLE stock_items ADD COLUMN image TEXT DEFAULT ''")
    except: pass
    conn.commit(); conn.close()


# ======================== MIGRATION V8 — EMPLOI DU TEMPS ========================

def migrate_v8():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_name TEXT NOT NULL,
            day_of_week INTEGER,
            start_time TEXT DEFAULT '08:00',
            end_time TEXT DEFAULT '17:00',
            break_start TEXT DEFAULT '12:00',
            break_end TEXT DEFAULT '13:00',
            schedule_type TEXT DEFAULT 'standard',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS presence_anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            merge_id INTEGER,
            employee_name TEXT,
            date TEXT,
            expected_start TEXT,
            expected_end TEXT,
            actual_start TEXT,
            actual_end TEXT,
            anomaly_type TEXT,
            status TEXT DEFAULT 'detectee',
            corrected_start TEXT,
            corrected_end TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()


def save_known_employees(names, services=None):
    """Sauvegarde les noms d'employés des fichiers de présence avec leur service."""
    if services is None: services = {}
    conn = get_db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS known_employees (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, service TEXT DEFAULT '', source TEXT DEFAULT 'pointeuse', created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
        conn.commit()
    except: pass
    try: conn.execute("ALTER TABLE known_employees ADD COLUMN service TEXT DEFAULT ''")
    except: pass
    for name in names:
        name = name.strip()
        if name:
            svc = services.get(name, '')
            try: conn.execute("INSERT OR REPLACE INTO known_employees (name, service) VALUES (?, ?)", (name, svc))
            except: pass
    conn.commit(); conn.close()

def get_known_employees():
    """Retourne tous les noms d'employés connus avec leur service."""
    conn = get_db()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS known_employees (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, service TEXT DEFAULT '', source TEXT DEFAULT 'pointeuse', created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
        conn.commit()
    except: pass
    rows = conn.execute("SELECT DISTINCT name, service FROM known_employees ORDER BY service, name").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def migrate_v9():
    conn = get_db()
    # Tech center extra fields
    for col in ['code', 'contact_name', 'tel', 'email', 'address', 'category', 'description']:
        try: conn.execute(f"ALTER TABLE tech_center ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    # Prospects extra fields
    for col in ['position', 'address', 'city', 'region', 'country', 'tags', 'lead_value', 'assigned_to', 'description', 'last_contact']:
        try: conn.execute(f"ALTER TABLE prospects ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()

def migrate_v10():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS bank_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, type TEXT DEFAULT 'caisse',
            bank_name TEXT, account_number TEXT,
            initial_balance REAL DEFAULT 0,
            current_balance REAL DEFAULT 0,
            status TEXT DEFAULT 'actif',
            notes TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS bank_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_account_id INTEGER, to_account_id INTEGER,
            amount REAL NOT NULL, description TEXT,
            reference TEXT, date TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    # Add account_id to treasury
    try: conn.execute("ALTER TABLE treasury ADD COLUMN account_id INTEGER DEFAULT 0")
    except: pass
    conn.commit(); conn.close()

def migrate_v11():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS prospect_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prospect_id INTEGER, content TEXT, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS prospect_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prospect_id INTEGER, title TEXT, status TEXT DEFAULT 'a_faire',
            priority TEXT DEFAULT 'normale', due_date TEXT, assigned_to TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS prospect_offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prospect_id INTEGER, title TEXT, amount REAL DEFAULT 0,
            status TEXT DEFAULT 'brouillon', description TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS prospect_reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prospect_id INTEGER, title TEXT, reminder_date TEXT,
            status TEXT DEFAULT 'actif', notes TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS prospect_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prospect_id INTEGER, filename TEXT, original_name TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v12():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS weekly_champion (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, full_name TEXT, role TEXT, department TEXT,
            week_start TEXT, week_end TEXT,
            nb_rapports INTEGER, avg_completion REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def get_current_champion():
    """Retourne le champion en cours (le plus récent)."""
    conn = get_db()
    row = conn.execute("SELECT * FROM weekly_champion ORDER BY week_end DESC, nb_rapports DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None

def update_weekly_champion():
    """Calcule et enregistre le champion de la semaine écoulée."""
    from datetime import datetime, timedelta
    conn = get_db()
    today = datetime.now().date()
    # Previous completed week (Mon-Sun)
    last_sunday = today - timedelta(days=today.weekday() + 1)
    last_monday = last_sunday - timedelta(days=6)
    ws = last_monday.strftime('%Y-%m-%d')
    we = last_sunday.strftime('%Y-%m-%d')
    
    # Check if already computed for this week
    existing = conn.execute("SELECT id FROM weekly_champion WHERE week_start=?", (ws,)).fetchone()
    if existing:
        conn.close(); return
    
    # Find top performer
    row = conn.execute("""
        SELECT rj.user_id, u.full_name, u.role, COUNT(DISTINCT rj.date) as nb,
               AVG(rj.completion_pct) as avg_c, rj.department
        FROM rapports_journaliers rj
        LEFT JOIN users u ON rj.user_id=u.id
        WHERE rj.date >= ? AND rj.date <= ?
        GROUP BY rj.user_id
        ORDER BY nb DESC, avg_c DESC
        LIMIT 1
    """, (ws, we)).fetchone()
    
    if row and row['nb'] > 0:
        conn.execute("""INSERT INTO weekly_champion 
            (user_id, full_name, role, department, week_start, week_end, nb_rapports, avg_completion)
            VALUES (?,?,?,?,?,?,?,?)""",
            (row['user_id'], row['full_name'], row['role'], row['department'] or '',
             ws, we, row['nb'], round(row['avg_c'] or 0)))
        conn.commit()
    conn.close()

def get_live_champion():
    """Retourne le leader actuel de la semaine en cours (mis à jour en temps réel)."""
    from datetime import datetime, timedelta
    conn = get_db()
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    ws = week_start.strftime('%Y-%m-%d')
    we = today.strftime('%Y-%m-%d')
    
    row = conn.execute("""
        SELECT rj.user_id, u.full_name, u.role, COUNT(DISTINCT rj.date) as nb,
               AVG(rj.completion_pct) as avg_c, rj.department
        FROM rapports_journaliers rj
        LEFT JOIN users u ON rj.user_id=u.id
        WHERE rj.date >= ? AND rj.date <= ?
        GROUP BY rj.user_id
        ORDER BY nb DESC, avg_c DESC
        LIMIT 1
    """, (ws, we)).fetchone()
    conn.close()
    
    if row and row['nb'] > 0:
        return {
            'user_id': row['user_id'], 'full_name': row['full_name'],
            'role': row['role'], 'department': row['department'] or '',
            'week_start': ws, 'week_end': we,
            'nb_rapports': row['nb'], 'avg_completion': round(row['avg_c'] or 0),
            'is_live': True
        }
    return None

def migrate_v13():
    conn = get_db()
    # Add extra fields to invoices
    for col, default in [('objet',''), ('items_json',''), ('total_ht','0'), ('tva','0'), ('total_ttc','0'),
                         ('devis_id','0'), ('due_date',''), ('payment_method',''), ('description','')]:
        try: conn.execute(f"ALTER TABLE invoices ADD COLUMN {col} TEXT DEFAULT '{default}'")
        except: pass
    # Weekly cash report table
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS weekly_cash_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_name TEXT, matricule TEXT, report_number TEXT,
            week_start TEXT, week_end TEXT, items_json TEXT,
            total_credit REAL DEFAULT 0, total_debit REAL DEFAULT 0,
            reste_caisse REAL DEFAULT 0,
            deposit_date TEXT, status TEXT DEFAULT 'brouillon',
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v14():
    conn = get_db()
    try: conn.execute("ALTER TABLE bank_accounts ADD COLUMN subtype TEXT DEFAULT 'courant'")
    except: pass
    # Entries table for caisse
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS caisse_entrees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT, date TEXT, source TEXT, montant REAL,
            description TEXT, payment_method TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v15():
    conn = get_db()
    # Add fields to projects
    for col in ['progress', 'budget_consumed', 'objectives']:
        try: conn.execute(f"ALTER TABLE projects ADD COLUMN {col} TEXT DEFAULT '0'")
        except: pass
    # Task comments
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS task_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER, user_id INTEGER, content TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v15():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS plan_comptable (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero TEXT UNIQUE NOT NULL,
            libelle TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'actif',
            categorie TEXT DEFAULT '',
            classe TEXT DEFAULT '',
            parent_id INTEGER,
            solde_debit REAL DEFAULT 0,
            solde_credit REAL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS ecritures_comptables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            journal TEXT DEFAULT 'OD',
            piece TEXT,
            compte_debit TEXT,
            compte_credit TEXT,
            libelle TEXT,
            montant REAL DEFAULT 0,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS bilans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exercice TEXT,
            date_cloture TEXT,
            total_actif REAL DEFAULT 0,
            total_passif REAL DEFAULT 0,
            resultat REAL DEFAULT 0,
            data_json TEXT,
            status TEXT DEFAULT 'brouillon',
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    
    # Insert default SYSCOHADA plan comptable if empty
    cnt = conn.execute("SELECT COUNT(*) FROM plan_comptable").fetchone()[0]
    if cnt == 0:
        comptes = [
            ('101','Capital social','passif','capitaux','1'),
            ('106','Réserves','passif','capitaux','1'),
            ('12','Résultat de l exercice','passif','capitaux','1'),
            ('131','Résultat net: bénéfice','passif','capitaux','1'),
            ('162','Emprunts et dettes','passif','dettes_financieres','1'),
            ('21','Immobilisations corporelles','actif','immobilise','2'),
            ('22','Terrains','actif','immobilise','2'),
            ('23','Bâtiments','actif','immobilise','2'),
            ('24','Matériel et outillage','actif','immobilise','2'),
            ('245','Matériel de transport','actif','immobilise','2'),
            ('25','Avances et acomptes versés','actif','immobilise','2'),
            ('27','Autres immobilisations financières','actif','immobilise','2'),
            ('28','Amortissements','actif','immobilise','2'),
            ('31','Marchandises','actif','circulant','3'),
            ('32','Matières premières','actif','circulant','3'),
            ('33','Autres approvisionnements','actif','circulant','3'),
            ('36','Produits finis','actif','circulant','3'),
            ('401','Fournisseurs','passif','dettes_circulant','4'),
            ('411','Clients','actif','circulant','4'),
            ('421','Personnel rémunérations dues','passif','dettes_circulant','4'),
            ('431','Sécurité sociale','passif','dettes_circulant','4'),
            ('441','État impôts sur les bénéfices','passif','dettes_circulant','4'),
            ('445','État TVA','passif','dettes_circulant','4'),
            ('471','Comptes d attente','actif','circulant','4'),
            ('512','Banque','actif','tresorerie','5'),
            ('517','Caisse','actif','tresorerie','5'),
            ('52','Banques comptes courants','actif','tresorerie','5'),
            ('531','Caisse en monnaie nationale','actif','tresorerie','5'),
            ('60','Achats','passif','charges','6'),
            ('61','Transports','passif','charges','6'),
            ('62','Services extérieurs','passif','charges','6'),
            ('63','Autres services extérieurs','passif','charges','6'),
            ('64','Impôts et taxes','passif','charges','6'),
            ('65','Autres charges','passif','charges','6'),
            ('66','Charges de personnel','passif','charges','6'),
            ('67','Frais financiers','passif','charges','6'),
            ('68','Dotations aux amortissements','passif','charges','6'),
            ('70','Ventes de marchandises','actif','produits','7'),
            ('71','Production vendue services','actif','produits','7'),
            ('72','Production stockée','actif','produits','7'),
            ('75','Autres produits','actif','produits','7'),
            ('77','Revenus financiers','actif','produits','7'),
            ('78','Reprises amortissements','actif','produits','7'),
        ]
        for num, lib, typ, cat, cls in comptes:
            try:
                conn.execute("INSERT OR IGNORE INTO plan_comptable (numero, libelle, type, categorie, classe) VALUES (?,?,?,?,?)",
                    (num, lib, typ, cat, cls))
            except: pass
    
    conn.commit(); conn.close()

def migrate_v16():
    conn = get_db()
    for col in ['objectives', 'client', 'budget_consumed']:
        try: conn.execute(f"ALTER TABLE projects ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    # Add deadline alerts column
    try: conn.execute("ALTER TABLE tasks ADD COLUMN reminder_date TEXT DEFAULT ''")
    except: pass
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS task_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER, user_id INTEGER, content TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        );
    ''')
    conn.commit(); conn.close()

def migrate_v17():
    conn = get_db()
    for col in ['last_contact', 'country']:
        try: conn.execute(f"ALTER TABLE prospects ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()

def migrate_v18():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS it_equipment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, type TEXT, brand TEXT, model TEXT,
            serial_number TEXT, assigned_to INTEGER, location TEXT,
            status TEXT DEFAULT 'actif', purchase_date TEXT,
            purchase_price REAL DEFAULT 0, warranty_end TEXT,
            notes TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS it_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, description TEXT,
            category TEXT DEFAULT 'incident',
            priority TEXT DEFAULT 'normal',
            status TEXT DEFAULT 'ouvert',
            requester_id INTEGER, assigned_to INTEGER,
            equipment_id INTEGER,
            resolution TEXT, resolved_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS it_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT, description TEXT,
            user_id INTEGER, ip_address TEXT,
            severity TEXT DEFAULT 'info',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v19():
    conn = get_db()
    new_cols = [
        'birth_place', 'birth_city', 'civil_status', 'nationality', 'religion',
        'id_type', 'id_expiry', 'id_place', 'resident', 'address', 'education_level',
        'work_location', 'bank_account', 'bank_name_emp', 'bank_holder',
        'fiscal_code', 'hourly_rate', 'facebook', 'linkedin', 'skype',
        'direction', 'email_signature', 'other_info', 'is_admin',
        'code_rh', 'birth_date', 'gender', 'blood_type',
        'emergency_contact', 'emergency_tel', 'photo'
    ]
    for col in new_cols:
        try: conn.execute(f"ALTER TABLE employees ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()

def migrate_v20():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS tracking_vehicles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            immatriculation TEXT NOT NULL,
            marque TEXT, modele TEXT, type TEXT DEFAULT 'voiture',
            couleur TEXT, annee TEXT,
            proprietaire TEXT, tel_proprietaire TEXT,
            gps_device_id TEXT, gps_brand TEXT DEFAULT 'Concox',
            gps_model TEXT, gps_sim TEXT, gps_imei TEXT,
            installation_date TEXT, installation_tech TEXT,
            status TEXT DEFAULT 'actif',
            last_lat REAL, last_lng REAL, last_speed REAL DEFAULT 0,
            last_address TEXT, last_update TEXT,
            notes TEXT, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS tracking_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id INTEGER, lat REAL, lng REAL,
            speed REAL DEFAULT 0, heading REAL DEFAULT 0,
            address TEXT, event_type TEXT DEFAULT 'position',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (vehicle_id) REFERENCES tracking_vehicles(id)
        );
        CREATE TABLE IF NOT EXISTS tracking_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id INTEGER, alert_type TEXT,
            message TEXT, lat REAL, lng REAL,
            acknowledged INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (vehicle_id) REFERENCES tracking_vehicles(id)
        );
        CREATE TABLE IF NOT EXISTS tracking_geofences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, lat REAL, lng REAL, radius REAL DEFAULT 500,
            vehicle_id INTEGER, active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v21():
    conn = get_db()
    # Enhanced messages
    for col, default in [('message_type', 'text'), ('file_url', ''), ('file_name', ''),
                         ('status', 'sent'), ('reply_to', ''), ('edited', '0'),
                         ('is_system', '0'), ('reactions', '')]:
        try: conn.execute(f"ALTER TABLE messages ADD COLUMN {col} TEXT DEFAULT '{default}'")
        except: pass
    
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS chat_channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, description TEXT,
            type TEXT DEFAULT 'group',
            created_by INTEGER, avatar TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS chat_channel_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER, user_id INTEGER,
            role TEXT DEFAULT 'member',
            joined_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(channel_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS chat_typing (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, channel TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS chat_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER, title TEXT,
            status TEXT DEFAULT 'ouvert',
            assigned_to INTEGER, priority TEXT DEFAULT 'normal',
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS chat_auto_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger_word TEXT, response TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    
    # Default channels
    try:
        conn.execute("INSERT OR IGNORE INTO chat_channels (id, name, description, type) VALUES (1, 'Général', 'Canal principal', 'group')")
        conn.execute("INSERT OR IGNORE INTO chat_channels (id, name, description, type) VALUES (2, 'Technique', 'Équipe technique', 'group')")
        conn.execute("INSERT OR IGNORE INTO chat_channels (id, name, description, type) VALUES (3, 'Commercial', 'Équipe commerciale', 'group')")
    except: pass
    
    conn.commit(); conn.close()

def migrate_v22():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS employee_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_type TEXT DEFAULT '',
            file_size INTEGER DEFAULT 0,
            category TEXT DEFAULT 'autre',
            uploaded_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES employees(id)
        );
    ''')
    conn.commit(); conn.close()

def migrate_v23():
    conn = get_db()
    for col in ['department', 'signature_date']:
        try: conn.execute(f"ALTER TABLE rh_contracts ADD COLUMN {col} TEXT DEFAULT ''")
        except: pass
    conn.commit(); conn.close()

def migrate_v24():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT DEFAULT '',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    try: conn.execute("INSERT OR IGNORE INTO app_settings (key, value) VALUES ('show_champion', '1')")
    except: pass
    conn.commit(); conn.close()

def migrate_v25():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS tender_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            source TEXT DEFAULT '',
            deadline TEXT DEFAULT '',
            category TEXT DEFAULT 'securite',
            active INTEGER DEFAULT 1,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v26():
    conn = get_db()
    # New payslip fields
    for col, typ in [('prime_mission','REAL DEFAULT 0'),('cnps_retraite_pat','REAL DEFAULT 0'),
                     ('cnps_prestation_pat','REAL DEFAULT 0'),('cnps_accident_pat','REAL DEFAULT 0'),
                     ('taxe_apprentissage','REAL DEFAULT 0'),('fdfp','REAL DEFAULT 0'),
                     ('cnps_retraite_sal','REAL DEFAULT 0')]:
        try: conn.execute(f"ALTER TABLE payslips ADD COLUMN {col} {typ}")
        except: pass
    # Absences table
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS absences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            type TEXT DEFAULT 'injustifiee',
            motif TEXT DEFAULT '',
            duree TEXT DEFAULT 'journee',
            justificatif INTEGER DEFAULT 0,
            notes TEXT DEFAULT '',
            status TEXT DEFAULT 'en_attente',
            approved_by INTEGER,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (employee_id) REFERENCES employees(id)
        );
    ''')
    for col, typ in [('status','TEXT DEFAULT "en_attente"'),('approved_by','INTEGER')]:
        try: conn.execute(f"ALTER TABLE absences ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def migrate_v27():
    conn = get_db()
    try: conn.execute("ALTER TABLE absences ADD COLUMN status TEXT DEFAULT 'en_attente'")
    except: pass
    try: conn.execute("ALTER TABLE absences ADD COLUMN approved_by INTEGER")
    except: pass
    conn.commit(); conn.close()

def migrate_v27():
    conn = get_db()
    for col, typ in [('status','TEXT DEFAULT "en_attente"'),('approved_by','INTEGER')]:
        try: conn.execute(f"ALTER TABLE absences ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def migrate_v28():
    conn = get_db()
    # Pre-populate tender links if empty
    cnt = conn.execute("SELECT COUNT(*) FROM tender_links").fetchone()[0]
    if cnt == 0:
        tenders = [
            ("SIGOMAP — Plateforme marchés publics CI", "https://sigomap.gouv.ci/", "SIGOMAP", "", "securite"),
            ("DGMP — Appels d'offres publics", "https://www.marchespublics.ci/appel_offre", "DGMP", "", "securite"),
            ("ARCOP — Régulateur marchés publics", "https://arcop.ci/", "ARCOP", "", "securite"),
            ("BCEAO — Marchés et achats", "https://www.bceao.int/fr/appels-offres/appels-offres-marches-publics-achats", "BCEAO", "", "securite"),
            ("J360 — Appels d'offres Côte d'Ivoire", "https://www.j360.info/appels-d-offres/afrique/cote-divoire/", "J360", "", "securite"),
            ("AppelOffres.net — Sécurité électronique CI", "https://www.appeloffres.net/?ve=0&pays=45", "appeloffres.net", "", "securite"),
        ]
        for t in tenders:
            conn.execute("INSERT INTO tender_links (title, url, source, deadline, category, active) VALUES (?,?,?,?,?,1)", t)
        conn.commit()
    conn.close()

def get_admin_smtp():
    conn = get_db()
    # Try admin user first (id=1), then any available
    s = conn.execute("SELECT * FROM smtp_settings WHERE user_id=1").fetchone()
    if not s:
        s = conn.execute("SELECT * FROM smtp_settings ORDER BY user_id LIMIT 1").fetchone()
    conn.close()
    return dict(s) if s else {'smtp_host': 'smtp.gmail.com', 'smtp_port': 587, 'smtp_user': '', 'smtp_pass': ''}

def migrate_v29():
    conn = get_db()
    for col, typ in [('prime_astreinte','REAL DEFAULT 0')]:
        try: conn.execute(f"ALTER TABLE payslips ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def migrate_v30():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            employee_id INTEGER,
            type TEXT DEFAULT 'info',
            title TEXT NOT NULL,
            message TEXT DEFAULT '',
            link TEXT DEFAULT '',
            read INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v31():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS stock_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    # Pre-populate from existing stock_items categories
    try:
        cats = conn.execute("SELECT DISTINCT category FROM stock_items WHERE category IS NOT NULL AND category != ''").fetchall()
        for cat in cats:
            try: conn.execute("INSERT OR IGNORE INTO stock_categories (name) VALUES (?)", (cat[0],))
            except: pass
        conn.commit()
    except: pass
    conn.close()

def migrate_v31():
    conn = get_db()
    for col, typ in [('cnps_number','TEXT DEFAULT ""'),('cnps_declared','INTEGER DEFAULT 0')]:
        try: conn.execute(f"ALTER TABLE employees ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def migrate_v32():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS stock_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    # Add category_id to stock_items if not exists
    try: conn.execute("ALTER TABLE stock_items ADD COLUMN category_id INTEGER DEFAULT 0")
    except: pass
    conn.commit(); conn.close()

def migrate_v33():
    """Set default permissions for concierge role."""
    conn = get_db()
    try:
        existing = conn.execute("SELECT COUNT(*) FROM role_permissions WHERE role='concierge'").fetchone()[0]
        if existing == 0:
            for perm in ['dashboard', 'concierge', 'rapports_j']:
                conn.execute("INSERT OR IGNORE INTO role_permissions (role, permission) VALUES (?,?)", ('concierge', perm))
            conn.commit()
    except: pass
    conn.close()

def migrate_v34():
    """Concierge tables."""
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS concierge_visiteurs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT, entreprise TEXT, motif TEXT,
            personne_visitee TEXT, date_visite TEXT DEFAULT CURRENT_TIMESTAMP,
            heure_arrivee TEXT, heure_depart TEXT, badge TEXT, notes TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS concierge_courrier (
            id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, expediteur TEXT, destinataire TEXT,
            objet TEXT, date TEXT, reference TEXT, statut TEXT DEFAULT 'recu',
            notes TEXT, created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS concierge_cles (
            id INTEGER PRIMARY KEY AUTOINCREMENT, nom_cle TEXT, emplacement TEXT,
            attribue_a TEXT, statut TEXT DEFAULT 'disponible', date_attribution TEXT,
            notes TEXT, created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS concierge_colis (
            id INTEGER PRIMARY KEY AUTOINCREMENT, expediteur TEXT, destinataire TEXT,
            description TEXT, date_reception TEXT, date_retrait TEXT, statut TEXT DEFAULT 'en_attente',
            notes TEXT, created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS concierge_salles (
            id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT, capacite INTEGER DEFAULT 10,
            equipements TEXT, statut TEXT DEFAULT 'disponible',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS concierge_reservations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, salle_id INTEGER, date TEXT,
            heure_debut TEXT, heure_fin TEXT, objet TEXT, reserve_par TEXT,
            statut TEXT DEFAULT 'confirmee', created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v35():
    """Add signature fields to payslips."""
    conn = get_db()
    for col, typ in [
        ('signed_at', 'TEXT DEFAULT ""'),
        ('signed_by', 'TEXT DEFAULT ""'),
        ('signature_data', 'TEXT DEFAULT ""'),
    ]:
        try: conn.execute(f"ALTER TABLE payslips ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def migrate_v36():
    """Client profile tables."""
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS client_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT, client_id INTEGER, content TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS client_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, client_id INTEGER, filename TEXT,
            original_name TEXT, file_type TEXT DEFAULT 'document',
            category TEXT DEFAULT 'general', notes TEXT DEFAULT '',
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS client_reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, client_id INTEGER, title TEXT,
            date TEXT, done INTEGER DEFAULT 0, created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit(); conn.close()

def migrate_v37():
    """Add signature_file to employees for imported signatures."""
    conn = get_db()
    try: conn.execute("ALTER TABLE employees ADD COLUMN signature_file TEXT DEFAULT ''")
    except: pass
    conn.commit(); conn.close()

def migrate_v38():
    """Calendar events - add columns to existing table."""
    conn = get_db()
    for col, typ in [('event_time','TEXT DEFAULT ""'),('category','TEXT DEFAULT "autre"'),('created_by','INTEGER DEFAULT 0')]:
        try: conn.execute(f"ALTER TABLE calendar_events ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def migrate_v39():
    """Add status to clients."""
    conn = get_db()
    try: conn.execute("ALTER TABLE clients ADD COLUMN client_status TEXT DEFAULT 'entreprise_sans_contrat'")
    except: pass
    conn.commit(); conn.close()

def migrate_v40():
    """Calendar events - add is_public for private/shared events."""
    conn = get_db()
    try: conn.execute("ALTER TABLE calendar_events ADD COLUMN is_public INTEGER DEFAULT 0")
    except: pass
    conn.commit(); conn.close()

def migrate_v41():
    """Interventions table - links projects/tasks to tech center and billing."""
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS interventions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT, title TEXT NOT NULL,
            type TEXT DEFAULT 'maintenance',
            project_id INTEGER, task_id INTEGER, client_id INTEGER,
            client_name TEXT, site_address TEXT,
            technician_id INTEGER, technician_name TEXT,
            scheduled_date TEXT, scheduled_time TEXT,
            start_date TEXT, end_date TEXT,
            duration_hours REAL DEFAULT 0,
            status TEXT DEFAULT 'planifiee',
            priority TEXT DEFAULT 'normale',
            description TEXT, rapport TEXT,
            material_used TEXT, material_cost REAL DEFAULT 0,
            labor_cost REAL DEFAULT 0, total_cost REAL DEFAULT 0,
            invoice_id INTEGER, is_billable INTEGER DEFAULT 1,
            photos TEXT, signature_client TEXT,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects(id),
            FOREIGN KEY (task_id) REFERENCES tasks(id),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
    ''')
    # Add intervention_id to tasks
    try: conn.execute("ALTER TABLE tasks ADD COLUMN intervention_id INTEGER DEFAULT 0")
    except: pass
    try: conn.execute("ALTER TABLE tasks ADD COLUMN client_id INTEGER DEFAULT 0")
    except: pass
    conn.commit(); conn.close()

def migrate_v42():
    """Multi-caisses + portail client tables."""
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS caisses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, description TEXT DEFAULT '',
            responsible_id INTEGER, responsible_name TEXT,
            solde_initial REAL DEFAULT 0, solde_actuel REAL DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS caisse_operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            caisse_id INTEGER NOT NULL, type TEXT NOT NULL,
            amount REAL NOT NULL, description TEXT,
            reference TEXT, category TEXT DEFAULT 'divers',
            intervention_id INTEGER, project_id INTEGER,
            source_caisse_id INTEGER, dest_caisse_id INTEGER,
            bank_account_id INTEGER,
            created_by INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (caisse_id) REFERENCES caisses(id)
        );
        CREATE TABLE IF NOT EXISTS client_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL, salt TEXT,
            full_name TEXT, email TEXT, tel TEXT,
            is_active INTEGER DEFAULT 1,
            last_login TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE TABLE IF NOT EXISTS client_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL, client_user_id INTEGER,
            title TEXT NOT NULL, type TEXT DEFAULT 'intervention',
            priority TEXT DEFAULT 'normale',
            description TEXT, site_address TEXT,
            status TEXT DEFAULT 'soumise',
            intervention_id INTEGER, assigned_to INTEGER,
            response TEXT, response_date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        CREATE TABLE IF NOT EXISTS client_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER, client_user_id INTEGER,
            user_id INTEGER, sender_type TEXT DEFAULT 'client',
            message TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    # Add caisse_id to existing tables
    for tbl in ['caisse_sorties','caisse_entrees','pieces_caisse','interventions']:
        try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN caisse_id INTEGER DEFAULT 0")
        except: pass
    conn.commit(); conn.close()

def migrate_v43():
    """Module settings + intervention fiches."""
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS module_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            module TEXT UNIQUE NOT NULL,
            is_active INTEGER DEFAULT 1,
            label TEXT, icon TEXT, description TEXT
        );
    ''')
    # Default modules
    modules = [
        ('comptabilite','Comptabilité','💰','Gestion financière, factures, écritures SYSCOHADA'),
        ('rh','Ressources Humaines','👥','Personnel, paie, congés, contrats'),
        ('projets','Gestion de Projets','📁','Projets, tâches, planning, interventions'),
        ('centre_technique','Centre Technique','🔧','Interventions, visites, équipements'),
        ('crm','CRM / Commercial','🎯','Clients, prospects, devis, visites'),
        ('concierge','Conciergerie','🔑','Visiteurs, courrier, colis, clés'),
        ('tracking','Tracking GPS','📡','Véhicules, géolocalisation, alertes'),
        ('stock','Stock & Achats','🛒','Inventaire, fournisseurs, commandes'),
        ('it','Informatique','🖥️','Parc info, helpdesk, sécurité'),
        ('portail_client','Portail Client','👤','Accès client, demandes, suivi'),
    ]
    for mod, label, icon, desc in modules:
        try: conn.execute("INSERT INTO module_settings (module, label, icon, description) VALUES (?,?,?,?)", (mod, label, icon, desc))
        except: pass
    conn.commit(); conn.close()

def migrate_v44():
    """Intervention daily reports."""
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS intervention_daily_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            intervention_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            report TEXT, progress INTEGER DEFAULT 0,
            technician_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (intervention_id) REFERENCES interventions(id)
        );
    ''')
    conn.commit(); conn.close()

def migrate_v45():
    """Intervention workflow: images + quality control."""
    conn = get_db()
    for col, typ in [('images','TEXT DEFAULT ""'),('status_note','TEXT DEFAULT ""')]:
        try: conn.execute(f"ALTER TABLE intervention_daily_reports ADD COLUMN {col} {typ}")
        except: pass
    # Intervention quality fields
    for col, typ in [('quality_report','TEXT DEFAULT ""'),('quality_date','TEXT'),('quality_by','INTEGER'),
                     ('delivery_date','TEXT'),('delivery_note','TEXT DEFAULT ""'),('client_validated','INTEGER DEFAULT 0'),
                     ('client_validation_date','TEXT')]:
        try: conn.execute(f"ALTER TABLE interventions ADD COLUMN {col} {typ}")
        except: pass
    conn.commit(); conn.close()

def migrate_v45():
    """Intervention workflow: images, quality, delivery columns."""
    conn = get_db()
    for col, default in [('images',''),('quality_report',''),('quality_date',''),('quality_by','0'),
                         ('delivery_date',''),('delivery_note',''),
                         ('client_validated','0'),('client_validation_date','')]:
        try: conn.execute(f"ALTER TABLE interventions ADD COLUMN {col} TEXT DEFAULT '{default}'")
        except: pass
    try: conn.execute("ALTER TABLE intervention_daily_reports ADD COLUMN images TEXT DEFAULT ''")
    except: pass
    conn.commit(); conn.close()

def migrate_v46():
    """Add client_code column to clients table and auto-generate codes for existing clients."""
    conn = get_db()
    # Add column
    try: conn.execute("ALTER TABLE clients ADD COLUMN client_code TEXT DEFAULT ''")
    except: pass
    # Auto-generate codes for clients that don't have one
    # Format: C XXX (e.g., C 001, C 420...)
    rows = conn.execute("SELECT id FROM clients WHERE client_code IS NULL OR client_code='' ORDER BY id ASC").fetchall()
    # Find the highest existing numeric code to continue the sequence
    max_num = 0
    for r in conn.execute("SELECT client_code FROM clients WHERE client_code LIKE 'C %'").fetchall():
        try:
            n = int(str(r['client_code']).replace('C ','').strip())
            if n > max_num: max_num = n
        except: pass
    next_num = max_num + 1
    for r in rows:
        code = f"C {next_num:03d}"
        try:
            conn.execute("UPDATE clients SET client_code=? WHERE id=?", (code, r['id']))
            next_num += 1
        except: pass
    conn.commit(); conn.close()

def generate_next_client_code():
    """Return next available client code C XXX."""
    conn = get_db()
    max_num = 0
    for r in conn.execute("SELECT client_code FROM clients WHERE client_code LIKE 'C %'").fetchall():
        try:
            n = int(str(r['client_code']).replace('C ','').strip())
            if n > max_num: max_num = n
        except: pass
    conn.close()
    return f"C {(max_num + 1):03d}"

def migrate_v47():
    """v47 : multi-caisses liées, validation admin comptes clients, géoloc demandes, profil portail."""
    conn = get_db()
    # 1) Rattacher une dépense (pièce de caisse) à une caisse
    try: conn.execute("ALTER TABLE pieces_caisse ADD COLUMN caisse_id INTEGER")
    except: pass
    # 2) Statut de validation du compte client (pending / approved / rejected)
    try: conn.execute("ALTER TABLE client_users ADD COLUMN account_status TEXT DEFAULT 'pending'")
    except: pass
    try: conn.execute("ALTER TABLE client_users ADD COLUMN approved_by INTEGER DEFAULT 0")
    except: pass
    try: conn.execute("ALTER TABLE client_users ADD COLUMN approved_at TEXT DEFAULT ''")
    except: pass
    try: conn.execute("ALTER TABLE client_users ADD COLUMN reject_reason TEXT DEFAULT ''")
    except: pass
    # 3) Photo de profil + infos perso pour le compte client
    try: conn.execute("ALTER TABLE client_users ADD COLUMN photo TEXT DEFAULT ''")
    except: pass
    try: conn.execute("ALTER TABLE client_users ADD COLUMN tel TEXT DEFAULT ''")
    except: pass
    try: conn.execute("ALTER TABLE client_users ADD COLUMN address TEXT DEFAULT ''")
    except: pass
    # 4) Géolocalisation sur les demandes clients
    try: conn.execute("ALTER TABLE client_requests ADD COLUMN gps_lat REAL")
    except: pass
    try: conn.execute("ALTER TABLE client_requests ADD COLUMN gps_lng REAL")
    except: pass
    try: conn.execute("ALTER TABLE client_requests ADD COLUMN gps_accuracy REAL")
    except: pass
    # Géolocalisation héritée sur les interventions (pour que le technicien l'ait aussi)
    try: conn.execute("ALTER TABLE interventions ADD COLUMN gps_lat REAL")
    except: pass
    try: conn.execute("ALTER TABLE interventions ADD COLUMN gps_lng REAL")
    except: pass
    # 5) TVA optionnelle sur devis et factures
    for tbl in ('devis', 'invoices'):
        try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN tva_active INTEGER DEFAULT 0")
        except: pass
        try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN tva_rate REAL DEFAULT 18")
        except: pass
        try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN tva_amount REAL DEFAULT 0")
        except: pass
        try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN redacteur TEXT DEFAULT ''")
        except: pass
        try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN redacteur_date TEXT DEFAULT ''")
        except: pass
    # 6) Les clients déjà existants doivent être approuvés (sinon ils ne pourraient plus se connecter)
    try: conn.execute("UPDATE client_users SET account_status='approved' WHERE account_status IS NULL OR account_status=''")
    except: pass
    
    # 7) Ajouter les nouvelles permissions aux rôles concernés (idempotent via INSERT OR IGNORE)
    new_perms_by_role = {
        'admin': ['grand_livre', 'balance', 'client_users_approve', 'caisse_multi'],
        'dg':    ['grand_livre', 'balance', 'caisse_multi'],
        'comptable': ['grand_livre', 'balance', 'caisse_multi'],
        'commercial': [],
        'rh':    [],
        'technicien': ['gps_itineraire'],  # accès bouton itinéraire GPS
        'resp_projet': ['gps_itineraire'],
        'informatique': ['gps_itineraire'],
    }
    for role, perms in new_perms_by_role.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    
    conn.commit(); conn.close()


def migrate_v48():
    """v48 : caisse_id sur entrées/sorties, table bank_transfers (virements banque→caisse avec validation DG)."""
    conn = get_db()
    # caisse_id sur entrées et sorties de caisse
    for tbl in ('caisse_entrees', 'caisse_sorties'):
        try: conn.execute(f"ALTER TABLE {tbl} ADD COLUMN caisse_id INTEGER")
        except: pass
    # Table des virements banque → caisse avec workflow validation DG (nouvelle)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS caisse_virements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reference TEXT UNIQUE,
                date TEXT,
                bank_account_id INTEGER,
                bank_name TEXT,
                caisse_id INTEGER NOT NULL,
                caisse_name TEXT,
                amount REAL NOT NULL,
                motif TEXT,
                status TEXT DEFAULT 'en_attente',
                requested_by INTEGER,
                requested_by_name TEXT,
                validated_by INTEGER DEFAULT 0,
                validated_by_name TEXT,
                validated_at TEXT,
                reject_reason TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except: pass
    # Ajouter les nouvelles permissions pour virement et demandes/comptes à valider
    new_perms = {
        'admin': ['virement_demande', 'virement_valide', 'client_requests_view', 'client_users_approve'],
        'dg':    ['virement_valide', 'client_users_approve', 'client_requests_view'],
        'comptable': ['virement_demande', 'client_requests_view'],
        'commercial': ['client_requests_view'],
        'resp_projet': ['client_requests_view'],
        'technicien': [],
    }
    for role, perms in new_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    
    conn.commit(); conn.close()


def migrate_v49():
    """v49 : Workflow intervention 5 étapes — contrôle qualité + date livraison + notation."""
    conn = get_db()
    new_cols = [
        ('end_work_at', 'TEXT'),
        ('end_work_by', 'INTEGER'),
        ('cq_status', "TEXT DEFAULT ''"),
        ('cq_at', 'TEXT'),
        ('cq_by', 'INTEGER'),
        ('cq_by_name', 'TEXT'),
        ('cq_comments', 'TEXT'),
        ('cq_photos', 'TEXT'),
        ('delivery_proposed_date', 'TEXT'),
        ('delivery_proposed_at', 'TEXT'),
        ('delivery_proposed_by', 'INTEGER'),
        ('delivery_client_status', "TEXT DEFAULT ''"),
        ('delivery_client_proposed_date', 'TEXT'),
        ('delivery_client_comment', 'TEXT'),
        ('delivery_client_answered_at', 'TEXT'),
        ('delivered_at', 'TEXT'),
        ('delivered_by', 'INTEGER'),
        ('delivery_signed_client', 'TEXT'),
        ('delivery_signed_coordinator', 'TEXT'),
        ('delivery_signed_technicien', 'TEXT'),
        ('delivery_bon_ref', 'TEXT'),
        ('rating_stars', 'INTEGER DEFAULT 0'),
        ('rating_comment', 'TEXT'),
        ('rating_at', 'TEXT'),
    ]
    for col, coltype in new_cols:
        try: conn.execute(f"ALTER TABLE interventions ADD COLUMN {col} {coltype}")
        except: pass
    # notifications: ajouter client_user_id pour notifier les clients du portail
    try: conn.execute("ALTER TABLE notifications ADD COLUMN client_user_id INTEGER")
    except: pass
    # client_messages : ajouter sender_id et sender_name pour tracer la réponse staff
    for col, coltype in [('sender_id', 'INTEGER'), ('sender_name', 'TEXT')]:
        try: conn.execute(f"ALTER TABLE client_messages ADD COLUMN {col} {coltype}")
        except: pass
    # Timeline pour traçabilité
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS intervention_timeline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                intervention_id INTEGER NOT NULL,
                step TEXT NOT NULL,
                title TEXT, description TEXT,
                actor_id INTEGER, actor_name TEXT, actor_role TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
    except: pass
    # Permissions
    new_perms = {
        'admin':        ['controle_qualite', 'livraison_intervention'],
        'dg':           ['controle_qualite', 'livraison_intervention'],
        'resp_projet':  ['controle_qualite', 'livraison_intervention'],
        'coordinateur': ['controle_qualite', 'livraison_intervention'],
        'technicien':   ['livraison_intervention'],
    }
    for role, perms in new_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    conn.commit(); conn.close()


def migrate_v50():
    """v50 : Ajouter updated_at sur interventions (pour le suivi temps réel des rapports clients)."""
    conn = get_db()
    try: conn.execute("ALTER TABLE interventions ADD COLUMN updated_at TEXT")
    except: pass
    try: conn.execute("ALTER TABLE interventions ADD COLUMN progress INTEGER DEFAULT 0")
    except: pass
    try: conn.execute("UPDATE interventions SET updated_at = COALESCE(updated_at, created_at, scheduled_date) WHERE updated_at IS NULL OR updated_at = ''")
    except: pass
    conn.commit(); conn.close()


def migrate_v51():
    """v51 : Module Équipements client (caméras, DVR, alarmes, clôtures...) + état de santé."""
    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS client_equipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            category TEXT,
            model TEXT,
            serial_number TEXT,
            location TEXT,
            installation_date TEXT,
            warranty_until TEXT,
            health_status TEXT DEFAULT 'ok',
            health_score INTEGER DEFAULT 80,
            last_intervention_id INTEGER,
            last_intervention_at TEXT,
            last_status_note TEXT,
            notes TEXT,
            photo TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v51 client_equipments: {e}")
    try: conn.execute("ALTER TABLE interventions ADD COLUMN equipment_id INTEGER")
    except: pass
    try: conn.execute("ALTER TABLE interventions ADD COLUMN equipment_health_after TEXT")
    except: pass
    try: conn.execute("ALTER TABLE interventions ADD COLUMN equipment_health_note TEXT")
    except: pass
    conn.commit(); conn.close()


def migrate_v52():
    """v52 : Module Gestion du budget par département."""
    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS dept_budgets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            department TEXT NOT NULL,
            period_label TEXT,
            period_type TEXT DEFAULT 'mensuel',
            period_start TEXT,
            period_end TEXT,
            amount_planned REAL DEFAULT 0,
            amount_spent REAL DEFAULT 0,
            notes TEXT,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1
        )""")
    except Exception as e: print(f"v52 dept_budgets: {e}")
    try: conn.execute("ALTER TABLE caisse_sorties ADD COLUMN department TEXT")
    except: pass
    try: conn.execute("ALTER TABLE caisse_sorties ADD COLUMN budget_id INTEGER")
    except: pass
    conn.commit(); conn.close()


def migrate_v53():
    """v53 : Permissions Budget + colonne department sur users."""
    conn = get_db()
    # Ajouter department aux utilisateurs (pour que un responsable ne voie que SON département)
    try: conn.execute("ALTER TABLE users ADD COLUMN department TEXT")
    except: pass
    # Permissions par défaut Budget par rôle
    default_budget_perms = {
        'admin':           ['budget_view', 'budget_edit'],
        'dg':              ['budget_view', 'budget_edit'],
        'comptable':       ['budget_view'],                    # comptable = lecture globale
        'rh':              ['budget_view_own'],                # responsable = son département
        'commercial':      ['budget_view_own'],
        'resp_projet':     ['budget_view_own'],
        'moyens_generaux': ['budget_view_own'],
        'informatique':    ['budget_view_own'],
        'proprietaire':    ['budget_view'],                    # propriétaire voit tout
    }
    for role, perms in default_budget_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    conn.commit(); conn.close()


def migrate_v54():
    """v54 : Logs de sécurité + 2FA + backup + badge tracking persistant."""
    conn = get_db()
    # Table dédiée aux événements sécurité
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS security_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            user_id INTEGER,
            username TEXT,
            ip TEXT,
            user_agent TEXT,
            path TEXT,
            details TEXT,
            severity TEXT DEFAULT 'info',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e:
        print(f"v54 security_logs: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_seclog_user ON security_logs(user_id)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_seclog_type ON security_logs(event_type)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_seclog_date ON security_logs(created_at)")
    except: pass
    # 2FA TOTP
    try: conn.execute("ALTER TABLE users ADD COLUMN totp_secret TEXT")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN totp_enabled INTEGER DEFAULT 0")
    except: pass
    # Persistance des "vu à" pour les badges (sinon ils réapparaissent à chaque login)
    try: conn.execute("ALTER TABLE users ADD COLUMN last_interventions_seen TEXT")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN last_my_interventions_seen TEXT")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN last_visites_seen TEXT")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN last_requests_seen TEXT")
    except: pass
    conn.commit(); conn.close()


def migrate_v55():
    """v55 : Module Pointage RH — Planning + Pointages + Géolocalisation."""
    conn = get_db()
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS hr_planning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            day_of_week INTEGER,
            heure_arrivee TEXT DEFAULT '08:00',
            heure_pause TEXT DEFAULT '12:00',
            heure_retour TEXT DEFAULT '13:00',
            heure_depart TEXT DEFAULT '17:00',
            tolerance_retard_minutes INTEGER DEFAULT 10,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v55 hr_planning: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_planning_user ON hr_planning(user_id)")
    except: pass
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS hr_pointages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            datetime_full TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            location_address TEXT,
            photo TEXT,
            status TEXT DEFAULT 'normal',
            ecart_minutes INTEGER DEFAULT 0,
            ip TEXT,
            user_agent TEXT,
            method TEXT DEFAULT 'button',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v55 hr_pointages: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_pointage_user_date ON hr_pointages(user_id, date)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_pointage_date ON hr_pointages(date)")
    except: pass
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS hr_zones_pointage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            radius_meters INTEGER DEFAULT 100,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v55 hr_zones_pointage: {e}")
    # Permissions par défaut Pointage par rôle
    default_pointage_perms = {
        'admin':           ['pointage', 'pointage_admin', 'pointage_edit'],
        'dg':              ['pointage', 'pointage_admin', 'pointage_edit'],
        'rh':              ['pointage', 'pointage_admin', 'pointage_edit'],
        'comptable':       ['pointage', 'pointage_admin'],
        'commercial':      ['pointage', 'pointage_dept'],
        'technicien':      ['pointage'],
        'resp_projet':     ['pointage', 'pointage_dept'],
        'moyens_generaux': ['pointage', 'pointage_dept'],
        'informatique':    ['pointage'],
        'concierge':       ['pointage'],
        'secretaire':      ['pointage'],
        'proprietaire':    ['pointage', 'pointage_admin'],
    }
    for role, perms in default_pointage_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    # Permissions par défaut Achats / Stock
    default_achats_perms = {
        'admin':           ['achats', 'achats_edit'],
        'dg':              ['achats', 'achats_edit'],
        'comptable':       ['achats', 'achats_edit'],
        'moyens_generaux': ['achats', 'achats_edit'],
        'resp_projet':     ['achats'],
    }
    for role, perms in default_achats_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    conn.commit(); conn.close()


def migrate_v56():
    """v56 : Pénalités retard + Modules RH + Multi-entreprises pointage."""
    conn = get_db()
    # Pénalités sur hr_pointages (champ calculé à l'enregistrement)
    try: conn.execute("ALTER TABLE hr_pointages ADD COLUMN penalty_amount REAL DEFAULT 0")
    except: pass
    # Configuration des pénalités (par entreprise)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS hr_penalty_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            penalty_per_minute REAL DEFAULT 0,
            grace_minutes INTEGER DEFAULT 0,
            currency TEXT DEFAULT 'XOF',
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v56 hr_penalty_config: {e}")
    
    # Modules RH activables/désactivables
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS hr_modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            module_key TEXT UNIQUE NOT NULL,
            label TEXT NOT NULL,
            description TEXT,
            icon TEXT,
            is_active INTEGER DEFAULT 1,
            sort_order INTEGER DEFAULT 0,
            config_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v56 hr_modules: {e}")
    # Modules par défaut
    default_modules = [
        ('pointage', 'Pointage', 'Présence employés via boutons + QR', '⏱️', 1, 1),
        ('penalites', 'Pénalités retard', 'Calcul automatique des pénalités selon retard', '💰', 1, 2),
        ('rapports_pdf', 'Rapports mensuels PDF', 'Export PDF par employé', '📄', 1, 3),
        ('absences_auto', 'Détection absences', 'Notification RH si pas de pointage matin', '⚠️', 1, 4),
        ('graphiques', 'Graphiques de présence', 'Visualisations stats mensuelles', '📊', 1, 5),
        ('badges', 'Badges employés', 'Cartes ID avec QR personnel', '🪪', 0, 6),
        ('cantine', 'Ticket cantine', 'Gestion repas employés', '🍽️', 0, 7),
        ('navette', 'Navette transport', 'Inscription navette journalière', '🚐', 0, 8),
        ('formation', 'Formations internes', 'Inscriptions et présences', '🎓', 0, 9),
        ('avances', 'Avances sur salaire', 'Demande/validation avances', '💵', 0, 10),
    ]
    for mk, lbl, desc, ic, ac, so in default_modules:
        try: conn.execute("""INSERT OR IGNORE INTO hr_modules
            (module_key, label, description, icon, is_active, sort_order)
            VALUES (?, ?, ?, ?, ?, ?)""", (mk, lbl, desc, ic, ac, so))
        except: pass
    
    # Multi-entreprises (espaces de pointage isolés pour autres clients/structures)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS pointage_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            welcome_message TEXT,
            primary_color TEXT DEFAULT '#1a7a6d',
            logo_data_uri TEXT,
            penalty_per_minute REAL DEFAULT 0,
            grace_minutes INTEGER DEFAULT 10,
            timezone TEXT DEFAULT 'Africa/Abidjan',
            is_active INTEGER DEFAULT 1,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v56 pointage_companies: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_pcomp_slug ON pointage_companies(slug)")
    except: pass
    
    # Employés rattachés à une entreprise (séparé de users WannyGest)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS pointage_company_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            salt TEXT,
            full_name TEXT NOT NULL,
            email TEXT,
            phone TEXT,
            poste TEXT,
            heure_arrivee TEXT DEFAULT '08:00',
            heure_pause TEXT DEFAULT '12:00',
            heure_retour TEXT DEFAULT '13:00',
            heure_depart TEXT DEFAULT '17:00',
            tolerance_retard_minutes INTEGER DEFAULT 10,
            is_active INTEGER DEFAULT 1,
            last_login TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_id, username)
        )""")
    except Exception as e: print(f"v56 pointage_company_users: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_pcu_company ON pointage_company_users(company_id)")
    except: pass
    
    # Pointages des employés multi-entreprises (table parallèle pour ne pas mélanger)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS pointage_company_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            company_user_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            datetime_full TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            photo TEXT,
            status TEXT DEFAULT 'normal',
            ecart_minutes INTEGER DEFAULT 0,
            penalty_amount REAL DEFAULT 0,
            ip TEXT,
            user_agent TEXT,
            method TEXT DEFAULT 'button',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v56 pointage_company_records: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_pcr_company_date ON pointage_company_records(company_id, date)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_pcr_user_date ON pointage_company_records(company_user_id, date)")
    except: pass
    
    # Détection absences : log des notifications déjà envoyées
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS hr_absence_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            alert_type TEXT,
            notified_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, date, alert_type)
        )""")
    except: pass
    
    conn.commit(); conn.close()


def migrate_v57():
    """v57 : Module Fournisseurs / Achats / Paiements (acomptes)."""
    conn = get_db()
    # Table fournisseurs (séparée d'achats_fournisseurs si différent)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT NOT NULL,
            telephone TEXT,
            email TEXT,
            adresse TEXT,
            contact TEXT,
            notes TEXT,
            is_active INTEGER DEFAULT 1,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v57 suppliers: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_supplier_name ON suppliers(nom)")
    except: pass
    
    # Table achats fournisseurs
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS supplier_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_id INTEGER NOT NULL,
            description TEXT NOT NULL,
            categorie TEXT,
            montant_total REAL NOT NULL,
            date TEXT,
            date_echeance TEXT,
            departement TEXT,
            notes TEXT,
            attachment TEXT,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v57 supplier_purchases: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_purch_supplier ON supplier_purchases(supplier_id)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_purch_date ON supplier_purchases(date)")
    except: pass
    
    # Table paiements (acomptes)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS supplier_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_id INTEGER NOT NULL,
            montant REAL NOT NULL,
            date TEXT NOT NULL,
            mode_paiement TEXT,
            reference TEXT,
            caisse_sortie_id INTEGER,
            notes TEXT,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except Exception as e: print(f"v57 supplier_payments: {e}")
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_pay_purchase ON supplier_payments(purchase_id)")
    except: pass
    
    # Permissions par défaut
    default_fournisseurs_perms = {
        'admin':           ['fournisseurs', 'fournisseurs_edit'],
        'dg':              ['fournisseurs', 'fournisseurs_edit'],
        'comptable':       ['fournisseurs', 'fournisseurs_edit'],
        'moyens_generaux': ['fournisseurs', 'fournisseurs_edit'],
        'resp_projet':     ['fournisseurs'],
        'commercial':      ['fournisseurs'],
    }
    for role, perms in default_fournisseurs_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    
    conn.commit(); conn.close()


def migrate_v58():
    """v58 : Fusion achats_fournisseurs → suppliers (one-time, idempotent)."""
    conn = get_db()
    # Vérifier que les 2 tables existent
    try:
        cnt_old = conn.execute("SELECT COUNT(*) FROM achats_fournisseurs").fetchone()[0]
    except:
        conn.close(); return
    
    # Ajouter une colonne 'legacy_id' à suppliers pour traçabilité, et faire la migration
    try: conn.execute("ALTER TABLE suppliers ADD COLUMN legacy_achats_id INTEGER")
    except: pass
    
    # Pour chaque fournisseur dans achats_fournisseurs non encore migré
    try:
        rows = conn.execute("""
            SELECT a.id, a.name, a.contact_name, a.tel, a.email, a.address, a.city, a.sector, a.notes
            FROM achats_fournisseurs a
            WHERE NOT EXISTS (SELECT 1 FROM suppliers s WHERE s.legacy_achats_id = a.id)
            AND a.name IS NOT NULL AND a.name != ''
        """).fetchall()
        migrated = 0
        for r in rows:
            # Vérifier doublon par nom (case insensitive)
            existing = conn.execute("SELECT id FROM suppliers WHERE LOWER(nom) = LOWER(?)",
                                   (r['name'],)).fetchone()
            if existing:
                # Marquer comme migré (lié au legacy_id)
                conn.execute("UPDATE suppliers SET legacy_achats_id=? WHERE id=?",
                            (r['id'], existing['id']))
            else:
                # Insérer
                full_addr = ((r['address'] or '') + (', ' + r['city'] if r['city'] else '')).strip(', ')
                conn.execute("""INSERT INTO suppliers
                    (nom, telephone, email, adresse, contact, notes, legacy_achats_id, is_active)
                    VALUES (?,?,?,?,?,?,?,1)""",
                    (r['name'], r['tel'] or '', r['email'] or '', full_addr,
                     r['contact_name'] or '', r['notes'] or '', r['id']))
                migrated += 1
        conn.commit()
        if migrated > 0:
            print(f"v58: {migrated} fournisseur(s) migré(s) de achats_fournisseurs vers suppliers")
    except Exception as e:
        print(f"v58 migration: {e}")
    
    conn.close()


def migrate_v59():
    """v59 : Module Comptabilité Pro (Sage-like SYSCOHADA).
    Tables : compta_comptes, compta_journaux, compta_ecritures, compta_lignes,
             compta_periodes, compta_compteurs, compta_lettrages, compta_lettrage_lignes."""
    conn = get_db()
    
    # 1. Plan comptable
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_comptes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero TEXT UNIQUE NOT NULL,
            nom TEXT NOT NULL,
            type TEXT NOT NULL,
            classe INTEGER,
            parent_numero TEXT,
            is_lettrable INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_compta_comptes_num ON compta_comptes(numero)")
    except: pass
    
    # 2. Journaux
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_journaux (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            nom TEXT NOT NULL,
            type TEXT,
            compte_contrepartie TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except: pass
    
    # 3. Écritures (en-tête)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_ecritures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            libelle TEXT NOT NULL,
            statut TEXT DEFAULT 'brouillard',
            numero_piece TEXT,
            piece_externe TEXT,
            tiers_id INTEGER,
            tiers_type TEXT,
            total_debit REAL DEFAULT 0,
            total_credit REAL DEFAULT 0,
            validated_at TEXT,
            validated_by INTEGER,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (journal_id) REFERENCES compta_journaux(id)
        )""")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_ecr_date ON compta_ecritures(date)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_ecr_journal ON compta_ecritures(journal_id)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_ecr_statut ON compta_ecritures(statut)")
    except: pass
    
    # 4. Lignes d'écriture
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_lignes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ecriture_id INTEGER NOT NULL,
            compte_id INTEGER NOT NULL,
            compte_numero TEXT,
            libelle TEXT,
            debit REAL DEFAULT 0,
            credit REAL DEFAULT 0,
            lettrage_code TEXT,
            tiers_id INTEGER,
            tiers_type TEXT,
            ordre INTEGER DEFAULT 0,
            FOREIGN KEY (ecriture_id) REFERENCES compta_ecritures(id) ON DELETE CASCADE,
            FOREIGN KEY (compte_id) REFERENCES compta_comptes(id)
        )""")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_ligne_ecr ON compta_lignes(ecriture_id)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_ligne_compte ON compta_lignes(compte_id)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_ligne_lettrage ON compta_lignes(lettrage_code)")
    except: pass
    
    # 5. Compteurs (pour numérotation auto)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_compteurs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id INTEGER NOT NULL,
            annee INTEGER NOT NULL,
            mois INTEGER NOT NULL,
            dernier_numero INTEGER DEFAULT 0,
            UNIQUE(journal_id, annee, mois)
        )""")
    except: pass
    
    # 6. Périodes
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_periodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            annee INTEGER NOT NULL,
            mois INTEGER NOT NULL,
            statut TEXT DEFAULT 'ouverte',
            cloture_at TEXT,
            cloture_by INTEGER,
            UNIQUE(annee, mois)
        )""")
    except: pass
    
    # 7. Lettrages (en-tête)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_lettrages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            date TEXT NOT NULL,
            compte_id INTEGER,
            tiers_id INTEGER,
            tiers_type TEXT,
            statut TEXT DEFAULT 'partiel',
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except: pass
    
    # 8. Lignes de lettrage (lien lettrage <-> lignes d'écriture)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS compta_lettrage_lignes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lettrage_id INTEGER NOT NULL,
            ligne_id INTEGER NOT NULL,
            montant REAL NOT NULL,
            FOREIGN KEY (lettrage_id) REFERENCES compta_lettrages(id) ON DELETE CASCADE,
            FOREIGN KEY (ligne_id) REFERENCES compta_lignes(id) ON DELETE CASCADE
        )""")
    except: pass
    
    # === Plan comptable SYSCOHADA initial (essentiels) ===
    plan_initial = [
        # Classe 1 — Capitaux
        ('101', 'Capital social', 'passif', 1),
        ('106', 'Réserves', 'passif', 1),
        ('120', 'Résultat net : bénéfice', 'passif', 1),
        ('129', 'Résultat net : perte', 'actif', 1),
        ('161', 'Emprunts obligataires', 'passif', 1),
        ('164', 'Emprunts auprès des établissements de crédit', 'passif', 1),
        # Classe 2 — Immobilisations
        ('201', 'Frais d\'établissement', 'actif', 2),
        ('213', 'Constructions', 'actif', 2),
        ('215', 'Installations techniques', 'actif', 2),
        ('218', 'Autres immobilisations corporelles', 'actif', 2),
        ('2182', 'Matériel de transport', 'actif', 2),
        ('2183', 'Matériel informatique', 'actif', 2),
        ('2184', 'Mobilier de bureau', 'actif', 2),
        # Classe 3 — Stocks
        ('311', 'Marchandises', 'actif', 3),
        ('322', 'Matières premières', 'actif', 3),
        # Classe 4 — Tiers
        ('401', 'Fournisseurs, dettes en compte', 'passif', 4),
        ('411', 'Clients', 'actif', 4),
        ('421', 'Personnel, avances et acomptes', 'actif', 4),
        ('422', 'Personnel, rémunérations dues', 'passif', 4),
        ('431', 'CNPS', 'passif', 4),
        ('441', 'État, impôt sur les bénéfices', 'passif', 4),
        ('445', 'État, TVA', 'passif', 4),
        ('4451', 'État, TVA collectée', 'passif', 4),
        ('4452', 'État, TVA déductible', 'actif', 4),
        ('467', 'Autres comptes débiteurs ou créditeurs', 'passif', 4),
        # Classe 5 — Trésorerie
        ('512', 'Banque', 'actif', 5),
        ('521', 'Banque (compte courant)', 'actif', 5),
        ('571', 'Caisse', 'actif', 5),
        ('581', 'Virements internes', 'actif', 5),
        # Classe 6 — Charges
        ('601', 'Achats de marchandises', 'charge', 6),
        ('604', 'Achats stockés de matières et fournitures', 'charge', 6),
        ('605', 'Autres achats', 'charge', 6),
        ('611', 'Transports sur achats', 'charge', 6),
        ('622', 'Locations et charges locatives', 'charge', 6),
        ('625', 'Primes d\'assurance', 'charge', 6),
        ('631', 'Frais bancaires', 'charge', 6),
        ('632', 'Rémunérations d\'intermédiaires', 'charge', 6),
        ('641', 'Impôts et taxes directs', 'charge', 6),
        ('661', 'Charges de personnel', 'charge', 6),
        ('664', 'Charges sociales', 'charge', 6),
        ('672', 'Intérêts', 'charge', 6),
        ('675', 'Autres charges financières', 'charge', 6),
        ('681', 'Dotations aux amortissements', 'charge', 6),
        # Classe 7 — Produits
        ('701', 'Ventes de marchandises', 'produit', 7),
        ('706', 'Services vendus', 'produit', 7),
        ('707', 'Produits annexes', 'produit', 7),
        ('711', 'Subventions d\'exploitation', 'produit', 7),
        ('775', 'Produits exceptionnels', 'produit', 7),
        ('791', 'Reprises sur provisions', 'produit', 7),
    ]
    for num, nom, typ, classe in plan_initial:
        # is_lettrable pour 401, 411, 421, 422
        lettrable = 1 if num in ('401', '411', '421', '422', '467') else 0
        try:
            conn.execute("""INSERT OR IGNORE INTO compta_comptes
                (numero, nom, type, classe, is_lettrable, is_active)
                VALUES (?, ?, ?, ?, ?, 1)""", (num, nom, typ, classe, lettrable))
        except: pass
    
    # === Journaux par défaut ===
    journaux_initial = [
        ('ACH', 'Journal des achats', 'achats', '401'),
        ('VTE', 'Journal des ventes', 'ventes', '411'),
        ('CAISSE', 'Journal de caisse', 'tresorerie', '571'),
        ('BANQUE', 'Journal de banque', 'tresorerie', '512'),
        ('OD', 'Opérations diverses', 'od', None),
        ('PAIE', 'Journal de paie', 'paie', '422'),
        ('ANO', 'Écritures à nouveau', 'an', None),
    ]
    for code, nom, typ, contrepartie in journaux_initial:
        try:
            conn.execute("""INSERT OR IGNORE INTO compta_journaux
                (code, nom, type, compte_contrepartie, is_active)
                VALUES (?, ?, ?, ?, 1)""", (code, nom, typ, contrepartie))
        except: pass
    
    # === Période du mois courant ouverte par défaut ===
    from datetime import datetime as _dt
    now = _dt.now()
    try:
        conn.execute("""INSERT OR IGNORE INTO compta_periodes
            (annee, mois, statut) VALUES (?, ?, 'ouverte')""",
            (now.year, now.month))
    except: pass
    
    # Permissions par défaut
    default_compta_perms = {
        'admin':     ['compta_pro', 'compta_pro_edit', 'compta_pro_valide', 'compta_pro_cloture'],
        'dg':        ['compta_pro', 'compta_pro_edit', 'compta_pro_valide', 'compta_pro_cloture'],
        'comptable': ['compta_pro', 'compta_pro_edit', 'compta_pro_valide'],
        'proprietaire': ['compta_pro'],
    }
    for role, perms in default_compta_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    
    conn.commit(); conn.close()


def migrate_v60():
    """v60 : Ajoute must_change_password aux employés pointage et utilisateurs internes."""
    conn = get_db()
    # Pointage company users
    try: conn.execute("ALTER TABLE pointage_company_users ADD COLUMN must_change_password INTEGER DEFAULT 0")
    except: pass
    try: conn.execute("ALTER TABLE pointage_company_users ADD COLUMN password_changed_at TEXT")
    except: pass
    # Users internes
    try: conn.execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER DEFAULT 0")
    except: pass
    try: conn.execute("ALTER TABLE users ADD COLUMN password_changed_at TEXT")
    except: pass
    conn.commit(); conn.close()


def migrate_v61():
    """v61 : Module Trésorerie séparé. Tables : tresorerie_mouvements, tresorerie_comptes_bancaires.
    Crée le rôle 'caissier' et distribue les permissions par défaut."""
    conn = get_db()
    
    # Comptes bancaires (registre des comptes en banque de l'entreprise)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS tresorerie_comptes_bancaires (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT NOT NULL,
            banque TEXT,
            numero_compte TEXT,
            iban TEXT,
            swift TEXT,
            devise TEXT DEFAULT 'XOF',
            solde_initial REAL DEFAULT 0,
            compta_compte_numero TEXT DEFAULT '512',
            is_active INTEGER DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except: pass
    
    # Mouvements de trésorerie (caisse + banque) — log de chaque mouvement
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS tresorerie_mouvements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            sens TEXT NOT NULL,
            source TEXT NOT NULL,
            source_id INTEGER,
            date TEXT NOT NULL,
            montant REAL NOT NULL,
            libelle TEXT,
            tiers_type TEXT,
            tiers_id INTEGER,
            tiers_nom TEXT,
            mode_paiement TEXT,
            reference TEXT,
            caisse_id INTEGER,
            banque_id INTEGER,
            ecriture_id INTEGER,
            statut TEXT DEFAULT 'enregistre',
            rapproche_at TEXT,
            created_by INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_treso_date ON tresorerie_mouvements(date)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_treso_type ON tresorerie_mouvements(type)")
    except: pass
    try: conn.execute("CREATE INDEX IF NOT EXISTS idx_treso_sens ON tresorerie_mouvements(sens)")
    except: pass
    
    # Permissions par défaut
    default_treso_perms = {
        'admin':       ['tresorerie', 'tresorerie_edit', 'tresorerie_rapprochement'],
        'dg':          ['tresorerie', 'tresorerie_edit', 'tresorerie_rapprochement'],
        'comptable':   ['tresorerie', 'tresorerie_edit', 'tresorerie_rapprochement'],
        'caissier':    ['tresorerie', 'tresorerie_edit', 'caisse_sortie', 'dashboard'],
        'proprietaire': ['tresorerie'],
    }
    for role, perms in default_treso_perms.items():
        for perm in perms:
            try: conn.execute("INSERT OR IGNORE INTO permissions (role, permission) VALUES (?, ?)", (role, perm))
            except: pass
    
    conn.commit(); conn.close()


# ======================== SERVICES COMPTABLES ========================

def compta_periode_est_ouverte(annee, mois):
    """Vérifie qu'une période est ouverte (autorise écriture)."""
    conn = get_db()
    try:
        row = conn.execute("SELECT statut FROM compta_periodes WHERE annee=? AND mois=?",
                          (annee, mois)).fetchone()
        if not row:
            # Auto-créer ouverte si n'existe pas
            conn.execute("INSERT OR IGNORE INTO compta_periodes (annee, mois, statut) VALUES (?, ?, 'ouverte')",
                        (annee, mois))
            conn.commit()
            conn.close(); return True
        result = row['statut'] == 'ouverte'
    except: result = True
    conn.close()
    return result


def compta_get_next_numero(journal_code, annee, mois):
    """Génère le prochain numéro de pièce : ACH-202604-0001"""
    conn = get_db()
    try:
        j = conn.execute("SELECT id FROM compta_journaux WHERE code=?", (journal_code,)).fetchone()
        if not j:
            conn.close(); return None
        jid = j['id']
        # Récupérer ou créer le compteur
        row = conn.execute("""SELECT dernier_numero FROM compta_compteurs
            WHERE journal_id=? AND annee=? AND mois=?""", (jid, annee, mois)).fetchone()
        if row:
            next_num = (row['dernier_numero'] or 0) + 1
            conn.execute("""UPDATE compta_compteurs SET dernier_numero=?
                WHERE journal_id=? AND annee=? AND mois=?""", (next_num, jid, annee, mois))
        else:
            next_num = 1
            conn.execute("""INSERT INTO compta_compteurs (journal_id, annee, mois, dernier_numero)
                VALUES (?, ?, ?, ?)""", (jid, annee, mois, next_num))
        conn.commit()
        conn.close()
        return f"{journal_code}-{annee}{mois:02d}-{next_num:04d}"
    except Exception as e:
        print(f"compta_get_next_numero: {e}")
        conn.close()
        return None


def compta_creer_ecriture(journal_code, date, libelle, lignes, user_id, piece_externe='', validate=False):
    """Crée une écriture en brouillard (ou validée si validate=True).
    lignes = [{'compte_numero':..., 'libelle':..., 'debit':..., 'credit':...}, ...]
    Retourne (id_ecriture, error_msg). Vérifie débit=crédit, période ouverte."""
    from datetime import datetime as _dt
    
    # Vérifier période
    try:
        d = _dt.strptime(date, '%Y-%m-%d')
        annee = d.year; mois = d.month
    except:
        return None, "Date invalide"
    
    if not compta_periode_est_ouverte(annee, mois):
        return None, f"La période {annee}-{mois:02d} est CLÔTURÉE. Aucune écriture autorisée."
    
    # Vérifier débit = crédit
    total_debit = sum(float(l.get('debit', 0) or 0) for l in lignes)
    total_credit = sum(float(l.get('credit', 0) or 0) for l in lignes)
    if abs(total_debit - total_credit) > 0.01:
        return None, f"Écriture non équilibrée : Débit {total_debit:.2f} ≠ Crédit {total_credit:.2f}"
    
    if total_debit <= 0:
        return None, "Montant total nul"
    
    if len(lignes) < 2:
        return None, "Une écriture doit contenir au moins 2 lignes"
    
    conn = get_db()
    try:
        # Récupérer journal
        j = conn.execute("SELECT id, code FROM compta_journaux WHERE code=?", (journal_code,)).fetchone()
        if not j:
            conn.close(); return None, f"Journal '{journal_code}' inexistant"
        
        # Statut
        statut = 'valide' if validate else 'brouillard'
        numero_piece = None
        validated_at = None
        validated_by = None
        if validate:
            numero_piece = compta_get_next_numero(journal_code, annee, mois)
            validated_at = _dt.now().isoformat()
            validated_by = user_id
        
        # Insert en-tête
        conn.execute("""INSERT INTO compta_ecritures
            (journal_id, date, libelle, statut, numero_piece, piece_externe,
             total_debit, total_credit, validated_at, validated_by, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (j['id'], date, libelle, statut, numero_piece, piece_externe,
             total_debit, total_credit, validated_at, validated_by, user_id))
        eid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        # Insert lignes
        for ordre, l in enumerate(lignes):
            num = (l.get('compte_numero') or '').strip()
            cpt = conn.execute("SELECT id FROM compta_comptes WHERE numero=?", (num,)).fetchone()
            if not cpt:
                conn.rollback(); conn.close()
                return None, f"Compte '{num}' inexistant. Créez-le d'abord."
            conn.execute("""INSERT INTO compta_lignes
                (ecriture_id, compte_id, compte_numero, libelle, debit, credit,
                 tiers_id, tiers_type, ordre)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (eid, cpt['id'], num, l.get('libelle', libelle),
                 float(l.get('debit', 0) or 0), float(l.get('credit', 0) or 0),
                 l.get('tiers_id'), l.get('tiers_type'), ordre))
        
        conn.commit()
        conn.close()
        return eid, None
    except Exception as e:
        conn.rollback()
        conn.close()
        return None, f"Erreur DB: {e}"


def compta_valider_ecriture(ecriture_id, user_id):
    """Valide une écriture brouillard → statut valide + numéro de pièce."""
    from datetime import datetime as _dt
    conn = get_db()
    try:
        e = conn.execute("""SELECT e.*, j.code as journal_code FROM compta_ecritures e
            LEFT JOIN compta_journaux j ON e.journal_id=j.id WHERE e.id=?""",
            (ecriture_id,)).fetchone()
        if not e:
            conn.close(); return False, "Écriture introuvable"
        if e['statut'] == 'valide':
            conn.close(); return False, "Écriture déjà validée"
        
        # Vérif équilibre encore une fois
        if abs(float(e['total_debit'] or 0) - float(e['total_credit'] or 0)) > 0.01:
            conn.close(); return False, "Écriture non équilibrée"
        
        # Vérif période
        d = _dt.strptime(e['date'], '%Y-%m-%d')
        if not compta_periode_est_ouverte(d.year, d.month):
            conn.close(); return False, "Période clôturée"
        
        # Numéro de pièce
        numero = compta_get_next_numero(e['journal_code'], d.year, d.month)
        conn.execute("""UPDATE compta_ecritures SET statut='valide', numero_piece=?,
            validated_at=?, validated_by=? WHERE id=?""",
            (numero, _dt.now().isoformat(), user_id, ecriture_id))
        conn.commit()
        conn.close()
        return True, numero
    except Exception as ex:
        conn.close()
        return False, f"Erreur: {ex}"


def compta_cloturer_periode(annee, mois, user_id):
    """Clôture une période (admin)."""
    from datetime import datetime as _dt
    conn = get_db()
    try:
        # Vérifier qu'aucune écriture en brouillard sur cette période
        bad = conn.execute("""SELECT COUNT(*) FROM compta_ecritures
            WHERE statut='brouillard' AND date LIKE ?""",
            (f'{annee}-{mois:02d}%',)).fetchone()[0]
        if bad and bad > 0:
            conn.close()
            return False, f"❌ {bad} écriture(s) en brouillard sur cette période. Validez-les ou supprimez-les avant de clôturer."
        
        conn.execute("""INSERT INTO compta_periodes (annee, mois, statut, cloture_at, cloture_by)
            VALUES (?, ?, 'cloturee', ?, ?)
            ON CONFLICT(annee, mois) DO UPDATE SET statut='cloturee', cloture_at=excluded.cloture_at, cloture_by=excluded.cloture_by""",
            (annee, mois, _dt.now().isoformat(), user_id))
        conn.commit()
        conn.close()
        return True, f"Période {annee}-{mois:02d} clôturée"
    except Exception as e:
        conn.close()
        return False, f"Erreur: {e}"


def compta_balance(date_from=None, date_to=None):
    """Balance comptable : solde par compte sur la période."""
    conn = get_db()
    where = "WHERE e.statut='valide'"
    params = []
    if date_from:
        where += " AND e.date >= ?"; params.append(date_from)
    if date_to:
        where += " AND e.date <= ?"; params.append(date_to)
    
    rows = conn.execute(f"""SELECT
        c.numero, c.nom, c.type, c.classe,
        COALESCE(SUM(l.debit), 0) as total_debit,
        COALESCE(SUM(l.credit), 0) as total_credit
        FROM compta_lignes l
        JOIN compta_ecritures e ON l.ecriture_id=e.id
        JOIN compta_comptes c ON l.compte_id=c.id
        {where}
        GROUP BY c.id ORDER BY c.numero""", tuple(params)).fetchall()
    conn.close()
    
    balance = []
    for r in rows:
        d = float(r['total_debit'] or 0); cr = float(r['total_credit'] or 0)
        solde = d - cr
        balance.append({
            'numero': r['numero'], 'nom': r['nom'],
            'type': r['type'], 'classe': r['classe'],
            'total_debit': d, 'total_credit': cr,
            'solde_debiteur': max(0, solde),
            'solde_crediteur': max(0, -solde),
        })
    return balance


def compta_grand_livre(compte_numero, date_from=None, date_to=None):
    """Grand livre d'un compte : tous les mouvements détaillés."""
    conn = get_db()
    where = "WHERE c.numero=? AND e.statut='valide'"
    params = [compte_numero]
    if date_from:
        where += " AND e.date >= ?"; params.append(date_from)
    if date_to:
        where += " AND e.date <= ?"; params.append(date_to)
    
    rows = conn.execute(f"""SELECT
        e.date, e.numero_piece, e.libelle as e_libelle,
        l.libelle as l_libelle, l.debit, l.credit, l.lettrage_code,
        j.code as journal_code, e.id as ecriture_id, l.id as ligne_id
        FROM compta_lignes l
        JOIN compta_ecritures e ON l.ecriture_id=e.id
        JOIN compta_comptes c ON l.compte_id=c.id
        JOIN compta_journaux j ON e.journal_id=j.id
        {where}
        ORDER BY e.date, e.id, l.ordre""", tuple(params)).fetchall()
    conn.close()
    
    lignes = []
    cumul = 0
    for r in rows:
        d = dict(r)
        cumul += float(d.get('debit') or 0) - float(d.get('credit') or 0)
        d['cumul'] = cumul
        lignes.append(d)
    return lignes


def compta_compte_resultat(date_from=None, date_to=None):
    """Compte de résultat : charges (classe 6) vs produits (classe 7)."""
    conn = get_db()
    where = "WHERE e.statut='valide'"
    params = []
    if date_from:
        where += " AND e.date >= ?"; params.append(date_from)
    if date_to:
        where += " AND e.date <= ?"; params.append(date_to)
    
    charges = [dict(r) for r in conn.execute(f"""SELECT
        c.numero, c.nom,
        COALESCE(SUM(l.debit), 0) - COALESCE(SUM(l.credit), 0) as solde
        FROM compta_lignes l
        JOIN compta_ecritures e ON l.ecriture_id=e.id
        JOIN compta_comptes c ON l.compte_id=c.id
        {where} AND c.type='charge'
        GROUP BY c.id HAVING solde != 0 ORDER BY c.numero""",
        tuple(params)).fetchall()]
    
    produits = [dict(r) for r in conn.execute(f"""SELECT
        c.numero, c.nom,
        COALESCE(SUM(l.credit), 0) - COALESCE(SUM(l.debit), 0) as solde
        FROM compta_lignes l
        JOIN compta_ecritures e ON l.ecriture_id=e.id
        JOIN compta_comptes c ON l.compte_id=c.id
        {where} AND c.type='produit'
        GROUP BY c.id HAVING solde != 0 ORDER BY c.numero""",
        tuple(params)).fetchall()]
    
    conn.close()
    
    total_charges = sum(c['solde'] for c in charges)
    total_produits = sum(p['solde'] for p in produits)
    resultat = total_produits - total_charges
    
    return {
        'charges': charges, 'produits': produits,
        'total_charges': total_charges, 'total_produits': total_produits,
        'resultat': resultat,
        'beneficiaire': resultat >= 0,
    }


def compta_bilan(date_to=None):
    """Bilan : actif vs passif (cumul depuis le début)."""
    conn = get_db()
    where = "WHERE e.statut='valide'"
    params = []
    if date_to:
        where += " AND e.date <= ?"; params.append(date_to)
    
    actifs = [dict(r) for r in conn.execute(f"""SELECT
        c.numero, c.nom,
        COALESCE(SUM(l.debit), 0) - COALESCE(SUM(l.credit), 0) as solde
        FROM compta_lignes l
        JOIN compta_ecritures e ON l.ecriture_id=e.id
        JOIN compta_comptes c ON l.compte_id=c.id
        {where} AND c.type='actif'
        GROUP BY c.id HAVING solde > 0 ORDER BY c.numero""",
        tuple(params)).fetchall()]
    
    passifs = [dict(r) for r in conn.execute(f"""SELECT
        c.numero, c.nom,
        COALESCE(SUM(l.credit), 0) - COALESCE(SUM(l.debit), 0) as solde
        FROM compta_lignes l
        JOIN compta_ecritures e ON l.ecriture_id=e.id
        JOIN compta_comptes c ON l.compte_id=c.id
        {where} AND c.type='passif'
        GROUP BY c.id HAVING solde > 0 ORDER BY c.numero""",
        tuple(params)).fetchall()]
    
    conn.close()
    
    total_actif = sum(a['solde'] for a in actifs)
    total_passif = sum(p['solde'] for p in passifs)
    
    # Résultat de l'exercice = composante du passif
    res = compta_compte_resultat(None, date_to)
    resultat_exo = res['resultat']
    
    return {
        'actifs': actifs, 'passifs': passifs,
        'total_actif': total_actif, 'total_passif': total_passif,
        'resultat_exercice': resultat_exo,
        'equilibre': abs(total_actif - (total_passif + resultat_exo)) < 0.01,
    }


def compta_lettrer(ligne_ids, user_id):
    """Lie ensemble plusieurs lignes (lettrage : facture + paiement).
    Si la somme débit == somme crédit → solde, sinon partiel.
    Retourne (code_lettrage, error)."""
    from datetime import datetime as _dt
    if not ligne_ids or len(ligne_ids) < 2:
        return None, "Sélectionnez au moins 2 lignes"
    
    conn = get_db()
    try:
        # Vérifier qu'elles concernent toutes le même compte (lettrable)
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM compta_lignes WHERE id IN ({})".format(','.join('?'*len(ligne_ids))),
            tuple(ligne_ids)).fetchall()]
        if len(rows) != len(ligne_ids):
            conn.close(); return None, "Certaines lignes introuvables"
        
        comptes = set(r['compte_id'] for r in rows)
        if len(comptes) > 1:
            conn.close(); return None, "Toutes les lignes doivent concerner le même compte"
        
        cpt_id = list(comptes)[0]
        cpt = conn.execute("SELECT numero, is_lettrable FROM compta_comptes WHERE id=?", (cpt_id,)).fetchone()
        if not cpt['is_lettrable']:
            conn.close(); return None, f"Le compte {cpt['numero']} n'est pas lettrable"
        
        # Vérif qu'aucune n'est déjà lettrée
        deja = [r for r in rows if r.get('lettrage_code')]
        if deja:
            conn.close(); return None, f"{len(deja)} ligne(s) déjà lettrée(s)"
        
        total_debit = sum(float(r['debit'] or 0) for r in rows)
        total_credit = sum(float(r['credit'] or 0) for r in rows)
        diff = abs(total_debit - total_credit)
        statut = 'solde' if diff < 0.01 else 'partiel'
        
        # Générer code lettrage (LXXX par compte)
        code = f"L{datetime.now().strftime('%Y%m%d%H%M%S')[-8:]}"
        
        # Créer en-tête
        conn.execute("""INSERT INTO compta_lettrages
            (code, date, compte_id, statut, created_by)
            VALUES (?, ?, ?, ?, ?)""",
            (code, _dt.now().strftime('%Y-%m-%d'), cpt_id, statut, user_id))
        lid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        
        # Lier
        for r in rows:
            montant = float(r['debit'] or 0) + float(r['credit'] or 0)
            conn.execute("""INSERT INTO compta_lettrage_lignes (lettrage_id, ligne_id, montant)
                VALUES (?, ?, ?)""", (lid, r['id'], montant))
            # Marquer la ligne
            conn.execute("UPDATE compta_lignes SET lettrage_code=? WHERE id=?", (code, r['id']))
        
        conn.commit()
        conn.close()
        return code, None
    except Exception as e:
        conn.close()
        return None, f"Erreur: {e}"


# ======================== SERVICES TRÉSORERIE ========================

def tresorerie_enregistrer_mouvement(type_mvt, sens, source, date, montant, libelle,
                                     mode_paiement='espece', user_id=None,
                                     tiers_type=None, tiers_id=None, tiers_nom='',
                                     reference='', source_id=None,
                                     caisse_id=None, banque_id=None,
                                     cpt_contrepartie=None,
                                     auto_compta=True):
    """Enregistre un mouvement de trésorerie ET génère automatiquement
    l'écriture comptable correspondante (en brouillard).
    
    Args:
        type_mvt : 'caisse' | 'banque'
        sens : 'entree' | 'sortie'
        source : 'manuel' | 'caisse_sortie' | 'paiement_fournisseur' | 'encaissement_client' | 'virement' | ...
        montant : montant positif (toujours)
        cpt_contrepartie : numéro de compte SYSCOHADA (ex: '601' pour achat, '411' pour client...)
                           Si None : on déduit selon source/sens.
    
    Retourne (mouvement_id, ecriture_id, error).
    """
    from datetime import datetime as _dt
    if montant <= 0:
        return None, None, "Montant doit être positif"
    if sens not in ('entree', 'sortie'):
        return None, None, "Sens invalide (entree/sortie)"
    if type_mvt not in ('caisse', 'banque'):
        return None, None, "Type invalide (caisse/banque)"
    
    # Compte trésorerie selon type
    cpt_treso = '571' if type_mvt == 'caisse' else '512'
    journal = 'CAISSE' if type_mvt == 'caisse' else 'BANQUE'
    
    # Déduire le compte de contrepartie si non fourni
    if not cpt_contrepartie:
        if source == 'paiement_fournisseur':
            cpt_contrepartie = '401'
        elif source == 'encaissement_client':
            cpt_contrepartie = '411'
        elif sens == 'sortie':
            cpt_contrepartie = '605'  # autres achats par défaut
        elif sens == 'entree':
            cpt_contrepartie = '707'  # produits annexes par défaut
        else:
            cpt_contrepartie = '467'  # divers
    
    conn = get_db()
    try:
        # 1. Créer le mouvement
        conn.execute("""INSERT INTO tresorerie_mouvements
            (type, sens, source, source_id, date, montant, libelle,
             tiers_type, tiers_id, tiers_nom, mode_paiement, reference,
             caisse_id, banque_id, statut, created_by)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (type_mvt, sens, source, source_id, date, montant, libelle,
             tiers_type, tiers_id, tiers_nom or '', mode_paiement, reference or '',
             caisse_id, banque_id, 'enregistre', user_id))
        mvt_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()
        conn.close()
    except Exception as e:
        try: conn.close()
        except: pass
        return None, None, f"Erreur DB mouvement: {e}"
    
    # 2. Générer l'écriture comptable (en brouillard)
    eid = None
    if auto_compta:
        try:
            # Sens entrée → débit trésorerie / crédit contrepartie
            # Sens sortie → débit contrepartie / crédit trésorerie
            if sens == 'entree':
                lignes = [
                    {'compte_numero': cpt_treso, 'libelle': libelle, 'debit': montant, 'credit': 0,
                     'tiers_id': tiers_id, 'tiers_type': tiers_type},
                    {'compte_numero': cpt_contrepartie, 'libelle': libelle, 'debit': 0, 'credit': montant,
                     'tiers_id': tiers_id, 'tiers_type': tiers_type},
                ]
            else:
                lignes = [
                    {'compte_numero': cpt_contrepartie, 'libelle': libelle, 'debit': montant, 'credit': 0,
                     'tiers_id': tiers_id, 'tiers_type': tiers_type},
                    {'compte_numero': cpt_treso, 'libelle': libelle, 'debit': 0, 'credit': montant,
                     'tiers_id': tiers_id, 'tiers_type': tiers_type},
                ]
            
            piece_externe = f"{source.upper()}-{mvt_id}"
            eid, err = compta_creer_ecriture(journal, date, libelle, lignes,
                                            user_id=user_id or 1, validate=False,
                                            piece_externe=piece_externe)
            if eid:
                # Lier l'écriture au mouvement
                conn = get_db()
                conn.execute("UPDATE tresorerie_mouvements SET ecriture_id=? WHERE id=?",
                            (eid, mvt_id))
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"compta auto: {e}")
    
    return mvt_id, eid, None


def tresorerie_solde_caisse(caisse_id=None, date_to=None):
    """Calcule le solde caisse : entrées - sorties depuis le début (ou jusqu'à date_to)."""
    conn = get_db()
    where = "WHERE type='caisse'"
    params = []
    if caisse_id:
        where += " AND caisse_id=?"; params.append(caisse_id)
    if date_to:
        where += " AND date<=?"; params.append(date_to)
    
    try:
        entrees = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM tresorerie_mouvements {where} AND sens='entree'", tuple(params)).fetchone()[0] or 0
        sorties = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM tresorerie_mouvements {where} AND sens='sortie'", tuple(params)).fetchone()[0] or 0
    except: entrees = sorties = 0
    conn.close()
    return float(entrees) - float(sorties)


def tresorerie_solde_banque(banque_id=None, date_to=None):
    """Calcule le solde banque (intégrant solde initial)."""
    conn = get_db()
    where = "WHERE type='banque'"
    params = []
    if banque_id:
        where += " AND banque_id=?"; params.append(banque_id)
    if date_to:
        where += " AND date<=?"; params.append(date_to)
    
    try:
        entrees = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM tresorerie_mouvements {where} AND sens='entree'", tuple(params)).fetchone()[0] or 0
        sorties = conn.execute(f"SELECT COALESCE(SUM(montant),0) FROM tresorerie_mouvements {where} AND sens='sortie'", tuple(params)).fetchone()[0] or 0
        # Soldes initiaux
        if banque_id:
            si = conn.execute("SELECT COALESCE(solde_initial,0) FROM tresorerie_comptes_bancaires WHERE id=?", (banque_id,)).fetchone()
            si = float(si[0]) if si else 0
        else:
            si = conn.execute("SELECT COALESCE(SUM(solde_initial),0) FROM tresorerie_comptes_bancaires WHERE COALESCE(is_active,1)=1").fetchone()[0] or 0
            si = float(si)
    except: entrees = sorties = si = 0
    conn.close()
    return si + float(entrees) - float(sorties)


def get_purchase_status(purchase_id):
    """Retourne (status, total_paye, reste, pct_paye) pour un achat."""
    conn = get_db()
    try:
        purchase = conn.execute("SELECT montant_total FROM supplier_purchases WHERE id=?",
                               (purchase_id,)).fetchone()
        if not purchase:
            conn.close(); return ('inconnu', 0, 0, 0)
        total = float(purchase['montant_total'] or 0)
        paid = conn.execute("SELECT COALESCE(SUM(montant), 0) FROM supplier_payments WHERE purchase_id=?",
                           (purchase_id,)).fetchone()[0] or 0
        paid = float(paid)
        reste = max(0, total - paid)
        pct = (paid / total * 100) if total > 0 else 0
        if paid <= 0: status = 'non_paye'
        elif paid >= total: status = 'solde'
        else: status = 'partiel'
        conn.close()
        return (status, paid, reste, pct)
    except Exception:
        conn.close()
        return ('inconnu', 0, 0, 0)


def get_supplier_summary(supplier_id):
    """Retourne le total des achats, total payé, reste pour un fournisseur."""
    conn = get_db()
    try:
        total_purch = conn.execute(
            "SELECT COALESCE(SUM(montant_total), 0) FROM supplier_purchases WHERE supplier_id=?",
            (supplier_id,)).fetchone()[0] or 0
        total_paid = conn.execute("""SELECT COALESCE(SUM(sp.montant), 0)
            FROM supplier_payments sp JOIN supplier_purchases p ON sp.purchase_id=p.id
            WHERE p.supplier_id=?""", (supplier_id,)).fetchone()[0] or 0
        conn.close()
        return {
            'total_purchases': float(total_purch),
            'total_paid': float(total_paid),
            'reste': max(0, float(total_purch) - float(total_paid)),
            'pct_paid': (float(total_paid) / float(total_purch) * 100) if total_purch else 0,
        }
    except Exception:
        conn.close()
        return {'total_purchases': 0, 'total_paid': 0, 'reste': 0, 'pct_paid': 0}


def compute_penalty(ecart_minutes, penalty_per_minute=0, grace_minutes=0):
    """Calcule la pénalité en F CFA pour un retard donné."""
    if ecart_minutes <= 0 or penalty_per_minute <= 0:
        return 0
    if ecart_minutes <= grace_minutes:
        return 0
    billable = ecart_minutes - grace_minutes
    return round(billable * penalty_per_minute, 2)


def get_company_penalty_config(company_id=None):
    """Récupère la config pénalité (de l'entreprise multi ou la config globale)."""
    conn = get_db()
    if company_id:
        row = conn.execute(
            "SELECT penalty_per_minute, grace_minutes FROM pointage_companies WHERE id=?",
            (company_id,)).fetchone()
        if row:
            conn.close()
            return {'penalty_per_minute': row['penalty_per_minute'] or 0,
                    'grace_minutes': row['grace_minutes'] or 0}
    # Sinon config globale
    try:
        row = conn.execute(
            "SELECT penalty_per_minute, grace_minutes FROM hr_penalty_config WHERE company_id IS NULL AND COALESCE(is_active,1)=1 LIMIT 1").fetchone()
        if row:
            conn.close()
            return {'penalty_per_minute': row['penalty_per_minute'] or 0,
                    'grace_minutes': row['grace_minutes'] or 0}
    except: pass
    conn.close()
    return {'penalty_per_minute': 0, 'grace_minutes': 0}


def is_module_active(module_key):
    """Vérifie si un module RH est activé."""
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT is_active FROM hr_modules WHERE module_key=?", (module_key,)).fetchone()
        conn.close()
        return bool(row and row['is_active'])
    except:
        conn.close()
        return False


def send_email_smtp(to_email, subject, body, attachments=None):
    """Envoie un email via SMTP. Utilise les variables d'env :
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM, SMTP_USE_TLS.
    Retourne True/False."""
    import os, smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders
    
    host = os.environ.get('SMTP_HOST', '')
    port = int(os.environ.get('SMTP_PORT', '587') or 587)
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_pwd = os.environ.get('SMTP_PASSWORD', '')
    smtp_from = os.environ.get('SMTP_FROM', smtp_user or 'noreply@ramyaci.tech')
    use_tls = os.environ.get('SMTP_USE_TLS', '1') in ('1', 'true', 'True')
    
    if not host or not smtp_user:
        return False  # Pas configuré
    
    try:
        msg = MIMEMultipart()
        msg['From'] = smtp_from
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # Pièces jointes
        if attachments:
            for filename, data, mime_type in attachments:
                part = MIMEBase(*mime_type.split('/'))
                part.set_payload(data)
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)
        
        with smtplib.SMTP(host, port, timeout=15) as server:
            server.ehlo()
            if use_tls:
                server.starttls()
                server.ehlo()
            server.login(smtp_user, smtp_pwd)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"send_email_smtp error: {e}")
        return False


def detect_absences_today():
    """Détecte les employés sans pointage 'arrivee' au-delà de leur horaire prévu.
    Retourne la liste des absents non encore notifiés ce jour."""
    today = datetime.now().strftime('%Y-%m-%d')
    now_time = datetime.now().strftime('%H:%M')
    conn = get_db()
    absent_users = []
    try:
        # Joindre planning + users actifs, exclure clients
        rows = conn.execute("""
            SELECT u.id, u.full_name, u.email, u.role, u.department,
                   COALESCE(p.heure_arrivee, '08:00') as h_arrivee,
                   COALESCE(p.tolerance_retard_minutes, 10) as tol
            FROM users u
            LEFT JOIN hr_planning p ON p.user_id=u.id AND COALESCE(p.is_active,1)=1
            WHERE COALESCE(u.is_active,1)=1
              AND u.role != 'client'
              AND NOT EXISTS (
                  SELECT 1 FROM hr_pointages hp
                  WHERE hp.user_id=u.id AND hp.date=? AND hp.type='arrivee'
              )
        """, (today,)).fetchall()
        
        for r in rows:
            # Déjà notifié aujourd'hui ?
            already = conn.execute(
                "SELECT 1 FROM hr_absence_alerts WHERE user_id=? AND date=? AND alert_type='no_arrival'",
                (r['id'], today)).fetchone()
            if already: continue
            
            # Heure prévue + tolérance + 30 min → on considère absent
            try:
                eh, em = r['h_arrivee'].split(':')[:2]
                expected_min = int(eh)*60 + int(em)
                cutoff_min = expected_min + int(r['tol']) + 30
                nh, nm = now_time.split(':')[:2]
                now_min = int(nh)*60 + int(nm)
                if now_min >= cutoff_min:
                    absent_users.append(dict(r))
                    # Marquer notifié (pour ne pas spammer)
                    conn.execute("""INSERT OR IGNORE INTO hr_absence_alerts
                        (user_id, date, alert_type) VALUES (?, ?, 'no_arrival')""",
                        (r['id'], today))
            except: pass
        conn.commit()
    except Exception as e:
        print(f"detect_absences_today: {e}")
    conn.close()
    return absent_users


def get_today_pointages(user_id, date_str=None):
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')
    conn = get_db()
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM hr_pointages WHERE user_id=? AND date=? ORDER BY datetime_full",
        (user_id, date_str)).fetchall()]
    conn.close()
    return rows


def get_user_planning(user_id):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM hr_planning WHERE user_id=? AND COALESCE(is_active,1)=1 LIMIT 1",
        (user_id,)).fetchone()
    conn.close()
    if row: return dict(row)
    return {'heure_arrivee': '08:00', 'heure_pause': '12:00',
            'heure_retour': '13:00', 'heure_depart': '17:00',
            'tolerance_retard_minutes': 10}


def can_pointer(user_id, type_action, date_str=None):
    """Retourne (bool, message). Applique l'ordre strict :
    1. Arrivée OBLIGATOIRE en premier (pas de pause/retour/départ sans arrivée).
    2. Retour OBLIGATOIRE après pause (pas de retour sans pause).
    3. Si pause faite, départ exige retour (pas de départ sans retour de pause).
    4. Pas de doublon le même jour."""
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')
    today_p = get_today_pointages(user_id, date_str)
    types_done = [p['type'] for p in today_p]
    
    labels = {'arrivee':"l'arrivée", 'pause':"le début de pause",
              'retour':"le retour de pause", 'depart':"le départ"}
    
    # Règle 0 : type connu
    if type_action not in ('arrivee', 'pause', 'retour', 'depart'):
        return False, "Type de pointage inconnu."
    
    # Règle 1 : pas de doublon
    if type_action in types_done:
        return False, f"❌ Vous avez déjà pointé {labels[type_action]} aujourd'hui."
    
    # Règle 2 : ARRIVÉE obligatoire avant tout
    if type_action != 'arrivee' and 'arrivee' not in types_done:
        return False, f"⛔ Vous devez d'abord pointer votre ARRIVÉE avant de pointer {labels[type_action]}."
    
    # Règle 3 : RETOUR exige PAUSE faite avant
    if type_action == 'retour' and 'pause' not in types_done:
        return False, "⛔ Vous devez d'abord pointer le DÉBUT DE PAUSE avant le retour de pause."
    
    # Règle 4 : si PAUSE faite mais pas RETOUR, on ne peut pas DÉPART (oblige à pointer retour avant)
    if type_action == 'depart' and 'pause' in types_done and 'retour' not in types_done:
        return False, "⛔ Vous êtes en pause. Pointez d'abord le RETOUR DE PAUSE avant votre départ."
    
    # Règle 5 : ordre PAUSE après ARRIVÉE (déjà couvert par règle 2)
    
    return True, "OK"


def compute_pointage_status(user_id, type_action, time_str, planning=None):
    if not planning:
        planning = get_user_planning(user_id)
    expected_field = {'arrivee':'heure_arrivee','pause':'heure_pause',
                      'retour':'heure_retour','depart':'heure_depart'}.get(type_action)
    if not expected_field: return 'normal', 0
    expected = planning.get(expected_field, '08:00')
    tolerance = int(planning.get('tolerance_retard_minutes', 10) or 10)
    try:
        eh, em = expected.split(':')[:2]
        ah, am = time_str.split(':')[:2]
        ecart = (int(ah)*60 + int(am)) - (int(eh)*60 + int(em))
        if type_action in ('arrivee', 'retour'):
            if ecart > tolerance: return 'retard', ecart
            elif ecart < -tolerance: return 'avance', ecart
        else:
            if ecart < -tolerance: return 'avance', ecart
            elif ecart > tolerance: return 'retard', ecart
        return 'normal', ecart
    except Exception:
        return 'normal', 0


def compute_work_duration(user_id, date_str=None):
    points = get_today_pointages(user_id, date_str)
    pts = {p['type']: p['datetime_full'] for p in points}
    work_min = 0; pause_min = 0
    def _d(a, b):
        try:
            return int((datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds() / 60)
        except: return 0
    if 'arrivee' in pts and 'depart' in pts:
        total = _d(pts['arrivee'], pts['depart'])
        if 'pause' in pts and 'retour' in pts:
            pause_min = _d(pts['pause'], pts['retour'])
            work_min = total - pause_min
        else:
            work_min = total
    elif 'arrivee' in pts and 'pause' in pts and 'retour' not in pts:
        work_min = _d(pts['arrivee'], pts['pause'])
    elif 'arrivee' in pts and 'pause' not in pts:
        work_min = _d(pts['arrivee'], datetime.now().isoformat())
    elif 'arrivee' in pts and 'retour' in pts and 'depart' not in pts:
        if 'pause' in pts:
            pause_min = _d(pts['pause'], pts['retour'])
            work_min = _d(pts['arrivee'], pts['pause']) + _d(pts['retour'], datetime.now().isoformat())
    return max(0, work_min), max(0, pause_min)


def haversine_meters(lat1, lon1, lat2, lon2):
    import math
    R = 6371000
    phi1 = math.radians(lat1); phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1); dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def is_in_authorized_zone(latitude, longitude):
    if latitude is None or longitude is None:
        return None, None
    conn = get_db()
    zones = [dict(r) for r in conn.execute(
        "SELECT * FROM hr_zones_pointage WHERE COALESCE(is_active,1)=1").fetchall()]
    conn.close()
    if not zones: return True, None
    for z in zones:
        d = haversine_meters(latitude, longitude, z['latitude'], z['longitude'])
        if d <= z['radius_meters']:
            return True, z['name']
    return False, None


def log_security_event(event_type, user_id=None, username=None, ip=None,
                       user_agent=None, path=None, details=None, severity='info'):
    """Helper pour journaliser un événement de sécurité."""
    conn = get_db()
    try:
        conn.execute("""INSERT INTO security_logs
            (event_type, user_id, username, ip, user_agent, path, details, severity)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (event_type, user_id, username, ip, (user_agent or '')[:500],
             path, (details or '')[:1000], severity))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()



    """Recalcule le montant dépensé d'un budget en sommant les caisse_sorties liées."""
    conn = get_db()
    try:
        b = conn.execute("SELECT department, period_start, period_end FROM dept_budgets WHERE id=?",
                         (budget_id,)).fetchone()
        if not b:
            conn.close(); return False
        # Somme des dépenses du département dans la période (status validé seulement)
        if b['period_start'] and b['period_end']:
            row = conn.execute("""SELECT COALESCE(SUM(montant), 0) as t FROM caisse_sorties
                WHERE department=? AND date >= ? AND date <= ?
                AND status IN ('valide', 'comptabilise')""",
                (b['department'], b['period_start'], b['period_end'])).fetchone()
        else:
            row = conn.execute("""SELECT COALESCE(SUM(montant), 0) as t FROM caisse_sorties
                WHERE department=? AND status IN ('valide', 'comptabilise')""",
                               (b['department'],)).fetchone()
        total = float(row['t'] or 0)
        conn.execute("UPDATE dept_budgets SET amount_spent=?, updated_at=? WHERE id=?",
                     (total, datetime.now().isoformat(), budget_id))
        conn.commit()
        return True
    except Exception:
        conn.rollback(); return False
    finally:
        conn.close()


def update_equipment_health(equipment_id, health_status, health_note='', intervention_id=None):
    """Met à jour l'état de santé d'un équipement suite à une intervention."""
    score_map = {'excellent': 100, 'ok': 80, 'attention': 50, 'panne': 20, 'hors_service': 0}
    score = score_map.get(health_status, 50)
    conn = get_db()
    try:
        conn.execute("""UPDATE client_equipments SET
            health_status=?, health_score=?, last_status_note=?,
            last_intervention_id=?, last_intervention_at=?, updated_at=?
            WHERE id=?""",
            (health_status, score, (health_note or '')[:500],
             intervention_id, datetime.now().isoformat(),
             datetime.now().isoformat(), equipment_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False
    finally:
        conn.close()


def recompute_budget_spent(budget_id):
    """Recalcule le montant dépensé d'un budget en sommant les caisse_sorties validées."""
    conn = get_db()
    try:
        b = conn.execute("SELECT department, period_start, period_end FROM dept_budgets WHERE id=?",
                         (budget_id,)).fetchone()
        if not b:
            conn.close(); return False
        if b['period_start'] and b['period_end']:
            row = conn.execute("""SELECT COALESCE(SUM(montant), 0) as t FROM caisse_sorties
                WHERE department=? AND date >= ? AND date <= ?
                AND status IN ('valide', 'comptabilise')""",
                (b['department'], b['period_start'], b['period_end'])).fetchone()
        else:
            row = conn.execute("""SELECT COALESCE(SUM(montant), 0) as t FROM caisse_sorties
                WHERE department=? AND status IN ('valide', 'comptabilise')""",
                               (b['department'],)).fetchone()
        total = float(row['t'] or 0)
        conn.execute("UPDATE dept_budgets SET amount_spent=?, updated_at=? WHERE id=?",
                     (total, datetime.now().isoformat(), budget_id))
        conn.commit()
        return True
    except Exception:
        conn.rollback(); return False
    finally:
        conn.close()
