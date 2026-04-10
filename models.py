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
        'admin': ['traitement', 'fichiers', 'clients', 'clients_edit', 'admin', 'dashboard', 'dashboard_general', 'envoyer', 'logs', 'contrats', 'comptabilite', 'comptabilite_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit', 'moyens_generaux', 'moyens_generaux_edit', 'informatique', 'projets', 'caisse_sortie', 'rapports_j', 'convertir_devis', 'resp_projet', 'resp_projet_edit', 'centre_technique', 'centre_technique_edit', 'chat', 'tracking'],
        'dg': ['traitement', 'fichiers', 'clients', 'clients_edit', 'admin', 'dashboard', 'dashboard_general', 'envoyer', 'logs', 'contrats', 'comptabilite', 'comptabilite_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit', 'moyens_generaux', 'moyens_generaux_edit', 'informatique', 'projets', 'caisse_sortie', 'rapports_j', 'convertir_devis', 'resp_projet', 'resp_projet_edit', 'centre_technique', 'centre_technique_edit', 'chat', 'tracking'],
        'rh': ['fichiers', 'clients', 'dashboard', 'envoyer', 'contrats', 'rapports_j', 'chat'],
        'technicien': ['traitement', 'dashboard', 'visites', 'rapports_j', 'centre_technique', 'chat'],
        'commercial': ['dashboard', 'clients', 'clients_edit', 'visites', 'visites_edit', 'proforma', 'proforma_edit', 'contrats', 'rapports_j', 'chat'],
        'comptable': ['dashboard', 'comptabilite', 'comptabilite_edit', 'clients', 'caisse_sortie', 'rapports_j', 'convertir_devis', 'chat'],
        'moyens_generaux': ['dashboard', 'moyens_generaux', 'moyens_generaux_edit', 'clients', 'rapports_j', 'chat'],
        'informatique': ['dashboard', 'informatique', 'traitement', 'visites', 'projets', 'rapports_j', 'centre_technique', 'chat'],
        'resp_projet': ['dashboard', 'resp_projet', 'resp_projet_edit', 'clients', 'rapports_j', 'proforma', 'chat'],
        'gestionnaire_projet': ['dashboard', 'resp_projet', 'resp_projet_edit', 'clients', 'clients_edit', 'rapports_j', 'proforma', 'proforma_edit', 'visites', 'centre_technique', 'chat'],
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

def create_client(name, tel='', email='', contact_name='', address='', notes='', created_by=None):
    conn = get_db()
    conn.execute("""
        INSERT INTO clients (name, tel, email, contact_name, address, notes, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (name, tel, email, contact_name, address, notes, created_by))
    conn.commit()
    client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
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
    conn = get_db()
    conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
    conn.commit()
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

def create_devis(**kwargs):
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
        e.code_rh, e.gender
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
            conn.execute("INSERT INTO plan_comptable (numero, libelle, type, categorie, classe) VALUES (?,?,?,?,?)",
                (num, lib, typ, cat, cls))
    
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
