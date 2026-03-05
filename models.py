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

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'ramya.db')


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
    ''')
    
    # Permissions par défaut — ajouter 'logs' pour admin
    default_perms = {
        'admin': ['traitement', 'fichiers', 'clients', 'admin', 'dashboard', 'envoyer', 'logs'],
        'rh': ['fichiers', 'clients', 'dashboard', 'envoyer'],
        'technicien': ['traitement', 'dashboard'],
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
