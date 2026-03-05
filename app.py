#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAMYA TECHNOLOGIE & INNOVATION
Application Web v3 — Gestion des Rapports de Pointage
Auth + Rôles + Dashboard + Clients + Fichiers RH
"""

import os, uuid, shutil, functools
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from flask import (Flask, render_template, request, send_file, flash,
                   redirect, url_for, jsonify, session, send_from_directory)
from werkzeug.utils import secure_filename

from rapport_core import extract_from_excel, generate_full_pdf
from merge_presence import generate_presence_xlsx
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
                    get_job_by_id, get_db_path)

app = Flask(__name__, template_folder=BASE_DIR, static_folder=BASE_DIR, static_url_path='/static')
app.secret_key = os.environ.get('SECRET_KEY', 'ramya-tech-2026-secret-v3')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['UPLOAD_FOLDER'] = os.path.join(BASE_DIR, 'uploads')
app.config['FILES_FOLDER'] = os.path.join(BASE_DIR, 'files')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['FILES_FOLDER'], exist_ok=True)

init_db()

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
ALL_PERMISSIONS = ['traitement', 'fichiers', 'clients', 'admin', 'dashboard', 'envoyer', 'logs']

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
    ctx = {'current_user': None, 'permissions': [], 'pending_count': 0}
    if 'user_id' in session:
        user = get_user_by_id(session['user_id'])
        if user:
            ctx['current_user'] = user
            ctx['permissions'] = get_role_permissions(user['role'])
            ctx['pending_count'] = len(get_jobs_by_status('traite'))
    return ctx


# ======================== AUTH ROUTES ========================

@app.route('/')
def welcome():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return render_template('welcome.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user = authenticate_user(request.form['username'], request.form['password'])
        if user:
            session['user_id'] = user['id']
            log_activity(user['id'], user['full_name'], 'Connexion', 
                        f"Connexion réussie", request.remote_addr)
            flash(f"Bienvenue {user['full_name']} !", "success")
            return redirect(url_for('dashboard'))
        flash("Identifiants incorrects", "error")
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        pwd = request.form['password']
        pwd2 = request.form['password2']
        if pwd != pwd2:
            flash("Les mots de passe ne correspondent pas", "error")
            return render_template('register.html')
        if len(pwd) < 6:
            flash("Le mot de passe doit faire au moins 6 caractères", "error")
            return render_template('register.html')
        ok, msg = create_user(
            request.form['username'], request.form['email'],
            pwd, request.form['full_name'], 'technicien'
        )
        if ok:
            flash("Compte créé ! Vous pouvez vous connecter.", "success")
            return redirect(url_for('login'))
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
    stats = get_dashboard_stats()
    return render_template('dashboard.html', page='dashboard', stats=stats)


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
        return jsonify({"success": True, "merge_id": merge_id, "client": result['client'],
            "employees": result['employees'], "rows": result['rows'], "filename": 'Presence_fusionnee.xlsx'})
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
            for n in ['logo_ramya_ROIND.png', 'logo.png']:
                c = os.path.join(BASE_DIR, n)
                if os.path.exists(c):
                    logo_path = os.path.join(job_dir, n)
                    shutil.copy2(c, logo_path)
                    break
        
        emps, detected_client = extract_from_excel(xlsx_path)
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
        pdf_name = f"{base}_RAPPORT_COMPLET.pdf"
        output_path = os.path.join(job_dir, pdf_name)
        
        generate_full_pdf(emps, output_path, provider_name, provider_info,
                         client_name, period, logo_path, hp=hp, client_info=client_info,
                         work_dir=job_dir)
        
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
        hp_text = f"{hp}h/jour" if hp > 0 else "Auto"
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

@app.route('/fichiers/marquer/<job_id>')
@permission_required('envoyer')
def fichiers_marquer(job_id):
    mark_job_sent(job_id, session['user_id'])
    user = get_user_by_id(session['user_id'])
    log_activity(session['user_id'], user['full_name'] if user else '?',
                'Envoi', f"Rapport {job_id} marqué comme envoyé", request.remote_addr)
    flash("Fichier marqué comme envoyé", "success")
    return redirect(url_for('fichiers'))


# ======================== CLIENTS ========================

@app.route('/clients')
@permission_required('clients')
def clients_page():
    clients = get_all_clients()
    return render_template('clients.html', page='clients', clients=clients)

@app.route('/clients/add', methods=['POST'])
@permission_required('clients')
def clients_add():
    create_client(
        request.form['name'], request.form.get('tel', ''),
        request.form.get('email', ''), request.form.get('contact_name', ''),
        request.form.get('address', ''), request.form.get('notes', ''),
        session['user_id']
    )
    flash("Client ajouté", "success")
    return redirect(url_for('clients_page'))

@app.route('/clients/edit/<int:cid>', methods=['GET', 'POST'])
@permission_required('clients')
def clients_edit(cid):
    client = get_client_by_id(cid)
    if not client:
        flash("Client non trouvé", "error")
        return redirect(url_for('clients_page'))
    if request.method == 'POST':
        update_client(cid, name=request.form['name'], tel=request.form.get('tel', ''),
                      email=request.form.get('email', ''), contact_name=request.form.get('contact_name', ''),
                      address=request.form.get('address', ''), notes=request.form.get('notes', ''))
        flash("Client modifié", "success")
        return redirect(url_for('clients_page'))
    return render_template('edit_client.html', page='clients', client=client)

@app.route('/clients/delete/<int:cid>')
@permission_required('clients')
def clients_delete(cid):
    delete_client(cid)
    flash("Client supprimé", "success")
    return redirect(url_for('clients_page'))


# ======================== ADMIN ========================

@app.route('/admin')
@permission_required('admin')
def admin_page():
    users = get_all_users()
    stats = get_dashboard_stats()
    role_perms = {r: get_role_permissions(r) for r in ['admin', 'rh', 'technicien']}
    return render_template('admin.html', page='admin', users=users, stats=stats,
                          all_permissions=ALL_PERMISSIONS, role_perms=role_perms)

@app.route('/admin/add', methods=['POST'])
@permission_required('admin')
def admin_add_user():
    ok, msg = create_user(
        request.form['username'], request.form['email'],
        request.form['password'], request.form['full_name'],
        request.form.get('role', 'technicien')
    )
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
    return render_template('edit_user.html', page='admin', edit_user=u)

@app.route('/admin/toggle/<int:uid>')
@permission_required('admin')
def admin_toggle_user(uid):
    u = get_user_by_id(uid)
    if u and u['role'] != 'admin':
        update_user(uid, is_active=0 if u['is_active'] else 1)
        flash(f"Utilisateur {'désactivé' if u['is_active'] else 'activé'}", "success")
    return redirect(url_for('admin_page'))

@app.route('/admin/permissions', methods=['POST'])
@permission_required('admin')
def admin_permissions():
    for role in ['rh', 'technicien']:
        perms = [p for p in ALL_PERMISSIONS if request.form.get(f'{role}_{p}')]
        update_role_permissions(role, perms)
    # Admin always has all
    update_role_permissions('admin', ALL_PERMISSIONS)
    flash("Permissions mises à jour", "success")
    return redirect(url_for('admin_page'))


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
        "name": "RAMYA Rapport de Pointage",
        "short_name": "RAMYA",
        "start_url": "/dashboard",
        "display": "standalone",
        "background_color": "#0d3b36",
        "theme_color": "#1a7a6d",
        "icons": [
            {"src": "/static/logo_ramya_ROIND.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/logo_ramya_ROIND.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return jsonify(manifest)

@app.route('/sw.js')
def service_worker():
    return app.send_static_file('sw.js') if os.path.exists(os.path.join(BASE_DIR, 'sw.js')) else ('', 204)


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


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
