#!/usr/bin/env python3
"""Routes des modules WannyGest — Projets, CRM, Stock, Trésorerie, etc."""

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, current_app
import os
from datetime import datetime
from models import (db_insert, db_get_all, db_get_by_id, db_update, db_delete, db_count, db_sum, get_db,
                    get_user_by_id, get_all_clients, get_all_users, log_activity,
                    has_permission, get_role_permissions, log_audit)
from functools import wraps

modules_bp = Blueprint('modules', __name__)

def login_required(f):
    @wraps(f)
    def dec(*a, **kw):
        if 'user_id' not in session: return redirect('/login')
        return f(*a, **kw)
    return dec

def perm_required(perm):
    def dec(f):
        @wraps(f)
        def inner(*a, **kw):
            if 'user_id' not in session: return redirect('/login')
            u = get_user_by_id(session['user_id'])
            if not u or not has_permission(u['role'], perm):
                flash("Accès non autorisé", "error"); return redirect('/dashboard')
            return f(*a, **kw)
        return inner
    return dec


# ======================== PROJETS ========================

@modules_bp.route('/projets')
@login_required
def projets():
    projects = db_get_all('projects')
    stats = {s: db_count('projects', {'status': s}) for s in ['non_commence','en_cours','en_attente','termine']}
    return render_template('mod_projets.html', page='projets', projects=projects, stats=stats)

@modules_bp.route('/projets/add', methods=['POST'])
@login_required
def projets_add():
    db_insert('projects', name=request.form['name'],
        client_id=int(request.form['client_id']) if request.form.get('client_id') else None,
        description=request.form.get('description',''), status=request.form.get('status','non_commence'),
        priority=request.form.get('priority','moyenne'), start_date=request.form.get('start_date',''),
        end_date=request.form.get('end_date',''), budget=float(request.form.get('budget',0) or 0),
        manager_id=int(request.form.get('manager_id',0) or 0) or None, created_by=session['user_id'])
    flash("Projet créé", "success"); return redirect(url_for('modules.projets'))

@modules_bp.route('/projets/<int:pid>/status/<status>')
@login_required
def projets_status(pid, status):
    if status in ('non_commence','en_cours','en_attente','annule','termine'):
        db_update('projects', pid, status=status); flash("Statut mis à jour", "success")
    return redirect(url_for('modules.projets'))


# ======================== TÂCHES ========================

@modules_bp.route('/taches')
@login_required
def taches():
    user_id = session['user_id']
    user = get_user_by_id(user_id)
    if user and user['role'] == 'admin':
        tasks = db_get_all('tasks')
    else:
        tasks = db_get_all('tasks', where={'assigned_to': user_id})
    return render_template('mod_taches.html', page='taches', tasks=tasks, users=get_all_users())

@modules_bp.route('/taches/add', methods=['POST'])
@login_required
def taches_add():
    db_insert('tasks', title=request.form['title'], project_id=int(request.form.get('project_id',0) or 0) or None,
        description=request.form.get('description',''), assigned_to=int(request.form.get('assigned_to',0) or 0) or session['user_id'],
        priority=request.form.get('priority','moyenne'), due_date=request.form.get('due_date',''), created_by=session['user_id'])
    flash("Tâche créée", "success"); return redirect(url_for('modules.taches'))

@modules_bp.route('/taches/<int:tid>/status/<status>')
@login_required
def taches_status(tid, status):
    if status in ('a_faire','en_cours','termine'):
        db_update('tasks', tid, status=status)
    return redirect(url_for('modules.taches'))


# ======================== PROSPECTS / CRM ========================

@modules_bp.route('/prospects')
@perm_required('clients')
def prospects():
    all_p = db_get_all('prospects')
    stats = {s: db_count('prospects', {'status': s}) for s in ['nouveau','contacte','qualifie','proposition','gagne','perdu']}
    stats['valeur'] = db_sum('prospects', 'estimated_value', {'status': 'gagne'})
    return render_template('mod_prospects.html', page='prospects', prospects=all_p, stats=stats, users=get_all_users())

@modules_bp.route('/prospects/add', methods=['POST'])
@perm_required('clients')
def prospects_add():
    db_insert('prospects', company=request.form.get('company', request.form.get('contact_name','')),
        contact_name=request.form.get('contact_name',''),
        tel=request.form.get('tel',''), email=request.form.get('email',''), source=request.form.get('source',''),
        estimated_value=float(request.form.get('estimated_value',0) or 0),
        notes=request.form.get('notes',''), description=request.form.get('description',''),
        position=request.form.get('position',''), city=request.form.get('city',''),
        region=request.form.get('region',''), assigned_to=request.form.get('assigned_to',''),
        created_by=session['user_id'])
    flash("Prospect ajouté", "success"); return redirect(url_for('modules.prospects'))

@modules_bp.route('/prospects/edit/<int:pid>', methods=['POST'])
@perm_required('clients')
def prospects_edit(pid):
    db_update('prospects', pid, contact_name=request.form.get('contact_name',''),
        company=request.form.get('company',''), tel=request.form.get('tel',''),
        email=request.form.get('email',''), source=request.form.get('source',''),
        status=request.form.get('status','nouveau'),
        estimated_value=float(request.form.get('estimated_value',0) or 0),
        position=request.form.get('position',''), city=request.form.get('city',''),
        region=request.form.get('region',''), assigned_to=request.form.get('assigned_to',''),
        description=request.form.get('description',''))
    flash("Prospect modifié", "success"); return redirect(url_for('modules.prospects'))

@modules_bp.route('/prospects/delete/<int:pid>')
@perm_required('clients')
def prospects_delete(pid):
    conn = get_db(); conn.execute("DELETE FROM prospects WHERE id=?", (pid,)); conn.commit(); conn.close()
    flash("Prospect supprimé", "success"); return redirect(url_for('modules.prospects'))

@modules_bp.route('/prospects/convert/<int:pid>')
@perm_required('clients')
def prospects_convert(pid):
    p = db_get_by_id('prospects', pid)
    if p:
        db_insert('clients', name=p.get('company') or p.get('contact_name',''),
            tel=p.get('tel',''), email=p.get('email',''),
            contact_name=p.get('contact_name',''), address=p.get('address',''),
            city=p.get('city',''), source='prospect',
            notes=f"Converti depuis prospect #{pid}")
        db_update('prospects', pid, status='gagne')
        flash("Prospect converti en client !", "success")
    return redirect(url_for('modules.prospects'))

@modules_bp.route('/prospects/<int:pid>/status/<status>')
@perm_required('clients')
def prospects_status(pid, status):
    if status in ('nouveau','contacte','qualifie','proposition','gagne','perdu'):
        db_update('prospects', pid, status=status)
        if status == 'gagne':
            p = db_get_by_id('prospects', pid)
            if p: 
                db_insert('clients', name=p['company'], tel=p.get('tel',''), email=p.get('email',''),
                           contact_name=p.get('contact_name',''), address=p.get('address',''),
                           city=p.get('city',''), sector=p.get('sector',''),
                           source='prospect', notes=f"Converti depuis prospect #{pid}",
                           created_by=session['user_id'])
            flash("Prospect converti en client !", "success")
    return redirect(url_for('modules.prospects'))


# ======================== STOCK ========================

@modules_bp.route('/stock')
@perm_required('clients')
def stock():
    tab = request.args.get('tab', 'articles')
    items = db_get_all('stock_items', order='name ASC')
    low_stock = [i for i in items if i['quantity'] <= i['min_stock']]
    total_value = sum(i['quantity'] * i['unit_price'] for i in items)
    
    conn = get_db()
    movements = [dict(r) for r in conn.execute("""SELECT sm.*, si.name as item_name, si.reference as item_ref, u.full_name 
        FROM stock_movements sm LEFT JOIN stock_items si ON sm.item_id=si.id
        LEFT JOIN users u ON sm.created_by=u.id ORDER BY sm.created_at DESC LIMIT 100""").fetchall()]
    
    # Stats
    entries = conn.execute("SELECT COALESCE(SUM(quantity),0) FROM stock_movements WHERE movement_type='entree'").fetchone()[0]
    exits = conn.execute("SELECT COALESCE(SUM(quantity),0) FROM stock_movements WHERE movement_type='sortie'").fetchone()[0]
    adjustments = conn.execute("SELECT COUNT(*) FROM stock_movements WHERE movement_type='ajustement'").fetchone()[0]
    
    # Categories for params
    categories = [dict(r) for r in conn.execute("SELECT DISTINCT category FROM stock_items WHERE category!='' ORDER BY category").fetchall()]
    locations = [dict(r) for r in conn.execute("SELECT DISTINCT location FROM stock_items WHERE location!='' ORDER BY location").fetchall()]
    conn.close()
    
    return render_template('mod_stock.html', page='stock', tab=tab, items=items, low_stock=low_stock,
        total_value=total_value, movements=movements, entries=entries, exits=exits,
        adjustments=adjustments, categories=categories, locations=locations)

@modules_bp.route('/stock/add', methods=['POST'])
@perm_required('clients')
def stock_add():
    image = ''
    if 'image' in request.files and request.files['image'].filename:
        f = request.files['image']
        ext = os.path.splitext(f.filename)[1].lower()
        if ext in ('.jpg','.jpeg','.png','.webp'):
            fname = f"stock_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
            img_dir = os.path.join(current_app.config['UPLOAD_FOLDER'], 'stock')
            os.makedirs(img_dir, exist_ok=True)
            f.save(os.path.join(img_dir, fname))
            image = fname
    db_insert('stock_items', name=request.form['name'], reference=request.form.get('reference',''),
        category=request.form.get('category',''), quantity=int(request.form.get('quantity',0) or 0),
        unit_price=float(request.form.get('unit_price',0) or 0), min_stock=int(request.form.get('min_stock',0) or 0),
        location=request.form.get('location',''), image=image)
    flash("Article ajouté", "success"); return redirect(url_for('modules.stock'))

@modules_bp.route('/stock/edit/<int:sid>', methods=['POST'])
@perm_required('clients')
def stock_edit(sid):
    db_update('stock_items', sid,
        name=request.form['name'], reference=request.form.get('reference',''),
        category=request.form.get('category',''),
        unit_price=float(request.form.get('unit_price',0) or 0),
        min_stock=int(request.form.get('min_stock',0) or 0),
        location=request.form.get('location',''))
    flash("Article modifié", "success"); return redirect(url_for('modules.stock'))

@modules_bp.route('/stock/delete/<int:sid>')
@perm_required('clients')
def stock_delete(sid):
    conn = get_db()
    conn.execute("DELETE FROM stock_items WHERE id=?", (sid,))
    conn.commit(); conn.close()
    flash("Article supprimé", "success"); return redirect(url_for('modules.stock'))

@modules_bp.route('/stock/movement', methods=['POST'])
@perm_required('clients')
def stock_movement():
    item_id = int(request.form['item_id'])
    qty = int(request.form['quantity'])
    mtype = request.form['movement_type']
    db_insert('stock_movements', item_id=item_id, movement_type=mtype, quantity=qty,
        reference=request.form.get('reference',''), notes=request.form.get('notes',''), created_by=session['user_id'])
    item = db_get_by_id('stock_items', item_id)
    if item:
        if mtype == 'entree':
            new_qty = item['quantity'] + qty
        elif mtype == 'sortie':
            new_qty = max(0, item['quantity'] - qty)
        elif mtype == 'ajustement':
            new_qty = qty  # Set absolute value
        else:
            new_qty = item['quantity']
        db_update('stock_items', item_id, quantity=new_qty)
    flash(f"Mouvement: {mtype} x{qty}", "success"); return redirect(url_for('modules.stock', tab='historique'))


# ======================== TRÉSORERIE ========================

@modules_bp.route('/tresorerie')
@perm_required('comptabilite')
def tresorerie():
    tab = request.args.get('tab', 'liste')
    conn = get_db()
    
    movements = [dict(r) for r in conn.execute("""SELECT t.*, ba.name as account_name FROM treasury t 
        LEFT JOIN bank_accounts ba ON t.account_id=ba.id ORDER BY t.created_at DESC LIMIT 100""").fetchall()]
    accounts = [dict(r) for r in conn.execute("SELECT * FROM bank_accounts ORDER BY name").fetchall()]
    transfers = [dict(r) for r in conn.execute("""SELECT bt.*, a1.name as from_name, a2.name as to_name 
        FROM bank_transfers bt LEFT JOIN bank_accounts a1 ON bt.from_account_id=a1.id 
        LEFT JOIN bank_accounts a2 ON bt.to_account_id=a2.id ORDER BY bt.created_at DESC LIMIT 50""").fetchall()]
    
    recettes = conn.execute("SELECT COALESCE(SUM(amount),0) FROM treasury WHERE movement_type='recette'").fetchone()[0]
    depenses = conn.execute("SELECT COALESCE(SUM(amount),0) FROM treasury WHERE movement_type='depense'").fetchone()[0]
    total_initial = conn.execute("SELECT COALESCE(SUM(initial_balance),0) FROM bank_accounts").fetchone()[0]
    solde = total_initial + recettes - depenses
    conn.close()
    
    return render_template('mod_tresorerie.html', page='tresorerie', tab=tab,
        movements=movements, accounts=accounts, transfers=transfers,
        recettes=recettes, depenses=depenses, solde=solde, total_initial=total_initial)

@modules_bp.route('/tresorerie/add', methods=['POST'])
@perm_required('comptabilite')
def tresorerie_add():
    account_id = int(request.form.get('account_id', 0) or 0)
    amount = float(request.form['amount'])
    mtype = request.form['movement_type']
    db_insert('treasury', movement_type=mtype, category=request.form.get('category',''),
        amount=amount, description=request.form.get('description',''),
        reference=request.form.get('reference',''), payment_method=request.form.get('payment_method',''),
        account_id=account_id, created_by=session['user_id'])
    # Update account balance
    if account_id:
        conn = get_db()
        if mtype == 'recette':
            conn.execute("UPDATE bank_accounts SET current_balance=current_balance+? WHERE id=?", (amount, account_id))
        else:
            conn.execute("UPDATE bank_accounts SET current_balance=current_balance-? WHERE id=?", (amount, account_id))
        conn.commit(); conn.close()
    flash("Mouvement enregistré", "success")
    return redirect(url_for('modules.tresorerie', tab='revenu' if mtype == 'recette' else 'retrait'))

@modules_bp.route('/tresorerie/account/add', methods=['POST'])
@perm_required('comptabilite')
def tresorerie_account_add():
    initial = float(request.form.get('initial_balance', 0) or 0)
    db_insert('bank_accounts', name=request.form['name'], type=request.form.get('type', 'caisse'),
        subtype=request.form.get('subtype', 'courant'),
        bank_name=request.form.get('bank_name', ''), account_number=request.form.get('account_number', ''),
        initial_balance=initial, current_balance=initial, notes=request.form.get('notes', ''))
    flash("Compte ajouté", "success")
    return redirect(url_for('modules.tresorerie', tab='solde_initial'))

@modules_bp.route('/tresorerie/account/delete/<int:aid>')
@perm_required('comptabilite')
def tresorerie_account_delete(aid):
    conn = get_db(); conn.execute("DELETE FROM bank_accounts WHERE id=?", (aid,)); conn.commit(); conn.close()
    flash("Compte supprimé", "success")
    return redirect(url_for('modules.tresorerie', tab='solde_initial'))

@modules_bp.route('/tresorerie/account/edit/<int:aid>', methods=['POST'])
@perm_required('comptabilite')
def tresorerie_account_edit(aid):
    new_initial = float(request.form.get('initial_balance', 0) or 0)
    conn = get_db()
    old = conn.execute("SELECT initial_balance, current_balance FROM bank_accounts WHERE id=?", (aid,)).fetchone()
    if old:
        diff = new_initial - old['initial_balance']
        new_current = old['current_balance'] + diff
        conn.execute("""UPDATE bank_accounts SET name=?, type=?, subtype=?, bank_name=?, account_number=?,
            initial_balance=?, current_balance=?, notes=?, status=? WHERE id=?""",
            (request.form.get('name',''), request.form.get('type','caisse'),
             request.form.get('subtype','courant'),
             request.form.get('bank_name',''), request.form.get('account_number',''),
             new_initial, new_current, request.form.get('notes',''),
             request.form.get('status','actif'), aid))
        conn.commit()
    conn.close()
    flash("Compte modifié", "success")
    return redirect(url_for('modules.tresorerie', tab='solde_initial'))

@modules_bp.route('/tresorerie/transfer/add', methods=['POST'])
@perm_required('comptabilite')
def tresorerie_transfer_add():
    from_id = int(request.form['from_account_id'])
    to_id = int(request.form['to_account_id'])
    amount = float(request.form['amount'])
    if from_id == to_id:
        flash("Les comptes source et destination doivent être différents", "error")
        return redirect(url_for('modules.tresorerie', tab='transfert'))
    db_insert('bank_transfers', from_account_id=from_id, to_account_id=to_id,
        amount=amount, description=request.form.get('description', ''),
        reference=request.form.get('reference', ''),
        date=request.form.get('date', ''), created_by=session['user_id'])
    conn = get_db()
    conn.execute("UPDATE bank_accounts SET current_balance=current_balance-? WHERE id=?", (amount, from_id))
    conn.execute("UPDATE bank_accounts SET current_balance=current_balance+? WHERE id=?", (amount, to_id))
    conn.commit(); conn.close()
    flash(f"Transfert de {amount:,.0f} F effectué", "success")
    return redirect(url_for('modules.tresorerie', tab='transfert'))


# ======================== CALENDRIER ========================

@modules_bp.route('/calendrier')
@login_required
def calendrier():
    events = db_get_all('calendar_events', order='start_date ASC', limit=50)
    return render_template('mod_utils.html', page='calendrier', events=events)

@modules_bp.route('/calendrier/add', methods=['POST'])
@login_required
def calendrier_add():
    db_insert('calendar_events', title=request.form['title'], description=request.form.get('description',''),
        start_date=request.form['start_date'], end_date=request.form.get('end_date',''),
        color=request.form.get('color','#1a3a5c'), user_id=session['user_id'])
    flash("Événement ajouté", "success"); return redirect(url_for('modules.calendrier'))


# ======================== TICKETS ========================

@modules_bp.route('/tickets')
@login_required
def tickets():
    all_t = db_get_all('tickets')
    stats = {s: db_count('tickets', {'status': s}) for s in ['ouvert','en_cours','resolu','ferme']}
    return render_template('mod_tickets.html', page='tickets', tickets=all_t, stats=stats, users=get_all_users(), clients=get_all_clients())

@modules_bp.route('/tickets/add', methods=['POST'])
@login_required
def tickets_add():
    db_insert('tickets', subject=request.form['subject'], description=request.form.get('description',''),
        client_id=int(request.form.get('client_id',0) or 0) or None, priority=request.form.get('priority','normale'),
        assigned_to=int(request.form.get('assigned_to',0) or 0) or None, created_by=session['user_id'])
    flash("Ticket créé", "success"); return redirect(url_for('modules.tickets'))

@modules_bp.route('/tickets/<int:tid>/status/<status>')
@login_required
def tickets_status(tid, status):
    if status in ('ouvert','en_cours','resolu','ferme'):
        db_update('tickets', tid, status=status)
    return redirect(url_for('modules.tickets'))


# ======================== DÉPENSES ========================

@modules_bp.route('/depenses')
@perm_required('comptabilite')
def depenses():
    all_e = db_get_all('expenses')
    total = db_sum('expenses', 'amount')
    pending = db_count('expenses', {'status': 'en_attente'})
    return render_template('mod_depenses.html', page='depenses', expenses=all_e, total=total, pending=pending)

@modules_bp.route('/depenses/add', methods=['POST'])
@perm_required('comptabilite')
def depenses_add():
    db_insert('expenses', category=request.form.get('category',''), amount=float(request.form['amount']),
        description=request.form.get('description',''), date=request.form.get('date',''),
        receipt_ref=request.form.get('receipt_ref',''), created_by=session['user_id'])
    flash("Dépense enregistrée", "success"); return redirect(url_for('modules.depenses'))

@modules_bp.route('/depenses/<int:eid>/approve')
@perm_required('comptabilite')
def depenses_approve(eid):
    db_update('expenses', eid, status='approuvee', approved_by=session['user_id'])
    flash("Dépense approuvée", "success"); return redirect(url_for('modules.depenses'))


# ======================== TODOS ========================

@modules_bp.route('/todos')
@login_required
def todos():
    user_todos = db_get_all('user_todos', where={'user_id': session['user_id']}, order='done ASC, due_date ASC')
    return render_template('mod_utils.html', page='todos', todos=user_todos)

@modules_bp.route('/todos/add', methods=['POST'])
@login_required
def todos_add():
    db_insert('user_todos', user_id=session['user_id'], title=request.form['title'],
        priority=request.form.get('priority','normale'), due_date=request.form.get('due_date',''))
    flash("Todo ajouté", "success"); return redirect(url_for('modules.todos'))

@modules_bp.route('/todos/<int:tid>/toggle')
@login_required
def todos_toggle(tid):
    todo = db_get_by_id('user_todos', tid)
    if todo: db_update('user_todos', tid, done=0 if todo['done'] else 1)
    return redirect(url_for('modules.todos'))

@modules_bp.route('/todos/<int:tid>/delete')
@login_required
def todos_delete(tid):
    db_delete('user_todos', tid); return redirect(url_for('modules.todos'))


# ======================== MOYENS GÉNÉRAUX ========================

@modules_bp.route('/moyens-generaux')
@login_required
def moyens_generaux():
    vehicules = db_get_all('mg_vehicules') if _table_exists('mg_vehicules') else []
    fournitures = db_get_all('mg_fournitures') if _table_exists('mg_fournitures') else []
    maintenance = db_get_all('mg_maintenance') if _table_exists('mg_maintenance') else []
    return render_template('mod_moyens.html', page='moyens',
        vehicules=vehicules, fournitures=fournitures, maintenance=maintenance)

@modules_bp.route('/moyens-generaux/vehicules', methods=['GET', 'POST'])
@login_required
def mg_vehicules():
    if request.method == 'POST':
        db_insert('mg_vehicules', immatriculation=request.form['immatriculation'],
            marque=request.form.get('marque',''), modele=request.form.get('modele',''),
            affectation=request.form.get('affectation',''), km=int(request.form.get('km',0) or 0),
            assurance_exp=request.form.get('assurance_exp',''), visite_exp=request.form.get('visite_exp',''),
            status=request.form.get('status','disponible'))
        flash("Véhicule ajouté", "success"); return redirect(url_for('modules.mg_vehicules'))
    items = db_get_all('mg_vehicules')
    return render_template('mod_mg_sub.html', page='vehicules', items=items)

@modules_bp.route('/moyens-generaux/fournitures', methods=['GET', 'POST'])
@login_required
def mg_fournitures():
    if request.method == 'POST':
        db_insert('mg_fournitures', name=request.form['name'], category=request.form.get('category',''),
            quantity=int(request.form.get('quantity',0) or 0), unit=request.form.get('unit',''),
            min_stock=int(request.form.get('min_stock',0) or 0))
        flash("Fourniture ajoutée", "success"); return redirect(url_for('modules.mg_fournitures'))
    items = db_get_all('mg_fournitures')
    return render_template('mod_mg_sub.html', page='fournitures', items=items)

@modules_bp.route('/moyens-generaux/maintenance', methods=['GET', 'POST'])
@login_required
def mg_maintenance():
    if request.method == 'POST':
        db_insert('mg_maintenance', equipment=request.form['equipment'], description=request.form.get('description',''),
            priority=request.form.get('priority','normale'), status='en_attente',
            requested_by=session['user_id'], date_requested=request.form.get('date_requested',''))
        flash("Demande de maintenance créée", "success"); return redirect(url_for('modules.mg_maintenance'))
    items = db_get_all('mg_maintenance')
    return render_template('mod_mg_sub.html', page='maintenance', items=items)

@modules_bp.route('/moyens-generaux/maintenance/<int:mid>/status/<status>')
@login_required
def mg_maintenance_status(mid, status):
    if status in ('en_cours','termine'):
        db_update('mg_maintenance', mid, status=status)
    return redirect(url_for('modules.mg_maintenance'))

def _table_exists(table_name):
    from models import get_db
    conn = get_db()
    r = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()
    conn.close()
    return r is not None
