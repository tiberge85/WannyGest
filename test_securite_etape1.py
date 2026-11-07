"""Tests de non-régression — correctifs de sécurité « étape 1 » (audit 2026-11).

Lance l'app sur une base jetable (PERSISTENT_DIR temporaire) et vérifie :
  U1 /static/ ne sert plus le code, la base ni les sous-dossiers
  U2 un login raté ne fait plus changer d'agence en gardant la session
  U3 le bypass RH (rôle ou département) ne donne plus les routes d'administration
  U4 plus de mot de passe admin fixe à la création d'une base / agence
  U5 compte client non approuvé refusé partout + notification échappée (XSS)
  E1/E2 bulletins de paie : lecture/signature et changement de statut contrôlés

Usage : python test_securite_etape1.py
"""
import os, sys, tempfile, sqlite3

tmp = tempfile.mkdtemp(prefix="wg_secu1_")
os.environ['PERSISTENT_DIR'] = tmp
os.environ.pop('INITIAL_ADMIN_PASSWORD', None)
os.environ['FORCE_HTTPS'] = '0'  # cookies non-Secure pour le client de test HTTP
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import app as A
import models

fails = 0
def ck(label, cond):
    global fails
    print(("  OK  " if cond else " FAIL ") + label)
    fails += 0 if cond else 1

A.app.config['TESTING'] = True
# Neutraliser le pointage d'arrivée obligatoire : sinon TOUTES les routes redirigent vers
# /pointage et les tests de refus passeraient pour une mauvaise raison.
A._user_has_pointed_today = lambda uid, kind: kind == 'arrivee'

def client():
    return A.app.test_client()

def login(c, username, password, agency_id=1):
    return c.post('/login', data={'agency_id': str(agency_id), 'username': username,
                                  'password': password}, follow_redirects=False)

def logged_user_id(c):
    with c.session_transaction() as s:
        return s.get('user_id'), s.get('agency_id')

def db(aid=1):
    con = sqlite3.connect(models.agency_db_path(aid)); con.row_factory = sqlite3.Row
    return con

# ---------------------------------------------------------------- U4
print("\n[U4] Compte admin par défaut")
models.clear_forced_agency()
pwd1 = models.LAST_GENERATED_ADMIN_PASSWORD
ck("mot de passe admin initial généré (pas de valeur fixe)", bool(pwd1) and pwd1 != 'admin2026')
c = client()
r = login(c, 'admin', 'admin2026')
ck("admin/admin2026 REFUSÉ sur une base neuve", r.status_code != 302)
r = login(c, 'admin', pwd1)
ck("admin + mot de passe généré accepté", r.status_code == 302)

# ---------------------------------------------------------------- U1
print("\n[U1] Fichiers sous /static/")
c = client()
ck("/static/app.py → 404", c.get('/static/app.py').status_code == 404)
ck("/static/models.py → 404", c.get('/static/models.py').status_code == 404)
ck("/static/render.yaml → 404", c.get('/static/render.yaml').status_code == 404)
ck("/static/data/ramya.db → 404", c.get('/static/data/ramya.db').status_code == 404)
ck("/static/.git/config → 404", c.get('/static/.git/config').status_code == 404)
ck("/static/uploads/x.png → 404 (sous-dossier)", c.get('/static/uploads/x.png').status_code == 404)
ck("/static/logo_wannygest.png → 200 (image racine toujours servie)",
   c.get('/static/logo_wannygest.png').status_code == 200)

# ---------------------------------------------------------------- U2
print("\n[U2] Changement d'agence par login raté")
aid2 = models.create_agency("Agence Test", "TST")
models.set_forced_agency(aid2)
ok, _ = models.create_user('bob_ag2', 'bob@t.ci', 'pw_bob_123', 'Bob Ag2', 'technicien')
models.clear_forced_agency()
c = client()
r = login(c, 'bob_ag2', 'pw_bob_123', agency_id=aid2)
uid, ag = logged_user_id(c)
ck("bob connecté dans l'agence 2", r.status_code == 302 and uid and ag == aid2)
login(c, 'bob_ag2', 'mauvais', agency_id=1)
uid, ag = logged_user_id(c)
ck("après login raté vers l'agence 1 : plus aucun user_id en session", uid is None)
r = c.get('/dashboard', follow_redirects=False)
ck("et /dashboard renvoie vers la connexion", r.status_code == 302 and '/login' in r.headers.get('Location', ''))

# ---------------------------------------------------------------- U3
print("\n[U3] Bypass RH")
models.create_user('rh_user', 'rh@t.ci', 'pw_rh_123', 'Mme RH', 'rh')
models.create_user('tech_dept_rh', 'tdr@t.ci', 'pw_tdr_123', 'Tech Dept RH', 'technicien')
con = db(); con.execute("UPDATE users SET department='Ressources Humaines' WHERE username='tech_dept_rh'")
con.commit(); con.close()
admin_id = db().execute("SELECT id FROM users WHERE username='admin'").fetchone()['id']
for uname, pw in (('rh_user', 'pw_rh_123'), ('tech_dept_rh', 'pw_tdr_123')):
    c = client()
    login(c, uname, pw)
    r = c.get('/admin/backup/download', follow_redirects=False)
    ck(f"{uname} : /admin/backup/download refusé", not (r.status_code == 200 and b'CREATE TABLE' in r.data))
    r = c.get('/admin/backup', follow_redirects=False)
    ck(f"{uname} : /admin/backup refusé", r.status_code != 200)
    r = c.post(f'/admin/edit/{admin_id}', data={'full_name': 'pirate', 'role': 'admin',
                                                'password': 'pirate123456'}, follow_redirects=False)
    name = db().execute("SELECT full_name FROM users WHERE id=?", (admin_id,)).fetchone()['full_name']
    ck(f"{uname} : ne peut pas modifier le compte admin", name != 'pirate')
# la RH garde ses modules RH
c = client(); login(c, 'rh_user', 'pw_rh_123')
r = c.get('/rh/paie', follow_redirects=False)
ck("rh_user : /rh/paie toujours accessible (bypass conservé hors admin)", r.status_code == 200)

# ---------------------------------------------------------------- U5
print("\n[U5] Portail client")
con = db()
con.execute("INSERT INTO clients (name, email, created_by) VALUES ('ACME SECURITE', 'contact@acme.ci', 1)")
con.execute("INSERT INTO clients (name, email, created_by) VALUES ('SANS EMAIL SARL', '', 1)")
con.commit(); con.close()
payload = '<img src=x onerror=alert(1)>'
c = client()
r = c.post('/portail/register', data={'username': 'intrus', 'password': 'intrus12345',
                                     'email': '', 'full_name': payload, 'company': 'ACME SECURITE'})
cu = db().execute("SELECT * FROM client_users WHERE username='intrus'").fetchone()
ck("inscription enregistrée en « pending »", cu is not None and cu['account_status'] == 'pending')
r = login(c, 'intrus', 'intrus12345')
ck("/login refuse le compte client non approuvé", '/portail/dashboard' not in r.headers.get('Location', ''))
with c.session_transaction() as s:
    s['client_user_id'] = cu['id']; s['client_id'] = cu['client_id']  # session forgée
r = c.get('/portail/dashboard', follow_redirects=False)
ck("portail_required refuse une session d'un compte non approuvé",
   r.status_code == 302 and r.headers.get('Location', '').endswith('/portail'))
# email vide : ne doit pas tomber sur « SANS EMAIL SARL »
c.post('/portail/register', data={'username': 'intrus2', 'password': 'intrus12345', 'email': '',
                                 'full_name': 'X', 'company': 'NOUVELLE BOITE'})
cu2 = db().execute("SELECT cu.client_id, c.name FROM client_users cu JOIN clients c ON c.id=cu.client_id "
                   "WHERE cu.username='intrus2'").fetchone()
ck("email vide : pas de rattachement à un client sans email", cu2 is not None and cu2['name'] == 'NOUVELLE BOITE')
# notification échappée
nid = db().execute("SELECT id FROM notifications WHERE type='client_register' AND user_id=? "
                   "ORDER BY id LIMIT 1", (admin_id,)).fetchone()
ck("notification créée pour l'admin", nid is not None)
if nid:
    c = client(); login(c, 'admin', pwd1)
    r = c.get(f'/notifications/{nid["id"]}/view')
    body = r.get_data(as_text=True)
    ck("notification : charge XSS rendue échappée", payload not in body and '&lt;img src=x' in body)
# rejet → is_active=0
c = client(); login(c, 'admin', pwd1)
c.post(f'/admin/client-users/reject/{cu["id"]}', data={'reason': 'test'})
row = db().execute("SELECT is_active, account_status FROM client_users WHERE id=?", (cu['id'],)).fetchone()
ck("refus : compte désactivé (is_active=0)", row['account_status'] == 'rejected' and row['is_active'] == 0)

# ---------------------------------------------------------------- E1 / E2
print("\n[E1/E2] Bulletins de paie")
models.create_user('tech1', 'tech1@t.ci', 'pw_tech_123', 'Paul Technicien', 'technicien')
models.create_employee(first_name='Awa', last_name='Kone', email='awa@t.ci', matricule='M001')
models.create_user('awa', 'awa@t.ci', 'pw_awa_123', 'Awa Kone', 'technicien')
emp_id = db().execute("SELECT id FROM employees WHERE matricule='M001'").fetchone()['id']
models.create_payslip(employee_id=emp_id, period='2026-10', net_salary=850000, status='en_attente_compta')
pid = db().execute("SELECT id FROM payslips WHERE employee_id=?", (emp_id,)).fetchone()['id']
c = client(); login(c, 'tech1', 'pw_tech_123')
r = c.get(f'/rh/paie/{pid}/sign', follow_redirects=False)
ck("technicien : bulletin d'un autre NON affiché", r.status_code == 302)
c.post(f'/rh/paie/{pid}/sign', data={'signature': 'x' * 80})
sig = db().execute("SELECT signature_data FROM payslips WHERE id=?", (pid,)).fetchone()['signature_data']
ck("technicien : ne peut pas signer le bulletin d'un autre", not sig)
c.get(f'/rh/paie/{pid}/status/valide_compta')
st = db().execute("SELECT status FROM payslips WHERE id=?", (pid,)).fetchone()['status']
ck("technicien : ne peut pas valider un bulletin (statut inchangé)", st == 'en_attente_compta')
c = client(); login(c, 'rh_user', 'pw_rh_123')
c.get(f'/rh/paie/{pid}/status/valide_compta')
st = db().execute("SELECT status FROM payslips WHERE id=?", (pid,)).fetchone()['status']
ck("RH : ne peut pas valider côté comptabilité (séparation RH/compta)", st == 'en_attente_compta')
c = client(); login(c, 'awa', 'pw_awa_123')
c.post(f'/rh/paie/{pid}/sign', data={'signature': 'y' * 80})
sig = db().execute("SELECT signature_data FROM payslips WHERE id=?", (pid,)).fetchone()['signature_data']
ck("titulaire : peut signer son propre bulletin", sig == 'y' * 80)

print()
print("ÉTAPE 1 SÉCURITÉ : TOUS OK ✅" if fails == 0 else f"{fails} ÉCHEC(S) ❌")
sys.exit(1 if fails else 0)
