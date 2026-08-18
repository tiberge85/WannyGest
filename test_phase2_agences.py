import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import os, tempfile, sqlite3, sys
tmp = tempfile.mkdtemp(prefix="wg_p2_")
os.environ['PERSISTENT_DIR'] = tmp
import models

fails = 0
def ck(l,c):
    global fails
    print(("  OK  " if c else " FAIL ")+l)
    if not c: fails+=1

# Agence 1 = base historique : on l'initialise (schéma models + admin par défaut)
models.init_db()
ck("agence 1 : base ramya.db créée", os.path.exists(models.DB_PATH))
c1 = models.get_db()
admin1 = c1.execute("SELECT username, role FROM users WHERE role='admin'").fetchone()
ck("agence 1 : admin par défaut présent", admin1 is not None and admin1['username']=='admin')
# on personnalise l'agence 1 pour prouver l'isolation
c1.execute("UPDATE users SET full_name='ADMIN AGENCE 1' WHERE username='admin'")
c1.execute("INSERT INTO users (username,email,password_hash,salt,full_name,role) VALUES ('jean_ag1','j1@a.ci','x','s','Jean Agence1','technicien')")
c1.commit(); c1.close()

# Création agence 2 (provisionnement complet)
aid2 = models.create_agency("WannyGest Bingerville", "BGV")
ck("agence 2 créée, id=2", aid2 == 2)
ags = models.list_agencies()
ck("registre liste 2 agences", len(ags) == 2)
ck("agence 2 a un fichier base distinct", models.agency_db_path(2).endswith("agence_002.db") and os.path.exists(models.agency_db_path(2)))

# Isolation : se placer sur agence 2 et vérifier qu'on ne voit PAS les users de l'agence 1
models.set_forced_agency(2)
c2 = models.get_db()
users2 = [dict(r) for r in c2.execute("SELECT username, full_name FROM users").fetchall()]
c2.close()
models.clear_forced_agency()
noms2 = {u['username'] for u in users2}
ck("agence 2 a SON propre admin par défaut", 'admin' in noms2)
ck("agence 2 NE VOIT PAS l'utilisateur jean_ag1 de l'agence 1", 'jean_ag1' not in noms2)
admin2_fullname = next((u['full_name'] for u in users2 if u['username']=='admin'), None)
ck("agence 2 admin != admin agence 1 (isolation)", admin2_fullname != 'ADMIN AGENCE 1')

# Retour agence 1 : jean_ag1 toujours là, non pollué par agence 2
models.set_forced_agency(1)
c1b = models.get_db()
noms1 = {r['username'] for r in c1b.execute("SELECT username FROM users").fetchall()}
c1b.close(); models.clear_forced_agency()
ck("agence 1 conserve jean_ag1", 'jean_ag1' in noms1)

# code dupliqué refusé
try:
    models.create_agency("Doublon", "BGV"); ck("code dupliqué refusé", False)
except ValueError:
    ck("code dupliqué refusé", True)

print()
print("TOUS OK ✅" if fails==0 else f"{fails} ÉCHEC(S) ❌")
sys.exit(1 if fails else 0)
