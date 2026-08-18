import os, tempfile, sys
sys.path.insert(0, 'os.path.dirname(os.path.abspath(__file__))')
tmp = tempfile.mkdtemp(prefix="wg_p3_")
os.environ['PERSISTENT_DIR'] = tmp
import app as A
import models

fails=0
def ck(l,c):
    global fails
    print(("  OK  " if c else " FAIL ")+l);  fails += 0 if c else 1

# Agence 1 : créer une utilisatrice propre
models.clear_forced_agency()
ok,_ = models.create_user('alice_ag1','alice@a.ci','pw_alice','Alice Ag1','admin'); ck("alice créée (agence 1)", ok)
# Agence 2 : provisionnée
aid2 = models.create_agency("WannyGest Cocody","COC"); ck("agence 2 créée", aid2==2)
models.set_forced_agency(2)
ok2,_ = models.create_user('bob_ag2','bob@a.ci','pw_bob','Bob Ag2','admin'); ck("bob créé (agence 2)", ok2)
models.clear_forced_agency()

c = A.app.test_client()

def login(agency_id, username, password):
    return c.post('/login', data={'agency_id':str(agency_id),'username':username,'password':password}, follow_redirects=False)

# Sélecteur visible au GET (2 agences)
g = c.get('/login'); ck("sélecteur d'agence présent au login", b'name="agency_id"' in g.data)

# alice -> agence 1 OK, agence 2 KO
r = login(1,'alice_ag1','pw_alice'); ck("alice se connecte dans SON agence (1) -> redirection", r.status_code==302)
c.get('/logout')
r = login(2,'alice_ag1','pw_alice'); ck("alice REFUSÉE dans l'agence 2 (isolation login)", r.status_code!=302)
c.get('/logout')

# bob -> agence 2 OK, agence 1 KO
r = login(2,'bob_ag2','pw_bob'); ck("bob se connecte dans SON agence (2) -> redirection", r.status_code==302)
c.get('/logout')
r = login(1,'bob_ag2','pw_bob'); ck("bob REFUSÉ dans l'agence 1 (isolation login)", r.status_code!=302)
c.get('/logout')

# admin par défaut de chaque agence
r = login(1,'admin','admin2026'); ck("admin/admin2026 OK sur agence 1", r.status_code==302); c.get('/logout')
r = login(2,'admin','admin2026'); ck("admin/admin2026 OK sur agence 2 (compte distinct)", r.status_code==302); c.get('/logout')

print()
print("PHASE 3 : TOUS OK ✅" if fails==0 else f"{fails} ÉCHEC(S) ❌")
sys.exit(1 if fails else 0)
