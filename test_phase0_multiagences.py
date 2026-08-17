#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test de non-régression — Phase 0 multi-agences.
Vérifie que la fondation est en place ET que le comportement reste
strictement identique à l'existant (agence n°1 = base historique).
N'écrit que dans un dossier temporaire jetable.
"""
import os, sys, tempfile, sqlite3, importlib

# Dossier de données jetable AVANT d'importer models (lit PERSISTENT_DIR à l'import)
tmp = tempfile.mkdtemp(prefix="wg_phase0_")
os.environ['PERSISTENT_DIR'] = tmp

import models  # noqa

def check(label, cond):
    print(("  OK  " if cond else " FAIL ") + label)
    if not cond:
        check.failed += 1
check.failed = 0

# 1) Chemins attendus
check("DB_PATH pointe ramya.db dans le dossier de données",
      models.DB_PATH == os.path.join(tmp, 'ramya.db'))
check("CONTROL_DB_PATH pointe _control.db à côté",
      models.CONTROL_DB_PATH == os.path.join(tmp, '_control.db'))

# 2) La base de contrôle se crée et amorce l'agence n°1
models._ensure_control_db()
check("_control.db créé", os.path.exists(models.CONTROL_DB_PATH))
c = sqlite3.connect(models.CONTROL_DB_PATH); c.row_factory = sqlite3.Row
row = c.execute("SELECT * FROM agencies WHERE id=1").fetchone()
c.close()
check("agence n°1 présente", row is not None)
check("agence n°1 pointe la base historique (ramya.db)",
      row is not None and row['db_filename'] == 'ramya.db')
check("agence n°1 active", row is not None and row['active'] == 1)

# 3) Hors contexte requête → agence n°1 (comportement inchangé)
check("current_agency_id() == 1 hors requête", models.current_agency_id() == 1)
check("agency_db_path(1) == DB_PATH", models.agency_db_path(1) == models.DB_PATH)

# 4) get_db() ouvre bien la base historique (iso-fonctionnel)
conn = models.get_db()
dblist = conn.execute("PRAGMA database_list").fetchall()
main_path = [r[2] for r in dblist if r[1] == 'main'][0]
conn.execute("CREATE TABLE IF NOT EXISTS _t(x)"); conn.commit()
conn.close()
check("get_db() ouvre la base de l'agence n°1 (ramya.db)",
      os.path.realpath(main_path) == os.path.realpath(models.DB_PATH))
check("ramya.db a bien été créé par get_db()", os.path.exists(models.DB_PATH))

# 5) Sécurité : agence inconnue → fallback base historique (jamais d'erreur/fuite)
check("agency_db_path(999 inconnue) retombe sur DB_PATH",
      models.agency_db_path(999) == models.DB_PATH)

# 6) Une 2e agence route vers une AUTRE base (preuve d'isolation physique)
c = sqlite3.connect(models.CONTROL_DB_PATH)
c.execute("INSERT INTO agencies (id, code, nom, db_filename, active) VALUES (2,'AG2','Test',?,1)",
          ('agence_02.db',))
c.commit(); c.close()
models._AGENCY_PATH_CACHE.clear()
p2 = models.agency_db_path(2)
check("agence n°2 route vers une base distincte",
      p2 == os.path.join(tmp, 'agence_02.db') and p2 != models.DB_PATH)

print()
if check.failed == 0:
    print("RESULTAT : TOUS LES TESTS PHASE 0 PASSENT ✅")
    sys.exit(0)
else:
    print(f"RESULTAT : {check.failed} test(s) en échec ❌")
    sys.exit(1)
