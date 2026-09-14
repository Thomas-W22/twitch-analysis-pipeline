"""
Analyses — exécute les outils d'analyse définis dans sql/analyses/.

Chaque outil est un fichier .sql autonome et commenté : la logique vit dans le
SQL (c'est l'objet du projet), ce module ne fait que l'exécuter et présenter
le résultat.

Usage :
    python -m src.analyses            # lance les 12 outils
    python -m src.analyses 01 07 11   # lance seulement ceux-là
"""
import sqlite3
import sys
import unicodedata
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "processed" / "twitch.db"
ANALYSES_DIR = Path(__file__).resolve().parent.parent / "sql" / "analyses"


def normaliser_tag(valeur: str) -> str | None:
    """Minuscule + suppression des accents.

    Sert à regrouper les variantes d'un même tag ("Français", "francais",
    "Français" en NFD...). SQLite ne sait pas retirer les accents nativement,
    d'où cette fonction enregistrée sur la connexion — voir
    sql/analyses/11_normalisation_tags.sql.
    """
    if valeur is None:
        return None
    decompose = unicodedata.normalize("NFD", valeur.lower())
    return "".join(ch for ch in decompose if unicodedata.category(ch) != "Mn")


def get_connection() -> sqlite3.Connection:
    """Ouvre la base en lecture et enregistre les fonctions Python utilisées par le SQL."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Base introuvable : {DB_PATH}\n"
            "Lance d'abord le chargement (src.load.load_all_snapshots)."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.create_function("norm_tag", 1, normaliser_tag)
    return conn


def lister_outils() -> list[Path]:
    """Retourne les fichiers d'analyse, dans l'ordre de leur numéro."""
    return sorted(ANALYSES_DIR.glob("*.sql"))


def executer(conn: sqlite3.Connection, chemin: Path) -> tuple[list[str], list[tuple]]:
    """Exécute un fichier .sql et renvoie (colonnes, lignes).

    Le fichier ne doit contenir qu'une seule requête (les CTE comptent pour une).
    """
    cur = conn.execute(chemin.read_text(encoding="utf-8"))
    return [d[0] for d in cur.description], cur.fetchall()


def afficher(titre: str, colonnes: list[str], lignes: list[tuple], max_lignes: int = 15) -> None:
    """Affiche un résultat en colonnes alignées."""
    print(f"\n{'─' * 78}\n{titre}\n")
    if not lignes:
        print("  (aucun résultat)")
        return

    # largeur de chaque colonne = le plus long entre l'en-tête et les valeurs
    apercu = lignes[:max_lignes]
    largeurs = [
        max(len(str(col)), *(len(str(l[i])) for l in apercu))
        for i, col in enumerate(colonnes)
    ]
    largeurs = [min(w, 40) for w in largeurs]

    def ligne_texte(valeurs):
        return "  " + " │ ".join(
            str(v)[:largeurs[i]].ljust(largeurs[i]) for i, v in enumerate(valeurs)
        )

    print(ligne_texte(colonnes))
    print("  " + "─┼─".join("─" * w for w in largeurs))
    for l in apercu:
        print(ligne_texte(["" if v is None else v for v in l]))
    if len(lignes) > max_lignes:
        print(f"  … et {len(lignes) - max_lignes} lignes de plus")


def main(filtres: list[str] | None = None) -> None:
    conn = get_connection()
    try:
        outils = lister_outils()
        if filtres:
            outils = [o for o in outils if any(o.name.startswith(f) for f in filtres)]
        if not outils:
            print("Aucun outil ne correspond.")
            return

        for chemin in outils:
            titre = chemin.stem.replace("_", " ")
            try:
                colonnes, lignes = executer(conn, chemin)
                afficher(titre, colonnes, lignes)
            except sqlite3.Error as exc:
                print(f"\n{'─' * 78}\n{titre}\n\n  ERREUR SQL : {exc}")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    main(sys.argv[1:] or None)
