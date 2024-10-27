import os
import csv
from git import Repo
from dotenv import load_dotenv
from datetime import datetime
import subprocess

# Charger les variables d'environnement
load_dotenv()
REPO_PATH = os.path.join("..", "data", "hive")
CSV_OUTPUT = os.getenv("CSV_OUTPUT")

# Variables des métriques à collecter
metrics = [
    "AvgCyclomatic", "AvgCyclomaticModified", "AvgCyclomaticStrict", "CountInput", "CountOutput",
    "CountPath", "CountLine", "CountStmt", "MaxNesting"
]


def get_git_tags(repo):
    """Récupère toutes les versions (tags) de Hive avec leurs dates."""
    tags = []
    for tag in repo.tags:
        commit_date = datetime.fromtimestamp(tag.commit.committed_date)
        tags.append((tag.name, commit_date))
    return tags


def checkout_commit(repo, commit_id):
    """Change le HEAD de Git au commit spécifié."""
    repo.git.checkout(commit_id)


def analyze_version(version):
    """Crée un projet Understand et analyse le code pour obtenir les métriques."""
    project_file = os.path.join("..","data", f"{version}.und")
    subprocess.run(["und", "create", "-languages", "java", "c++", project_file],
                   cwd=os.path.dirname(__file__))
    subprocess.run(["und", "add", REPO_PATH, project_file], cwd=os.path.dirname(__file__))
    subprocess.run(["und", "settings", "-metricsOutputFile", f"data/{version}_metrics.csv", project_file],
                   cwd=os.path.dirname(__file__))
    subprocess.run(["und", "analyze", project_file], cwd=os.path.dirname(__file__))


def collect_metrics(version, commit_id):
    """Exécute Understand pour collecter les métriques du commit spécifié."""
    analyze_version(version)
    metrics_path = os.path.join(REPO_PATH, f"{version}_metrics.csv")
    with open(metrics_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield {
                "Version": version,
                "CommitId": commit_id,
                "Fichier": row["File"],
                **{metric: row[metric] for metric in metrics}
            }


def write_metrics_to_csv(data):
    """Écrit les métriques collectées dans un fichier CSV."""
    fieldnames = ["Version", "CommitId", "Fichier"] + metrics
    with open(CSV_OUTPUT, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def main():
    repo = Repo(REPO_PATH)

    # Détecter la branche par défaut
    branch = 'main' if 'main' in repo.heads else 'master'

    tags = get_git_tags(repo)
    all_metrics = []

    for version, date in tags:
        # Récupère le dernier commit avant la date de la version
        commit = next(repo.iter_commits(rev=branch, until=date), None)

        if commit:
            checkout_commit(repo, commit.hexsha)
            metrics = list(collect_metrics(version, commit.hexsha))
            all_metrics.extend(metrics)

    write_metrics_to_csv(all_metrics)
    print(f"Métriques sauvegardées dans {CSV_OUTPUT}")


if __name__ == "__main__":
    main()
