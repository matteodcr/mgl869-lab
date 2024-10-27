import os
import requests
import csv
import json
import logging
import git
import re
from pathlib import Path
from rich.progress import SpinnerColumn, Progress
from multiprocessing import Pool, cpu_count
from functools import partial

from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TaskID

load_dotenv()

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()]
)
logger = logging.getLogger("JiraExport")
console = Console()

# Configuration Jira
JIRA_URL = "https://issues.apache.org/jira/rest/api/2/search"
jql_query = '''project = "HIVE" 
               AND issuetype = "Bug" 
               AND status = "Resolved" 
               AND resolution = "Fixed"
               AND fixVersion >= "2.0.0"
               '''
params_jira = {
    'jql': jql_query,
    'fields': 'id,key,fixVersions,versions,summary',
    'maxResults': 1000,
    'expand': 'names'
}

# Configuration Git
REPO_URL = "https://github.com/apache/hive.git"
REPO_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'hive')


def ensure_repo_exists():
    """Clone le dépôt s'il n'existe pas déjà."""
    if not os.path.exists(REPO_PATH):
        with Progress(SpinnerColumn(), transient=True) as progress:
            task = progress.add_task("Clonage du dépôt...", start=False)
            logger.info(f"Clonage du dépôt {REPO_URL}...")
            progress.start_task(task)
            git.Repo.clone_from(REPO_URL, REPO_PATH)
            logger.info("Clonage terminé.")
    return git.Repo(REPO_PATH)


def search_commit(jira_key):
    """
    Fonction de recherche pour un seul ticket, adaptée pour le traitement parallèle.
    """
    try:
        repo = git.Repo(REPO_PATH)
        for commit in repo.iter_commits():
            if jira_key in commit.message:
                files_changed = list(commit.stats.files.keys())
                return {
                    'key': jira_key,
                    'commit_id': commit.hexsha[:7],
                    'file_paths': ', '.join(files_changed)
                }
    except Exception as e:
        logger.error(f"Erreur lors de la recherche pour {jira_key}: {str(e)}")

    return {
        'key': jira_key,
        'commit_id': "N/A",
        'file_paths': "N/A"
    }


def process_issues_batch(issues, batch_size=10):
    """
    Traite un lot de tickets en parallèle
    """
    with Pool(processes=max(1, cpu_count() - 1)) as pool:
        keys = [issue['key'] for issue in issues]
        results = []

        with Progress() as progress:
            task = progress.add_task("Traitement des tickets...", total=len(keys))

            # Traitement par lots pour éviter de surcharger la mémoire
            for i in range(0, len(keys), batch_size):
                batch = keys[i:i + batch_size]
                batch_results = pool.map(search_commit, batch)
                results.extend(batch_results)
                progress.update(task, advance=len(batch))

    return {result['key']: result for result in results}


def main():
    try:
        # S'assurer que le dépôt existe
        repo = ensure_repo_exists()

        # Requête Jira
        response = requests.get(
            JIRA_URL,
            params=params_jira,
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()

        issues = response.json().get('issues', [])
        logger.info(f"Nombre de tickets Jira récupérés : {len(issues)}")

        # Création du dossier data si nécessaire
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        os.makedirs(data_dir, exist_ok=True)

        # Traitement parallèle des recherches Git
        logger.info("Démarrage de la recherche parallèle des commits...")
        commit_results = process_issues_batch(issues)
        logger.info("Recherche des commits terminée.")

        # Création du fichier CSV
        csv_path = os.path.join(data_dir, 'bugs_hive.csv')
        with open(csv_path, mode='w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(
                ['Bug ID', 'Key', 'Summary', 'Fix Versions', 'Affected Versions', 'Commit ID', 'File Paths'])

            # Afficher la progression
            with Progress() as progress:
                task = progress.add_task("Écriture des résultats...", total=len(issues))
                total_tickets = 0

                for issue in issues:
                    try:
                        fields = issue['fields']
                        commit_info = commit_results.get(issue['key'],
                                                         {'commit_id': 'N/A', 'file_paths': 'N/A'})

                        # Extraction des versions
                        fix_versions = fields.get('fixVersions', [])
                        affected_versions = fields.get('versions', [])

                        fix_versions_str = ', '.join(ver['name'] for ver in fix_versions) if fix_versions else "N/A"
                        affected_versions_str = ', '.join(
                            ver['name'] for ver in affected_versions) if affected_versions else "N/A"

                        # Vérification des valeurs N/A et log des avertissements
                        if fix_versions_str == "N/A":
                            logger.warning(f"Fix Versions manquant pour le ticket {issue['key']}")
                        if affected_versions_str == "N/A":
                            logger.debug(f"Affected Versions manquant pour le ticket {issue['key']}")
                        if commit_info['commit_id'] == "N/A":
                            logger.warning(f"Commit ID manquant pour le ticket {issue['key']}")
                        elif commit_info['file_paths'] == "N/A":
                            logger.warning(f"File Paths manquant pour le ticket {issue['key']}")

                        # Écriture dans le CSV
                        csv_writer.writerow([
                            issue['id'],
                            issue['key'],
                            fields['summary'],
                            fix_versions_str,
                            affected_versions_str,
                            commit_info['commit_id'],
                            commit_info['file_paths']
                        ])
                        total_tickets += 1

                        # Mise à jour de la progression
                        progress.update(task, advance=1)

                    except KeyError as e:
                        logger.debug(f"Erreur de champ manquant dans le ticket {issue.get('key', 'Inconnu')}: {str(e)}")
                    except Exception as e:
                        logger.error(f"Erreur inconnue pour le ticket {issue.get('key', 'Inconnu')}: {str(e)}")

        logger.info(f"\nLes données ont été exportées dans bugs_hive.csv avec succès.")
        logger.info(f"Nombre total de tickets traités : {total_tickets}")

    except requests.exceptions.RequestException as e:
        logger.error("Erreur de connexion à l'API Jira.")
        logger.error(f"Statut HTTP: {response.status_code}, Détails de l'erreur : {str(e)}")
    except json.JSONDecodeError:
        logger.error("Erreur de décodage JSON dans la réponse de l'API Jira.")
    except Exception as e:
        logger.error(f"Erreur inconnue : {str(e)}")


if __name__ == "__main__":
    main()