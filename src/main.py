import os
import requests
import csv
import json
import logging

from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress

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

# API Jira
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

# API GITHUB
GITHUB_REPO = "apache/hive"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/commits"
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')


def get_github_commit_info(jira_key):
    headers = {
        'Authorization': f'token {GITHUB_TOKEN}',  # Authentification ajoutée
        'Accept': 'application/vnd.github.v3+json'
    }
    params = {'q': jira_key, 'per_page': 5}
    response = requests.get(GITHUB_API_URL, headers=headers, params=params)
    if response.status_code == 200:
        commits = response.json()
        if commits:
            first_commit = commits[0]
            commit_sha = first_commit['sha']

            # Récupérer les fichiers modifiés pour ce commit
            files_response = requests.get(f"{GITHUB_API_URL}/{commit_sha}", headers=headers)
            if files_response.status_code == 200:
                files = files_response.json().get('files', [])
                file_paths = ', '.join([file['filename'] for file in files])
                return commit_sha, file_paths
    return "N/A", "N/A"


try:
    # Envoi de la requête sans authentification
    response = requests.get(
        JIRA_URL,
        params=params_jira,
        headers={'Content-Type': 'application/json'}
    )
    response.raise_for_status()

    issues = response.json().get('issues', [])
    logger.info(f"Nombre de tickets Jira récupérés : {len(issues)}")

    # Assure que le dossier 'data' existe
    if not os.path.exists('data'):
        os.makedirs('data')

    # Création du fichier CSV dans le dossier 'data'
    with open(os.path.join('data', 'bugs_hive.csv'), mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(
            ['Bug ID', 'Key', 'Summary', 'Fix Versions', 'Affected Versions', 'Commit ID', 'File Paths'])

        # Afficher la progression avec Rich
        with Progress() as progress:
            task = progress.add_task("Traitement des tickets Jira...", total=len(issues))
            total_tickets = 0

            for issue in issues:
                try:
                    fields = issue['fields']

                    # Extraction des versions
                    fix_versions = fields.get('fixVersions', [])
                    affected_versions = fields.get('versions', [])

                    fix_versions_str = ', '.join(ver['name'] for ver in fix_versions) if fix_versions else "N/A"
                    affected_versions_str = ', '.join(
                        ver['name'] for ver in affected_versions) if affected_versions else "N/A"

                    # Récupérer l'ID du commit et les fichiers associés via GitHub
                    commit_id, file_paths = get_github_commit_info(issue['key'])

                    # Vérification des valeurs N/A et log des avertissements
                    if fix_versions_str == "N/A":
                        logger.warning(f"Fix Versions manquant pour le ticket {issue['key']}")
                    if affected_versions_str == "N/A":
                        logger.debug(f"Affected Versions manquant pour le ticket {issue['key']}")
                    if commit_id == "N/A":
                        logger.warning(f"Commit ID manquant pour le ticket {issue['key']}")
                    if file_paths == "N/A":
                        logger.warning(f"File Paths manquant pour le ticket {issue['key']}")

                    # Écriture dans le CSV
                    csv_writer.writerow([
                        issue['id'],
                        issue['key'],
                        fields['summary'],
                        fix_versions_str,
                        affected_versions_str,
                        commit_id,
                        file_paths
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
