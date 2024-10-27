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


def setup_logging():
    """Configure le système de logging avec Rich pour une meilleure lisibilité"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler()]
    )
    return logging.getLogger("JiraExport"), Console()


def get_jira_config():
    """Retourne la configuration pour l'API Jira avec les paramètres de requête"""
    jql_query = '''project = "HIVE" 
                   AND issuetype = "Bug" 
                   AND status = "Resolved" 
                   AND resolution = "Fixed"
                   AND fixVersion >= "2.0.0"
                   '''
    return {
        'url': "https://issues.apache.org/jira/rest/api/2/search",
        'params': {
            'jql': jql_query,
            'fields': 'id,key,fixVersions,versions,summary',
            'maxResults': 1000,
            'expand': 'names'
        }
    }


def get_git_config():
    """Retourne la configuration pour le dépôt Git"""
    return {
        'repo_url': "https://github.com/apache/hive.git",
        'repo_path': os.path.join(os.path.dirname(__file__), '..', 'data', 'hive')
    }


def ensure_repo_exists(repo_url, repo_path):
    """
    Clone ou ouvre le dépôt Git.

    Args:
        repo_url: URL du dépôt à cloner
        repo_path: Chemin local où cloner/ouvrir le dépôt

    Returns:
        git.Repo: Instance du dépôt Git
    """
    if not os.path.exists(repo_path):
        with Progress(SpinnerColumn(), transient=True) as progress:
            task = progress.add_task("Clonage du dépôt...", start=False)
            logging.getLogger("JiraExport").info(f"Clonage du dépôt {repo_url}...")
            progress.start_task(task)
            git.Repo.clone_from(repo_url, repo_path)
            logging.getLogger("JiraExport").info("Clonage terminé.")
    return git.Repo(repo_path)


def fetch_jira_issues(url, params):
    """
    Récupère les tickets Jira via l'API.

    Args:
        url: URL de l'API Jira
        params: Paramètres de la requête

    Returns:
        list: Liste des tickets récupérés
    """
    response = requests.get(
        url,
        params=params,
        headers={'Content-Type': 'application/json'}
    )
    response.raise_for_status()
    return response.json().get('issues', [])


def search_commit(jira_key, repo_path):
    """
    Recherche un commit correspondant à un ticket Jira.

    Args:
        jira_key: Identifiant du ticket Jira
        repo_path: Chemin du dépôt Git

    Returns:
        dict: Informations sur le commit trouvé
    """
    try:
        repo = git.Repo(repo_path)
        for commit in repo.iter_commits():
            if jira_key in commit.message:
                return {
                    'key': jira_key,
                    'commit_id': commit.hexsha[:7],
                    'file_paths': ', '.join(list(commit.stats.files.keys()))
                }
    except Exception as e:
        logging.getLogger("JiraExport").error(f"Erreur lors de la recherche pour {jira_key}: {str(e)}")

    return {
        'key': jira_key,
        'commit_id': "N/A",
        'file_paths': "N/A"
    }


def process_issues_batch(issues, repo_path, batch_size=10):
    """
    Traite les tickets Jira par lots en parallèle.

    Args:
        issues: Liste des tickets à traiter
        repo_path: Chemin du dépôt Git
        batch_size: Taille des lots

    Returns:
        dict: Résultats des recherches de commits
    """
    with Pool(processes=max(1, cpu_count() - 1)) as pool:
        keys = [issue['key'] for issue in issues]
        results = []

        with Progress() as progress:
            task = progress.add_task("Traitement des tickets...", total=len(keys))

            search_commit_partial = partial(search_commit, repo_path=repo_path)
            for i in range(0, len(keys), batch_size):
                batch = keys[i:i + batch_size]
                batch_results = pool.map(search_commit_partial, batch)
                results.extend(batch_results)
                progress.update(task, advance=len(batch))

    return {result['key']: result for result in results}


def format_versions(versions):
    """Formate une liste de versions en chaîne de caractères"""
    return ', '.join(ver['name'] for ver in versions) if versions else "N/A"


def write_results_to_csv(issues, commit_results, csv_path):
    """
    Écrit les résultats dans un fichier CSV.

    Args:
        issues: Liste des tickets Jira
        commit_results: Résultats des recherches de commits
        csv_path: Chemin du fichier CSV de sortie
    """
    logger = logging.getLogger("JiraExport")
    total_tickets = 0

    with open(csv_path, mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(
            ['Bug ID', 'Key', 'Summary', 'Fix Versions', 'Affected Versions', 'Commit ID', 'File Paths'])

        with Progress() as progress:
            task = progress.add_task("Écriture des résultats...", total=len(issues))

            for issue in issues:
                try:
                    fields = issue['fields']
                    commit_info = commit_results.get(issue['key'], {'commit_id': 'N/A', 'file_paths': 'N/A'})

                    fix_versions_str = format_versions(fields.get('fixVersions', []))
                    affected_versions_str = format_versions(fields.get('versions', []))

                    if fix_versions_str == "N/A":
                        logger.warning(f"Fix Versions manquant pour le ticket {issue['key']}")
                    if affected_versions_str == "N/A":
                        logger.debug(f"Affected Versions manquant pour le ticket {issue['key']}")
                    if commit_info['commit_id'] == "N/A":
                        logger.warning(f"Commit ID manquant pour le ticket {issue['key']}")
                    elif commit_info['file_paths'] == "N/A":
                        logger.warning(f"File Paths manquant pour le ticket {issue['key']}")

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
                    progress.update(task, advance=1)

                except KeyError as e:
                    logger.debug(f"Erreur de champ manquant dans le ticket {issue.get('key', 'Inconnu')}: {str(e)}")
                except Exception as e:
                    logger.error(f"Erreur inconnue pour le ticket {issue.get('key', 'Inconnu')}: {str(e)}")

    return total_tickets


def main():
    """Point d'entrée principal du script"""
    try:
        logger, console = setup_logging()
        load_dotenv()

        jira_config = get_jira_config()
        git_config = get_git_config()

        repo = ensure_repo_exists(git_config['repo_url'], git_config['repo_path'])
        issues = fetch_jira_issues(jira_config['url'], jira_config['params'])
        logger.info(f"Nombre de tickets Jira récupérés : {len(issues)}")

        data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
        os.makedirs(data_dir, exist_ok=True)

        logger.info("Démarrage de la recherche parallèle des commits...")
        commit_results = process_issues_batch(issues, git_config['repo_path'])
        logger.info("Recherche des commits terminée.")

        csv_path = os.path.join(data_dir, 'bugs_hive.csv')
        total_tickets = write_results_to_csv(issues, commit_results, csv_path)

        logger.info(f"\nLes données ont été exportées dans bugs_hive.csv avec succès.")
        logger.info(f"Nombre total de tickets traités : {total_tickets}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur de connexion à l'API Jira: {str(e)}")
    except json.JSONDecodeError:
        logger.error("Erreur de décodage JSON dans la réponse de l'API Jira.")
    except Exception as e:
        logger.error(f"Erreur inconnue : {str(e)}")


if __name__ == "__main__":
    main()