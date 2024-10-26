import os
import requests
import csv
import json
import logging
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

# Configuration du logger avec Rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()]
)
logger = logging.getLogger("JiraExport")
console = Console()

# URL de l'API de recherche Jira publique
JIRA_URL = "https://issues.apache.org/jira/rest/api/2/search"

# JQL modifié pour filtrer les versions >= 3.0
jql_query = '''project = "HIVE" 
               AND issuetype = "Bug" 
               AND status = "Resolved" 
               AND resolution = "Fixed"
               AND fixVersion >= "2.0.0"
               '''

# Paramètres de la requête
params = {
    'jql': jql_query,
    'fields': 'id,key,fixVersions,versions,summary',
    'maxResults': 1000,
    'expand': 'names'
}

try:
    # Envoi de la requête sans authentification
    response = requests.get(
        JIRA_URL,
        params=params,
        headers={'Content-Type': 'application/json'}
    )
    response.raise_for_status()

    logger.info(f"Réponse reçue avec succès (status code: {response.status_code})")
    issues = response.json().get('issues', [])
    
    # Vérification et affichage de la structure du premier ticket
    if issues:
        console.print("\n[bold cyan]Structure du premier ticket :[/bold cyan]")
        console.print_json(data=issues[0], indent=2)
        console.print("\n[bold cyan]Champs disponibles :[/bold cyan]")
        console.print(list(issues[0]['fields'].keys()))
    
    # Ensure the 'data' directory exists
    if not os.path.exists('data'):
        os.makedirs('data')

    # Open the file in the 'data' directory
    with open(os.path.join('data', 'bugs_hive.csv'), mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Bug ID', 'Key', 'Summary', 'Fix Versions', 'Affected Versions'])
        
        total_tickets = 0
        for issue in issues:
            try:
                fields = issue['fields']
                
                # Extraction des versions
                fix_versions = fields.get('fixVersions', [])
                affected_versions = fields.get('versions', [])
                
                fix_versions_str = ', '.join(ver['name'] for ver in fix_versions) if fix_versions else "N/A"
                affected_versions_str = ', '.join(ver['name'] for ver in affected_versions) if affected_versions else "N/A"
                
                # Écriture dans le CSV
                csv_writer.writerow([
                    issue['id'],
                    issue['key'],
                    fields['summary'],
                    fix_versions_str,
                    affected_versions_str
                ])
                total_tickets += 1
                
                # Log de debug pour le premier ticket
                if total_tickets == 1:
                    logger.debug("Versions pour le premier ticket:")
                    logger.debug(f"Fix Versions: {fix_versions_str}")
                    logger.debug(f"Affected Versions: {affected_versions_str}")
                
            except KeyError as e:
                logger.error(f"Erreur de champ manquant dans le ticket {issue.get('key', 'Inconnu')}: {str(e)}")
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
