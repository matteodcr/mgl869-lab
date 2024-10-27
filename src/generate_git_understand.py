import subprocess
import csv
import os
from datetime import datetime
import re
from typing import Dict, List, Tuple
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
from rich.panel import Panel
from rich.table import Table
from pathlib import Path
import sys
from rich.traceback import install
from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError

# Configuration
HIVE_REPO_PATH = "data/hive"  # Chemin vers le dépôt Hive
OUTPUT_CSV = "hive_metrics.csv"  # Nom du fichier de sortie
TEMP_DB_PREFIX = "understand_db_"  # Préfixe pour les bases de données temporaires
LOG_LEVEL = "INFO"  # Niveau de log (INFO, DEBUG, WARNING, ERROR)

# Installation du traceback Rich
install(show_locals=True)

console = Console()


class HiveMetricsCollector:
    def __init__(self):
        self.repo_path = Path(HIVE_REPO_PATH).resolve()
        self.output_csv = Path(OUTPUT_CSV).resolve()

        # Initialisation du repo Git
        try:
            self.repo = Repo(self.repo_path)
            if self.repo.bare:
                self.log_error("Le dépôt Git est bare")
                sys.exit(1)
        except InvalidGitRepositoryError:
            self.log_error(f"{self.repo_path} n'est pas un dépôt Git valide")
            sys.exit(1)

        self.metrics = [
            # File level metrics
            "AvgCyclomatic", "AvgCyclomaticModified", "AvgCyclomaticStrict",
            "AvgEssential", "AvgLine", "AvgLineBlank", "AvgLineCode", "AvgLineComment",
            "CountDeclClass", "CountDeclExecutableUnit", "CountDeclFunction",
            "CountDeclInstanceVariable", "CountDeclMethod", "CountDeclMethodAll",
            "CountDeclMethodDefault", "CountDeclMethodPrivate", "CountDeclMethodProtected",
            "CountDeclMethodPublic", "CountLine", "CountLineBlank", "CountLineCode",
            "CountLineCodeDecl", "CountLineCodeExe", "CountLineComment", "CountLineInactive",
            "CountLinePreprocessor", "CountSemicolon", "CountStmt", "CountStmtDecl",
            "CountStmtExe", "MaxCyclomatic", "MaxCyclomaticModified", "MaxCyclomaticStrict",
            "RatioCommentToCode", "SumCyclomatic", "SumCyclomaticModified",
            "SumCyclomaticStrict", "SumEssential",

            # Method level metrics (basic 5)
            "CountInput", "CountOutput", "CountPath", "MaxNesting", "CountPath"
        ]

    def log_debug(self, message: str):
        if LOG_LEVEL in ["DEBUG"]:
            console.print(f"[grey50][DEBUG] {message}[/grey50]")

    def log_info(self, message: str):
        if LOG_LEVEL in ["DEBUG", "INFO"]:
            console.print(f"[cyan]{message}[/cyan]")

    def log_warning(self, message: str):
        if LOG_LEVEL in ["DEBUG", "INFO", "WARNING"]:
            console.print(f"[yellow]{message}[/yellow]")

    def log_error(self, message: str):
        console.print(f"[red]ERROR: {message}[/red]")

    def get_version_commits(self) -> List[Tuple[str, str]]:
        """
        Récupère les commits correspondant aux versions de Hive en utilisant GitPython.
        """
        with console.status("[bold green]Récupération des versions...") as status:
            try:
                # Récupérer tous les tags triés par date
                tags = sorted(
                    self.repo.tags,
                    key=lambda t: t.commit.committed_datetime
                )

                version_commits = []
                for tag in tags:
                    version = tag.name
                    commit = tag.commit
                    version_commits.append((version, commit.hexsha))
                    self.log_debug(f"Version trouvée: {version} ({commit.hexsha[:8]})")

            except GitCommandError as e:
                self.log_error(f"Erreur lors de la récupération des tags Git: {e}")
                sys.exit(1)

        # Afficher un résumé des versions trouvées
        table = Table(title="Versions détectées")
        table.add_column("Version", justify="right", style="cyan")
        table.add_column("Commit ID", style="magenta")
        table.add_column("Date", style="green")
        table.add_column("Base de données", style="blue")

        for version, commit_id in version_commits:
            commit = self.repo.commit(commit_id)
            commit_date = commit.committed_datetime.strftime("%Y-%m-%d %H:%M")
            db_name = f"{TEMP_DB_PREFIX}{version.replace('.', '_')}"
            table.add_row(version, commit_id[:8], commit_date, db_name)

        console.print(table)
        return version_commits

    def checkout_version(self, commit_id: str):
        """
        Checkout une version spécifique du code avec GitPython
        """
        try:
            self.log_debug(f"Checkout du commit {commit_id[:8]}")
            self.repo.git.checkout(commit_id)
        except GitCommandError as e:
            self.log_error(f"Erreur lors du checkout du commit {commit_id}: {e}")
            sys.exit(1)

    def create_understand_db(self, db_path: str):
        """
        Crée une base de données Understand pour le code actuel
        """
        with console.status("[bold green]Création de la base de données Understand...") as status:
            try:
                # Créer la base de données
                self.log_debug("Création de la base de données Understand")
                subprocess.run(f"und create -languages java c++ -db {db_path}",
                               shell=True, check=True, capture_output=True)

                # Ajouter les fichiers source
                self.log_debug("Ajout des fichiers source")
                subprocess.run(f"und add {self.repo_path} -db {db_path}",
                               shell=True, check=True, capture_output=True)

                # Analyser
                self.log_debug("Analyse du code")
                subprocess.run(f"und analyze -db {db_path}",
                               shell=True, check=True, capture_output=True)

            except subprocess.CalledProcessError as e:
                self.log_error(f"Erreur lors de la création de la base Understand: {e}")
                if os.path.exists(db_path):
                    os.remove(db_path)
                sys.exit(1)

    def get_metrics(self, db_path: str) -> List[Dict]:
        """
        Extrait les métriques de la base de données Understand
        """
        metrics_list = []
        metrics_str = ",".join(self.metrics)

        with console.status("[bold green]Extraction des métriques...") as status:
            try:
                cmd = f"und metrics {metrics_str} -db {db_path}"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
                self.log_debug(f"Extraction des métriques depuis {db_path}")
            except subprocess.CalledProcessError as e:
                self.log_error(f"Erreur lors de l'extraction des métriques: {e}")
                return []

            for line in result.stdout.splitlines()[1:]:
                values = line.strip().split(',')
                if len(values) == len(self.metrics) + 1:
                    metrics_dict = {
                        'File': values[0],
                        **{metric: value for metric, value in zip(self.metrics, values[1:])}
                    }
                    metrics_list.append(metrics_dict)

        return metrics_list

    def clean_repo(self):
        """
        Nettoie le repo Git en cas d'interruption
        """
        try:
            self.log_debug("Nettoyage du repo Git")
            self.repo.git.clean('-fd')
            self.repo.git.checkout('main')  # ou 'master' selon le repo
        except GitCommandError as e:
            self.log_warning(f"Erreur lors du nettoyage du repo: {e}")

    def collect_all_metrics(self):
        """
        Collecte toutes les métriques pour toutes les versions
        """
        try:
            console.print(Panel.fit(
                "[bold green]Démarrage de la collecte des métriques[/bold green]\n" +
                f"[cyan]Dépôt: {self.repo_path}[/cyan]\n" +
                f"[cyan]Sortie: {self.output_csv}[/cyan]\n" +
                f"[cyan]Nombre de métriques: {len(self.metrics)}[/cyan]",
                title="Hive Metrics Collector"
            ))

            version_commits = self.get_version_commits()
            total_versions = len(version_commits)

            with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            ) as progress:
                task = progress.add_task("[cyan]Traitement des versions...", total=total_versions)

                with open(self.output_csv, 'w', newline='') as csvfile:
                    fieldnames = ['Version', 'CommitId', 'File'] + self.metrics
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()

                    for version, commit_id in version_commits:
                        progress.update(task, description=f"[cyan]Version {version}")

                        # Checkout la version
                        self.checkout_version(commit_id)

                        # Créer une base de données Understand temporaire
                        db_path = f"{TEMP_DB_PREFIX}{version.replace('.', '_')}"
                        self.create_understand_db(db_path)

                        # Collecter les métriques
                        metrics_list = self.get_metrics(db_path)

                        # Écrire dans le CSV
                        for metrics in metrics_list:
                            row = {
                                'Version': version,
                                'CommitId': commit_id,
                                **metrics
                            }
                            writer.writerow(row)

                        # Nettoyer
                        if os.path.exists(db_path):
                            os.remove(db_path)
                            self.log_debug(f"Suppression de la base de données temporaire {db_path}")

                        progress.advance(task)

            self.log_info(f"\n✓ Collecte terminée ! Résultats sauvegardés dans: {self.output_csv}")

        finally:
            # Toujours nettoyer le repo à la fin
            self.clean_repo()


def main():
    try:
        collector = HiveMetricsCollector()
        collector.collect_all_metrics()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interruption utilisateur détectée. Nettoyage...[/yellow]")
        collector.clean_repo()
        sys.exit(1)
    except Exception as e:
        console.print_exception()
        # Tenter de nettoyer même en cas d'erreur
        try:
            collector.clean_repo()
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()