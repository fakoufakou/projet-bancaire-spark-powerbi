# Orchestration du pipeline
Le pipeline bancaire est exécuté de manière **journalière** selon une logique batch.

# Ordre d’exécution
1. Bronze → Argent
2. Argent → Or
3. Export des données vers Power BI

# Planification
L’exécution peut être planifiée quotidiennement via :
- cron (Linux / WSL)
- un ordonnanceur externe (Airflow, Azure Data Factory, etc.)

# Planification de l’exécution
```bash
0 2 * * * spark-submit run_pipeline.py
