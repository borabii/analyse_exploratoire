# Utiliser une image Python officielle comme image de base
FROM python:3.11-slim

# Créer un répertoire de travail pour l'application
WORKDIR /app

# Copier le fichier requirements.txt dans le conteneur
COPY requirements.txt /app/requirements.txt

# Installer les dépendances
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copier l'application Streamlit dans le conteneur
COPY app.py /app/app.py
COPY output/ /app/output

# Exposer le port utilisé par Streamlit
EXPOSE 8501

# Commande pour exécuter l'application Streamlit
CMD ["streamlit", "run", "app.py"]