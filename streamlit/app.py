import streamlit as st
import pandas as pd
import joblib
import plotly.express as px
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
import os
st.markdown("""
    <style>
    /* Style général pour les métriques */
    .stMetric {
        padding: 10px;
        border-radius: 10px;
        background-color: #f4f4f9;
        margin-bottom: 10px;
        font-size: 16px;
        font-weight: bold;
    }

    /* Style pour les titres */
    .stMarkdown h1, h2, h3 {
        font-family: 'Arial', sans-serif;
        color: #343a40;
        font-weight: bold;
    }

    /* Style pour les tableaux */
    table {
        width: 100%;
        border-collapse: collapse;
        margin: 20px 0;
        font-size: 16px;
        text-align: left;
    }
    table th, table td {
        padding: 12px;
        border-bottom: 1px solid #ddd;
    }
    table th {
        background-color: #f8f9fa;
        color: #212529;
    }
    table tr:nth-child(even) {
        background-color: #f2f2f2;
    }

    /* Style pour les colonnes */
    .stColumn {
        margin-right: 20px;
    }

    </style>
""", unsafe_allow_html=True)


# Charger les données
file_path = './output/Dpe_Join_Enedis.csv'
 
df = pd.read_csv(file_path, low_memory=False)

# Forcer les types numériques pour les colonnes de consommation
df['consommation_annuelle_moyenne_par_site_de_l_adresse_mwh'] = pd.to_numeric(df['consommation_annuelle_moyenne_par_site_de_l_adresse_mwh'], errors='coerce')
df['Surface_habitable_logement'] = pd.to_numeric(df['Surface_habitable_logement'], errors='coerce')
df['Conso_5_usages_par_m²_é_primaire'] = pd.to_numeric(df['Conso_5_usages_par_m²_é_primaire'], errors='coerce')

# Convertir MWh en kWh pour la consommation réelle
df['consommation_reelle_kwh'] = df['consommation_annuelle_moyenne_par_site_de_l_adresse_mwh'] * 1000

# Calculer la consommation annuelle DPE estimée
df['consommation_dpe_annuelle'] = df['Conso_5_usages_par_m²_é_primaire'] * df['Surface_habitable_logement']

# Calculer la consommation réelle par m²
df['consommation_reelle_par_m2'] = df['consommation_reelle_kwh'] / df['Surface_habitable_logement']

# Charger le modèle et les colonnes utilisées lors de l'entraînement
best_model = joblib.load('./output/meilleur_modele.pkl')
columns_used_for_training = joblib.load('./output/columns_used_for_training.pkl')  # Colonnes utilisées à l'entraînement

# Colonnes catégorielles à encoder
cat_cols = ['Type_installation_ECS_(général)', 'Qualité_isolation_menuiseries', 'Qualité_isolation_murs', 
            'Modèle_DPE', 'Indicateur_confort_été', 'Type_énergie_n°1', 
            'Date_fin_validité_DPE', 'Type_bâtiment', 'Zone_climatique_', 
            'Type_installation_chauffage', 'Type_énergie_principale_chauffage', 
            'Qualité_isolation_enveloppe', 'Etiquette_GES', 'Etiquette_DPE', 
            'Qualité_isolation_plancher_bas', 'Qualité_isolation_plancher_haut_comble_aménagé', 
            'Besoin_refroidissement', 'Besoin_ECS']

# Colonnes numériques à exclure
columns_to_exclude = [
    'consommation_annuelle_totale_de_l_adresse_mwh', 
    'consommation_annuelle_moyenne_par_site_de_l_adresse_mwh', 
    'consommation_annuelle_moyenne_de_la_commune_mwh',
    'nombre_de_logements', 'consommation_estimee_dpe_mwh', 'consommation_dpe_annuelle',
]

# Vérifier si l'encodeur ordinal est disponible, sinon le générer
if os.path.exists('./output/ordinal_encoder.pkl'):
    ordinal_encoder = joblib.load('./output/ordinal_encoder.pkl')
else:
    df_cat = df[cat_cols].astype(str)
    ordinal_encoder = OrdinalEncoder()
    encoded_cat = ordinal_encoder.fit_transform(df_cat)
    joblib.dump(ordinal_encoder, 'ordinal_encoder.pkl')

# Vérifier si le scaler est disponible, sinon le générer
if os.path.exists('output/scaler.pkl'):
    scaler = joblib.load('./output/scaler.pkl')
else:
    features_num = df.select_dtypes(include=[float]).drop(columns=columns_to_exclude)
    scaler = StandardScaler()
    scaler.fit(features_num)
    joblib.dump(scaler, './output/scaler.pkl')

# Appliquer l'encodage et la normalisation
df_cat = df[cat_cols].astype(str)
encoded_cat = ordinal_encoder.transform(df_cat)
encoded_cat_df = pd.DataFrame(encoded_cat, columns=cat_cols)

# Sélectionner les colonnes numériques
features_num = df.select_dtypes(include=[float]).drop(columns=columns_to_exclude)

# Combiner les colonnes numériques et catégorielles encodées
features = pd.concat([features_num, encoded_cat_df], axis=1)

# Supprimer les colonnes supplémentaires et ajouter celles manquantes
extra_features = set(features.columns) - set(columns_used_for_training)
features = features.drop(columns=list(extra_features))

missing_features = set(columns_used_for_training) - set(features.columns)
for col in missing_features:
    features[col] = 0  # Default value for missing columns

# S'assurer que les colonnes sont dans le même ordre que lors de l'entraînement
features = features[columns_used_for_training]

# Normaliser les données
features_scaled = scaler.transform(features)

# Faire la prédiction sur tout le dataset et convertir en kWh si nécessaire
df['Consommation_Predite'] = best_model.predict(features_scaled)

# Convertir la prédiction en numérique pour éviter les erreurs et multiplier par 1000 pour avoir en kWh
df['Consommation_Predite'] = pd.to_numeric(df['Consommation_Predite'], errors='coerce') * 1000

# **Maintenant, appliquer les filtres après la prédiction**
# Calculer l'écart après la prédiction
df['ecart_conso'] = df['consommation_reelle_kwh'] - df['consommation_dpe_annuelle']

# Filtrer les adresses avec un écart entre -800 et 1000 kWh
df_filtered = df[(df['ecart_conso'] >= -800) & (df['ecart_conso'] <= 1000)]

# Filtrer les adresses en fonction de l'année
st.sidebar.header("Filtres")
selected_year = st.sidebar.selectbox("Sélectionnez l'année", options=[2021, 2022, 2023])
filtered_by_year = df_filtered[df_filtered['annee'] == selected_year]

# Filtrer par adresse
addresses = filtered_by_year['Adresse_(BAN)'].unique()
selected_address = st.sidebar.selectbox("Adresse", options=addresses)

# Filtrer les données par l'adresse sélectionnée
filtered_by_address = filtered_by_year[filtered_by_year['Adresse_(BAN)'] == selected_address]

# Calculer les métriques et les afficher
if not filtered_by_address.empty:
    real_consumption = filtered_by_address['consommation_reelle_kwh'].iloc[0]
    dpe_consumption = filtered_by_address['consommation_dpe_annuelle'].iloc[0]
    predicted_consumption = filtered_by_address['Consommation_Predite'].iloc[0]
    surface_area = filtered_by_address['Surface_habitable_logement'].iloc[0]

    # Calcul de l'écart et du pourcentage
    consumption_diff = real_consumption - dpe_consumption
    percentage_diff = (consumption_diff / dpe_consumption) * 100

    # Affichage des métriques
    st.subheader(f"Consommation pour l'adresse : {selected_address}")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(label="Consommation Réelle (kWh)", value=f"{real_consumption:.2f}")
    col2.metric(label="Consommation Estimée DPE (kWh)", value=f"{dpe_consumption:.2f}")
    col3.metric(label="Consommation Prédite (kWh)", value=f"{predicted_consumption:.2f}")
    col4.metric(label="Écart (kWh)", value=f"{consumption_diff:.2f}", delta=f"{consumption_diff:.2f}")

    # Informations sur le logement
    st.subheader("Informations du logement")
    col5, col6, col7 = st.columns(3)
    col5.metric(label="Surface habitable (m²)", value=f"{surface_area:.2f} m²")
    col6.metric(label="Type de bâtiment", value=f"{filtered_by_address['Type_bâtiment'].iloc[0]}")
    col7.metric(label="Classe DPE", value=f"{filtered_by_address['Etiquette_DPE'].iloc[0]}")

    col8, col9 = st.columns(2)
    col8.metric(label="Consommation DPE par m²", value=f"{filtered_by_address['Conso_5_usages_par_m²_é_primaire'].iloc[0]:.2f} kWh/m²")
    col9.metric(label="Consommation réelle par m²", value=f"{filtered_by_address['consommation_reelle_par_m2'].mean():.2f} kWh/m²")
 # **Affichage des informations supplémentaires du logement sous forme de tableau HTML stylé**
    table_html = f"""
    <table style="width:100%; border-collapse: collapse; font-family: Arial, sans-serif;">
      <tr style="background-color: #f2f2f2;">
        <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Propriété</th>
        <th style="padding: 8px; border: 1px solid #ddd; text-align: left;">Valeur</th>
      </tr>
      <tr>
        <td style="padding: 8px; border: 1px solid #ddd;">Type de Chauffage</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{filtered_by_address['Type_installation_chauffage'].iloc[0]}</td>
      </tr>
      <tr>
        <td style="padding: 8px; border: 1px solid #ddd;">Énergie Principale Chauffage</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{filtered_by_address['Type_énergie_principale_chauffage'].iloc[0]}</td>
      </tr>
      <tr>
        <td style="padding: 8px; border: 1px solid #ddd;">Qualité Isolation Murs</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{filtered_by_address['Qualité_isolation_murs'].iloc[0]}</td>
      </tr>
      <tr>
        <td style="padding: 8px; border: 1px solid #ddd;">Qualité Isolation Menuiseries</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{filtered_by_address['Qualité_isolation_menuiseries'].iloc[0]}</td>
      </tr>
      <tr>
        <td style="padding: 8px; border: 1px solid #ddd;">Zone Climatique</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{filtered_by_address['Zone_climatique_'].iloc[0]}</td>
      </tr>
      <tr>
        <td style="padding: 8px; border: 1px solid #ddd;">Modèle DPE</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{filtered_by_address['Modèle_DPE'].iloc[0]}</td>
      </tr>
      <tr>
        <td style="padding: 8px; border: 1px solid #ddd;">Indicateur Confort Été</td>
        <td style="padding: 8px; border: 1px solid #ddd;">{filtered_by_address['Indicateur_confort_été'].iloc[0]}</td>
      </tr>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)
    # Calcul de la classe DPE réelle et estimée
    dpe_intervals = [
        (0, 50, "A"),
        (51, 90, "B"),
        (91, 150, "C"),
        (151, 230, "D"),
        (231, 330, "E"),
        (331, 450, "F"),
        (451, float('inf'), "G")
    ]

    def determine_dpe_class(consumption_per_m2):
        for lower_bound, upper_bound, dpe_class in dpe_intervals:
            if lower_bound <= consumption_per_m2 <= upper_bound:
                return dpe_class
        return "Inconnu"

    classe_dpe_reelle = determine_dpe_class(filtered_by_address['consommation_reelle_par_m2'].mean())
    dpe_class = determine_dpe_class(filtered_by_address['consommation_dpe_annuelle'].iloc[0] / surface_area)

    st.metric(label="Classe DPE réelle", value=f"{classe_dpe_reelle}")
    # st.metric(label="Classe DPE estimée", value=f"{dpe_class}")

    # Modification du passage de classe DPE en utilisant la moyenne de l'intervalle
    dpe_class_consumption_avg = {
        'A': 25,  # Moyenne de l'intervalle 0-50
        'B': 70.5,  # Moyenne de l'intervalle 51-90
        'C': 120.5,  # Moyenne de l'intervalle 91-150
        'D': 190.5,  # Moyenne de l'intervalle 151-230
        'E': 280.5,  # Moyenne de l'intervalle 231-330
        'F': 390.5,  # Moyenne de l'intervalle 331-450
        'G': 500  # Moyenne de l'intervalle 451+
    }

    # Sélection de la classe DPE cible
    initial_class = classe_dpe_reelle
    target_class = st.selectbox("Sélectionnez la classe DPE cible", options=['A', 'B', 'C', 'D', 'E', 'F', 'G'])
if target_class != initial_class:
    # Calcul de la consommation avant et après passage
    consumption_before =filtered_by_address['consommation_reelle_par_m2'].mean()
    consumption_after = dpe_class_consumption_avg[target_class]

    # Calcul de la différence de consommation
    total_consumption_difference = (consumption_after - consumption_before) * surface_area
    total_consumption_difference_per_m2 = total_consumption_difference / surface_area

    # Calcul du gain annuel en coût
    st.sidebar.subheader("Tarification")
    tariff_option = st.sidebar.radio(
        "Choisir le tarif d'électricité :",
        ('Base', 'Heures Pleines', 'Heures Creuses')
    )
    tariff_euro_kwh = 0.2516 if tariff_option == 'Base' else 0.27 if tariff_option == 'Heures Pleines' else 0.2068
    annual_gain = total_consumption_difference * tariff_euro_kwh

    # Calcul du pourcentage de changement de consommation
  # Calcul du pourcentage de changement de consommation
    # percent_change = (abs(total_consumption_difference) / consumption_before) * 100


    # Affichage des résultats avec des symboles et des couleurs
    if consumption_before > consumption_after:
        result_text = "Réduction de la consommation"
        symbol = "↑"  # Utiliser un symbole vert pour un gain
        color = "green"  # Couleur verte pour gain
        cost_text = "Réduction du coût énergétique"
    else:
        result_text = "Augmentation de la consommation"
        symbol = "↓"  # Utiliser un symbole rouge pour une perte
        color = "red"  # Couleur rouge pour perte
        cost_text = "Augmentation du coût énergétique"

   # Définir des styles pour différents types de texte et couleurs
    st.subheader(f"📊 **Passage de la classe {initial_class} à {target_class}**")

    # Utilisation de markdown pour styliser les résultats
    st.markdown(f"""
    <div style="background-color:#f9f9f9;padding:10px;border-radius:5px;">
        <p style="color:{color};font-size:20px;">
            {symbol} <strong>{result_text}</strong> : 
            <strong>{abs(total_consumption_difference):.2f} kWh</strong> 
        </p>
        <p style="font-size:18px;">• Différence de consommation par m² : <strong>{total_consumption_difference_per_m2:.2f} kWh/m²</strong></p>
        <p style="font-size:18px;">• {cost_text} : <strong>{annual_gain:.2f} € par an</strong> au tarif de <strong>{tariff_euro_kwh:.4f} €/kWh</strong></p>
        <p style="font-size:18px;">• Nouvelle consommation totale après passage : <strong>{real_consumption + total_consumption_difference:.2f} kWh</strong></p>
    </div>
    """, unsafe_allow_html=True)


    # Visualisation des consommations avec plotly
    fig = px.bar(
        x=['Consommation Réelle', 'Consommation Estimée DPE', 'Consommation Prédite'],
        y=[real_consumption, dpe_consumption, predicted_consumption],
        labels={'x': 'Type de Consommation', 'y': 'Consommation (kWh)'},
        title=f"Comparaison des Consommations pour {selected_address}"
    )
    st.plotly_chart(fig)