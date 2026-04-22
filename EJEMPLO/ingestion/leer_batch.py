import pandas as pd
import requests

def leer_datos_batch(subject='cooking'):
    url = f"https://openlibrary.org/subjects/{subject}.json?limit=10"
    response = requests.get(url)
    data = response.json()
    df = pd.json_normalize(data['works'])
    df = df[['title', 'key', 'first_publish_year']]
    print(f"Batch pulled {len(df)} book records.")
    return df