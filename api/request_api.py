import pandas as pd
import requests
from rocketry import Rocketry
from rocketry.conds import cron

app = Rocketry()

def get_uf():
    try:
        response = requests.get('http://servicodados.ibge.gov.br/api/v1/localidades/estados')
        response.raise_for_status()  

        uf_metadata = response.json()
        uf_siglas = [uf['sigla'] for uf in uf_metadata]
        return uf_siglas
    except requests.exceptions.RequestException as e:
        raise Exception(f'Erro na requisição: {e}')

def get_estado(uf: str):
    url = f'http://servicodados.ibge.gov.br/api/v3/malhas/estados/{uf}/metadados'

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return False

def get_area_uf(list_uf: list):
    area_uf = {}

    for i in list_uf:
        data = get_estado(uf=i)
        metadata_area_uf = data[0]['area']['dimensao'] if data else None
        area_uf[i] = metadata_area_uf

    return area_uf

def get_metadata_municipio(uf: str):
    url = f'http://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios'

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return False

def get_municipios(list_uf_siglas: list):
    ufs_municipios = {}

    for uf in list_uf_siglas:
        metadata = get_metadata_municipio(uf)
        ufs_municipio = [municipio['nome'] for municipio in metadata] if metadata else []
        ufs_municipios[uf] = ufs_municipio

    return ufs_municipios


@app.task(cron('* * * * *'))
def etl_run():
    # Obtendo dados
    lista_ufs_sligas = get_uf()
    lista_ufs_area = get_area_uf(lista_ufs_sligas)
    lista_ufs_municipios = get_municipios(lista_ufs_sligas)


    # Dados de Área por UF
    uf_area_df = pd.DataFrame(list(lista_ufs_area.items()), columns=['UF', 'Area'])
    uf_area_df['Area'] = uf_area_df['Area'].astype(float)
    uf_area_df_ordenada = uf_area_df.sort_values(by='Area', ascending=False)

    # Lista de Municipios por UF
    lista_ufs_municipios_df = pd.DataFrame(lista_ufs_municipios.items(), columns=['UF', 'Municipios'])
    lista_ufs_municipios_df_exploded = lista_ufs_municipios_df.explode('Municipios')

    # Salvar em UF em CSV
    uf_area_df_ordenada.to_csv(
        '/Users/augusto.scafi/Desktop/Estudos/Cursos/DataEngineer/data-engineer-project/data/csv/uf_area.csv',
          index=False,
            sep=';',
              encoding='utf-8'
              )

    # Salvar em Parquet particionado por UF
    lista_ufs_municipios_df_exploded.to_parquet(
        '/Users/augusto.scafi/Desktop/Estudos/Cursos/DataEngineer/data-engineer-project/data/parquet/municipios',
          index=False,
            partition_cols=['UF']
            )
    

if __name__ == '__main__':
    app.run()