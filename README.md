# data-engineer-project

Este projeto é baseado no desafio proposto pela DataWay na conclusão do curso de Data Engineer Kickstart no qual consiste em um script Python dedicado a realizar o processo de ETL (Extração, Transformação e Carga) de dados geográficos relacionados aos estados brasileiros. Utilizando a API do IBGE, o script coleta informações sobre os estados, calcula suas áreas, e obtém uma lista de municípios associados a cada estado. Os resultados são armazenados em arquivos CSV e Parquet.

## Dependências

- **pandas:** Biblioteca para manipulação e análise de dados.
- **requests:** Biblioteca para realizar requisições HTTP.
- **Rocketry:** Utilizado para agendamento de tarefas automatizadas.

Instale as dependências utilizando o seguinte comando:

```
pip install pandas requests rocketry
```

## Como Executar

1. Clone este repositório:

```
git clone https://github.com/Augusto-c-s/data-engineer-project.git
cd data-engineer-project
```

2. Execute o script Python:

```
python request_api.py
```

O script será executado automaticamente a cada minuto, realizando o ETL e salvando os resultados nos diretórios especificados.

## Estrutura do Projeto

- **etl_run:** Função principal que realiza o processo de ETL e agendamento para executar a cada 1 minuto.
- **DataEngineer/data-engineer-project/data/csv/uf_area.csv:** Arquivo CSV contendo informações sobre a área de cada estado.
- **DataEngineer/data-engineer-project/data/parquet/municipios:** Diretório contendo arquivos Parquet particionados por estado, com informações sobre os municípios.

## Expressão Cron

O script está agendado para ser executado a cada minuto usando a expressão cron ' * * * * *'.
