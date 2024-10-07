# Mtrix Challenge

Este projeto tem como objetivo resolver os desafios propostos, criando um ambiente Docker com Spark, PostgreSQL e scripts de ETL utilizando PySpark.

## Estrutura de Arquivos

### 1. `spark/sparkmtrix.py`
Este arquivo contém as funções essenciais que são utilizadas em todos os processos de ETL para a resolução dos problemas, como funções para leitura e escrita de dados, criação de sessões Spark, definição de propriedades de conexão, entre outras. Ele facilita a manutenção do código e evita repetições.

### 2. `src/saved_tables`
Esta pasta, inicialmente vazia, é copiada para dentro do ambiente Docker e serve para armazenar as tabelas nos formatos Parquet e CSV, conforme proposto nos Desafios 1 e 2. Isso facilita tanto a visualização quanto a importação das tabelas para fora do ambiente Docker, se necessário.

### 3. `src/sparkapplication`
Esta pasta contém os scripts de ETL que resolvem os desafios propostos.

### 4. `src/csv_files`
Esta pasta armazena as tabelas de vendas dos distribuidores. Elas são copiadas para o ambiente Docker e recriadas no banco de dados PostgreSQL durante a configuração do ambiente.

### 5. `.env`
Este arquivo armazena as credenciais e o nome do banco de dados PostgreSQL.

### 6. `requirements.txt`
Especifica as dependências necessárias para o funcionamento dos scripts de ETL, incluindo a versão do PySpark a ser instalada, para evitar problemas com futuras atualizações.

### 7. `start_services.sh`
Este script inicia o PostgreSQL e realiza todas as configurações necessárias para o funcionamento do desafio, como a criação das tabelas e concessões de acesso ao banco de dados.

## Como Utilizar

1. Execute o seguinte comando para construir e subir o ambiente Docker:

   ```bash
   docker-compose up --build
   ```
2. Após o ambiente estar "em pé", execute o comando abaixo para acessar o bash do contêiner:

```bash
docker exec -it <imagem> /bin/bash
 ```
3. Dentro do bash, navegue até a pasta que contém os scripts de ETL:

```bash
cd app/sparkapplication/
 ```

4. Execute os arquivos desejados para resolver os desafios:

Para o Desafio 1 (Novos Clientes por Mês):

```bash
python3 ex1_new_clients_by_month.py
 ```

Para o Desafio 2 (Vendas dos Distribuidores):

```bash
python3 ex2_distributor_sales.py
 ```

Para o Desafio 3 (Ranking Top 10 Vendedores):

```bash
python3 ex3_ranking_top10_sellers.py
 ```
