#!/bin/bash

#Iniciando o PostgreSQL
service postgresql start

#Criando o usuário, banco de dados com as variáveis de ambiente e permitindo acesso ao usuario
su postgres -c "psql -c \"CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}';\""
su postgres -c "psql -c \"CREATE DATABASE ${DB_NAME};\""
su postgres -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};\""

#Criando as tabelas distribuidores e vendas
su postgres -c "psql -d ${DB_NAME} -c \"CREATE TABLE distribuidores (
  COD_DISTRIBUICAO VARCHAR(255), 
  NM_DISTRIBUICAO VARCHAR(255)
);\""

su postgres -c "psql -d ${DB_NAME} -c \"CREATE TABLE vendas (
  DATA_VENDA DATE, 
  COD_DISTRIBUICAO INTEGER, 
  CLIENTE INTEGER, 
  NM_VENDEDOR VARCHAR(255), 
  QT_VENDA DECIMAL, 
  VL_VENDA DECIMAL
);\""

#Concede permissões de SELECT, INSERT, UPDATE e DELETE nas tabelas para o usuário descrito no arquivo .env
su postgres -c "psql -d ${DB_NAME} -c \"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE distribuidores TO ${DB_USER};\""
su postgres -c "psql -d ${DB_NAME} -c \"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE vendas TO ${DB_USER};\""

#Importa os dados dos arquivos CSV armazenados na pasta csv_files para as tabelas
su postgres -c "psql -d ${DB_NAME} -c \"\\copy distribuidores(COD_DISTRIBUICAO, NM_DISTRIBUICAO) FROM '/app/csv_files/distribuidores.csv' DELIMITER ';' CSV HEADER;\""
su postgres -c "psql -d ${DB_NAME} -c \"\\copy vendas(DATA_VENDA, COD_DISTRIBUICAO, CLIENTE, NM_VENDEDOR, QT_VENDA, VL_VENDA) FROM '/app/csv_files/vendas.csv' DELIMITER ';' CSV HEADER;\""

tail -f /dev/null
