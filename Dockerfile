FROM openjdk:11-jre-slim

ARG SPARK_VERSION=3.4.3
ARG HADOOP_VERSION=3
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

#Instalando dependencias (vim, python3, pip, postgresql)
RUN apt-get update && apt-get install -y vim && \
    apt-get install -y python3 python3-pip postgresql postgresql-contrib wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

    
#Instalando Spark versao 3.4.3 conforme apontado no argumento  SPARK_VERSION
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

#Copiando o arquivo requirements.txt para o contêiner
COPY requirements.txt /app/requirements.txt

#Instalacao dos requerimentos via pip3
RUN pip3 install -r /app/requirements.txt

#Expondo a porta do PostgreSQL
EXPOSE 5432

#Copia os arquivos necessários para a imagem(
# sparkmtrix - Ambiente spark com metodos e funcoes facilitando a manutencao do codigo
# csv_files - Tabelas distribuidores e vendas
# sparkapplication - Scripts dos desafios
# saved_tables - Pasta onde as tabelas de resultado serao armazenadas
# )
COPY spark/sparkmtrix.py /app/sparkapplication/sparkmtrix.py
COPY src/csv_files /app/csv_files
COPY src/sparkapplication /app/sparkapplication
COPY src/saved_tables /app/sparkapplication/saved_tables

#Copia o script de inicialização para dentro do container
COPY start_services.sh /start_services.sh
RUN chmod +x /start_services.sh

#Comando para iniciar os serviços
CMD ["/bin/bash", "/start_services.sh"]
