# Databricks notebook source
import requests 
from pyspark.sql import functions as F

# COMMAND ----------

def extraindo_dados(date, base = "BRL"):
    url = f"https://api.apilayer.com/exchangerates_data/{date}?base={base}"
    
    headers= {"apikey": "lIK6TfMgeK0q66YiVCwiXPep0XoVsIU6"}
    #APIlIK6TfMgeK0q66YiVCwiXPep0XoVsIU6
    parametro = {"base":base}

    response = requests.request("GET", url, headers=headers, params=parametro)

    if response.status_code != 200:
        raise Exception("Não consegui estrair dados")

    status_code = response.status_code
    return response.json()

# COMMAND ----------

def salvar_arqivo(dados):
    ano, mes, dia = dados['date'].split("-")
    path = f"dbfs:/databricks-results/bronze/"
    df = dados_para_dataframe(dados)
    df_convertido = spark.createDataFrame(df,schema=['moeda','taxa'])
    df_convertido = df_convertido.withColumn("data",F.lit(f'{ano}-{mes}-{dia}'))
    df_convertido = df_convertido.withColumn("ano",F.lit(f'{ano}'))
    df_convertido = df_convertido.withColumn("mes",F.lit(f'{mes}'))
    df_convertido.write.partitionBy("ano","mes").mode("overwrite").parquet(path)

# COMMAND ----------

def dados_para_dataframe (dado_json): 
        dados_tupla = [(moeda, float (taxa)) for moeda, taxa in dado_json["rates"].items()]
        return dados_tupla

# COMMAND ----------

salvar_arqivo(extraindo_dados("2024-06-18"))
