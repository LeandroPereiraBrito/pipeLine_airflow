# Databricks notebook source
import requests 
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

dbutils.widgets.text("data_execucao","")
data_execucao = dbutils.widgets.get("data_execucao")

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

def conferir_dados(ano,mes):
    path = f"dbfs:/databricks-results/bronze/ano={ano}/mes={mes}/"
    try:
        df = spark.read.parquet(path)
        lista = []
        lista = [str(i['data']) for i in df.select('data').dropDuplicates().collect()]   
        return lista
    except:
        return None

# COMMAND ----------

def datas(data_execucao):
    formato = "%Y-%m-%d"
    # Convierte la cadena a un objeto datetime
    data = datetime.strptime(data_execucao, formato)
    periodo = []
    list_mes = [] 
    for i in range(3):
        ano,mes,dia = str(data - relativedelta(months=i)).split("-")
        datas = conferir_dados(ano,mes)
        dia_periodo = datetime.strptime(f'{ano}-{mes}-01', formato)
        
        while mes == str(dia_periodo)[5:7]:
            dia_periodo = dia_periodo + timedelta(days=1)
            salvar_arqivo(extraindo_dados(str(dia_periodo)[:10]))
            


# COMMAND ----------

datas(data_execucao)

# COMMAND ----------

#dbutils.fs.rm("dbfs:/databricks-results/",True)

# COMMAND ----------

#display(dbutils.fs.ls("dbfs:/databricks-results/bronze/"))

# COMMAND ----------


