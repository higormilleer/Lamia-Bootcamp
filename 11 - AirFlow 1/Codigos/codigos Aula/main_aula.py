from airflow import DAG
from datetima import datetime, timedelta
from airflow.sensors.http_sensor import HtppSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib,operators.spark_submite_operator import SparkSubmitOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
import json
import csv
import requests

default_args = {"owner": "airflow", #responsavel pelo DAG
                "start_date": datetime(2024, 12, 17), #data de inicio
                "depends_on_past": False, #execucao de hoje nao depende da execucao de dias anteriores
                "email_on_failure": False,#nao envia email caso falhe 
                "email_on_retry": False, #ou ocorra uma nova tentativa
                "email": "higormiller55@gmail.com", #destinatario 
                "retries": 1, #caso falhar, tera mais uma tentativa
                "retry_delay": timedelta(minutes=5) #com um intervalo de 5 minutos
               }

def download_rates():
    with open('/usr/local/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://api.exchangeratesapi.io/latest?base=' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

with DAG(dag_id = "forex_data_pipeline, #nome ao dag
        schedule_onterval = "@daily", #execucao diaria
        defalut_args=default_args,  
        catchup = False) as dag: #evita execucao de tarefas passaas caso o airflow fique desligado ou iniciado apos a data de inicio

    is_forex_rates_available = HttpSensor(
        task_id = "is_forex_rates_available",
        method = "GET", #metodo HTTP a ser usado 
        http_conn_id= "forex_api", #conexão HTTP configurada no Airflow
        endpoint = "latest", #endpoint da api que sera chamado
        response_check = lambda response: "rates" in response.text, #valida se a resposta contem "rates"
        poke_interval = 5, #intervalo das tentativas para verificacao em segundos
        timeout = 20 #tempo maximo de esperar antes de considerar a tarefa como falha, em segundos tambem
    )

    is_forex_currencies_file_available = FileSensor(
        task_id = "is_forex_currencies_file_available", 
        fs_conn_id = "fqrex_path", #id para realizar a conexao com o sistema de arquivo
        filepath = "forex_currencies.csv", #caminho do arquivo que vai ser monitorado 
        poke_interval = 5,
        timeout = 20
    )

    dowloading_rates = PythonOperator(
        task_id = "dowloading_rates",
        python_callable = download_rates #funcao que sera chamada quano a task for executada
    )

    saving_rates = BashOperator(
        task_id = "saving_rates",
        bash_comand = """" #Comando Bash que será executado
                hdfs dfs -mkdir -p /forex && \ #cria o diretorio "/forex" no HDFS
                hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex #faz a copia do arquivo "forex_rates.json" do diretorio local do Airflow para o diretorio "/forex" no HDFS
        """
    )

    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn", #conexao configurada no Airflow para o Hive
        hql=""" # Script HiveQL (HQL) que sera executado
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED  #define o formato da tabela
            FIELDS TERMINATED BY ',' #mostra que os campos tem que ser separados por virgulas
            STORED AS TEXTFILE #define que os dadps serap armazenados como arquivo de texto
        """
    )

    forex_processing = SparkSubmitOperator(
        task_id = "forex_procesing",
        conn_id = "spark_conn", #conexao para spark
        application = "/usr/local/airflow/dags/scripts/forex_processing.py",#caminho so script q sera executado
        verbose = False #define se as logs serao exibidas, reduzindo a verbosidade
    )

    send_email_notificacao = EmailOperator(
        task_id = "send_email",
        to = "airflow_course@gmail.com",#email que sera enviado a notificacao
        subject = "forex_data_pipeline",#assunto do email
        html_content = "<h3> forex_data_pipeline succeed <h3>" #conteudo do email (html)
    )

    send_slack_notif = SlackAPIPostOperator(
        task_id = "sending_slack",
        token="xoxp-8579173-857917367089-866283586917-0fca5e3947f6803768037628da7c7ib007dde",
        username="Higor", #nome remetente
        text="DAG forex_data_pipeline: DONE", #mensagem que sera enviada para o canal do Slack
        channel = "#airflow-exploit" #canal do Slack onde a mensagem sera postada
    )

    #ordem de execucao das tarefas no DAG
    is_forex_rates_available  >> is_forex_currencies_file_available >> dowloading_rates >> saving_rates >> creating_forex_rates_table >> forex_processing >> send_email_notificacao >> send_slack_notif