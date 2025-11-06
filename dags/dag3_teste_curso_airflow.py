from airflow import DAG
from datetime import datetime as dt
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

# Testando com o BranchPythonOperator

default_args = {
    "owner" : "Joao Vitor",
    "start_date": dt(2025, 11, 6)

}

dag = DAG (
    "dag_3_branch_operator_airflow",
    default_args = default_args,
    schedule_interval="50 11 * * 1-5",
    max_active_runs=1,
    catchup = False # Significa que o airflow nao executar dags atrasadas...
)

def media_aluno():
    numero = 3

    if numero >= 6:
        return "Aprovado"
    
    elif numero >= 4 and numero < 6:
        return "Recuperação"
    
    else:
        return "Exame"
    

inicio = DummyOperator(
    task_id = "Inicio",
    dag = dag

)

task_funcao_media = BranchPythonOperator(
    task_id = "task_funcao_media",
    python_callable = media_aluno,
    dag = dag
)

fim = DummyOperator(
    task_id = "Fim",
    dag = dag
)

Aprovado = DummyOperator(
    task_id = "Aprovado",
    dag = dag
)

Recuperação = DummyOperator(
    task_id = "Recuperação",
    dag = dag
)

Exame = DummyOperator(
    task_id = "Exame",
    dag = dag
)

inicio >> task_funcao_media 
task_funcao_media  >> [Aprovado,Recuperação,Exame] 
[Aprovado,Recuperação,Exame] >> fim


    

        


