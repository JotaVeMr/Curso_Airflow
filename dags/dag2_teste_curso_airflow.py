from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator



default_args = {
    "owner": "Joao Vutor",
    "start_date": datetime(2025, 11, 5),
}

dag = DAG (
    "da_2_teste_curso_airflow", # sempre no primeiro parametro colocarmos o nome da nossa DAG
    default_args=default_args,
    schedule_interval="15 10 * * 1-5", # intervalo de execução da dag
    max_active_runs=1, # quantidade de task executada 
)

task_number_one = DummyOperator(
    task_id = 'task_number_one',
    dag = dag

)

def function_joao():
    idade = 18
    if idade >= 18:
        return "Já pode dirigir"
    return "Não pode dirigir"

task_funcao_joao = PythonOperator(
    task_id = 'task_funcao_joao',
    python_callable = function_joao,
    dag = dag
)

def function_for():
    lista = 'joao', 'maria','fabricio'
    for i in lista:
        print(i)
    return lista # Retorna a lista criada

task_funcao_com_for = PythonOperator(
    task_id = 'task_funcao_com_for',
    python_callable = function_for,
    dag = dag
)

task_final = DummyOperator(
    task_id = 'task_final',
    dag = dag
)


task_number_one >> task_funcao_joao >> task_funcao_com_for >> task_final # Ordenação de task