from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator




default_args = {
    "owner": "João Vitor",
    "start_date": datetime(2025, 11, 5),
}


dag = DAG(
    "dag_teste_curso_airflow", # sempre no primeiro parametro colocarmos o nome da nossa DAG
    default_args=default_args,
    schedule_interval="15 10 * * 1-5", # intervalo de execução da dag
    max_active_runs=1, # quantidade de task executada
    #catchup=False,  -> Esse comando faz com que nao executa dag passadas, e somente Dags atuais
)


primeira_task = DummyOperator( # DummyOperator serve somente para vizualizacao
    task_id="primeira_task", # nome da task
    dag=dag
)


def test_function():
    print('NOSSA PRIMEIRA DAG')
    print('NOSSA PRIMEIRA DAG - TESTE PRINT 2')


task_python_operator = PythonOperator(
    task_id='task_python_operator',
    python_callable=test_function,
    dag=dag
)


ultima_task = DummyOperator(
    task_id="ultima_task",
    dag=dag
)


ultima_task_finaleira = DummyOperator(
    task_id="ultima_task_finaleira",
    dag=dag
)

task_joao = DummyOperator(
    task_id = "task_joao",  # Nome que aparecerá dentro do airflow
    dag=dag
)

def task_joao_funcao():
    print("Ola mundo")
    print("Meu nome é João Vitor")


task_joao_python_operator = PythonOperator( # Crio minha task
    task_id='task_joao_python_operator', # Passo o nome da minha task
    python_callable=task_joao_funcao, # Passo o nome da minha função que eu criei
    dag=dag # chama a Dag Criada
)

def chamar_joao():
    print("Olá joao, como voce está? ")
    print("Estou bem e voce? ")

task_chamando_joao = PythonOperator(
    task_id = 'task_chamando_joao',
    python_callable = chamar_joao,
    dag = dag
)


primeira_task >> task_python_operator >> ultima_task >> ultima_task_finaleira >> task_joao >> task_joao_python_operator >> task_chamando_joao  # ordenacao das tasks