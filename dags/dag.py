#liberias de tiempo
import datetime
from datetime import date, timedelta
#librerias airflow
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
#librerias de datos
from scripts.library.tools import sensor
from scripts.library.tools import data_access
from scripts.library.tools import fixture_file
from scripts.library.tools import identifier
from scripts.library.tools import season_number

#credenciales API
API_HOST = "v3.football.api-sports.io"
API_KEY = "2bf3f0f2cb1fcdef1792bd2b5f275e58"
#credenciales AWS
AWS_ID='AKIA2ZOTMWKPMOK7M5KA'
AWS_KEY='tfqDzSb9TKhgIFpxYajX6C3TFkWZAfTKH4QESdNQ'
REGION='us-east-2'




#creacion del objeto premier_league para acceder a los datos
premier_league = data_access(API_HOST, API_KEY, AWS_KEY, AWS_ID, REGION, bucket='premier-league-stats', 
                              season = season_number(), league = 39, folder_name =  "premier_week_{}".format(date.today()))


#argumentos por defecto para airflow
default_args = {

    'owner': 'Roberto Marin',
    'start_date': datetime.datetime(2023, 5, 6), 
    'schedule_interval': '0 20 * * 0',
}

#DAG
with DAG(dag_id="dag_football", catchup=False,
         default_args=default_args,
      

) as dag:
    #tareas
    task1 = PythonSensor(task_id='web_check', python_callable=sensor, poke_interval=60*60*24) #airflow sensor (airflow)
    task2 = PythonOperator(task_id='get_teams_json', python_callable=premier_league.teams_json, dag=dag) #api request (request)
    task3 = PythonOperator(task_id='convert_teams_csv', python_callable=premier_league.teams_csv, dag=dag) # data operations (Pandas)
    task4 = PythonOperator(task_id='get_web_fixtures_csv', python_callable=fixture_file, dag=dag) # web scrapping (Scrapy)
    task5 = PythonOperator(task_id='identify_next_fixtures', python_callable=identifier, dag=dag) # data/text processing (fuzzyWuzzy)
    task6 = PythonOperator(task_id='get_teams_data_json', python_callable=premier_league.teams_stats_json, dag=dag) # api request
    task7 = PythonOperator(task_id='convert_teams_data_csv', python_callable=premier_league.teams_stats_csv, dag=dag) #data operations (Pandas)
    task8 = PythonOperator(task_id='get_player_data_json', python_callable=premier_league.players_stats_json, dag=dag) #api request (request)
    task9 = PythonOperator(task_id='convert_player_data_csv', python_callable=premier_league.players_stats_csv, dag=dag) # data operations (Pandas)
    task10 = PythonOperator(task_id='upload_data_aws', python_callable=premier_league.to_aws_bucket, dag=dag) #Boto3 (Amazon Cloud - S3)


#dependencias entre tareas
task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task9 >> task10


