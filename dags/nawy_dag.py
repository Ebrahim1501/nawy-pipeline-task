from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from datetime import datetime
import sys
import os
import pandas as pd


dbt_path=os.path.join(os.path.dirname(__file__), '..','include','nawy_dbt_project')
soda_path=os.path.join(os.path.dirname(__file__),'..','include','nawy_soda_project')
soda_config_name="configuration.yml"
soda_connection_name='nawy_task_destination'

def  convert_seeds_to_csv(): 
    seed_path = os.path.join(dbt_path, 'seeds')
    for file in os.listdir(seed_path):
        if file.endswith(".xlsx"):
            file_path = os.path.join(seed_path, file)
            base_name = file.replace(".xlsx", "").lower().replace(" ", "_")
            csv_path = os.path.join(seed_path, f"{base_name}.csv")
            df = pd.read_excel(file_path)
            df.to_csv(csv_path, index=False)
            
dag=DAG(dag_id="nawy_real_estate_pipeline",start_date=datetime(2024,1,1),end_date=None,schedule="@daily",catchup=False,tags=['nawy','medallian'])






t0=BashOperator(task_id="installing_required_dependencies", #--->installing dbt dependencies before running
                        bash_command=f""" cd {dbt_path} && dbt clean &&  dbt deps --no-version-check """,dag=dag  
)

t1=PythonOperator(
        task_id='convert_excel_seeds_to_csv', #-->convert any excelsheet to csv file to be ingested by the dbt seet
        python_callable=convert_seeds_to_csv   ,dag=dag   )







t2=BashOperator(
    task_id='ingesting_data_from_sheets', #---->ingesting the files using dbt seed
    bash_command=f""" cd {dbt_path} && dbt seed  """,dag=dag                 
)



t3=BashOperator(
    task_id='loading_data_into_staging_layer', #--->just loads ingested data from the seed tables into the bronze layer....
    bash_command=f""" cd {dbt_path} && dbt run -m models/bronze  """,dag=dag                   
)

t4=BashOperator(
    task_id='silver_layer_transformations',#----->light transformation on the data in the silver layer

    bash_command=f""" cd {dbt_path} && dbt run -m models/silver """,
    dag=dag                   
)

t5=BashOperator(
    task_id='soda_quality_checks_silver_layer', #--> Soda data quality checks for the sales_transformed and leads_transformed tables in the silver layer

    bash_command=f""" cd {soda_path} && soda scan  -d {soda_connection_name} -c {soda_config_name} {os.path.join('checks','silver')} || true""",
    dag=dag                   
)



t6=BashOperator(
    task_id='build_scd2_table',#->runs snapshot to track changes in lead scd2 dim table

    bash_command=f""" cd {dbt_path} && dbt snapshot """ 
    ,dag=dag                   
)


t7=BashOperator(
    task_id='build_fact_and_dim_tables',
    bash_command=f""" cd {dbt_path} && dbt run -m models/gold  """ 
    ,dag=dag                   
)
t8=BashOperator(
    task_id='soda_quality_checks_gold_layer', #--->simple soda quality checks over the gold layer

    bash_command=f""" cd {soda_path} && soda scan  -d {soda_connection_name} -c {soda_config_name} {os.path.join('checks','gold')} || true""", 

    dag=dag                   
)







t0>>t1>>t2>>t3>>t4>>t5>>t6>>t7>>t8
