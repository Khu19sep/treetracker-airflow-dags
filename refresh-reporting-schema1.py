from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import csv

default_args = {
		'owner': 'airflow',
		'depend_on_post':False,
		'start date': datetime(2021, 11,6, 22, 30),
		'retries': 1, 
		'retries_delay': timedelta(minutes=1)
}

t_path_n_file = "/tmp/treeDataDenormalized.csv"
destination_table = "khushi_denormalized"

def copy_from_db_to_csv_file():
	sql = """SELECT
			  trees.uuid AS capture_uuid,
			  planter.first_name AS planter_first_name,
			  planter.last_name AS planter_last_name,
			  planter.phone AS planter_identifier,
			  trees.lat AS lat,
			  trees.lon AS lon,
			  trees.note AS note,
			  trees.approved AS approved,
			  planting_organization.stakeholder_uuid AS planting_organization_uuid,
			  planting_organization.name AS planting_organization_name,
			  tree_species.name AS species
			  FROM trees
			  JOIN planter
			  ON planter.id = trees.planter_id
			  LEFT JOIN entity AS planting_organization
			  ON planting_organization.id = trees.planting_organization_id
			  LEFT JOIN tree_species
			  ON trees.species_id = tree_species.id
			  WHERE trees.active = true
			  AND planter_identifier IS NOT NULL
		 """
	pg_hook = PostgresHook(postgres_conn_id='greenstand_database', schema='treetracker_dev')
	connection = pg_hook.get_conn()
	cursor = connection.cursor()
	SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(sql)
	with open(t_path_n_file, 'w') as f_output:
		cursor.copy_expert(SQL_for_file_output, f_output)

def insert_db_from_csv_file():
	sql = """khushi_denormalized(capture_uuid, planter_first_name, planter_last_name, planter_identifier, lat, lon, note, approved,planting_organization_uuid, planting_organization_name,species)
		  """
	pg_hook = PostgresHook(postgres_conn_id='greenstand_database', schema='treetracker_dev')
	connection = pg_hook.get_conn()
	cursor = connection.cursor()
	SQL_for_file_input = "COPY {0} FROM STDIN WITH CSV HEADER".format(sql)

	with open(t_path_n_file, 'r') as f_output_Db:
		#cursor.copy_from(f_output_Db, destination_table, sep=',', columns=['capture_uuid', 'planter_first_name', 'planter_last_name', 'planter_identifier', 'lat', 'lon', 'note', 'approved','planting_organization_uuid', 'planting_organization_name','species'])
		cursor.copy_expert(sql=SQL_for_file_input, file=f_output_Db)
	connection.commit()

def delete_denormalized_table_data():
	sqlQuery="""
			DELETE FROM khushi_denormalized;
		  """
	hook = PostgresHook(postgres_conn_id='greenstand_database', schema='treetracker_dev')
	hook.run(sqlQuery)    
	for output in hook.conn.notices:
		print(output)


with DAG(
	dag_id="refresh-reporting-schema1",
	start_date=datetime(2020, 2, 2),
	schedule_interval="@once",
	catchup=False,
	) as dag:
	
	startTask = DummyOperator(task_id="startTask")
	deleteTableTask = PythonOperator(task_id = "deleteTableTask", python_callable=delete_denormalized_table_data)
	copyDbTOCSVTask = PythonOperator(task_id = "copyDbTOCSVTask", python_callable=copy_from_db_to_csv_file)
	insertCSVTODbTask = PythonOperator(task_id = "insertCSVTODbTask", python_callable=insert_db_from_csv_file)
	startTask.set_downstream(copyDbTOCSVTask)
	copyDbTOCSVTask.set_downstream(deleteTableTask)
	deleteTableTask.set_downstream(insertCSVTODbTask)

