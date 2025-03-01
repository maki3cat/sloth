import time
import json
import sqlite3
from history import TaskInstance

def create_tables():
    cur.execute("CREATE TABLE IF NOT EXISTS tb_tasks (id integer primary key, pipeline_id integer, name TEXT, parameters TEXT, status integer default 0)")
    cur.execute("CREATE TABLE IF NOT EXISTS tb_pipelines (id integer primary key, name TEXT, tasks TEXT)")
    con.commit()

def insert_task(task):
    parameters = task.serialize_parameters()
    print(parameters, type(task.name))
    cur.execute("INSERT INTO tb_tasks (pipeline_id, name, parameters) VALUES (?, ?, ?)", (task.pipeline_id, task.name, parameters))
    con.commit()

def complete_task(task_id):
    cur.execute("UPDATE tb_tasks SET status=1 WHERE id=?", (task_id,))

def query_tasks(pipeline_id):
    cur.execute("SELECT * FROM tb_tasks WHERE pipeline_id=?", (pipeline_id,))
    rows = cur.fetchall()
    for row in rows:
        task = TaskInstance(row[1], row[0], row[2])
        task.deserialize_parameters(row[3])
        print(task)

def delete_tables():
    # delete tables
    cur.execute("DROP TABLE IF EXISTS tb_tasks")
    cur.execute("DROP TABLE IF EXISTS tasks")
    con.commit()

con = sqlite3.connect("pipeline.db")
cur = con.cursor()
create_tables()


def task_save(pipeline_id, name, *args, **kwargs):
    task = TaskInstance(pipeline_id, None, name, *args, **kwargs)
    insert_task(task)

if __name__ == '__main__':
    pipeline_id = time.time()
    task = TaskInstance(pipeline_id=pipeline_id, task_id=None, name='crawl_save_raw', args=('arg1', 'arg2'), kwargs={'url': 'http://example.com'})
    insert_task(task)
    query_tasks(pipeline_id)
    delete_tables()