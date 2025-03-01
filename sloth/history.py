import json
import sqlite3
from enum import Enum

class TaskInstance:
    """
    todo: currently we don't have language-neutral serialization format
    """
    def __init__(self, pipeline_instance_id, task_id, name, status, *args, **kwargs):
        self.name = name # very important to distinguish the task
        self.args = args
        self.kwargs = kwargs
        self.pipeline_instance_id = pipeline_instance_id
        self.task_id = task_id
        self.status = status    

    def __str__(self):
        return f"TaskInstance(pipeline_id={self.pipeline_id}, task_id={self.task_id}, name={self.name}, args={self.args}, kwargs={self.kwargs})"

    def serialize_parameters(self):
        data = {
            'args': self.args,
            'kwargs': self.kwargs
        }
        return json.dumps(data)

    def deserialize_parameters(self, json_data):
        data = json.loads(json_data)
        self.args = data['args']
        self.kwargs = data['kwargs']

class Status(Enum):
    INIT = 0
    COMPLETED = 1
    FAILED = 2
    
class PipelineInstance:
    def __init__(self, name, tasks, status=Status.INIT, version=0, id=None):
        self.name = name
        self.tasks = tasks
        self.version = version
        self.status =  status
        self.id = id

class InstanceHistory:
    def __init__(self, task_instances:list[TaskInstance], instance: PipelineInstance):
        self.task_instances = task_instances
        self.pipeline_status = instance.status
        self.pipeline_version = instance.version

con = sqlite3.connect("pipeline.db")
cur = con.cursor()

def create_tables():
    cur.execute("CREATE TABLE IF NOT EXISTS tb_tasks (id integer primary key, pipeline_instance_id integer, name TEXT, parameters TEXT, status integer default 0)")
    cur.execute("CREATE TABLE IF NOT EXISTS tb_pipelines (id integer primary key, name TEXT, version integer, status integer default 0)")
    con.commit()

def delete_tables():
    cur.execute("DROP TABLE IF EXISTS tb_tasks")
    cur.execute("DROP TABLE IF EXISTS tasks")
    con.commit()

create_tables()

def insert_task(task: TaskInstance):
    parameters = task.serialize_parameters()
    cur.execute("INSERT INTO tb_tasks (pipeline_instance_id, name, parameters, status) VALUES (?, ?, ?, ?)", (
        task.pipeline_id, task.name, parameters, task.status))
    con.commit()

def complete_task(task_id: int):
    cur.execute("UPDATE tb_tasks SET status=1 WHERE id=?", (task_id,))

def query_tasks(pipeline_id):
    cur.execute("SELECT * FROM tb_tasks WHERE pipeline_id=?", (pipeline_id,))
    rows = cur.fetchall()
    for row in rows:
        task = TaskInstance(row[1], row[0], row[2])
        task.deserialize_parameters(row[3])
        print(task)

def query_history(pipeline_id: int):
    cur.execute("SELECT * FROM tb_tasks WHERE pipeline_id=?", (pipeline_id,))
    rows = cur.fetchall()
    tasks = []
    for row in rows:
        task = TaskInstance(row[1], row[0], row[2])
        task.deserialize_parameters(row[3])
        tasks.append(task)
    return InstanceHistory(tasks, Status.INIT)

def task_save(pipeline_id, name, *args, **kwargs):
    task = TaskInstance(pipeline_id, None, name, *args, **kwargs)
    insert_task(task)