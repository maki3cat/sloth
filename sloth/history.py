import json
import sqlite3
from enum import Enum

class Status(Enum):
    INIT = 0
    COMPLETED = 1
    FAILED = 2

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
    
class PipelineInstance:
    def __init__(self, name, tasks, status=Status.INIT, version=0, id=None):
        self.name = name
        self.tasks = tasks
        self.version = version
        self.status =  status
        self.id = id

class PipelineContext:
    def __init__(self, task_instances:list[TaskInstance], instance: PipelineInstance):
        self.pipeline_status = instance.status
        self.pipeline_version = instance.version
        self.task_map = dict([(task.name, task) for task in task_instances])

con = sqlite3.connect("pipeline.db")

def create_tables():
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS tb_tasks (id integer primary key, pipeline_instance_id integer, name TEXT, parameters TEXT, status integer default 0)")
    cur.execute("CREATE TABLE IF NOT EXISTS tb_pipelines (id integer primary key, name TEXT, version integer default 0, status integer default 0)")
    con.commit()

def delete_tables():
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS tb_tasks")
    cur.execute("DROP TABLE IF EXISTS tb_pipelines")
    con.commit()

delete_tables() # todo: remove this
create_tables()

def insert_task(task: TaskInstance)->int:
    # todo: this cursor is not thread safe?
    cur = con.cursor()
    parameters = task.serialize_parameters()
    cur.execute("INSERT INTO tb_tasks (pipeline_instance_id, name, parameters, status) VALUES (?, ?, ?, ?)", (
        task.pipeline_id, task.name, parameters, task.status))
    id = cur.lastrowid
    con.commit()
    return id

def task_save(pipeline_id, name, *args, **kwargs):
    task = TaskInstance(pipeline_id, None, name, *args, **kwargs)
    insert_task(task)

def complete_task(task_id: int)->None:
    cur = con.cursor()
    cur.execute("UPDATE tb_tasks SET status=1 WHERE id=?", (task_id,))
    return 

def insert_pipeline(pipeline: PipelineInstance)->int:
    cur = con.cursor()
    cur.execute("INSERT INTO tb_pipelines (name, version, status) VALUES (?, ?, ?)", (pipeline.name, pipeline.version, pipeline.status))
    id = cur.lastrowid
    con.commit()
    return id

def query_history(pipeline_instance_id: int)->PipelineContext:
    cur = con.cursor()
    instance = cur.execute("SELECT id, name, version, status FROM tb_pipelines WHERE id=?", (pipeline_instance_id,)).fetchone()
    if instance is None:
        return None
    cur.execute("SELECT * FROM tb_tasks WHERE pipeline_instance_id=?", (pipeline_instance_id,))
    rows = cur.fetchall()
    tasks = []
    for row in rows:
        task = TaskInstance(row[1], row[0], row[2])
        task.deserialize_parameters(row[3])
        tasks.append(task)
    history = PipelineContext(tasks, instance)
    return history