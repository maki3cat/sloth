import json

class TaskInstance:
    """
    todo: currently we don't have language-neutral serialization format
    """
    def __init__(self, pipeline_id, task_id, name, *args, **kwargs):
        self.name = name # very important to distinguish the task
        self.args = args
        self.kwargs = kwargs
        self.pipeline_id = pipeline_id
        self.task_id = task_id

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
    def __init__(self, name, tasks, version=0):
        self.name = name
        self.tasks = tasks
        self.version = version

class InstanceHistory:
    def __init__(self, task_instances:list[TaskInstance], status):
        self.task_instances = task_instances
        self.status = status

from enum import Enum
class Status(Enum):
    INIT = 0
    COMPLETED = 1
    FAILED = 2
    