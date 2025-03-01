# todo: why this is called history
from sloth.history import query_history, Status, complete_task, insert_task, InstanceHistory

pipeline_registry = {}

# todo: feature, sanity check, names should be unique of each task
def pipeline(func):
    # pipeline_name = func.__name__
    def wrapper(pipeline_instance_id: int, *args, **wargs):
        print("instnace_id", pipeline_instance_id)
        if pipeline_instance_id == None:
            pipeline_instance_id = uuid.uuid4().hex
        # todo: should have a better way to generate pipeline_id
        data = query_history(pipeline_instance_id)
        print("Pipeline started...")

        # todo: how to set this history into users function?
        func(instance_history = data, *args, **wargs)
        print("Pipeline ended...")
    return wrapper

def task(func):
    def wrapper(instance_history: InstanceHistory, *args, **wargs):
        cur_task_name = func.__name__
        for task in instance_history.task_map:
            if task.name == cur_task_name:
                if task.status == Status.COMPLETED:
                    print(f"Task {cur_task_name} already completed, skipping...")
                    return

        print(f"Task {cur_task_name} started...")
        func(*args, **wargs)
        print(f"Task {cur_task_name} completed...")

        if cur_task_name in instance_history.task_map:
            instance_history.task_map[cur_task_name].status = Status.COMPLETED
            complete_task(instance_history.task_map[cur_task_name].id)
        else:
            insert_task(cur_task_name, *args, **wargs)
    return wrapper


if __name__ == "__main__":
    print("Hello, World!")
    task(func=None)
    pipeline(func=None)