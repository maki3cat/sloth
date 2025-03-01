import history
from history import query_history, Status, complete_task, insert_task

# todo: feature, sanity check, names should be unique of each task
def pipeline(func):
    def wrapper(pipeline_instance_id: int, *args, **wargs):
        # todo: should have a better way to generate pipeline_id
        history = query_history(pipeline_instance_id)
        print("Pipeline started...")
        func(*args, **wargs, history=history)
        print("Pipeline ended...")
    return wrapper

def task(func):
    def wrapper(instance_history: history.InstanceHistory, *args, **wargs):
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