import history

# todo: feature, sanity check, names should be unique of each task
def pipeline(func):
    def wrapper(*args, **wargs):
        # todo: should have a better way to generate pipeline_id
        pipeline_id = 123
        history = get_history()
        print("Pipeline started...")
        func(*args, **wargs, history=history)
        print("Pipeline ended...")
    return wrapper

def task(func):
    def wrapper(history: history.InstanceHistory, *args, **wargs):
        print("Activity started...")
        print(f"Function name: {func.__name__}")

        name = func.__name__
        for task in history.task_instances:
            if task.name == name:
                if task.status == 1:
                    print(f"Task {name} already completed, skipping...")
                    return
                return

        print(f"Function args: {args}")
        print(f"Function wargs: {wargs}")
        func(*args, **wargs)
        print("Activity ended...")
    return wrapper

def get_history():
    return history.InstanceHistory()