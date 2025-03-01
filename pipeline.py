import uuid 

# todo: feature, sanity check, names should be unique of each task
def task(func):
    def wrapper(*args, **wargs):
        print("Activity started...")
        print(f"Function name: {func.__name__}")
        print(f"Function args: {args}")
        print(f"Function wargs: {wargs}")
        func(*args, **wargs)
        print("Activity ended...")
    return wrapper

def pipeline(func):
    def wrapper(*args, **wargs):
        print("Pipeline started...")
        func(*args, **wargs)
        print("Pipeline ended...")
    return wrapper