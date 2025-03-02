"""
this is a pipeline to generate synthetic data
"""
import uuid
from sloth.pipeline import pipeline, task, call_step
from sloth.history import PipelineContext

LLM_urls = ["GPT", "CLAUD", "DEEPSEEK", "GROK"]

# todo: should learn how to generate data as real
# todo: how to avoid asking uses to add this pipeline_context by hand?
# todo: invasion into signature
@pipeline
def generate_synthetic_data(context: PipelineContext, batch_id: str=None, count_per_LLM: int=10):
    if batch_id is None:
        batch_id = uuid.uuid4().hex
    # prompt = "some fixed url for symptom BPD"
    for url in LLM_urls:
        for i in range(count_per_LLM):
        # todo: should add worker of activity so that this will not blocking start task of 
            call_step(call_LLM_and_save, batch_id, i, url)
    # todo: wait for all the call to return
    # todo: tolerate stagger, and other failures
    accumulate_data()

@task
def call_LLM_and_save(context: PipelineContext, batch_id: str, task_count: int, url: str)->str:
    print(f"checking idempotent for bactch_id-task_count: {batch_id}-{task_count}")
    print(f"Calling LLM with url: {url}")
    print(f"Saving the data")

@task
def accumulate_data():
    print("Accumulating the data for next step")

if __name__ == "__main__":
    print("Hello, World!")
    generate_synthetic_data(pipeline_instance_id=1991)