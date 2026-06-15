from synaflow.core.dag import Dag


def output_key(dag: Dag, producer: str, consumer: str) -> str:
    if len(dag.consumers_of(producer)) > 1:
        return f"{producer}__{consumer}"
    return producer


def should_publish_output(step_name: str) -> bool:
    return not step_name.startswith("_")


def is_terminal_step(dag: Dag, step_name: str) -> bool:
    return not should_publish_output(step_name) or not dag.consumers_of(step_name)
