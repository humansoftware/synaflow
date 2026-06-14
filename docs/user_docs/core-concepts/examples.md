# Examples

Every SynaFlow pipeline can be visualized with [`scripts/visualize_dag.py`](https://github.com/humansoftware/synaflow/blob/main/scripts/visualize_dag.py).

## complex_parallel

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/complex_parallel.py)

```mermaid
flowchart TD
    step1["step1<br/><i>Stream[int, None, None]</i>"]
    step2["step2<br/><i>Stream[int, None, None]</i>"]
    step3["step3<br/><i>Stream[int, None, None]</i>"]
    step4["step4<br/><i>Stream[int, None, None]</i>"]
    step5["step5<br/><i>None</i>"]
    base --> step1
    step1 --> step2
    step2 --> step3
    step1 --> step4
    step3 --> step5
    step4 --> step5
```


## complex_parallel_mixed

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/complex_parallel_mixed.py)

```mermaid
flowchart TD
    step1["step1<br/><i>Stream[int, None, None]</i>"]
    step2["step2<br/><i>Stream[int, None, None]</i>"]
    step3["step3<br/><i>Stream[int, None, None]</i>"]
    step4["step4<br/><i>Stream[int, None, None]</i>"]
    step5["step5<br/><i>None</i>"]
    base --> step1
    step1 --> step2
    step2 --> step3
    step1 --> step4
    step2 --> step5
    step4 --> step5
```


## deep_sub_pipelines

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/deep_sub_pipelines.py)

```mermaid
flowchart TD
    l2_each__adapter["l2_each__adapter<br/><i>Stream[Level2Params]</i>"]
    l2_each__l3_res__adapter["l2_each__l3_res__adapter<br/><i>ListType(<class 'tests.execution.sync_engine.corpus.deep_sub_pipelines.Level3Params'>)</i>"]
    l2_each__l3_res["l2_each__l3_res<br/><i>ListType(<class 'int'>)</i>"]
    l2_each["l2_each<br/><i>ListType(<class 'int'>)</i>"]
    l2_single__adapter["l2_single__adapter<br/><i>Level2Params</i>"]
    l2_single__l3_res__adapter["l2_single__l3_res__adapter<br/><i>Level3Params</i>"]
    l2_single__l3_res["l2_single__l3_res<br/><i>int</i>"]
    l2_single["l2_single<br/><i>int</i>"]
    consolidate["consolidate<br/><i>dict</i>"]
    values --> l2_each__adapter
    l2_each__adapter --> l2_each__l3_res__adapter
    l2_each__l3_res__adapter --> l2_each__l3_res
    l2_each__l3_res --> l2_each
    values --> l2_single__adapter
    l2_single__adapter --> l2_single__l3_res__adapter
    l2_single__l3_res__adapter --> l2_single__l3_res
    l2_single__l3_res --> l2_single
    l2_each --> consolidate
    l2_single --> consolidate
```


## diamond

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/diamond.py)

```mermaid
flowchart TD
    start["start<br/><i>int</i>"]
    branch_a["branch_a<br/><i>int</i>"]
    branch_b["branch_b<br/><i>int</i>"]
    merge["merge<br/><i>int</i>"]
    base_val --> start
    start --> branch_a
    start --> branch_b
    branch_a --> merge
    branch_b --> merge
```


## error_handling

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/error_handling.py)

```mermaid
flowchart TD
    gen["gen<br/><i>Stream[int, None, None]</i>"]
    consumer["consumer<br/><i>None</i>"]
    gen --> consumer
```


## explicit_modes

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/explicit_modes.py)

```mermaid
flowchart TD
    emit["emit<br/><i>Stream[int, None, None]</i>"]
    double["double<br/><i>ListType(<class 'int'>)</i>"]
    summarize["summarize<br/><i>int</i>"]
    items --> emit
    emit --> double
    double --> summarize
```


## fibonacci

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/fibonacci.py)

```mermaid
flowchart TD
    fibonacci_generator["fibonacci_generator<br/><i>Stream[int, None, None]</i>"]
    square_numbers["square_numbers<br/><i>Stream[int, None, None]</i>"]
    consumer["consumer<br/><i>None</i>"]
    count --> fibonacci_generator
    fibonacci_generator --> square_numbers
    square_numbers --> consumer
```


## linear

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/linear.py)

```mermaid
flowchart TD
    numbers["numbers<br/><i>Stream[int, None, None]</i>"]
    transformer["transformer<br/><i>ListType(<class 'int'>)</i>"]
    consumer["consumer<br/><i>None</i>"]
    count --> numbers
    numbers --> transformer
    transformer --> consumer
```


## mixed_fanout

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/mixed_fanout.py)

```mermaid
flowchart TD
    gen["gen<br/><i>Stream[int, None, None]</i>"]
    lazy["lazy<br/><i>tuple[bool, list[int]]</i>"]
    eager["eager<br/><i>tuple[bool, list[int]]</i>"]
    count --> gen
    gen --> lazy
    gen --> eager
```


## sub_pipelines

[:fontawesome-brands-github: Source](https://github.com/humansoftware/synaflow/blob/main/tests/execution/sync_engine/corpus/sub_pipelines.py)

```mermaid
flowchart TD
    my_text_processor__adapter["my_text_processor__adapter<br/><i>Stream[BParams]</i>"]
    my_text_processor__func_b1["my_text_processor__func_b1<br/><i>ListType(<class 'str'>)</i>"]
    my_text_processor["my_text_processor<br/><i>ListType(<class 'int'>)</i>"]
    consolidate["consolidate<br/><i>int</i>"]
    raw_texts --> my_text_processor__adapter
    my_text_processor__adapter --> my_text_processor__func_b1
    my_text_processor__func_b1 --> my_text_processor
    my_text_processor --> consolidate
```


---
*Diagrams auto-generated from the test corpus.*
