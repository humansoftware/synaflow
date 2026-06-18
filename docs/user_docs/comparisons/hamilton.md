# SynaFlow & Hamilton

[Hamilton](https://github.com/DAGWorks-Inc/hamilton) is the other Python
framework that uses function signatures to automatically build DAGs. Both read
your type hints and wire dependencies without manual graph construction. But
the data model underneath is fundamentally different.

## How they wire

| | SynaFlow | Hamilton |
|---|---|---|
| **Wiring rule** | Parameter name matches producer name | Function name becomes output column |
| **Smart binding** | ✅ `item` → `items`, `user_list` → `users` | ❌ exact match required |
| **DRY** | Natural synonyms, no renaming needed | Must align function names meticulously |
| **Example** | `def transform(item: User)` binds to step `items` | `def items(users: pd.Series)` — name IS the column |

=== "SynaFlow"

    ```python
    # Step name "items" produces Iterator[User]
    # Parameter "item" (singular) binds to "items" automatically

    def transform(item: User) -> User:
        return item

    def collector(transform: list[User]) -> None:
        print(len(transform))

    p = pipeline(
        steps=[
            step("items", fn=producer),
            step("transform", fn=transform),  # "item" → "items" via smart binding
            step("collector", fn=collector),
        ],
    )
    ```

=== "Hamilton"

    ```python
    # Function name IS the output column — must match exactly

    def items(users: pd.Series) -> pd.Series:
        return users

    def transform(items: pd.Series) -> pd.Series:
        return items  # "items" must match function above exactly

    def collector(transform: pd.Series) -> pd.Series:
        return transform
    ```

## Data model

| | SynaFlow | Hamilton |
|---|---|---|
| **Default flow** | Lazy streaming (`Iterator[T]`) | DataFrame columns (materialized) |
| **Memory** | One item per step — generators | Entire column in memory |
| **Multiple consumers** | Auto `tee` in lockstep, bounded handoff when configured | Single consumer per column |
| **Materialization** | Consumer-driven: ask for `list[T]` → materialize | Always materialized |
| **Generators** | Native: `yield` in any step | Not supported at user level |
| **Streaming to disk** | Transparent via materializer factories | Manual code in each function |
| **Typed scalars** | `int`, `str`, `User`, any type | Primarily DataFrames/Series |

## Side-by-side: streaming vs columnar

=== "SynaFlow (streaming)"

    ```python
    from collections.abc import Generator, Iterator

    def producer(count: int) -> Generator[int, None, None]:
        yield from range(count)          # streams one item at a time

    def doubler(producer: int) -> int:   # EACH mode: called per item
        return producer * 2

    def eager(doubler: list[int]) -> int: # ALL mode: materialize
        return sum(doubler)

    def lazy(doubler: Iterator[int]) -> None: # ALL mode: lazy stream
        for x in doubler:
            print(x)
    ```

=== "Hamilton (columnar)"

    ```python
    import pandas as pd

    def producer(count: int) -> pd.Series:
        return pd.Series(range(count))   # entire column in memory

    def doubler(producer: pd.Series) -> pd.Series:
        return producer * 2              # vectorized over full column

    def eager(doubler: pd.Series) -> float:
        return doubler.sum()             # already in memory
    ```

## When to use each

| Use case | SynaFlow | Hamilton |
|---|---|---|
| **Streaming millions of rows** | ✅ lockstep + bounded handoff, one item at a time | ❌ full DataFrame in memory |
| **Feature engineering** | Possible but not specialized | ✅ purpose-built |
| **Notebook to production** | ✅ plain Python functions | ✅ `@parameterize` decorators |
| **Event-based processing** | ✅ lazy by default, idempotent | ❌ batch-oriented |
| **Multiple consumers, one producer** | ✅ auto `tee` + `max_in_flight` window | ❌ single consumer per column |
| **Persistence to disk/S3/DB** | ✅ materializer factories | ❌ manual code |
| **Sync + async from same definition** | ✅ identical DAG | ❌ sync only |
| **Export to Airflow/Prefect** | ✅ DAG JSON contract | ✅ via Hamilton UI |
| **Learning curve** | Low (plain functions) | Medium (DataFrame, decorator API) |

## The complementary use

SynaFlow and Hamilton are not competitors — they solve different layers of the
stack. You could use Hamilton for feature engineering over DataFrames inside a
SynaFlow step, or export a SynaFlow DAG to run in a Hamilton driver.

Both frameworks share the philosophy of **convention over configuration** and
**type-driven DAG construction**. The difference is what flows through the
edges: **individual items** vs. **entire columns**.
