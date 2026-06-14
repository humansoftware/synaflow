# Semantic Naming & Smart Binding

SynaFlow automatically resolves parameter names to step outputs using **Base Dataset Names** — even when you use synonyms.

## Base Dataset Name

Every step produces a **Base Dataset** whose name is the absolute plural form of the step name:

| Step name | Base Dataset |
|---|---|
| `user` | `users` |
| `users` | `users` |
| `user_list` | `users` |
| `person` | `people` |
| `fetched_securities` | `fetched_securities` |

Stripping common suffixes (`_list`, `_set`, `_dict`, `_tuple`) and pluralizing via `inflect` ensures consistent naming.

## Smart Binding

You don't need to match step names exactly. Any synonym works:

```python
def items() -> Generator[User, None, None]:   # step name: "items"
    ...

def transform(item: User) -> User:             # singular → binds to "items"
    ...

def collector(items_list: list[User]) -> None: # suffixed → binds to "items"
    ...
```

The framework matches `item` (singular) and `items_list` (suffixed) to the producer `items` by comparing their Base Dataset names.

## Conflict Prevention

Two steps with the same Base Dataset name raise an error at build time:

```python
# ❌ ValueError: 'user' and 'users' both map to Base Dataset 'users'
pipeline(
    steps=[
        step("user", fn=fn1),
        step("users", fn=fn2),
    ]
)
```

Similarly, one function with two parameters mapping to the same base name is rejected.

## Naming Convention

- **EACH-mode consumers** naturally use the **singular** form (`item: User`)
- **ALL-mode consumers** naturally use the **plural** form (`items: list[User]`)
- The framework handles the mapping transparently — you write what is semantically correct.
