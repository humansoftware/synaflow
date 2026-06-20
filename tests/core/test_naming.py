import pytest

from synaflow.core.naming import Scope, get_base_dataset_name


@pytest.mark.parametrize(
    "name,expected",
    [
        ("user", "users"),
        ("users", "users"),
        ("user_list", "users"),
        ("item_set", "items"),
        ("record_dict", "records"),
        ("fetched_securities", "fetched_securities"),
        ("fetched_security", "fetched_securities"),
        ("raw_user_list", "raw_users"),
        ("person", "people"),
        ("people", "people"),
        ("data", "data"),
        ("status", "statuses"),
        ("statuses", "statuses"),
        ("s1", "s1"),
        ("step_2", "step_2"),
        ("item", "items"),
    ],
)
def test_get_base_dataset_name(name, expected):
    assert get_base_dataset_name(name) == expected


def test_scope_builds_nested_step_keys():
    sub = Scope("incl")

    assert str(sub) == "incl"
    assert str(sub.scope("validator")) == "incl__validator"
    assert sub("validator") == "incl__validator"


def test_scope_rejects_empty_parts():
    with pytest.raises(ValueError, match="non-empty"):
        Scope("")
