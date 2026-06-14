"""
Semantic step naming and smart binding: resolve synonyms to a canonical
Base Dataset name so users can reference datasets naturally (singular,
plural, suffixed) without manual wiring.
"""

import inflect

_engine = inflect.engine()

_SUFFIXES = {"_list", "_set", "_dict", "_tuple"}


def get_base_dataset_name(name: str) -> str:
    """Return the absolute plural Base Dataset name.

    Strips common collection suffixes, then pluralizes the last word
    (only when it is purely alphabetic — technical names are kept as-is).

    >>> get_base_dataset_name("user")
    'users'
    >>> get_base_dataset_name("users")
    'users'
    >>> get_base_dataset_name("user_list")
    'users'
    >>> get_base_dataset_name("fetched_securities")
    'fetched_securities'
    >>> get_base_dataset_name("person")
    'people'
    >>> get_base_dataset_name("s1")
    's1'
    """
    cleaned = name
    for suffix in sorted(_SUFFIXES, key=len, reverse=True):
        if cleaned.endswith(suffix) and cleaned != suffix.lstrip("_"):
            cleaned = cleaned[: -len(suffix)]
            break

    parts = cleaned.split("_")
    if not parts:
        return cleaned

    last = parts[-1]
    if last.isalpha():
        if not _engine.singular_noun(last):
            plural = _engine.plural_noun(last)
            if plural:
                parts[-1] = plural

    return "_".join(parts)
