from __future__ import annotations

from copy import deepcopy

from legendmeta import police


def test_len_nested():
    assert police.len_nested({}) == 0
    assert police.len_nested({"a": 1}) == 1
    assert police.len_nested({"a": 1, "b": 2}) == 2
    assert police.len_nested({"a": 1, "b": {"c": 2}}) == 3
    assert police.len_nested({"a": 1, "b": {"c": 2, "d": "boh"}}) == 4


def test_validate_keys():
    template = {"a": 1, "b": 2, "c": {"d": "sick!", "e": 420}}

    assert police.validate_keys_recursive(template, template)
    assert police.validate_keys_recursive({"a": 3}, template)
    assert police.validate_keys_recursive({"a": 3, "c": {"d": -1}}, template)

    assert not police.validate_keys_recursive({}, template)
    assert not police.validate_keys_recursive({"z": 1}, template)
    assert not police.validate_keys_recursive({"z": 1, "h": {"b": 2}}, template)
    assert not police.validate_keys_recursive({"c": {"b": 2}}, template)


def test_validate_dict_schema():
    template = {"a": 0, "b": 0, "c": {"d": r"^\d*$", "e": 4.2}}

    case = {"a": 3, "b": 65, "c": {"d": "123", "e": 6.9}}

    assert police.validate_dict_schema(case, template)

    case1 = deepcopy(case)
    case1["c"]["d"] = "sick!"
    assert not police.validate_dict_schema(case1, template)

    case1 = deepcopy(case)
    case1["a"] = "sick!"
    assert not police.validate_dict_schema(case1, template)

    case1 = deepcopy(case)
    case1["a"] = 3.3
    assert not police.validate_dict_schema(case1, template)

    case1 = deepcopy(case)
    case1["z"] = 1
    assert not police.validate_dict_schema(case1, template)
    assert police.validate_dict_schema(case1, template, greedy=False)

    case1 = deepcopy(case)
    case1["c"]["z"] = 1
    assert not police.validate_dict_schema(case1, template)
