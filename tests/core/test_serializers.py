import io
import dataclasses
from synaflow.serializers.csv import CsvSerializer


@dataclasses.dataclass
class MyRow:
    name: str
    age: int


def test_csv_serializer_custom_delimiter():
    serializer = CsvSerializer(delimiter=";")
    f = io.StringIO()
    serializer.serialize(f, [{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    assert f.getvalue().replace("\r\n", "\n") == "a;b\n1;2\n3;4\n"


def test_csv_serializer_custom_fieldnames():
    serializer = CsvSerializer(fieldnames=["b", "a"])
    f = io.StringIO()
    serializer.serialize(f, [{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    assert f.getvalue().replace("\r\n", "\n") == "b,a\n2,1\n4,3\n"


def test_csv_serializer_dataclass_rows():
    serializer = CsvSerializer()
    f = io.StringIO()
    serializer.serialize(f, [MyRow(name="alice", age=30), MyRow(name="bob", age=25)])
    assert f.getvalue().replace("\r\n", "\n") == "name,age\nalice,30\nbob,25\n"


def test_csv_serializer_single_string_data():
    serializer = CsvSerializer()
    f = io.StringIO()
    serializer.serialize(f, "hello")
    assert f.getvalue().replace("\r\n", "\n") == "hello\n"
