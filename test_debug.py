from synaflow.core.exceptions import PipelineStopException

e = PipelineStopException("foo", cause=ValueError("stop"))
print(repr(e))
print(repr(e.cause))
print(repr(e.cause or e))
