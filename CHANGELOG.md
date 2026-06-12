# CHANGELOG



## v0.5.1 (2026-06-12)

### Chore

* chore: remove __pycache__ and add to gitignore ([`62810dc`](https://github.com/humansoftware/synaflow/commit/62810dce66e841ec59c2323571acb970d071c9a4))

* chore: sync uv.lock version bump ([`1900d63`](https://github.com/humansoftware/synaflow/commit/1900d63efba19d4c4311963d2f14873399fd8877))

* chore: remove throwaway automation scripts ([`27c1579`](https://github.com/humansoftware/synaflow/commit/27c1579594990caace19c010eed2d3bf0c1d4774))

* chore: fix isort and black conflict and format files ([`ba72755`](https://github.com/humansoftware/synaflow/commit/ba7275597b91bd02cc87e9b770ad342ae35dcba2))

### Documentation

* docs: append sub-pipelines to roadmap ([`fbd8f09`](https://github.com/humansoftware/synaflow/commit/fbd8f095c3d97fb03a728bcb5012e13771f583d4))

* docs: add project roadmap ([`6025b45`](https://github.com/humansoftware/synaflow/commit/6025b45cc92e56fad4478bd2c70a7a16a7132ece))

### Fix

* fix: change default OnError to CONTINUE ([`9c59894`](https://github.com/humansoftware/synaflow/commit/9c598942e0db0b834ca364607085b19d36920e61))

* fix: force materialization for steps using OnError.STOP ([`5df1c4b`](https://github.com/humansoftware/synaflow/commit/5df1c4beedaf973d7017139982dac6c340303033))

### Test

* test: add param injection tests for intermediate steps ([`2637aac`](https://github.com/humansoftware/synaflow/commit/2637aac2f756c046624ed22155a53fdc8ac4796e))

* test: improve OnError.STOP validation test ([`9ce3532`](https://github.com/humansoftware/synaflow/commit/9ce3532d0b1d8cef131e8e01009803c71bfcddf8))

* test: add downstream verification for OnError.CONTINUE ([`c57dbb3`](https://github.com/humansoftware/synaflow/commit/c57dbb3759506aa07dad9c8c4c99381e7dda8c61))

* test: set asyncio_mode auto and remove manual async markers ([`e44b6ca`](https://github.com/humansoftware/synaflow/commit/e44b6caa5ecf577ed7550cdf6fe8ecf32a112de0))

* test: add parity test and gitignore ([`4291756`](https://github.com/humansoftware/synaflow/commit/4291756b6d3ef3b1b2aec64cc9126a06f97c11f6))

### Unknown

* Merge pull request #2 from humansoftware/feature/roadmap

fix: Materialization bug on OnError.STOP ([`0b38bb7`](https://github.com/humansoftware/synaflow/commit/0b38bb775530145c544f7641730bfdb43ee31133))

* Merge pull request #1 from humansoftware/feature/roadmap

docs: add project roadmap ([`3d3f3a4`](https://github.com/humansoftware/synaflow/commit/3d3f3a4a38081cc8ecf017b18cbf71e710dd6259))


## v0.5.0 (2026-06-12)

### Feature

* feat: canonical stream json and runner guard tests ([`5f4828b`](https://github.com/humansoftware/synaflow/commit/5f4828b7cec7ad70142bd3ba56f29598d1b2b84b))

### Test

* test: add validation test for mixed sync/async pipelines ([`615c98b`](https://github.com/humansoftware/synaflow/commit/615c98bcbcc86202f78f5e8838f15d9d8782482f))


## v0.4.1 (2026-06-12)

### Fix

* fix: async materialization, queue fallbacks, and error propagation ([`5b2f249`](https://github.com/humansoftware/synaflow/commit/5b2f249f09e088438db73c5059d27bb1f23bc44e))

### Test

* test: rename sync/async to test_sync/test_async for static imports ([`e3f396b`](https://github.com/humansoftware/synaflow/commit/e3f396b9fe78d85e14fca8058e16995a57ba8456))

* test: move corpus to tests/sync/corpus and tests/async/corpus ([`44843fb`](https://github.com/humansoftware/synaflow/commit/44843fba5c105dd149c8c1d8c5906ca7e79cb848))

* test: split corpus into sync_topologies and async_topologies ([`a575f06`](https://github.com/humansoftware/synaflow/commit/a575f06dd56bd95801cec933f0e1d3415f36cea7))


## v0.4.0 (2026-06-12)

### Feature

* feat: enforce strict sync/async pipeline color boundaries during validation ([`bd5ef62`](https://github.com/humansoftware/synaflow/commit/bd5ef62654d51f280ba17eec4757efb02fb9af3e))


## v0.3.0 (2026-06-12)

### Documentation

* docs: clarify trade-offs between corpus and unit tests in HACKING.md ([`ea002d6`](https://github.com/humansoftware/synaflow/commit/ea002d608e0eefb8d36c3b98cd6001595031f82c))

* docs: add HACKING.md with contribution guidelines and architectural principles ([`6b1fb0d`](https://github.com/humansoftware/synaflow/commit/6b1fb0d8af5d2e95b56344d1eb86bea1c67e4f4c))

### Feature

* feat: implement AsyncPipelineExecutor with async streaming via asyncio.Queue ([`8103255`](https://github.com/humansoftware/synaflow/commit/810325519a7c800dd4473618b2ccbe44e314f3b5))

### Refactor

* refactor: replace magic strings with Enums and classes in executor ([`70bfb5f`](https://github.com/humansoftware/synaflow/commit/70bfb5fe41c6a3fcc5ce51010bb23dede6579750))

### Test

* test: refactor all runner tests to use parameterized fixture and order-independent contract assertions ([`347da87`](https://github.com/humansoftware/synaflow/commit/347da87460f7f07dbf6eb83d85efc72d9e2fcfc2))


## v0.2.0 (2026-06-11)

### Chore

* chore: apply pre-commit formatting ([`58da73e`](https://github.com/humansoftware/synaflow/commit/58da73eb7bb8365eba53e3495733d89fc9ac00ce))

### Documentation

* docs: Add Execution Semantics and Custom Runners section ([`acce236`](https://github.com/humansoftware/synaflow/commit/acce2363aa00db4edee77a10141e3b507f3feeac))

* docs: Add DAG JSON to README and rename fixtures to corpus ([`c078184`](https://github.com/humansoftware/synaflow/commit/c0781841ce57c8513ebeaf31d6b121ee8bfa5533))

### Feature

* feat: add fibonacci streaming generator to corpus ([`bb3624a`](https://github.com/humansoftware/synaflow/commit/bb3624a88c4a1b9dbcd354b3c36a35cd4c0f7fd4))

* feat: decouple topological sort into PipelineDef and expand corpus ([`a98980d`](https://github.com/humansoftware/synaflow/commit/a98980dea1093e85df02bc99a6710702a6c1f151))


## v0.1.0 (2026-06-11)

### Documentation

* docs: Add MIT License ([`5bbed31`](https://github.com/humansoftware/synaflow/commit/5bbed317dbfdee421e2c29630b5de029dbd652af))

* docs: Add framework comparisons to README ([`d8f2c53`](https://github.com/humansoftware/synaflow/commit/d8f2c53e3440ee470760e3276c0f8c894a63cba3))

### Feature

* feat: Add CI/CD workflows for testing and semantic release ([`30f4218`](https://github.com/humansoftware/synaflow/commit/30f4218b2b265c39ce56f17b33572c24b928ab4f))

### Fix

* fix: remove build_command from semantic release ([`3232a3e`](https://github.com/humansoftware/synaflow/commit/3232a3e5d33621803955ed341ee5bd3a29c10d8c))

* fix: bump python-semantic-release to v9.8.1 to fix debian repository error ([`e691d1c`](https://github.com/humansoftware/synaflow/commit/e691d1cc19905a6d81f0634911540ddc337fd97c))

### Unknown

* Add pre-commit, improve README, update org in pyproject ([`6c8fbe8`](https://github.com/humansoftware/synaflow/commit/6c8fbe884d0f7f812980f554cbe22a58056faada))

* Initial commit for SynaFlow open source release ([`1cd3c94`](https://github.com/humansoftware/synaflow/commit/1cd3c94f77314898ebb5834e5e2f5d1e8780c224))
