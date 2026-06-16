import re
from pathlib import Path

p_sync = Path("synaflow/execution/sync_engine/executor.py")
content = p_sync.read_text()

# Fix _collect_iterator
collect_new = """        except PipelineStopException:
            raise
        except Exception as exc:"""
content = content.replace("        except Exception as exc:", collect_new, 1)

# Fix _unroll_step
unroll_new = """                except PipelineStopException:
                    raise
                except Exception as exc:"""
content = content.replace("                except Exception as exc:", unroll_new, 1) # Wait, there are multiple try-excepts in unroll_step!

p_sync.write_text(content)
