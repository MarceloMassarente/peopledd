# Experimental modules

Code here is **not** wired into `run_pipeline_graph` or the worker. Use for time-boxed spikes only.

## LangGraph linear spike

Demonstrates a minimal two-node `StateGraph` (step A then step B) to compare ergonomics with the native `GraphRunner` + `pipeline_linear` layout.

```bash
pip install -e ".[langgraph-spike]"
python -m peopledd.experimental.langgraph_linear_spike
```

If `langgraph` is not installed, the module exits with a short message (no error).
