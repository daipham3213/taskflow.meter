# Examples

Each script runs on its own, against a temporary database where it needs one.

| Script | What it shows |
| --- | --- |
| [`attach_in_process.py`](attach_in_process.py) | Watching a flow from inside the process running it: per-task progress and the flow's graph, with no database |
| [`serve_persistence.py`](serve_persistence.py) | Serving the API over a taskflow logbook a separate thread is writing to, the read-only path |

```bash
python examples/attach_in_process.py
python examples/serve_persistence.py    # then curl localhost:8080
```

The collector deployment does not need an example script: it is two commands.

```bash
taskflow-meter upgrade --store-url postgresql://.../meter
taskflow-meter collect --amqp-url amqp://broker// --store-url postgresql://.../meter
taskflow-meter serve   --store-url postgresql://.../meter
```
