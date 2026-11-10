"""End-to-end demo against a running stack (API + dispatcher + worker).

    python scripts/demo.py [--base http://localhost:8000] [--key dev-key] [--reject]

Creates a document-review workflow with fan-out/fan-in, a flaky step that is retried,
and a human approval gate; starts a run, approves (or rejects) it, prints the result.
"""

import argparse
import json
import time

import httpx

WORKFLOW = {
    "name": "doc-review",
    "description": "Analyse a document, gate on human approval, then generate code.",
    "nodes": [
        {"id": "extract", "type": "transform", "config": {"template": "Document:\n{{input.document}}"}},
        {"id": "summarize", "type": "llm",
         "config": {"system": "You are a precise technical analyst.",
                    "prompt": "Summarise the key requirements in this document:\n{{nodes.extract.text}}"}},
        {"id": "classify", "type": "mock", "config": {"output": "feature-request", "fail_times": 1},
         "retry": {"max_attempts": 3, "backoff_seconds": 0.2}},
        {"id": "review", "type": "approval", "config": {"message": "Approve the summary before code generation?"}},
        {"id": "codegen", "type": "llm",
         "config": {"prompt": "Write a Python function implementing ({{nodes.classify.value}}):\n{{nodes.summarize.text}}"}},
    ],
    "edges": [
        {"source": "extract", "target": "summarize"},
        {"source": "extract", "target": "classify"},
        {"source": "summarize", "target": "review"},
        {"source": "classify", "target": "review"},
        {"source": "review", "target": "codegen"},
    ],
}


def wait_for(client: httpx.Client, run_id: str, statuses: set[str], timeout: float = 60) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = client.get(f"/v1/runs/{run_id}").raise_for_status().json()
        if run["status"] in statuses:
            return run
        time.sleep(0.3)
    raise TimeoutError(f"run {run_id} did not reach {statuses}; last={run['status']}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://localhost:8000")
    p.add_argument("--key", default="dev-key")
    p.add_argument("--reject", action="store_true")
    args = p.parse_args()

    with httpx.Client(base_url=args.base, headers={"X-API-Key": args.key, "X-Actor-Id": "demo-reviewer"},
                      timeout=30) as c:
        wf = c.post("/v1/workflows", json=WORKFLOW).raise_for_status().json()
        print(f"workflow {wf['name']} v{wf['version']} -> {wf['id']}")

        key = f"demo-{time.time()}"
        run = c.post(f"/v1/workflows/{wf['id']}/runs", headers={"Idempotency-Key": key},
                     json={"input": {"document": "Users need CSV export of their invoices."}}).raise_for_status().json()
        again = c.post(f"/v1/workflows/{wf['id']}/runs", headers={"Idempotency-Key": key},
                       json={"input": {"document": "Users need CSV export of their invoices."}})
        assert again.json()["id"] == run["id"], "idempotent start returned a different run"
        print(f"run {run['id']} started (duplicate start returned the same run, HTTP {again.status_code})")

        run = wait_for(c, run["id"], {"WAITING_APPROVAL", "FAILED"})
        print(f"status: {run['status']}; pending approvals: {run['pending_approvals']}")
        for step in run["steps"]:
            print(f"  {step['node_id']:<10} {step['status']:<17} attempts={step['attempts']}")

        decision = "reject" if args.reject else "approve"
        print(c.post(f"/v1/runs/{run['id']}/approve", json={"decision": decision, "comment": "looks good"}).json())
        dup = c.post(f"/v1/runs/{run['id']}/approve", json={"decision": "approve"})
        print(f"second decision -> HTTP {dup.status_code} {dup.json()['detail']}")

        run = wait_for(c, run["id"], {"SUCCEEDED", "FAILED", "REJECTED", "CANCELLED"})
        print(f"final status: {run['status']}  error: {run['error']}")
        if run["output"]:
            print(json.dumps(run["output"]["result"], indent=2)[:1500])


if __name__ == "__main__":
    main()
