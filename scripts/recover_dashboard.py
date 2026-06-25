import json
from pathlib import Path

TRANSCRIPT = Path(
    r"C:\Users\naras\.cursor\projects\c-Users-naras-Desktop-cloudmetrics\agent-transcripts"
    r"\46849c41-970c-402d-b0cc-d7808b92f014\46849c41-970c-402d-b0cc-d7808b92f014.jsonl"
)
OUT = Path(r"C:\Users\naras\Desktop\cloudmetrics\server\templates\dashboard.html")

candidates = []
with TRANSCRIPT.open(encoding="utf-8") as handle:
    for line_no, line in enumerate(handle, 1):
        if "dashboard.html" not in line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        for part in obj.get("message", {}).get("content", []):
            if part.get("type") != "tool_use" or part.get("name") != "Write":
                continue
            payload = part.get("input", {})
            if not str(payload.get("path", "")).endswith("dashboard.html"):
                continue
            contents = payload.get("contents", "")
            candidates.append(
                (
                    line_no,
                    len(contents),
                    contents.strip().endswith("</html>"),
                    "view-cost-optimization" in contents,
                    "loadTenantBranding" in contents,
                    contents,
                )
            )

for item in sorted(candidates, key=lambda row: row[1], reverse=True):
    print(item[:5])

complete = [c for c in candidates if c[2] and c[4]]
if not complete:
    complete = [c for c in candidates if c[2]]
if not complete:
    raise SystemExit("No complete dashboard.html snapshot found")

best = max(complete, key=lambda item: item[1])
OUT.write_text(best[5], encoding="utf-8")
print(f"Restored dashboard from transcript line {best[0]} ({best[1]} bytes)")
