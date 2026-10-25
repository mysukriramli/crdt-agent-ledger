import os
import glob
import json
import hashlib
import duckdb
from datetime import datetime, timezone

DB_FILE = "data/crdt_mesh.db"
HISTORY_DIR = "data/history"
INBOX_DIR = "inbox"
EXPORT_DIR = "public"
API_DIR = os.path.join(EXPORT_DIR, "v1")

def compute_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()

def harvest_pending_deltas():
    deltas = []
    processed_files = []
    
    for filepath in sorted(glob.glob(os.path.join(INBOX_DIR, "delta_*.json"))):
        if os.path.islink(filepath):
            continue
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "operations" in data and "agent_id" in data:
                    deltas.append(data)
                    processed_files.append(filepath)
        except Exception as e:
            print(f"Skipping malformed delta {filepath}: {e}")
            
    return deltas, processed_files

def build_static_api(conn):
    os.makedirs(API_DIR, exist_ok=True)
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. Converged Live Entity State
    cur = conn.execute("""
        SELECT entity_id, property_key, property_value, lamport_timestamp, wall_time_utc, agent_id
        FROM crdt_state
        WHERE is_deleted = false
        ORDER BY entity_id ASC, property_key ASC
    """)
    
    converged_entities = {}
    for row in cur.fetchall():
        e_id, key, val, l_ts, w_time, agent = row
        if e_id not in converged_entities:
            converged_entities[e_id] = {
                "_entity_id": e_id,
                "_meta": {"last_lamport": l_ts, "last_updated_utc": w_time, "last_agent": agent}
            }
        try:
            converged_entities[e_id][key] = json.loads(val)
        except Exception:
            converged_entities[e_id][key] = val
        if l_ts > converged_entities[e_id]["_meta"]["last_lamport"]:
            converged_entities[e_id]["_meta"]["last_lamport"] = l_ts
            converged_entities[e_id]["_meta"]["last_updated_utc"] = w_time
            converged_entities[e_id]["_meta"]["last_agent"] = agent

    with open(os.path.join(API_DIR, "state.json"), "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {"updated_at": now_iso, "total_entities": len(converged_entities)},
            "entities": converged_entities
        }, f, indent=2)

    # 2. Vector Clock Map
    cur_vc = conn.execute("SELECT agent_id, last_lamport_timestamp, last_sync_utc FROM vector_clocks ORDER BY agent_id ASC")
    vector_clocks = {row[0]: {"last_lamport": row[1], "last_sync_utc": row[2]} for row in cur_vc.fetchall()}

    with open(os.path.join(API_DIR, "vector_clock.json"), "w", encoding="utf-8") as f:
        json.dump({"metadata": {"updated_at": now_iso}, "vector_clocks": vector_clocks}, f, indent=2)

    # 3. Token-Efficient Digest
    lines = [
        "# CRDT Conflict-Free State Mesh",
        f"> Last Sync: {now_iso} UTC",
        "> Protocol: State-Based LWW-Element-Dictionary CRDT (Deterministic Convergence)",
        "",
        "## Active Converged Entities"
    ]
    for e_id, e_data in list(converged_entities.items())[:25]:
        meta = e_data.get("_meta", {})
        props = {k: v for k, v in e_data.items() if not k.startswith("_")}
        lines.append(f"- **{e_id}** [Lamport: {meta.get('last_lamport')} | Agent: `{meta.get('last_agent')}`]: {json.dumps(props, separators=(',', ':'))}")

    with open(os.path.join(EXPORT_DIR, "llms.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    # 4. Minimal Web Dashboard
    html = f"""<!DOCTYPE html><html><head><title>CRDT Agent Ledger</title></head>
    <body style='font-family:system-ui,sans-serif;max-width:900px;margin:2rem auto;line-height:1.6;padding:0 1rem;'>
    <h1>🧬 CRDT Agent State Mesh</h1>
    <p>Conflict-free replicated data plane for concurrent autonomous multi-agent state convergence.</p>
    <h3>Machine Feeds</h3>
    <ul>
      <li><a href='v1/state.json'>/v1/state.json</a> (Converged Global State)</li>
      <li><a href='v1/vector_clock.json'>/v1/vector_clock.json</a> (Global Vector Clocks)</li>
      <li><a href='llms.txt'>llms.txt</a> (Token-Efficient Digest)</li>
      <li><a href='state.parquet'>state.parquet</a> (Columnar Parquet Snapshot)</li>
    </ul>
    <h3>Live Materialized Entities ({len(converged_entities)})</h3>
    <pre style='background:#f4f4f4;padding:1rem;border-radius:4px;overflow-x:auto;'>{json.dumps(dict(list(converged_entities.items())[:10]), indent=2)}</pre>
    </body></html>"""
    with open(os.path.join(EXPORT_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

def run():
    os.makedirs(INBOX_DIR, exist_ok=True)
    os.makedirs("data", exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)

    conn = duckdb.connect(DB_FILE)
    with open("schema.sql", "r", encoding="utf-8") as f:
        conn.execute(f.read())

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    date_str = now.strftime("%Y-%m-%d")
    year_str = now.strftime("%Y")
    month_str = now.strftime("%m")

    month_dir = os.path.join(HISTORY_DIR, year_str, month_str)
    os.makedirs(month_dir, exist_ok=True)
    daily_history_file = os.path.join(month_dir, f"{date_str}.jsonl")

    deltas, processed_files = harvest_pending_deltas()

    conn.execute("BEGIN TRANSACTION")

    for delta in deltas:
        delta_id = delta.get("delta_id", hashlib.sha256(json.dumps(delta).encode("utf-8")).hexdigest()[:16])
        agent_id = str(delta.get("agent_id", "unknown_agent")).replace("'", "''")
        base_lamport = int(delta.get("lamport_timestamp", 1))
        operations = delta.get("operations", [])

        # Update Vector Clock
        conn.execute(f"""
            INSERT INTO vector_clocks (agent_id, last_lamport_timestamp, last_sync_utc)
            VALUES ('{agent_id}', {base_lamport}, '{now_iso}')
            ON CONFLICT (agent_id) DO UPDATE SET
                last_lamport_timestamp = GREATEST(vector_clocks.last_lamport_timestamp, {base_lamport}),
                last_sync_utc = '{now_iso}'
        """)

        # Execute LWW-Element Convergence
        for op in operations:
            e_id = str(op.get("entity_id", "")).replace("'", "''")
            prop_key = str(op.get("key", "")).replace("'", "''")
            raw_val = json.dumps(op.get("value", "")) if not isinstance(op.get("value"), str) else op.get("value")
            prop_val = raw_val.replace("'", "''")
            op_lamport = int(op.get("lamport_timestamp", base_lamport))
            wall_time = str(op.get("wall_time_utc", now_iso)).replace("'", "''")
            is_del = True if op.get("type", "SET").upper() == "DELETE" else False

            existing = conn.execute(f"""
                SELECT lamport_timestamp, agent_id FROM crdt_state
                WHERE entity_id = '{e_id}' AND property_key = '{prop_key}'
            """).fetchone()

            # LWW Tie-breaking: Higher Lamport wins; lexicographical agent_id breaks identical timestamp ties
            should_update = False
            if not existing:
                should_update = True
            else:
                curr_lts, curr_agent = existing
                if op_lamport > curr_lts:
                    should_update = True
                elif op_lamport == curr_lts and agent_id > curr_agent:
                    should_update = True

            if should_update:
                conn.execute(f"""
                    INSERT OR REPLACE INTO crdt_state (
                        entity_id, property_key, property_value,
                        lamport_timestamp, wall_time_utc, agent_id, is_deleted
                    ) VALUES (
                        '{e_id}', '{prop_key}', '{prop_val}',
                        {op_lamport}, '{wall_time}', '{agent_id}', {is_del}
                    )
                """)

        commit_id = conn.execute("SELECT nextval('seq_commit_id')").fetchone()[0]
        conn.execute(f"""
            INSERT INTO crdt_ledger (commit_id, timestamp_utc, delta_id, agent_id, lamport_timestamp, operations_count)
            VALUES ({commit_id}, '{now_iso}', '{delta_id}', '{agent_id}', {base_lamport}, {len(operations)})
        """)

        ledger_entry = {
            "commit_id": commit_id,
            "timestamp_utc": now_iso,
            "delta_id": delta_id,
            "agent_id": agent_id,
            "lamport_timestamp": base_lamport,
            "operations_count": len(operations)
        }
        with open(daily_history_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(ledger_entry) + "\n")

    conn.execute("COMMIT")

    # Remove processed delta files
    for filepath in processed_files:
        try:
            os.remove(filepath)
        except Exception:
            pass

    conn.execute(f"COPY (SELECT * FROM crdt_state WHERE is_deleted = false) TO '{EXPORT_DIR}/state.parquet' (FORMAT PARQUET)")
    build_static_api(conn)
    conn.close()

if __name__ == "__main__":
    run()
