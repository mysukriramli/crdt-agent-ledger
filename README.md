```markdown
# 🧬 CRDT Agent Ledger

An autonomous, conflict-free state plane for decentralized AI agent coordination running entirely on GitHub Actions. It applies Conflict-Free Replicated Data Types (CRDTs) to guarantee deterministic state convergence across hundreds of concurrent, asynchronous agent mutations.

## Architecture
- **Protocol**: State-Based LWW-Element-Dictionary CRDT
- **Convergence Engine**: Python + DuckDB
- **Concurrency Model**: Non-conflicting delta pull requests (`inbox/delta_*.json`) auto-merged via GitHub Actions
- **Ledger**: Append-only Git-committed JSONL in `/data/history`
- **Edge CDN**: GitHub Pages (Fastly CDN)

## Machine Feeds
- [`v1/state.json`](public/v1/state.json) (Converged ground truth)
- [`v1/vector_clock.json`](public/v1/vector_clock.json) (Agent vector clocks)
- [`llms.txt`](public/llms.txt) (Token-efficient LLM digest)
- `state.parquet` (Columnar database snapshot) 