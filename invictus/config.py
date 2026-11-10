from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All settings come from INVICTUS_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="INVICTUS_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://invictus:invictus@localhost:5433/invictus"
    redis_url: str = "redis://localhost:6380/0"

    # "key:tenant,key2:tenant2". Each API key maps to exactly one tenant.
    api_keys: str = "dev-key:demo"

    # Queue / lease
    stream_key: str = "invictus:runs"
    dead_letter_key: str = "invictus:runs:dlq"
    consumer_group: str = "workers"
    lease_seconds: float = 30.0
    max_deliveries: int = 10          # Redis deliveries before a message is dead-lettered
    max_run_recoveries: int = 5       # lost-lease takeovers before a run is failed as poison

    # Worker
    worker_concurrency: int = 2
    poll_block_ms: int = 2000

    # Dispatcher (outbox relay + sweeper)
    dispatcher_interval_seconds: float = 0.5
    sweep_interval_seconds: float = 15.0
    sweep_stale_seconds: float = 60.0

    # Quotas
    max_active_runs_per_tenant: int = 100

    # Tools
    llm_provider: str = "fake"        # "fake" or "anthropic"
    llm_model: str = "claude-opus-5-5"
    http_allowed_hosts: str = ""      # comma-separated allowlist; empty disables the http tool
    enable_mock_tool: bool = True     # deterministic test tool used for fault injection

    # Fault injection: "node_id:point[:max_recoveries]" entries, comma-separated. See faults.py.
    faults: str = ""

    def api_key_map(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for pair in filter(None, (p.strip() for p in self.api_keys.split(","))):
            key, _, tenant = pair.partition(":")
            if key and tenant:
                out[key] = tenant
        return out

    def allowed_http_hosts(self) -> set[str]:
        return {h.strip().lower() for h in self.http_allowed_hosts.split(",") if h.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()
