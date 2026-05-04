"""
Configuration management for distributed sync system.
Loads from environment variables and .env files.
"""
import os
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class RaftConfig:
    """Raft consensus algorithm configuration."""
    election_timeout_min: float = float(os.getenv("ELECTION_TIMEOUT_MIN", "0.15"))   # seconds
    election_timeout_max: float = float(os.getenv("ELECTION_TIMEOUT_MAX", "0.30"))   # seconds
    heartbeat_interval: float = float(os.getenv("HEARTBEAT_INTERVAL", "0.05"))       # seconds
    rpc_timeout: float = float(os.getenv("RPC_TIMEOUT", "0.10"))                     # seconds
    max_log_entries_per_rpc: int = int(os.getenv("MAX_LOG_ENTRIES_PER_RPC", "100"))
    snapshot_threshold: int = int(os.getenv("SNAPSHOT_THRESHOLD", "1000"))


@dataclass
class NodeConfig:
    """Node configuration."""
    node_id: str = os.getenv("NODE_ID", "node-1")
    host: str = os.getenv("NODE_HOST", "127.0.0.1")
    port: int = int(os.getenv("NODE_PORT", "8000"))
    peers: List[str] = field(default_factory=lambda: [
        p.strip() for p in os.getenv("PEERS", "").split(",") if p.strip()
    ])
    data_dir: str = os.getenv("DATA_DIR", "./data")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


@dataclass
class RedisConfig:
    """Redis connection configuration."""
    host: str = os.getenv("REDIS_HOST", "127.0.0.1")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))
    password: Optional[str] = os.getenv("REDIS_PASSWORD", None)
    max_connections: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))


@dataclass
class LockConfig:
    """Distributed lock configuration."""
    default_ttl: float = float(os.getenv("LOCK_DEFAULT_TTL", "30.0"))
    max_ttl: float = float(os.getenv("LOCK_MAX_TTL", "300.0"))
    retry_interval: float = float(os.getenv("LOCK_RETRY_INTERVAL", "0.05"))
    deadlock_detection_interval: float = float(os.getenv("DEADLOCK_DETECTION_INTERVAL", "5.0"))


@dataclass
class QueueConfig:
    """Distributed queue configuration."""
    virtual_nodes: int = int(os.getenv("QUEUE_VIRTUAL_NODES", "150"))
    replication_factor: int = int(os.getenv("QUEUE_REPLICATION_FACTOR", "2"))
    max_message_size: int = int(os.getenv("MAX_MESSAGE_SIZE", "1048576"))   # 1MB
    message_ttl: int = int(os.getenv("MESSAGE_TTL", "86400"))               # 24h
    ack_timeout: float = float(os.getenv("ACK_TIMEOUT", "30.0"))


@dataclass
class CacheConfig:
    """Distributed cache configuration."""
    max_size: int = int(os.getenv("CACHE_MAX_SIZE", "1000"))
    ttl: float = float(os.getenv("CACHE_TTL", "300.0"))
    replacement_policy: str = os.getenv("CACHE_REPLACEMENT_POLICY", "LRU")
    coherence_protocol: str = os.getenv("CACHE_COHERENCE_PROTOCOL", "MESI")
    metrics_interval: float = float(os.getenv("CACHE_METRICS_INTERVAL", "10.0"))


@dataclass
class AppConfig:
    """Top-level application configuration."""
    raft: RaftConfig = field(default_factory=RaftConfig)
    node: NodeConfig = field(default_factory=NodeConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    lock: LockConfig = field(default_factory=LockConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)


# Singleton config instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    global _config
    if _config is None:
        _config = AppConfig()
    return _config