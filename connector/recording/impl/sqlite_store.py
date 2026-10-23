"""用途：按 profile/run 隔离追加原始事件，不覆盖回放循环或跨环境拼接。"""

from pathlib import Path
import sqlite3

from connector.types import Event


class SqliteStore:
    """连接仅归录制线程所有，事件序号保留接收顺序。"""

    def __init__(self, path: Path, profile: str, run_id: str):
        """创建新录制文件；拒绝覆盖已有 run。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise FileExistsError("recording run already exists")
        self.connection = sqlite3.connect(path)
        self.connection.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE recording (
                run_id TEXT PRIMARY KEY, profile TEXT NOT NULL,
                schema_version INTEGER NOT NULL
            );
            CREATE TABLE events (
                sequence INTEGER PRIMARY KEY, kind TEXT NOT NULL,
                channel TEXT NOT NULL, received_ns INTEGER NOT NULL,
                request_id INTEGER NOT NULL, is_last INTEGER NOT NULL,
                error_id INTEGER NOT NULL, payload TEXT NOT NULL
            );
            CREATE INDEX events_kind ON events(kind, sequence);
        """)
        self.connection.execute("INSERT INTO recording VALUES (?, ?, ?)", (run_id, profile, 1))
        self.connection.commit()

    def append(self, event: Event) -> None:
        """追加已复制的原始事件；事务由消费线程批量提交。"""
        self.connection.execute(
            """INSERT INTO events
               (kind, channel, received_ns, request_id, is_last, error_id, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                event.kind, event.channel, event.received_ns, event.request_id,
                int(event.is_last), event.error_id, event.payload,
            ),
        )

    def flush(self) -> None:
        """提交已接收批次，HTTP 只公开已经持久化的最新快照。"""
        self.connection.commit()

    def close(self) -> None:
        """提交尾部事件并关闭句柄。"""
        try:
            self.connection.commit()
        finally:
            self.connection.close()
