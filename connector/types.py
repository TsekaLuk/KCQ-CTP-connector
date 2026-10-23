"""用途：定义跨原生回调、录制线程和 HTTP 的不可变契约。"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol

from connector.errors import Failure

Channel = Literal["md", "td", "local"]

class Operation(StrEnum):
    """只读会话允许的请求；不包含交易动作。"""

    AUTHENTICATE = "authenticate"
    LOGIN = "login"
    CATALOG = "catalog"


class SessionPort(Protocol):
    """运行时依赖的原生边界，不依赖同模块的具体实现。"""

    def request(self, channel: Channel, operation: Operation) -> int:
        """发送只读请求并返回 requestId。"""
        ...

    def subscribe(self) -> None:
        """订阅已配置的具体合约。"""
        ...


class Kind(StrEnum):
    """原生事件类别；事件 payload 只包含已复制的基本值。"""

    SDK = "sdk"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    AUTHENTICATED = "authenticated"
    LOGIN = "login"
    INSTRUMENT = "instrument"
    SUBSCRIBED = "subscribed"
    SNAPSHOT = "snapshot"
    ERROR = "error"
    GAP = "gap"
    STOPPED = "stopped"


@dataclass(frozen=True)
class Event:
    """JSON 字符串在回调返回后不再持有 SDK 内存。"""

    kind: Kind
    channel: Channel
    received_ns: int
    payload: str = "{}"
    request_id: int = 0
    is_last: bool = True
    error_id: int = 0


@dataclass(frozen=True)
class Instrument:
    """保存上游具体期货合约；symbol 同时用于订阅且必须唯一。"""

    raw_id: str
    exchange: str
    name: str
    product_id: str
    tick_size: float
    volume_multiple: int
    min_order_volume: int
    expire_date: str

    @property
    def id(self) -> str:
        """组合交易所和原始大小写，形成来源内稳定 ID。"""
        return f"{self.exchange}.{self.raw_id}"


@dataclass(frozen=True)
class State:
    """由单个消费线程发布快照；HTTP 不读写内部可变集合。"""

    md: str = "not_started"
    td: str = "not_started"
    catalog_ready: bool = False
    instruments: tuple[Instrument, ...] = ()
    subscribed: tuple[str, ...] = ()
    snapshot_count: int = 0
    latest_snapshots: tuple[str, ...] = ()
    sdk: str | None = None
    run_id: str | None = None
    resources_released: bool = True
    failure: Failure | None = None
