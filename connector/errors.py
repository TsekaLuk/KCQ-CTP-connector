"""用途：集中定义诊断错误和 V1 协议错误，避免散落字符串。"""

from dataclasses import dataclass
from enum import StrEnum


class Code(StrEnum):
    """诊断错误码；用于本地排障和录制审计。"""

    CONFIG_MISSING = "CONFIG_MISSING"
    CONFIG_INVALID = "CONFIG_INVALID"
    SDK_UNAVAILABLE = "SDK_UNAVAILABLE"
    SDK_CHANNEL_MISMATCH = "SDK_CHANNEL_MISMATCH"
    NATIVE_FAILURE = "NATIVE_FAILURE"
    REQUEST_REJECTED = "REQUEST_REJECTED"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    DISCONNECTED = "DISCONNECTED"
    QUEUE_OVERFLOW = "QUEUE_OVERFLOW"
    CALLBACK_FAILURE = "CALLBACK_FAILURE"
    RECORDING_FAILURE = "RECORDING_FAILURE"
    SYMBOL_UNAVAILABLE = "SYMBOL_UNAVAILABLE"
    STOP_TIMEOUT = "STOP_TIMEOUT"
    DIAGNOSIS_TIMEOUT = "DIAGNOSIS_TIMEOUT"


class ProtocolCode(StrEnum):
    """沿用 KCQ V1 的确定性失败契约。"""

    UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"
    UPSTREAM_UNAVAILABLE = "UPSTREAM_UNAVAILABLE"
    INVALID_REQUEST = "INVALID_REQUEST"
    INSTRUMENT_NOT_FOUND = "INSTRUMENT_NOT_FOUND"


@dataclass(frozen=True)
class Failure:
    """保存可公开的诊断；不保存 SDK 原始报错文本或凭据。"""

    code: Code
    message: str
    upstream_code: int | None = None


class ConnectorError(Exception):
    """将安全诊断从 I/O 边界交给运行时。"""

    def __init__(self, failure: Failure):
        """接收脱敏失败，不拼接底层异常内容。"""
        super().__init__(failure.message)
        self.failure = failure
