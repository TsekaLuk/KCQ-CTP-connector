"""用途：解析不可变环境配置，仅在此处读取本地 TOML 和凭据环境变量。"""

import os
from pathlib import Path
import tomllib
from typing import Literal, Mapping, Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError
from pydantic import field_validator, model_validator

from connector.errors import Code, ConnectorError, Failure

Profile = Literal["simnow", "simnow-replay", "openctp-replay"]
SOURCE_IDS: dict[Profile, str] = {
    "simnow": "ctp-simnow",
    "simnow-replay": "ctp-simnow-replay",
    "openctp-replay": "ctp-openctp-replay",
}
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8093
SDK_VERSION = "6.7.7.1"
SECRET_OVERRIDES = {
    "CTP_BROKER_ID": "broker_id",
    "CTP_USER_ID": "user_id",
    "CTP_PASSWORD": "password",
    "CTP_APP_ID": "app_id",
    "CTP_AUTH_CODE": "auth_code",
}


class Config(BaseModel):
    """一个服务进程绑定一个环境，运行期间不切换原生库。"""

    model_config = ConfigDict(frozen=True, extra="forbid")
    profile: Profile
    sdk_channel: Literal["ctp", "tts"]
    md_fronts: tuple[str, ...] = Field(min_length=1)
    td_fronts: tuple[str, ...] = Field(min_length=1)
    broker_id: str = Field(min_length=1, repr=False)
    user_id: str = Field(min_length=1, repr=False)
    password: SecretStr = Field(repr=False)
    auth_required: bool = False
    app_id: str = Field(default="", repr=False)
    auth_code: SecretStr = Field(default=SecretStr(""), repr=False)
    recording_symbols: tuple[str, ...] = ()
    data_dir: Path = Path("data")
    queue_capacity: int = Field(default=20000, ge=1)
    bind_host: str = DEFAULT_HOST
    port: int = Field(default=DEFAULT_PORT, ge=1, le=65535)

    @field_validator("md_fronts", "td_fronts")
    @classmethod
    def valid_fronts(cls, fronts: tuple[str, ...]) -> tuple[str, ...]:
        """拒绝未填模板和含账号的非 CTP TCP 地址。"""
        for front in fronts:
            url = urlsplit(front)
            if (
                url.scheme != "tcp" or not url.hostname or not url.port
                or url.username or url.password or url.path or url.query or url.fragment
            ):
                raise ValueError("invalid front")
        return fronts

    @model_validator(mode="after")
    def valid_profile(self) -> Self:
        """核对环境与库通道、认证参数及录制合约列表。"""
        required_channel = "tts" if self.profile == "openctp-replay" else "ctp"
        if self.sdk_channel != required_channel:
            raise ValueError("profile and SDK channel disagree")
        if not self.password.get_secret_value():
            raise ValueError("password required")
        if self.auth_required and (not self.app_id or not self.auth_code.get_secret_value()):
            raise ValueError("authentication fields required")
        if len(set(self.recording_symbols)) != len(self.recording_symbols):
            raise ValueError("duplicate recording symbols")
        if any(not symbol or not symbol.isascii() for symbol in self.recording_symbols):
            raise ValueError("raw instrument IDs required")
        return self

    @property
    def source_id(self) -> str:
        """返回隔离实时与回放缓存的稳定来源 ID。"""
        return SOURCE_IDS[self.profile]


def load_config(path: Path, environ: Mapping[str, str]) -> Config:
    """读取明确路径；错误仅公开字段位置，避免 Pydantic 输入泄露密码。"""
    if not path.is_file():
        raise ConnectorError(Failure(
            Code.CONFIG_MISSING,
            "缺少本地配置：复制 config.example.toml 为 config.toml 并填写前置与账号。",
        ))
    try:
        with path.open("rb") as handle:
            values = tomllib.load(handle)
        for variable, field in SECRET_OVERRIDES.items():
            if variable in environ:
                values[field] = environ[variable]
        config = Config.model_validate(values)
        if not config.data_dir.is_absolute():
            config = config.model_copy(update={"data_dir": path.resolve().parent / config.data_dir})
        return config
    except ValidationError as error:
        fields = sorted({
            ".".join(str(part) for part in item["loc"]) or "profile/authentication"
            for item in error.errors(include_input=False, include_url=False)
        })
        raise ConnectorError(Failure(
            Code.CONFIG_INVALID, "请检查配置字段：" + ", ".join(fields),
        )) from None
    except (OSError, ValueError):
        raise ConnectorError(Failure(
            Code.CONFIG_INVALID, "无法读取配置或 TOML 格式错误；请检查本地文件。",
        )) from None


def config_path(argument: str | None = None) -> Path:
    """解析 CLI 或 CTP_CONFIG；默认读取当前后端目录的 config.toml。"""
    if argument is not None:
        return Path(argument)
    return Path(os.environ.get("CTP_CONFIG", "config.toml"))
