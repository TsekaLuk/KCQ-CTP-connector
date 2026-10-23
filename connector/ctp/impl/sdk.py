"""用途：只读核验 SDK 版本和动态库指纹；绝不在服务中切换通道。"""

import base64
import hashlib
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path
import platform
import subprocess
import tempfile

from connector.config import SDK_VERSION
from connector.errors import Code, ConnectorError, Failure

LINUX_SDK_LOCALE = "zh_CN.GB18030"


def verify_linux_locale() -> None:
    """导入 Linux 绑定前只读检查 locale，避免 C++ 静态初始化直接终止进程。"""
    if platform.system() != "Linux":
        return
    try:
        result = subprocess.run(
            ["locale", "-a"], capture_output=True, text=True, check=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        raise ConnectorError(Failure(
            Code.SDK_UNAVAILABLE, "无法查询 Linux locale；请安装 locales 并生成 zh_CN.GB18030。",
        )) from None
    if LINUX_SDK_LOCALE.casefold() not in {name.strip().casefold() for name in result.stdout.splitlines()}:
        raise ConnectorError(Failure(
            Code.SDK_UNAVAILABLE, "Linux SDK 需要 zh_CN.GB18030；请按 README 生成该 locale 后重试。",
        ))


def digest(path: Path) -> bytes:
    """读取原生库，返回 SHA256；不加载或修改该文件。"""
    return hashlib.sha256(path.read_bytes()).digest()


def verify_channel(channel: str) -> None:
    """标准库核对 wheel RECORD；TTS 核对离线准备的两份库缓存。"""
    package = distribution("openctp-ctp")
    if package.version != SDK_VERSION or package.files is None:
        raise ConnectorError(Failure(Code.SDK_UNAVAILABLE, "SDK 版本不符，请按 uv.lock 安装。"))
    libraries = [
        entry for entry in package.files
        if entry.name.startswith(("thostmduser", "libthostmduser", "thosttrader", "libthosttrader"))
        and entry.suffix in (".dll", ".so", ".dylib")
    ]
    if len(libraries) != 2:
        raise ConnectorError(Failure(Code.SDK_UNAVAILABLE, "未找到完整的 Md/Td 原生库。"))
    tts_dir = None
    if channel == "tts":
        if platform.system() not in ("Linux", "Windows"):
            raise ConnectorError(Failure(
                Code.SDK_CHANNEL_MISMATCH, "TTS 尚未验证 Mac 原生库，请使用已准备的 Linux x86_64 / Windows 环境。",
            ))
        tts_dir = Path(distribution("openctp-ctp-channels").locate_file(
            "openctp_ctp_channels/_chan_tts",
        ))
    for entry in libraries:
        actual = digest(Path(package.locate_file(entry)))
        if channel == "ctp":
            fingerprint = base64.urlsafe_b64encode(actual).decode("ascii").rstrip("=")
            matches = entry.hash is not None and entry.hash.mode == "sha256" and entry.hash.value == fingerprint
        else:
            role = "mduser" if "mduser" in entry.name else "trader"
            matches = tts_dir is not None and tts_dir.is_dir() and any(
                role in candidate.name and candidate.is_file() and digest(candidate) == actual
                for candidate in tts_dir.iterdir()
            )
        if not matches:
            raise ConnectorError(Failure(
                Code.SDK_CHANNEL_MISMATCH,
                "动态库与配置通道不匹配；在独立 .venv 准备两份库后重新启动，不能只换前置地址。",
            ))


def inspect_sdk(channel: str = "ctp", exercise: bool = False) -> dict[str, str | bool]:
    """核对后导入绑定；可离线创建和释放 API，不连接任何前置。"""
    try:
        verify_linux_locale()
        verify_channel(channel)
        from openctp_ctp import mdapi, tdapi

        report: dict[str, str | bool] = {
            "package": version("openctp-ctp"),
            "python": platform.python_version(),
            "os": platform.system(),
            "arch": platform.machine(),
            "channel": channel,
            "libraryFingerprintsVerified": True,
            "mdVersion": mdapi.CThostFtdcMdApi.GetApiVersion(),
            "tdVersion": tdapi.CThostFtdcTraderApi.GetApiVersion(),
            "instancesCreatedAndReleased": False,
        }
        if exercise:
            with tempfile.TemporaryDirectory(prefix="kcq-ctp-sdk-") as directory:
                for name, factory in (
                    ("md", mdapi.CThostFtdcMdApi.CreateFtdcMdApi),
                    ("td", tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi),
                ):
                    flow = Path(directory) / name
                    flow.mkdir()
                    api = factory(str(flow) + "/")
                    if api is None:
                        raise RuntimeError("native factory returned null")
                    api.Release()
            report["instancesCreatedAndReleased"] = True
        return report
    except ConnectorError:
        raise
    except (ImportError, OSError, RuntimeError, PackageNotFoundError):
        raise ConnectorError(Failure(
            Code.SDK_UNAVAILABLE,
            "原生 SDK 不可用；核对 Python 3.12、OS/CPU、uv.lock 与库通道。",
        )) from None
