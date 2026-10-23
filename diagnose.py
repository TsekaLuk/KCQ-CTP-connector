"""用途：提供无需凭据的 SDK 自检和有明确截止时间的真实连接诊断。"""

import argparse
from dataclasses import asdict
import json
import os
from time import monotonic, sleep

from connector.config import config_path
from connector.ctp.impl.sdk import inspect_sdk
from connector.errors import Code, ConnectorError, Failure
from connector.runtime import Runtime


def main() -> int:
    """输出脱敏 JSON；超时或任一失败均返回非零退出码。"""
    parser = argparse.ArgumentParser(description="KCQ CTP SDK/连接诊断")
    parser.add_argument("--sdk-only", action="store_true", help="只核对库并创建/释放 API，不联网")
    parser.add_argument("--sdk-channel", choices=("ctp", "tts"), default="ctp")
    parser.add_argument("--config")
    parser.add_argument("--seconds", type=float, default=30, help="真实连接最长观察时间")
    arguments = parser.parse_args()
    if arguments.seconds <= 0:
        parser.error("--seconds 必须大于 0")
    if arguments.sdk_only:
        try:
            print(json.dumps(inspect_sdk(arguments.sdk_channel, exercise=True), ensure_ascii=False, indent=2))
            return 0
        except ConnectorError as error:
            print(json.dumps({"failure": asdict(error.failure)}, ensure_ascii=False, indent=2))
            return 1
    runtime = Runtime(config_path(arguments.config), os.environ)
    deadline = monotonic() + arguments.seconds
    runtime.start()
    stop_reason = None
    try:
        while monotonic() < deadline:
            if runtime.snapshot().failure is not None or runtime.diagnostic_complete():
                break
            sleep(0.1)
        report = runtime.report()
        complete = report["diagnosticComplete"]
        if not complete and report["failure"] is None:
            stop_reason = Failure(
                Code.DIAGNOSIS_TIMEOUT,
                "观察时间内未完成登录、目录或指定合约的快照接收；检查前置、账号及交易时段。",
            )
        if runtime.snapshot().catalog_ready:
            report["instruments"] = [asdict(item) for item in runtime.snapshot().instruments]
    finally:
        runtime.stop(stop_reason)
    if runtime.snapshot().failure is not None:
        report["failure"] = asdict(runtime.snapshot().failure)
        complete = False
    report["diagnosticComplete"] = complete
    report["resourcesReleased"] = runtime.snapshot().resources_released
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
