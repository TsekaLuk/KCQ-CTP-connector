"""用途：启动单 worker 本地诊断服务，不开启 reload 或自动切换环境。"""

import argparse
import os

import uvicorn

from connector.api import create_app
from connector.config import DEFAULT_HOST, DEFAULT_PORT, config_path
from connector.runtime import Runtime


def main() -> None:
    """读取本地配置；缺配置仍提供可操作的诊断接口。"""
    parser = argparse.ArgumentParser(description="KCQ CTP 只读诊断与录制服务")
    parser.add_argument("--config", help="本地 TOML 路径，也可设置 CTP_CONFIG")
    arguments = parser.parse_args()
    runtime = Runtime(config_path(arguments.config), os.environ)
    host = DEFAULT_HOST if runtime.config is None else runtime.config.bind_host
    port = DEFAULT_PORT if runtime.config is None else runtime.config.port
    uvicorn.run(create_app(runtime), host=host, port=port, workers=1, access_log=False)


if __name__ == "__main__":
    main()
