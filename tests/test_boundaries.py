"""用途：验证配置脱敏、真实 SDK 字段复制、录制持久化和无配置 API。"""

from contextlib import closing
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import socket
import subprocess
import sys
import tempfile
import unittest

import httpx
from openctp_ctp import mdapi

from connector.api import API_BASE, create_app
from connector.config import load_config
from connector.ctp.impl.callbacks import Mailbox, MdSpi
from connector.errors import Code, ConnectorError
from connector.recording.impl.sqlite_store import SqliteStore
from connector.runtime import Runtime
from connector.types import Event, Kind

ROOT = Path(__file__).resolve().parents[1]


def write_loopback_config(directory: Path) -> Path:
    """用真实关闭端口生成本机网络失败配置，不提供市场样本或远程账号。"""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        front = f"tcp://127.0.0.1:{listener.getsockname()[1]}"
    template = (ROOT / "config.example.toml").read_text()
    template = template.replace("tcp://REPLACE_WITH_CURRENT_MD_FRONT:PORT", front)
    template = template.replace("tcp://REPLACE_WITH_CURRENT_TD_FRONT:PORT", front)
    template = template.replace('broker_id = ""', 'broker_id = "local"')
    template = template.replace('user_id = ""', 'user_id = "local"')
    template = template.replace('password = ""', 'password = "local"')
    path = directory / "config.toml"
    path.write_text(template)
    return path


class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    """不伪造市场行情，测试真实文件、SDK 结构默认值和 HTTP 边界。"""

    def test_missing_configuration_has_no_side_effects(self):
        """缺配置不创建数据目录或联网线程，诊断不得通过。"""
        with tempfile.TemporaryDirectory() as directory:
            runtime = Runtime(Path(directory) / "absent.toml", {})
            runtime.start()
            self.assertFalse(runtime.diagnostic_complete())
            self.assertEqual(runtime.snapshot().failure.code, Code.CONFIG_MISSING)
            self.assertEqual(list(Path(directory).iterdir()), [])
            runtime.stop()

    def test_configuration_does_not_echo_password(self):
        """无效模板的错误不能包含环境变量覆盖的密码。"""
        secret = "password-must-not-appear"
        with self.assertRaises(ConnectorError) as caught:
            load_config(ROOT / "config.example.toml", {"CTP_PASSWORD": secret})
        self.assertNotIn(secret, str(caught.exception))
        self.assertNotIn(secret, json.dumps(asdict(caught.exception.failure)))
        self.assertEqual(caught.exception.failure.code, Code.CONFIG_INVALID)

    def test_configuration_paths_and_profile_isolation(self):
        """真实 TOML 解析保留相对存储位置并隔离回放来源。"""
        with tempfile.TemporaryDirectory() as directory:
            path = write_loopback_config(Path(directory))
            path.write_text(path.read_text().replace(
                'profile = "simnow"', 'profile = "simnow-replay"',
            ))
            config = load_config(path, {
                "CTP_BROKER_ID": "local", "CTP_USER_ID": "local", "CTP_PASSWORD": "secret",
            })
            self.assertEqual(config.source_id, "ctp-simnow-replay")
            self.assertEqual(config.data_dir, Path(directory).resolve() / "data")
            self.assertNotIn("secret", repr(config))

    def test_native_response_is_copied_before_callback_returns(self):
        """用 SDK 默认结构验证复制后不持有原生引用，不把它作为行情样本。"""
        mailbox = Mailbox(4)
        spi = MdSpi(mailbox)
        login = mdapi.CThostFtdcRspUserLoginField()
        info = mdapi.CThostFtdcRspInfoField()
        spi.OnRspUserLogin(login, info, 19, True)
        event = mailbox.queue.get_nowait()
        self.assertEqual(event.kind, Kind.LOGIN)
        self.assertEqual(event.request_id, 19)
        self.assertEqual(json.loads(event.payload), {"TradingDay": login.TradingDay})
        del login
        self.assertIsInstance(json.loads(event.payload)["TradingDay"], str)

    def test_queue_overflow_is_observable(self):
        """控制事件填满队列后必须出现独立错误标志。"""
        mailbox = Mailbox(1)
        event = Event(Kind.CONNECTED, "md", 1)
        mailbox.put(event)
        mailbox.put(event)
        self.assertTrue(mailbox.overflow.is_set())
        self.assertEqual(mailbox.queue.qsize(), 1)

    def test_sqlite_preserves_event_order_and_run(self):
        """真实 SQLite 重开后仍保留控制事件、纳秒时间及回放身份。"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "recording.sqlite3"
            store = SqliteStore(path, "openctp-replay", "run-one")
            store.append(Event(Kind.CONNECTED, "md", 1234567890123456789))
            store.append(Event(Kind.STOPPED, "local", 1234567890123456790))
            store.close()
            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(
                    connection.execute("SELECT run_id, profile FROM recording").fetchone(),
                    ("run-one", "openctp-replay"),
                )
                rows = connection.execute(
                    "SELECT kind, received_ns FROM events ORDER BY sequence",
                ).fetchall()
                self.assertEqual(rows, [
                    (Kind.CONNECTED, 1234567890123456789),
                    (Kind.STOPPED, 1234567890123456790),
                ])
            with self.assertRaises(FileExistsError):
                SqliteStore(path, "simnow", "run-two")

    def test_native_connection_timeout_releases_resources(self):
        """实际初始化原生网络线程并连接关闭端口，截止后必须关闭且记录缺口。"""
        with tempfile.TemporaryDirectory() as directory:
            path = write_loopback_config(Path(directory))
            result = subprocess.run(
                [sys.executable, str(ROOT / "diagnose.py"), "--config", str(path), "--seconds", "0.5"],
                capture_output=True, text=True, timeout=15,
            )
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            report = json.loads(result.stdout)
            self.assertFalse(report["diagnosticComplete"])
            self.assertFalse(report["marketDataVerified"])
            self.assertTrue(report["resourcesReleased"])
            self.assertTrue(report["sdk"]["libraryFingerprintsVerified"])
            self.assertIn(report["failure"]["code"], (Code.DIAGNOSIS_TIMEOUT, Code.DISCONNECTED))
            database = next((Path(directory) / "data").rglob("recording.sqlite3"))
            with closing(sqlite3.connect(database)) as connection:
                kinds = [row[0] for row in connection.execute("SELECT kind FROM events ORDER BY sequence")]
                self.assertIn(Kind.GAP, kinds)
                self.assertEqual(kinds[-1], Kind.STOPPED)

    async def test_missing_config_http_and_unsupported_bars(self):
        """真实 FastAPI 生命周期诚实返回降级，能力错误符合 V1。"""
        with tempfile.TemporaryDirectory() as directory:
            runtime = Runtime(Path(directory) / "absent.toml", {})
            app = create_app(runtime)
            async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://localhost",
            ) as client:
                self.assertFalse((await client.get("/health")).json()["chartReady"])
                self.assertEqual(
                    (await client.get("/diagnostics")).json()["failure"]["code"], Code.CONFIG_MISSING,
                )
                probe = (await client.get(f"{API_BASE}/sources/ctp-simnow/probe")).json()["data"]
                self.assertEqual(probe["status"], "degraded")
                self.assertNotIn("bars", probe["capabilities"])
                response = await client.post(f"{API_BASE}/bars", json={})
                self.assertEqual(response.json()["error"]["code"], "UNSUPPORTED_CAPABILITY")
                response = await client.post(
                    f"{API_BASE}/instruments/search", json={"sourceId": "ctp-simnow", "keyword": "", "limit": 10},
                )
                self.assertEqual(response.json()["error"]["code"], "UPSTREAM_UNAVAILABLE")
                response = await client.post(f"{API_BASE}/instruments/search", json={"password": "hidden"})
                self.assertEqual(response.status_code, 422)
                self.assertNotIn("hidden", response.text)


if __name__ == "__main__":
    unittest.main()
