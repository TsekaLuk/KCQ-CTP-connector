"""用途：单线程驱动只读连接和录制，向 API 发布不可变诊断快照。"""

from dataclasses import asdict, replace
import json
from pathlib import Path
from queue import Empty
import sqlite3
from threading import Event as Flag, Lock, Thread
from time import monotonic, time_ns
from typing import Mapping
from uuid import uuid4

from connector.config import Config, load_config
from connector.ctp.impl.sdk import inspect_sdk
from connector.errors import Code, ConnectorError, Failure
from connector.recording.impl.sqlite_store import SqliteStore
from connector.types import Channel, Event, Instrument, Kind, Operation, SessionPort, State


def is_diagnosis_complete(config: Config | None, state: State) -> bool:
    """纯判定：指定合约全部确认订阅并收到行情，且目录完整、没有失败。"""
    if config is None or state.failure is not None:
        return False
    return (
        state.md == "logged_in" and state.td == "logged_in" and state.catalog_ready
        and set(state.subscribed) == set(config.recording_symbols)
        and len(state.latest_snapshots) == len(config.recording_symbols)
    )


class Runtime:
    """所有原生操作和写库都在 worker；读取方只能取得冻结状态。"""

    def __init__(self, path: Path, environ: Mapping[str, str]):
        """解析配置；缺配置保持可诊断且不创建任何网络连接。"""
        self.config: Config | None = None
        self._lock = Lock()
        self._state = State()
        self._stop = Flag()
        self._worker: Thread | None = None
        self._catalog: dict[str, Instrument] = {}
        self._latest: dict[str, str] = {}
        self._pending: dict[str, tuple[Kind, int]] = {}
        self._subscribe_requested = False
        try:
            self.config = load_config(path, environ)
        except ConnectorError as error:
            self._state = replace(self._state, failure=error.failure)

    def snapshot(self) -> State:
        """返回一次一致读取的不可变状态。"""
        with self._lock:
            return self._state

    def _publish(self, **changes) -> None:
        """由唯一写者替换完整快照；不把可变 SDK 对象泄露出去。"""
        with self._lock:
            self._state = replace(self._state, **changes)

    def start(self) -> None:
        """配置有效才启动唯一录制线程。"""
        if self.config is None or self._worker is not None:
            return
        self._worker = Thread(target=self._run, name="ctp-recorder", daemon=True)
        self._worker.start()

    def stop(self, reason: Failure | None = None) -> None:
        """请求关闭并等待资源释放；超时明确标记，不能宣称录制完整。"""
        if reason is not None and self.snapshot().failure is None:
            self._publish(failure=reason)
        self._stop.set()
        if self._worker is not None:
            self._worker.join(timeout=10)
            if self._worker.is_alive():
                self._publish(failure=Failure(Code.STOP_TIMEOUT, "原生库关闭超时，录制尾部尚未确认。"))

    def diagnostic_complete(self) -> bool:
        """有录制合约时，每个合约都必须收到过快照才通过诊断。"""
        return is_diagnosis_complete(self.config, self.snapshot())

    def report(self) -> dict:
        """公开安全诊断，不返回配置、前置地址或账号。"""
        state = self.snapshot()
        return {
            "stage": "connection-and-raw-capture",
            "sourceId": None if self.config is None else self.config.source_id,
            "profile": None if self.config is None else self.config.profile,
            "diagnosticComplete": is_diagnosis_complete(self.config, state),
            "marketDataVerified": state.snapshot_count > 0,
            "chartReady": False,
            "md": state.md,
            "td": state.td,
            "catalogReady": state.catalog_ready,
            "instrumentCount": len(state.instruments),
            "subscribed": state.subscribed,
            "snapshotCount": state.snapshot_count,
            "latestSnapshots": [json.loads(payload) for payload in state.latest_snapshots],
            "sdk": None if state.sdk is None else json.loads(state.sdk),
            "runId": state.run_id,
            "resourcesReleased": state.resources_released,
            "failure": None if state.failure is None else asdict(state.failure),
        }

    def _run(self) -> None:
        """核验 SDK 后消费事件；任何丢帧或持久化失败均停止采集。"""
        config = self.config
        if config is None:
            return
        store = None
        session = None
        mailbox = None
        try:
            sdk = inspect_sdk(config.sdk_channel)
            from connector.ctp.impl.callbacks import Mailbox
            from connector.ctp.impl.session import Session

            run_id = uuid4().hex
            directory = config.data_dir / config.profile / run_id
            store = SqliteStore(directory / "recording.sqlite3", config.profile, run_id)
            sdk_json = json.dumps(sdk, ensure_ascii=False)
            store.append(Event(Kind.SDK, "local", time_ns(), sdk_json))
            store.flush()
            self._publish(sdk=sdk_json, run_id=run_id, md="connecting", td="connecting")
            mailbox = Mailbox(config.queue_capacity)
            session = Session(config, mailbox, directory / "flow")
            self._publish(resources_released=False)
            session.start()
            flushed_at = monotonic()
            while not self._stop.is_set():
                if mailbox.overflow.is_set():
                    raise ConnectorError(Failure(
                        Code.QUEUE_OVERFLOW, "回调队列已溢出，录制存在缺口；停止采集，请检查磁盘与订阅规模。",
                    ))
                if mailbox.callback_failure.is_set():
                    raise ConnectorError(Failure(Code.CALLBACK_FAILURE, "原生回调字段复制失败，录制已停止。"))
                try:
                    event = mailbox.queue.get(timeout=0.2)
                except Empty:
                    store.flush()
                    continue
                store.append(event)
                self._handle(event, session, store)
                if monotonic() - flushed_at >= 0.5:
                    store.flush()
                    flushed_at = monotonic()
        except ConnectorError as error:
            self._publish(failure=error.failure)
        except (OSError, sqlite3.Error):
            self._publish(failure=Failure(Code.RECORDING_FAILURE, "录制目录或 SQLite 写入失败，请检查磁盘和权限。"))
        except Exception:
            # SDK setter 可能把输入拼入异常；这里只公开固定诊断。
            self._publish(failure=Failure(Code.NATIVE_FAILURE, "原生调用失败，请核对前置、字段长度及 SDK 配置。"))
        finally:
            try:
                if session is not None:
                    session.close()
                self._publish(resources_released=True)
            except Exception:
                self._publish(failure=Failure(Code.NATIVE_FAILURE, "原生 API 释放失败，请结束进程后重新自检。"))
            try:
                if mailbox is not None and store is not None:
                    # Release 后不再有回调，保留已接受但尚未消费的原始尾部。
                    while True:
                        try:
                            store.append(mailbox.queue.get_nowait())
                        except Empty:
                            break
                    if self.snapshot().failure is None:
                        if mailbox.overflow.is_set():
                            self._publish(failure=Failure(
                                Code.QUEUE_OVERFLOW, "关闭前回调队列溢出，录制存在缺口。",
                            ))
                        elif mailbox.callback_failure.is_set():
                            self._publish(failure=Failure(
                                Code.CALLBACK_FAILURE, "关闭前原生回调复制失败，录制存在缺口。",
                            ))
                    state = self.snapshot()
                    if state.failure is not None:
                        store.append(Event(
                            Kind.GAP, "local", time_ns(),
                            json.dumps(asdict(state.failure), ensure_ascii=False),
                        ))
                    store.append(Event(Kind.STOPPED, "local", time_ns()))
            except Exception:
                self._publish(failure=Failure(Code.RECORDING_FAILURE, "关闭时未能完整保存录制尾部。"))
            finally:
                if store is not None:
                    try:
                        store.close()
                    except (OSError, sqlite3.Error):
                        self._publish(failure=Failure(Code.RECORDING_FAILURE, "录制事务提交失败。"))
                if self.snapshot().resources_released:
                    self._publish(md="stopped", td="stopped")

    def _request(self, session: SessionPort, channel: Channel, operation: Operation, response: Kind) -> None:
        """保存请求身份，拒绝断线之前的迟到响应。"""
        request_id = session.request(channel, operation)
        self._pending[channel] = (response, request_id)

    def _handle(self, event: Event, session: SessionPort, store: SqliteStore) -> None:
        """驱动只读登录状态机；目录只在完整请求完成后发布。"""
        config = self.config
        if config is None:
            return
        fields = json.loads(event.payload)
        if event.kind == Kind.DISCONNECTED:
            # 阶段一采用失败即停，避免未验证的自动重连制造连续行情假象。
            self._publish(**{event.channel: "disconnected"}, catalog_ready=False)
            raise ConnectorError(Failure(
                Code.DISCONNECTED, f"{event.channel} 前置断线；已记录缺口，请重新运行诊断。",
                fields["reason"],
            ))
        if event.error_id:
            raise ConnectorError(Failure(
                Code.UPSTREAM_ERROR,
                f"{event.channel} {event.kind} 失败；检查所选环境的账号、认证与前置，未自动改变认证策略。",
                event.error_id,
            ))
        match event.kind:
            case Kind.CONNECTED:
                self._publish(**{event.channel: "connected"})
                if event.channel == "td" and config.auth_required:
                    self._request(session, "td", Operation.AUTHENTICATE, Kind.AUTHENTICATED)
                else:
                    self._request(session, event.channel, Operation.LOGIN, Kind.LOGIN)
            case Kind.AUTHENTICATED | Kind.LOGIN | Kind.INSTRUMENT:
                if self._pending.get(event.channel) != (event.kind, event.request_id):
                    return
                if event.kind == Kind.AUTHENTICATED and event.is_last:
                    self._request(session, "td", Operation.LOGIN, Kind.LOGIN)
                elif event.kind == Kind.LOGIN and event.is_last:
                    self._publish(**{event.channel: "logged_in"})
                    if event.channel == "td":
                        self._request(session, "td", Operation.CATALOG, Kind.INSTRUMENT)
                elif event.kind == Kind.INSTRUMENT:
                    if fields and fields["ProductClass"] == "1" and fields["IsTrading"]:
                        instrument = Instrument(
                            fields["InstrumentID"], fields["ExchangeID"], fields["InstrumentName"],
                            fields["ProductID"], fields["PriceTick"], fields["VolumeMultiple"],
                            fields["MinLimitOrderVolume"], fields["ExpireDate"],
                        )
                        self._catalog[instrument.id] = instrument
                    if event.is_last:
                        store.flush()
                        self._publish(
                            catalog_ready=True,
                            instruments=tuple(sorted(self._catalog.values(), key=lambda item: item.id)),
                        )
            case Kind.SUBSCRIBED:
                raw_id = fields.get("InstrumentID")
                if raw_id in config.recording_symbols:
                    self._publish(subscribed=tuple(sorted(set(self.snapshot().subscribed) | {raw_id})))
            case Kind.SNAPSHOT:
                raw_id = fields["InstrumentID"]
                if raw_id not in config.recording_symbols:
                    return
                store.flush()
                self._latest[raw_id] = event.payload
                self._publish(
                    snapshot_count=self.snapshot().snapshot_count + 1,
                    latest_snapshots=tuple(self._latest[key] for key in sorted(self._latest)),
                )
        state = self.snapshot()
        if (
            state.md == "logged_in" and state.catalog_ready
            and config.recording_symbols and not self._subscribe_requested
        ):
            for symbol in config.recording_symbols:
                if sum(item.raw_id == symbol for item in state.instruments) != 1:
                    raise ConnectorError(Failure(
                        Code.SYMBOL_UNAVAILABLE,
                        "录制合约不在当前可交易期货目录中或存在歧义；请先查询目录再配置原始代码。",
                    ))
            session.subscribe()
            self._subscribe_requested = True
