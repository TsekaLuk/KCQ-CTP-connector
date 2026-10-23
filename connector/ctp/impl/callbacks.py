"""用途：CTP 回调只复制基本字段并入队，不在原生线程登录、写库或聚合。"""

import json
from queue import Full, Queue
from threading import Event as Flag
from time import time_ns

from openctp_ctp import mdapi, tdapi

from connector.types import Channel, Event, Kind

SNAPSHOT_FIELDS = (
    "InstrumentID", "ExchangeID", "TradingDay", "ActionDay", "UpdateTime",
    "UpdateMillisec", "LastPrice", "Volume", "Turnover", "OpenInterest",
    "BidPrice1", "BidVolume1", "AskPrice1", "AskVolume1", "OpenPrice",
    "HighestPrice", "LowestPrice", "PreClosePrice", "PreSettlementPrice",
    "UpperLimitPrice", "LowerLimitPrice",
)
INSTRUMENT_FIELDS = (
    "InstrumentID", "InstrumentName", "ExchangeID", "ProductID", "ProductClass",
    "IsTrading", "PriceTick", "VolumeMultiple", "MinLimitOrderVolume", "ExpireDate",
)


class Mailbox:
    """有界跨线程队列；溢出或复制失败通过独立标志通知消费线程。"""

    def __init__(self, capacity: int):
        """创建指定容量，错误标志不依赖队列空位。"""
        self.queue: Queue[Event] = Queue(maxsize=capacity)
        self.overflow = Flag()
        self.callback_failure = Flag()

    def put(self, event: Event) -> None:
        """不阻塞 SDK 回调；丢帧会令录制失败并留下缺口。"""
        try:
            self.queue.put_nowait(event)
        except Full:
            self.overflow.set()

    def copy(self, kind: Kind, channel: Channel, fields, info=None, request_id=0, is_last=True) -> None:
        """在回调内完成 JSON 复制，异常不越过 SWIG 边界。"""
        try:
            self.put(Event(
                kind, channel, time_ns(),
                json.dumps(fields, ensure_ascii=False, allow_nan=False),
                request_id, is_last, 0 if info is None else int(info.ErrorID),
            ))
        except Exception:
            self.callback_failure.set()


class MdSpi(mdapi.CThostFtdcMdSpi):
    """行情回调；生命周期由 Session 持有，避免 SWIG 引用提前释放。"""

    def __init__(self, mailbox: Mailbox):
        """关联唯一采集队列。"""
        super().__init__()
        self.mailbox = mailbox

    def OnFrontConnected(self):
        """通知消费线程发起行情登录。"""
        self.mailbox.copy(Kind.CONNECTED, "md", {})

    def OnFrontDisconnected(self, reason):
        """记录断线原因；不在回调重连或写库。"""
        self.mailbox.copy(Kind.DISCONNECTED, "md", {"reason": int(reason)})

    def OnRspUserLogin(self, login, info, request_id, is_last):
        """只保留交易日，不录制账号或登录响应中的身份信息。"""
        self.mailbox.copy(
            Kind.LOGIN, "md", {} if login is None else {"TradingDay": login.TradingDay},
            info, request_id, is_last,
        )

    def OnRspSubMarketData(self, instrument, info, request_id, is_last):
        """复制单个订阅确认，不将最后一条确认等同于全部成功。"""
        self.mailbox.copy(
            Kind.SUBSCRIBED, "md",
            {} if instrument is None else {"InstrumentID": instrument.InstrumentID},
            info, request_id, is_last,
        )

    def OnRtnDepthMarketData(self, snapshot):
        """保留原始时间和累计量；这里不把快照解释为逐笔成交。"""
        try:
            fields = {name: getattr(snapshot, name) for name in SNAPSHOT_FIELDS}
            self.mailbox.copy(Kind.SNAPSHOT, "md", fields)
        except Exception:
            self.mailbox.callback_failure.set()

    def OnRspError(self, info, request_id, is_last):
        """只公开错误码，避免记录上游可能回显的敏感文本。"""
        self.mailbox.copy(Kind.ERROR, "md", {}, info, request_id, is_last)


class TdSpi(tdapi.CThostFtdcTraderSpi):
    """仅接收认证、登录及合约查询响应，不实现交易动作。"""

    def __init__(self, mailbox: Mailbox):
        """关联同一个单写者队列。"""
        super().__init__()
        self.mailbox = mailbox

    def OnFrontConnected(self):
        """通知消费线程按配置选择认证或登录。"""
        self.mailbox.copy(Kind.CONNECTED, "td", {})

    def OnFrontDisconnected(self, reason):
        """复制交易前置断线状态。"""
        self.mailbox.copy(Kind.DISCONNECTED, "td", {"reason": int(reason)})

    def OnRspAuthenticate(self, response, info, request_id, is_last):
        """认证响应不包含账号、AppID 或 AuthCode。"""
        self.mailbox.copy(Kind.AUTHENTICATED, "td", {}, info, request_id, is_last)

    def OnRspUserLogin(self, login, info, request_id, is_last):
        """复制交易日供诊断，不确认结算单。"""
        self.mailbox.copy(
            Kind.LOGIN, "td", {} if login is None else {"TradingDay": login.TradingDay},
            info, request_id, is_last,
        )

    def OnRspQryInstrument(self, instrument, info, request_id, is_last):
        """逐条复制目录，完整性由消费线程根据 requestId 和 isLast 判定。"""
        try:
            fields = {} if instrument is None else {
                name: getattr(instrument, name) for name in INSTRUMENT_FIELDS
            }
            self.mailbox.copy(Kind.INSTRUMENT, "td", fields, info, request_id, is_last)
        except Exception:
            self.mailbox.callback_failure.set()

    def OnRspError(self, info, request_id, is_last):
        """复制只含数值的上游错误。"""
        self.mailbox.copy(Kind.ERROR, "td", {}, info, request_id, is_last)
