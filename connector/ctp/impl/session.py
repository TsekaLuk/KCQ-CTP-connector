"""用途：由消费线程调用原生 API，仅开放登录、目录查询和行情订阅。"""

from itertools import count
from pathlib import Path

from openctp_ctp import mdapi, tdapi

from connector.config import Config
from connector.ctp.impl.callbacks import Mailbox, MdSpi, TdSpi
from connector.errors import Code, ConnectorError, Failure
from connector.types import Channel, Operation


class Session:
    """持有 API 和 SPI 强引用；不提供报单、撤单或账户能力。"""

    def __init__(self, config: Config, mailbox: Mailbox, directory: Path):
        """准备配置和请求号；API 在 start 中创建。"""
        self.config = config
        self.directory = directory
        self.md_spi = MdSpi(mailbox)
        self.td_spi = TdSpi(mailbox)
        self.md = None
        self.td = None
        self.ids = count(1)

    def start(self) -> None:
        """创建独立流目录、注册回调和用户配置的前置后启动网络线程。"""
        for name in ("md", "td"):
            (self.directory / name).mkdir(parents=True)
        self.md = mdapi.CThostFtdcMdApi.CreateFtdcMdApi(str(self.directory / "md") + "/")
        self.td = tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi(str(self.directory / "td") + "/")
        if self.md is None or self.td is None:
            raise ConnectorError(Failure(Code.NATIVE_FAILURE, "原生 API 创建失败。"))
        self.md.RegisterSpi(self.md_spi)
        self.td.RegisterSpi(self.td_spi)
        for front in self.config.md_fronts:
            self.md.RegisterFront(front)
        for front in self.config.td_fronts:
            self.td.RegisterFront(front)
        self.md.Init()
        self.td.Init()

    def request(self, channel: Channel, operation: Operation) -> int:
        """发起一次只读请求，返回用于过滤过期响应的 requestId。"""
        request_id = next(self.ids)
        api = self.md if channel == "md" else self.td
        module = mdapi if channel == "md" else tdapi
        if operation == Operation.AUTHENTICATE:
            request = tdapi.CThostFtdcReqAuthenticateField()
            request.BrokerID = self.config.broker_id
            request.UserID = self.config.user_id
            request.AppID = self.config.app_id
            request.AuthCode = self.config.auth_code.get_secret_value()
            result = api.ReqAuthenticate(request, request_id)
        elif operation == Operation.LOGIN:
            request = module.CThostFtdcReqUserLoginField()
            request.BrokerID = self.config.broker_id
            request.UserID = self.config.user_id
            request.Password = self.config.password.get_secret_value()
            result = api.ReqUserLogin(request, request_id)
        elif operation == Operation.CATALOG:
            result = api.ReqQryInstrument(tdapi.CThostFtdcQryInstrumentField(), request_id)
        else:
            raise ValueError("unsupported read-only operation")
        self.check_result(result, operation)
        return request_id

    def subscribe(self) -> None:
        """一次订阅明确配置且已在目录核实的合约，不默认订阅全市场。"""
        symbols = [symbol.encode("ascii") for symbol in self.config.recording_symbols]
        self.check_result(self.md.SubscribeMarketData(symbols, len(symbols)), "subscribe")

    @staticmethod
    def check_result(result: int, operation: str) -> None:
        """区分请求发送失败和异步上游错误；不猜测重试或切换认证。"""
        if result != 0:
            raise ConnectorError(Failure(
                Code.REQUEST_REJECTED,
                f"CTP {operation} 请求未发送成功，请检查连接和限流后重启诊断。",
                result,
            ))

    def close(self) -> None:
        """从消费线程释放 API；不从回调线程 Release。"""
        try:
            if self.md is not None:
                self.md.Release()
                self.md = None
        finally:
            if self.td is not None:
                self.td.Release()
                self.td = None
