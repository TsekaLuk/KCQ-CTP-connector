"""用途：提供安全诊断和 V1 只读目录；未实现的 K 线能力明确拒绝。"""

from contextlib import asynccontextmanager
from dataclasses import asdict
from time import time_ns
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from connector.errors import ProtocolCode
from connector.runtime import Runtime

API_BASE = "/api/v1/market-data"
CAPABILITIES = {"assetClasses": ["futures"], "liveBars": False}
STAGE_MESSAGE = "当前仅支持连接诊断、合约目录和原始快照录制，尚未提供 K 线。"


class SearchRequest(BaseModel):
    """复用 KCQ V1 目录请求形状，限制结果数量。"""

    model_config = ConfigDict(extra="forbid")
    sourceId: str
    keyword: str
    limit: int = Field(ge=1, le=500)
    assetClasses: list[str] | None = None


def envelope(data) -> dict:
    """为成功响应生成独立 requestId。"""
    return {"data": data, "requestId": uuid4().hex}


def failure(code: ProtocolCode, message: str, status: int = 400) -> JSONResponse:
    """生成 V1 失败外壳，不返回请求体或底层异常。"""
    return JSONResponse(
        {"error": {"code": code, "message": message}, "requestId": uuid4().hex},
        status_code=status,
    )


def create_app(runtime: Runtime) -> FastAPI:
    """装配单进程生命周期及目录路由，不创建交易接口。"""
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """启动录制，并在 HTTP 服务关闭后释放原生资源。"""
        runtime.start()
        try:
            yield
        finally:
            runtime.stop()

    app = FastAPI(title="KCQ CTP connector", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError):
        """覆盖 FastAPI 默认的输入回显，保持安全的 V1 错误形状。"""
        return failure(ProtocolCode.INVALID_REQUEST, "请求字段不符合 V1 目录协议。", 422)

    @app.get("/health")
    def health():
        """进程存活不代表上游或 K 线能力可用。"""
        return {"process": "running", "status": "degraded", "chartReady": False}

    @app.get("/diagnostics")
    def diagnostics():
        """公开版本、连接阶段、数值错误及最近收到的原始行情。"""
        return runtime.report()

    @app.get(f"{API_BASE}/sources/{{source_id}}/probe")
    def probe(source_id: str):
        """当前 profile 才可探测；阶段一始终明确声明无 K 线。"""
        config = runtime.config
        if config is None:
            return envelope({
                "status": "degraded", "checkedAt": time_ns() // 1_000_000,
                "message": runtime.snapshot().failure.message, "capabilities": CAPABILITIES,
            })
        if source_id != config.source_id:
            return failure(ProtocolCode.UPSTREAM_UNAVAILABLE, "服务当前配置不属于该来源。", 503)
        state = runtime.snapshot()
        message = STAGE_MESSAGE if state.failure is None else state.failure.message
        return envelope({
            "status": "degraded", "checkedAt": time_ns() // 1_000_000,
            "message": message, "capabilities": CAPABILITIES,
        })

    @app.post(f"{API_BASE}/instruments/search")
    def search(query: SearchRequest):
        """搜索本次登录完整返回的具体期货合约，不回退到旧目录。"""
        config = runtime.config
        state = runtime.snapshot()
        if (
            config is None or query.sourceId != config.source_id
            or not state.catalog_ready or state.failure is not None
        ):
            return failure(ProtocolCode.UPSTREAM_UNAVAILABLE, "当前来源合约目录尚未就绪，请检查 /diagnostics。", 503)
        if query.assetClasses is not None and "futures" not in query.assetClasses:
            return envelope({"items": []})
        keyword = query.keyword.casefold().strip()
        items = []
        for instrument in state.instruments:
            if keyword not in f"{instrument.raw_id} {instrument.name} {instrument.exchange}".casefold():
                continue
            items.append({
                "id": instrument.id, "sourceId": config.source_id,
                "symbol": instrument.raw_id, "name": instrument.name,
                "assetClass": "futures", "exchange": instrument.exchange,
                "tickSize": instrument.tick_size, "lotSize": instrument.min_order_volume,
                "providerRef": asdict(instrument),
                "capabilities": {},
            })
            if len(items) >= query.limit:
                break
        return envelope({"items": items})

    @app.post(f"{API_BASE}/bars")
    def bars():
        """不把尚未验证的聚合能力包装成空历史。"""
        return failure(ProtocolCode.UNSUPPORTED_CAPABILITY, STAGE_MESSAGE)

    @app.get(f"{API_BASE}/sources/{{source_id}}/stream")
    def stream(source_id: str):
        """原始快照不能冒充 KCQ 的 K 线 SSE。"""
        return failure(ProtocolCode.UNSUPPORTED_CAPABILITY, STAGE_MESSAGE)

    return app
