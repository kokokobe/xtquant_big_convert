# coding: utf-8
"""临时 shm 客户端配置（测试用，勿提交）。配套环境变量:
BIGQMT_CLIENT_CONFIG_MODULE=_client_config_shm
"""
BIGQMT_ACCOUNT_ID = '52625295'
BIGQMT_RPC_TIMEOUT_SECONDS = 30.0
BIGQMT_REDIS_CONFIG = {
    "transport": 'shm',
}
# enabled 必须 False：FormulaServer 直连(127.0.0.1:58600)是回环 TCP，
# 模型交易面板运行期间会被 QMT 10s 审计记非法IP并全面板杀策略
# （2026-09-23 15:30 实锤 illegal IP: 127.0.0.1:60470=客户端临时端口）。
# 要测 fastpath 数字：先停掉面板里的桥策略再跑。
BIGQMT_FORMULA_SERVER_CONFIG = {"enabled": False}
BIGQMT_LOCAL_CACHE_CONFIG = {"enabled": True, "dir": None, "fallback_rpc": True, "format": "auto"}
