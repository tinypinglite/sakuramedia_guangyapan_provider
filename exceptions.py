"""光鸭私有接口的错误类型与脱敏工具。"""

from __future__ import annotations

import re


class GuangyapanError(RuntimeError):
    """光鸭接口层错误基类。"""


class GuangyapanAuthError(GuangyapanError):
    """refresh_token 失效或 access_token 无法获取。"""


class GuangyapanNotFoundError(GuangyapanError):
    """文件 / 目录 / 任务不存在（code 143 / 145）。"""


class GuangyapanRateLimitError(GuangyapanError):
    """被限流（code 127），短退避后可重试。"""


class GuangyapanObjectNotReadyError(GuangyapanError):
    """文件刚落盘，CDN 还在处理（声明长度但返回 0 字节）。"""


class GuangyapanReadBudgetExceeded(GuangyapanError):
    """单次读取超过预算（缩略图/剪辑的安全阀），不是服务故障、不应该被重试。"""


class GuangyapanCloudResourceNotFound(GuangyapanError):
    """云添加被平台拒绝：它的 BT 网络里找不到这个资源（code 352）。"""


class GuangyapanCloudUrlUnsupported(GuangyapanError):
    """云添加被平台拒绝：链接形态不受支持（code 350）。"""


class GuangyapanRequestError(GuangyapanError):
    """其它请求 / 协议错误。"""


_URL_PATTERN = re.compile(r"(?i)\b(?:https?://|magnet:)[^\s<>\"']+")
_TOKEN_PATTERN = re.compile(r"(?i)\b(gy\.[A-Za-z0-9_\-]{8,}|Bearer\s+[A-Za-z0-9._\-]+)")


def safe_error_message(exc: BaseException) -> str:
    """保留上游诊断信息，但不泄漏直链、签名与 token。"""
    message = _URL_PATTERN.sub("[URL]", str(exc))
    message = _TOKEN_PATTERN.sub("[TOKEN]", message)
    return " ".join(message.split())[:512]
