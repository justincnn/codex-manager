"""
新版 IMAP 提供者
使用 outlook.live.com:993 + consumers Token 端点
引入进程级 IMAPConnectionPool 连接复用和 IMAP IDLE
"""

import email
import imaplib
import logging
import select
import time
import threading
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional

from ..base import EmailMessage
from ..account import OutlookAccount
from ..token_manager import TokenManager
from .base import OutlookProvider, ProviderConfig


logger = logging.getLogger(__name__)


class IMAPConnectionPool:
    """进程级 IMAP 连接池，按 email 复用 IMAP4_SSL 连接"""

    IMAP_HOST = "outlook.live.com"
    IMAP_PORT = 993

    def __init__(self):
        self._connections: Dict[str, imaplib.IMAP4_SSL] = {}
        self._lock = threading.Lock()

    def get_connection(
        self,
        email_addr: str,
        token: str,
        timeout: int = 30,
    ) -> imaplib.IMAP4_SSL:
        """获取或新建 IMAP 连接"""
        with self._lock:
            conn = self._connections.get(email_addr)
            if conn:
                try:
                    conn.noop()
                    return conn
                except Exception:
                    self._close_one(email_addr)

            conn = imaplib.IMAP4_SSL(self.IMAP_HOST, self.IMAP_PORT, timeout=timeout)
            auth_str = f"user={email_addr}\x01auth=Bearer {token}\x01\x01"
            conn.authenticate("XOAUTH2", lambda _: auth_str.encode("utf-8"))
            self._connections[email_addr] = conn
            logger.debug(f"[{email_addr}] IMAP 新连接已建立")
            return conn

    def invalidate(self, email_addr: str):
        """废弃连接（认证失败时调用）"""
        with self._lock:
            self._close_one(email_addr)

    def _close_one(self, email_addr: str):
        conn = self._connections.pop(email_addr, None)
        if conn:
            try:
                conn.logout()
            except Exception:
                pass


# 模块级单例连接池
_imap_pool = IMAPConnectionPool()


class IMAPNewProvider(OutlookProvider):
    """
    新版 IMAP 提供者
    通过连接池复用连接，支持 IMAP IDLE
    """

    IMAP_HOST = "outlook.live.com"
    IMAP_PORT = 993

    def __init__(
        self,
        account: OutlookAccount,
        config: Optional[ProviderConfig] = None,
    ):
        super().__init__(account, config)
        self._conn: Optional[imaplib.IMAP4_SSL] = None
        self._token_manager: Optional[TokenManager] = None

        if not account.has_oauth():
            logger.warning(
                f"[{self.account.email}] IMAP_NEW 需要 OAuth2 配置 (client_id + refresh_token)"
            )

    def _get_token_manager(self) -> TokenManager:
        if not self._token_manager:
            self._token_manager = TokenManager(
                self.account,
                proxy_url=self.config.proxy_url,
                timeout=self.config.timeout,
                service_id=self.config.service_id,
            )
        return self._token_manager

    def connect(self) -> bool:
        """从连接池获取连接"""
        if not self.account.has_oauth():
            logger.debug(f"[{self.account.email}] 跳过 IMAP_NEW（无 OAuth）")
            return False

        try:
            tm = self._get_token_manager()
            token = tm.get_access_token()
            if not token:
                logger.error(f"[{self.account.email}] 获取 IMAP Token 失败")
                return False

            self._conn = _imap_pool.get_connection(
                self.account.email, token, self.config.timeout
            )
            self._connected = True
            self.record_success()
            logger.debug(f"[{self.account.email}] IMAP 连接就绪（连接池）")
            return True

        except imaplib.IMAP4.error as e:
            err = str(e)
            # Token 失效时强制刷新并重试一次
            if "AUTHENTICATE" in err or "invalid" in err.lower():
                logger.warning(f"[{self.account.email}] XOAUTH2 认证失败，尝试刷新 Token")
                _imap_pool.invalidate(self.account.email)
                try:
                    tm = self._get_token_manager()
                    token = tm.get_access_token(force_refresh=True)
                    if token:
                        self._conn = _imap_pool.get_connection(
                            self.account.email, token, self.config.timeout
                        )
                        self._connected = True
                        self.record_success()
                        return True
                except Exception as retry_e:
                    self.record_failure(str(retry_e))
                    logger.error(f"[{self.account.email}] Token 刷新后重连失败: {retry_e}")
            else:
                self.record_failure(err)
                logger.error(f"[{self.account.email}] IMAP 连接失败: {e}")
            self._connected = False
            self._conn = None
            return False

        except Exception as e:
            self.record_failure(str(e))
            logger.error(f"[{self.account.email}] IMAP 连接失败: {e}")
            self._connected = False
            self._conn = None
            return False

    def disconnect(self):
        """归还连接池（不 logout，保持复用）"""
        self._connected = False
        self._conn = None

    def get_recent_emails(
        self,
        count: int = 20,
        only_unseen: bool = True,
    ) -> List[EmailMessage]:
        """获取最近的邮件"""
        if not self._connected:
            if not self.connect():
                return []

        try:
            self._conn.select("INBOX", readonly=True)
            flag = "UNSEEN" if only_unseen else "ALL"
            status, data = self._conn.search(None, flag)

            if status != "OK" or not data or not data[0]:
                return []

            ids = data[0].split()
            recent_ids = ids[-count:][::-1]

            emails = []
            for msg_id in recent_ids:
                try:
                    msg = self._fetch_email(msg_id)
                    if msg:
                        emails.append(msg)
                except Exception as e:
                    logger.warning(f"[{self.account.email}] 解析邮件失败 (ID: {msg_id}): {e}")

            return emails

        except Exception as e:
            self.record_failure(str(e))
            logger.error(f"[{self.account.email}] 获取邮件失败: {e}")
            _imap_pool.invalidate(self.account.email)
            self._connected = False
            self._conn = None
            return []

    def _fetch_email(self, msg_id: bytes) -> Optional[EmailMessage]:
        """获取并解析单封邮件"""
        status, data = self._conn.fetch(msg_id, "(RFC822)")
        if status != "OK" or not data or not data[0]:
            return None

        raw = b""
        for part in data:
            if isinstance(part, tuple) and len(part) > 1:
                raw = part[1]
                break

        if not raw:
            return None

        return _parse_email(raw)

    def wait_for_new_email_idle(self, timeout: int = 25) -> bool:
        """
        RFC 2177 IMAP IDLE 实现。
        发送 IDLE 命令，等待服务器推送 EXISTS/RECENT，然后发送 DONE。
        Returns True 表示有新邮件推送，False 表示超时或异常（调用方降级轮询）。
        """
        if not self._connected:
            if not self.connect():
                return False

        try:
            self._conn.select("INBOX", readonly=True)
        except Exception as e:
            logger.warning(f"[{self.account.email}] IDLE 前 SELECT 失败: {e}")
            return False

        logger.info(f"[{self.account.email}] 进入 IMAP IDLE 等待模式（超时 {timeout}s）")

        sock = self._conn.socket()
        tag = self._conn._new_tag().decode() if isinstance(self._conn._new_tag(), bytes) else self._conn._new_tag()

        try:
            # 发送 IDLE 命令
            self._conn.send(f"{tag} IDLE\r\n".encode())

            # 等待 "+" 延续响应
            deadline = time.time() + timeout
            buf = b""
            while time.time() < deadline:
                ready = select.select([sock], [], [], min(2.0, deadline - time.time()))
                if ready[0]:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    if b"+ " in buf or b"+\r\n" in buf:
                        break

            # 等待 EXISTS / RECENT 推送
            got_new = False
            buf = b""
            while time.time() < deadline:
                remaining = deadline - time.time()
                ready = select.select([sock], [], [], min(2.0, remaining))
                if ready[0]:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    if b"EXISTS" in buf or b"RECENT" in buf:
                        got_new = True
                        break

            return got_new

        except Exception as e:
            logger.warning(f"[{self.account.email}] IMAP IDLE 异常: {e}")
            return False

        finally:
            # 发送 DONE 结束 IDLE
            try:
                self._conn.send(b"DONE\r\n")
                # 读取 IDLE 结束响应（避免缓冲区污染后续命令）
                deadline2 = time.time() + 5
                resp_buf = b""
                while time.time() < deadline2:
                    ready = select.select([sock], [], [], 1.0)
                    if ready[0]:
                        chunk = sock.recv(4096)
                        if not chunk:
                            break
                        resp_buf += chunk
                        if tag.encode() in resp_buf:
                            break
            except Exception:
                # DONE 发送失败则废弃连接
                _imap_pool.invalidate(self.account.email)
                self._connected = False
                self._conn = None

    def test_connection(self) -> bool:
        """测试 IMAP 连接"""
        try:
            with self:
                self._conn.select("INBOX", readonly=True)
            return True
        except Exception as e:
            logger.warning(f"[{self.account.email}] IMAP 连接测试失败: {e}")
            return False


def _parse_email(raw: bytes) -> EmailMessage:
    """解析原始邮件为 EmailMessage"""
    msg = email.message_from_bytes(raw)

    def _decode(val):
        if not val:
            return ""
        parts = decode_header(str(val))
        result = ""
        for part, charset in parts:
            if isinstance(part, bytes):
                try:
                    result += part.decode(charset or "utf-8", errors="replace")
                except (LookupError, UnicodeDecodeError):
                    result += part.decode("utf-8", errors="replace")
            else:
                result += str(part)
        return result

    subject = _decode(msg.get("Subject", ""))
    sender = _decode(msg.get("From", ""))
    recipients = [_decode(msg.get("To", ""))]

    received_at = None
    received_ts = 0
    date_str = msg.get("Date", "")
    if date_str:
        try:
            received_at = parsedate_to_datetime(date_str)
            received_ts = int(received_at.timestamp())
        except Exception:
            pass

    body = ""
    body_preview = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if "attachment" not in cd.lower() and ct in ("text/plain", "text/html"):
                try:
                    charset = part.get_content_charset() or "utf-8"
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode(charset, errors="replace")
                        break
                except Exception:
                    pass
    else:
        try:
            charset = msg.get_content_charset() or "utf-8"
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode(charset, errors="replace")
        except Exception:
            pass

    body_preview = body[:200].strip()

    msg_id = msg.get("Message-ID", "").strip("<>")
    if not msg_id:
        msg_id = f"{sender}_{received_ts}"

    return EmailMessage(
        id=msg_id,
        subject=subject,
        sender=sender,
        recipients=recipients,
        body=body,
        body_preview=body_preview,
        received_at=received_at,
        received_timestamp=received_ts,
    )
