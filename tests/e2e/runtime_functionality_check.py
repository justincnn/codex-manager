import argparse
import asyncio
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import httpx
import websockets


STALE_ERROR = "服务启动时检测到未完成的历史任务，已标记失败，请重新发起。"


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _connect_db(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=5)


def _fetchone_dict(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...]) -> Dict[str, Any]:
    conn.row_factory = sqlite3.Row
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else {}


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _health_check(client: httpx.Client, report: Dict[str, Any]) -> None:
    response = client.get("/api/registration/tasks", params={"page": 1, "page_size": 1})
    report["health"] = {"status_code": response.status_code, "body": response.json()}
    _assert(response.status_code == 200, "健康检查失败")


async def _collect_task_websocket(ws_url: str, task_uuid: str) -> Dict[str, Any]:
    endpoint = f"{ws_url}/api/ws/task/{task_uuid}"
    messages: List[Dict[str, Any]] = []
    started_at = time.time()

    async with websockets.connect(endpoint, open_timeout=10, close_timeout=5) as websocket:
        while time.time() - started_at < 30:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=10)
            payload = json.loads(raw_message)
            messages.append(payload)
            if payload.get("type") == "status" and payload.get("status") in {"completed", "failed"}:
                break

    logs = [message for message in messages if message.get("type") == "log"]
    statuses = [message for message in messages if message.get("type") == "status"]
    return {
        "messages": messages,
        "log_count": len(logs),
        "status_count": len(statuses),
        "live_log_count": sum(1 for message in logs if "timestamp" in message),
        "final_status": statuses[-1]["status"] if statuses else None,
    }


def _poll_task_completion(client: httpx.Client, task_uuid: str) -> Dict[str, Any]:
    deadline = time.time() + 20
    while time.time() < deadline:
        response = client.get(f"/api/registration/tasks/{task_uuid}")
        response.raise_for_status()
        payload = response.json()
        if payload["status"] in {"completed", "failed"}:
            return payload
        time.sleep(0.2)
    raise TimeoutError(f"任务未在预期时间内结束: {task_uuid}")


def _validate_live_database(
    db_path: Path,
    task_uuid: str,
    batch_id: str,
    checks: Dict[str, Any],
    report: Dict[str, Any],
) -> None:
    with _connect_db(db_path) as conn:
        seeded = _fetchone_dict(
            conn,
            "SELECT email, access_token, refresh_token, token_sync_status FROM accounts WHERE email = ?",
            (checks["seeded_account_email"],),
        )
        tokenless = _fetchone_dict(
            conn,
            "SELECT email, access_token, refresh_token, token_sync_status FROM accounts WHERE email = ?",
            (checks["tokenless_account_email"],),
        )
        partial = _fetchone_dict(
            conn,
            "SELECT email, access_token, refresh_token, token_sync_status FROM accounts WHERE email = ?",
            (checks["partial_account_email"],),
        )
        task_row = _fetchone_dict(
            conn,
            "SELECT task_uuid, status, logs, result FROM registration_tasks WHERE task_uuid = ?",
            (task_uuid,),
        )
        outlook_row = _fetchone_dict(
            conn,
            "SELECT config FROM email_services WHERE id = ?",
            (checks["outlook_service_id"],),
        )

    _assert(seeded.get("token_sync_status") == "pending", "seeded 账号 token_sync_status 异常")
    _assert(tokenless.get("access_token") == "mock-access-token-updated", "tokenless 账号 access_token 未写入")
    _assert(tokenless.get("token_sync_status") == "pending", "tokenless 账号 token_sync_status 异常")
    _assert(partial.get("access_token") == "mock-access-token-partial", "partial 账号 access_token 丢失")
    _assert(partial.get("refresh_token") == "", "partial 账号 refresh_token 未清空")
    _assert(partial.get("token_sync_status") == "pending", "partial 账号 token_sync_status 异常")
    _assert(task_row.get("status") == "completed", "模拟任务数据库状态不是 completed")
    _assert(task_row.get("logs"), "模拟任务日志未落库")

    task_result = json.loads(task_row["result"]) if task_row.get("result") else {}
    outlook_config = json.loads(outlook_row["config"]) if outlook_row.get("config") else {}
    second_account = next(
        account for account in outlook_config.get("accounts", [])
        if account.get("email") == checks["outlook_account_email"]
    )

    batch_snapshot = task_result["hardening_checks"]["batch_counter"]["snapshot"]
    backoff_states = task_result["hardening_checks"]["otp_timeout_backoff"]["states"]

    _assert(second_account["refresh_token"] == "new-second", "Outlook refresh_token 未更新")
    _assert(batch_snapshot["completed"] == 3, "批量 completed 计数异常")
    _assert(batch_snapshot["success"] == 2, "批量 success 计数异常")
    _assert(batch_snapshot["failed"] == 1, "批量 failed 计数异常")
    _assert(batch_snapshot["status"] == "completed", "批量状态异常")
    _assert(batch_snapshot["finished"] is True, "批量 finished 标记异常")
    _assert(backoff_states[-1]["delay_seconds"] == 3600, "OTP 深度冷却未生效")
    _assert(backoff_states[-1]["failures"] == 3, "OTP 连续失败次数异常")

    report["database"] = {
        "task_uuid": task_uuid,
        "batch_id": batch_id,
        "seeded_account": seeded,
        "tokenless_account": tokenless,
        "partial_account": partial,
        "task_result": task_result,
        "outlook_second_account": second_account,
    }


def run_live_mode(base_url: str, ws_url: str, db_path: Path, report_path: Path) -> None:
    report: Dict[str, Any] = {"mode": "live", "base_url": base_url, "db_path": str(db_path)}
    with httpx.Client(base_url=base_url, timeout=httpx.Timeout(10, read=30)) as client:
        _health_check(client, report)

        create_response = client.post(
            "/api/registration/create",
            json={
                "email_service_type": "tempmail",
                "start_delay_ms": 600,
                "log_delay_ms": 150,
            },
        )
        create_response.raise_for_status()
        created = create_response.json()
        task_uuid = created["task"]["task_uuid"]
        batch_id = created["batch_id"]
        checks = created["checks"]
        report["create"] = created

        ws_report = asyncio.run(_collect_task_websocket(ws_url, task_uuid))
        report["websocket"] = ws_report
        _assert(ws_report["final_status"] == "completed", "WebSocket 未收到 completed 状态")
        _assert(ws_report["log_count"] >= 4, "WebSocket 日志数量不足")
        _assert(ws_report["live_log_count"] >= 1, "未捕获到实时日志广播")

        task_payload = _poll_task_completion(client, task_uuid)
        report["task"] = task_payload
        runtime_checks = {
            **checks,
            "outlook_service_id": task_payload["result"]["hardening_checks"]["outlook_refresh"]["service_id"],
            "backoff_service_id": task_payload["result"]["hardening_checks"]["otp_timeout_backoff"]["service_id"],
        }

        batch_response = client.get(f"/api/registration/batch/{batch_id}")
        batch_response.raise_for_status()
        report["batch_api"] = batch_response.json()
        _assert(report["batch_api"]["completed"] == 3, "批量状态 API completed 异常")
        _assert(report["batch_api"]["success"] == 2, "批量状态 API success 异常")
        _assert(report["batch_api"]["failed"] == 1, "批量状态 API failed 异常")
        _assert(report["batch_api"]["finished"] is True, "批量状态 API finished 异常")

    _validate_live_database(db_path, task_uuid, batch_id, runtime_checks, report)
    _write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def run_prepare_recovery_mode(db_path: Path, state_path: Path) -> None:
    stale_task_uuid = f"stale-{uuid.uuid4()}"
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with _connect_db(db_path) as conn:
        conn.execute(
            """
            INSERT INTO registration_tasks (task_uuid, status, logs, created_at, started_at)
            VALUES (?, 'running', '[00:00:00] stale task', ?, ?)
            """,
            (stale_task_uuid, now, now),
        )
        conn.commit()

    payload = {
        "stale_task_uuid": stale_task_uuid,
        "db_path": str(db_path),
        "prepared_at": now,
    }
    _write_json(state_path, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def run_verify_recovery_mode(base_url: str, db_path: Path, state_path: Path, report_path: Path) -> None:
    state = _load_json(state_path)
    report: Dict[str, Any] = {
        "mode": "verify-recovery",
        "base_url": base_url,
        "db_path": str(db_path),
        "state": state,
    }

    with httpx.Client(base_url=base_url, timeout=httpx.Timeout(10, read=30)) as client:
        _health_check(client, report)

    with _connect_db(db_path) as conn:
        stale_task = _fetchone_dict(
            conn,
            "SELECT task_uuid, status, error_message, logs, completed_at FROM registration_tasks WHERE task_uuid = ?",
            (state["stale_task_uuid"],),
        )

    _assert(stale_task.get("status") == "failed", "僵尸任务未在重启后标记为 failed")
    _assert(stale_task.get("error_message") == STALE_ERROR, "僵尸任务 error_message 不匹配")
    _assert(STALE_ERROR in (stale_task.get("logs") or ""), "僵尸任务日志未追加系统收敛说明")
    _assert(bool(stale_task.get("completed_at")), "僵尸任务 completed_at 缺失")

    report["recovery"] = stale_task
    _write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="真实服务功能可用性验证脚本")
    parser.add_argument("--mode", choices=["live", "prepare-recovery", "verify-recovery"], required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:15555")
    parser.add_argument("--ws-url", default="ws://127.0.0.1:15555")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--report-path", default="tests_runtime/runtime_functionality_report.json")
    parser.add_argument("--state-path", default="tests_runtime/runtime_recovery_state.json")
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    report_path = Path(args.report_path).resolve()
    state_path = Path(args.state_path).resolve()

    if args.mode == "live":
        run_live_mode(args.base_url, args.ws_url, db_path, report_path)
        return
    if args.mode == "prepare-recovery":
        run_prepare_recovery_mode(db_path, state_path)
        return
    run_verify_recovery_mode(args.base_url, db_path, state_path, report_path)


if __name__ == "__main__":
    main()
