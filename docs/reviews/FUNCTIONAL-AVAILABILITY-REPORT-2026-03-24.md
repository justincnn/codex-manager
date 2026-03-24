# 功能可用性实测报告

日期: 2026-03-24
范围:
- 启动命令: `uv run python -m src.web.app`
- 监听地址: `http://127.0.0.1:15555`
- 隔离数据库: `tests_runtime/e2e_runtime_1774308869.db`
- 实测脚本: `tests/e2e/runtime_functionality_check.py`

## 1. 执行摘要

本次按真实服务链路完成了以下验证:

- 服务存活检查通过。
- `POST /api/registration/create` 可创建受控模拟任务。
- `GET /api/ws/task/{task_uuid}` 可实时推送日志与状态，任务完成时收到 `completed`。
- 任务完成后数据库状态符合 Task 1、Task 5 预期。
- 批量计数探针通过 `/api/registration/batch/{batch_id}` 验证，符合 Task 2 预期。
- 重启后僵尸任务被自动标记失败，符合 Task 4 预期。

结论:

- 本次新增的真实服务验证 harness 可用。
- Task 1-5 中本次可通过真实服务直接观测的加固点均已生效。

## 2. 实测过程

### 2.1 端口处理

`15555` 端口初始被已有容器 `codex-manager-webui-1` 占用。为执行指定启动命令，先停止该容器，实测结束后已恢复。

### 2.2 执行命令

1. 静态验证
   - `uv run python -m pytest tests/test_account_token_sync_status.py tests/test_batch_task_manager.py tests/test_task_manager_status_broadcast.py tests/test_task_recovery.py tests/test_registration_email_service_failover.py`
   - 结果: `16 passed`
   - `uv run python -m py_compile src/web/app.py src/web/routes/registration.py tests/e2e/runtime_functionality_check.py`
   - 结果: 退出码 `0`

2. 真实服务启动
   - `APP_DATABASE_URL='sqlite:////Volumes/Work/code/codex-manager/tests_runtime/e2e_runtime_1774308869.db' APP_HOST='127.0.0.1' APP_PORT='15555' uv run python -m src.web.app`

3. live 实测
   - `uv run python tests/e2e/runtime_functionality_check.py --mode live --base-url http://127.0.0.1:15555 --ws-url ws://127.0.0.1:15555 --db-path /Volumes/Work/code/codex-manager/tests_runtime/e2e_runtime_1774308869.db --report-path /Volumes/Work/code/codex-manager/tests_runtime/runtime_functionality_report_1774308869.json`

4. recovery 准备
   - `uv run python tests/e2e/runtime_functionality_check.py --mode prepare-recovery --db-path /Volumes/Work/code/codex-manager/tests_runtime/e2e_runtime_1774308869.db --state-path /Volumes/Work/code/codex-manager/tests_runtime/runtime_recovery_state_1774308869.json`

5. 服务重启后 recovery 实测
   - `uv run python tests/e2e/runtime_functionality_check.py --mode verify-recovery --base-url http://127.0.0.1:15555 --db-path /Volumes/Work/code/codex-manager/tests_runtime/e2e_runtime_1774308869.db --state-path /Volumes/Work/code/codex-manager/tests_runtime/runtime_recovery_state_1774308869.json --report-path /Volumes/Work/code/codex-manager/tests_runtime/runtime_recovery_report_1774308869.json`

## 3. 验证结果

### 3.1 服务存活

- `GET /api/registration/tasks?page=1&page_size=1` 返回 `200`。

### 3.2 模拟任务创建与 WebSocket

- 创建任务 UUID: `a8f4da41-354c-4d89-9634-c582a032c70b`
- 批量探针 ID: `2e8cfce4-bf20-4f0b-8839-a94e8e141472`
- WebSocket 收到 3 条状态消息:
  - `pending`
  - `running`
  - `completed`
- WebSocket 收到 6 条实时日志，包含:
  - Token 同步探针写库
  - OTP 超时退避 3 次
  - 批量计数探针完成

判定:

- 日志不是任务结束后一次性补发，而是在运行过程中实时推送。

### 3.3 Task 1 验证: Token 同步

数据库中以下账号状态正确:

- `mock-seeded-a8f4da41@example.test`
  - `access_token` 已保存
  - `refresh_token` 已保存
  - `token_sync_status = pending`

- `mock-tokenless-a8f4da41@example.test`
  - 先创建无 token，再更新 `access_token`
  - `token_sync_status = pending`

- `mock-partial-a8f4da41@example.test`
  - 清空 `refresh_token` 后仍保留 `access_token`
  - `token_sync_status = pending`

Outlook 配置探针:

- `mock-outlook-a8f4da41@example.test`
  - `refresh_token` 已从 `old-second` 更新为 `new-second`

### 3.4 Task 2 验证: 批量计数

`GET /api/registration/batch/2e8cfce4-bf20-4f0b-8839-a94e8e141472` 返回:

- `total = 3`
- `completed = 3`
- `success = 2`
- `failed = 1`
- `finished = true`
- `progress = 3/3`

判定:

- 批量计数与任务结果一致，收口正确。

### 3.5 Task 3 验证: 单任务状态广播

任务完成时，WebSocket 最后一条状态消息为:

- `status = completed`
- `email = mock-seeded-a8f4da41@example.test`
- `email_service = tempmail`

判定:

- 单任务状态广播已生效。

### 3.6 Task 4 验证: 僵尸任务恢复

重启前手工插入任务:

- `stale-e738842e-74d8-400d-859e-1b283eab1a95`
- 初始状态: `running`

重启后观测结果:

- 状态变为 `failed`
- `error_message = 服务启动时检测到未完成的历史任务，已标记失败，请重新发起。`
- `logs` 已追加系统收敛日志
- `completed_at` 已写入

服务启动日志同时出现:

- `已收敛 1 个僵尸任务: stale-e7`

### 3.7 Task 5 验证: OTP 超时退避

模拟任务内部连续触发 3 次二阶段 OTP 超时，记录到任务结果:

- 第 1 次: `failures = 1`, `delay_seconds = 30`
- 第 2 次: `failures = 2`, `delay_seconds = 60`
- 第 3 次: `failures = 3`, `delay_seconds = 3600`

判定:

- 深度冷却逻辑已生效。

## 4. 产物

- `tests/e2e/runtime_functionality_check.py`
- `tests_runtime/runtime_functionality_report_1774308869.json`
- `tests_runtime/runtime_recovery_report_1774308869.json`
- `tests_runtime/runtime_recovery_state_1774308869.json`
- `tests_runtime/e2e_runtime_1774308869.db`

## 5. 观察到的问题

- `python -m src.web.app` 启动时会出现 `runpy` 的重复导入告警，但不影响服务启动与本次验证结果。
- 启动日志中打印的 `host` 和 `database` 仍显示为数据库配置值 `0.0.0.0 / sqlite:///data/database.db`，与本次通过环境变量注入的真实运行参数不一致。实测链路实际使用了隔离数据库，但日志口径存在偏差。
