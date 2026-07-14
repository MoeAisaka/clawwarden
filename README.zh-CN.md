# Clawwarden

面向 OpenClaw 无人值守工作流的持久化、带策略门禁控制面。

它把短暂的生命周期 Hook 转换为 SQLite WAL 持久事件，由独立 Worker 负责租约、重试、死信、恢复血缘与审查队列；Watchdog 独立检查 Worker 存活，Kill Switch 可立即停止消费但不丢事件。

## 默认安全边界

- 默认不自动提交任何长期记忆。
- 默认不自动恢复失败任务。
- 默认不自动重启 Gateway。
- 默认不备份 `openclaw.json` 或 nmem。
- 交易、凭证、生产破坏性动作、外部发布与 live skill 始终需要额外授权。
- 配额、Artifact、nmem 都是可选集成，不存在时显式显示 `disabled`。
- 默认只保存 Prompt 与回复摘要的哈希；必须显式开启才会在本地状态库保存原文。
- SQLite 状态库创建时强制为仅所有者可读写。
- 配置、数据库、日志、会话、备份和凭证均被 Git 忽略与秘密扫描器拦截。

## 本地验收

```bash
python3 -m unittest discover -s tests -v
node --test plugin/openclaw/tests/*.test.mjs
./scripts/preflight.sh
```

`preflight.sh` 同时运行确定性脱敏扫描与 gitleaks；缺少 gitleaks 时拒绝提交和推送。
