# 公共默认 User-Agent

在 `data/config.json`（或 `--config` 指定的配置文件）顶层添加以下配置，修改后重启 OpenList：

```json
{
  "user_agent": "MyClient/1.0",
  "user_agent_nt": "MyWindowsClient/1.0"
}
```

| 配置项 | 环境变量 | 用途 |
| --- | --- | --- |
| `user_agent` | `OPENLIST_USER_AGENT` | 公共 HTTP API 客户端及显式使用 `base.UserAgent` 的请求 |
| `user_agent_nt` | `OPENLIST_USER_AGENT_NT` | 使用 Windows 版公共 UA（`base.UserAgentNT`）的请求 |

两个配置项独立生效。未配置时保留各自原有的内置浏览器 UA；有效配置值为空字符串时也回退到内置值。

环境变量沿用现有配置规则：默认覆盖配置文件；`force: true` 时不读取环境变量；使用 `--no-prefix` 时，变量名为 `USER_AGENT` 和 `USER_AGENT_NT`。

这些配置只控制公共默认值。驱动显式指定的 UA、存储配置中的自定义 UA，以及驱动选择透传的客户端 UA，仍按各自的逻辑生效。普通下载透明代理仍由客户端请求头与驱动返回的下载请求头决定 UA。
