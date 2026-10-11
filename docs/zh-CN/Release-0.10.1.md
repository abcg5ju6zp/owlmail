# OwlMail 0.10.1

发布日期：2026-10-11

此维护版本修复普通 SMTP listener 的关闭竞态，并更新编译器与依赖。

## 修复

在 `smtp.Server.Serve` 接管 listener 前，`Close` 直接关闭普通 SMTP listener。快速启动后立即停止不再留下占用端口的 listener 和阻塞的服务协程。

## 更新

- 使用 Go 1.27.2 构建。
- 升级 soulteary kit 和第三方依赖，包括 SQLite/modernc 与 AWS SDK。
- 从同一不可变 tag 发布二进制和多架构容器镜像。

## 升级

使用 `ghcr.io/soulteary/owlmail:0.10.1` 或已验证的镜像 digest。运行时 API、配置和存储格式兼容 0.10.0，并保留其安全保护。更早版本的部署还应阅读 [0.10.0 迁移说明](Release-0.10.0.md)。
