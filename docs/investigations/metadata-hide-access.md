# 隐藏路径显示密码框：现状与目标

用户最新确认：隐藏已阻止访问，但显示密码页面；要求撤回之前的权限改造计划，改为让隐藏路径和不存在路径表现相同。

修复前的调用链：

- `server/common/check.go` 的 `CanAccess` 对隐藏、可读用户限制和密码失败都返回 false。
- `server/handles/fsread.go` 的 list/get/dirs 对 false 统一返回业务码 403。
- 前端 `src/hooks/usePath.ts` 收到 403 就进入 `NeedPassword`，`src/pages/home/Obj.tsx` 显示密码框。
- 真正缺失路径的存储错误直接以 500 返回，可能带逐层包装信息；只将隐藏改为 404 仍可从 API 区分两者。

实现：list/get/dirs 将现有规则命中的隐藏路径和真实缺失路径统一为 `{"code":404,"message":"object not found","data":null}`，使用原有 HTTP 200 JSON 封装。前端因此显示相同错误视图，不显示密码框，不包含目标属性或链接。普通密码保护及其他权限保持各自的现有响应。

测试和实现记录见 [TDD 计划](../testing/metadata-hide-tdd.md)。
