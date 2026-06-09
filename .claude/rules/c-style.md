# C 编码规范

## 命名
- 所有函数、结构体、枚举使用 snake_case
- 公开 API 必须加 `thinbt_` 前缀
- 头文件保护用 `THINBT_<MODULE>_H` 格式

## 错误处理
- 不要用 assert 处理运行时错误，assert 只用于调试断言
- 函数返回错误码（int），错误信息输出到 stderr
- 所有返回值必须检查，不能忽略

## 内存
- 调用者分配/释放，公开接口在文档注释中标注所有权
- 不要偷偷在内部 malloc，分配和释放要在同一层级
- 动态分配后必须检查 NULL

## 平台兼容
- 用 `#ifdef _WIN32` / `#ifndef _WIN32` 隔离平台代码
- 不要用 POSIX-only 函数，除非有对应的 Windows 替代方案
- 跨平台代码同时测试 Linux 和 Windows 路径

## 头文件
- include 顺序：先项目头文件，再系统头文件
- 项目头文件用 `#include "xxx.h"`，不用 `<>`

## 编译
- 必须兼容 `gcc -std=c99 -Wall -Wextra`，不能有 warning
- 新增 .c 文件记得加到 Makefile
