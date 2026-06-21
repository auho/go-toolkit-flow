#!/usr/bin/env bash
#
# Go 测试性能分析脚本
# 用途：运行指定测试用例，生成 CPU profile、trace 文件，并提供常用分析命令
#
# 用法：
#   ./scripts/profile_test.sh <test_name> [package] [timeout]
#
# 示例：
#   ./scripts/profile_test.sh TestExecConsumerBatch ./flow/ 300s
#   ./scripts/profile_test.sh TestExecConsumerBatch
#
# 生成的文件（默认在 /tmp 下）：
#   cpu.prof   — CPU 采样 profile
#   trace.out  — 执行 trace
#   net.pprof  — 网络阻塞 profile（从 trace 提取）
#   sync.pprof — 同步阻塞 profile
#   sched.pprof — 调度延迟 profile
#
set -euo pipefail

# === 参数解析 ===
TEST_NAME="${1:-TestExecConsumerBatch}"
PACKAGE="${2:-./flow/}"
TIMEOUT="${3:-300s}"

# === 输出目录 ===
OUT_DIR="/tmp"
CPU_PROF="${OUT_DIR}/cpu.prof"
TRACE_FILE="${OUT_DIR}/trace.out"
NET_PPROF="${OUT_DIR}/net.pprof"
SYNC_PPROF="${OUT_DIR}/sync.pprof"
SCHED_PPROF="${OUT_DIR}/sched.pprof"

echo "=========================================="
echo " Go 测试性能分析"
echo "=========================================="
echo " 测试用例: ${TEST_NAME}"
echo " 包目录:   ${PACKAGE}"
echo " 超时:     ${TIMEOUT}"
echo " 输出目录: ${OUT_DIR}"
echo "=========================================="
echo ""

# === Step 1: 运行测试并生成 profile ===
echo "[1/3] 运行测试并生成 CPU profile 和 trace..."
echo "命令: go test -run ${TEST_NAME} -v -count=1 -timeout ${TIMEOUT} \\"
echo "        -cpuprofile ${CPU_PROF} -trace ${TRACE_FILE} ${PACKAGE}"
echo ""

go test -run "${TEST_NAME}" -v -count=1 -timeout "${TIMEOUT}" \
	-cpuprofile "${CPU_PROF}" \
	-trace "${TRACE_FILE}" \
	"${PACKAGE}" 2>&1 | tail -10

echo ""
echo "已生成:"
echo "  CPU profile: ${CPU_PROF}"
echo "  Trace:       ${TRACE_FILE}"
echo ""

# === Step 2: 从 trace 提取阻塞 profile ===
echo "[2/3] 从 trace 提取阻塞 profile..."

echo "  - 提取网络阻塞 (net)..."
go tool trace -pprof=net "${TRACE_FILE}" > "${NET_PPROF}" 2>&1

echo "  - 提取同步阻塞 (sync)..."
go tool trace -pprof=sync "${TRACE_FILE}" > "${SYNC_PPROF}" 2>&1

echo "  - 提取调度延迟 (sched)..."
go tool trace -pprof=sched "${TRACE_FILE}" > "${SCHED_PPROF}" 2>&1

echo ""
echo "已生成:"
echo "  网络阻塞:  ${NET_PPROF}"
echo "  同步阻塞:  ${SYNC_PPROF}"
echo "  调度延迟:  ${SCHED_PPROF}"
echo ""

# === Step 3: 输出分析结果摘要 ===
echo "[3/3] 分析结果摘要"
echo "=========================================="
echo ""
echo "--- CPU Profile（判断 CPU 密集 vs I/O 密集）---"
echo "命令: go tool pprof -top -cum ${CPU_PROF}"
echo ""
go tool pprof -top -cum "${CPU_PROF}" 2>&1 | head -20
echo ""

echo "--- 网络阻塞 Profile（I/O 等待分析）---"
echo "命令: go tool pprof -text -cum ${NET_PPROF}"
echo ""
go tool pprof -text -cum "${NET_PPROF}" 2>&1 | head -20
echo ""

echo "--- 调度延迟 Profile（goroutine 调度分析）---"
echo "命令: go tool pprof -text -cum ${SCHED_PPROF}"
echo ""
go tool pprof -text -cum "${SCHED_PPROF}" 2>&1 | head -20
echo ""

echo "=========================================="
echo " 分析完成"
echo "=========================================="
echo ""
echo "生成的文件:"
echo "  ${CPU_PROF}     — CPU 采样数据"
echo "  ${TRACE_FILE}   — 执行 trace（可用 go tool trace 查看 Web 界面）"
echo "  ${NET_PPROF}    — 网络阻塞 profile"
echo "  ${SYNC_PPROF}   — 同步阻塞 profile"
echo "  ${SCHED_PPROF}  — 调度延迟 profile"
echo ""
echo "进阶分析命令:"
echo ""
echo "  # 交互式 CPU profile 分析"
echo "  go tool pprof ${CPU_PROF}"
echo "  # (pprof) top -cum        # 按累计时间排序"
echo "  # (pprof) list <func>     # 查看函数源码及逐行采样"
echo "  # (pprof) web             # 生成调用图（需 graphviz）"
echo ""
echo "  # 聚焦特定函数的网络阻塞"
echo "  go tool pprof -text -cum -focus=\"scanRows\" ${NET_PPROF}"
echo ""
echo "  # 启动 trace Web 界面"
echo "  go tool trace ${TRACE_FILE}"
echo ""
echo "  # 查看完整 trace 时间线（goroutine 调度、GC、系统调用等）"
echo "  # 浏览器打开 http://localhost:8080"
echo ""
