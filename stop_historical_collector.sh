#!/bin/bash
# 多交易所BTC价格历史数据采集器 - 停止脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="historical_collector.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "❌ 找不到PID文件，历史数据采集器可能没有运行"
    exit 1
fi

PID=$(cat "$PID_FILE")

if ! ps -p $PID > /dev/null 2>&1; then
    echo "❌ 历史数据采集器进程不存在 (PID: $PID)"
    echo "🧹 清理PID文件..."
    rm -f "$PID_FILE"
    exit 1
fi

echo "🛑 正在停止历史数据采集器 (PID: $PID)..."

# 发送SIGTERM信号优雅退出
kill -TERM $PID

# 等待进程退出
for i in {1..10}; do
    if ! ps -p $PID > /dev/null 2>&1; then
        echo "✅ 历史数据采集器已成功停止"
        rm -f "$PID_FILE"
        exit 0
    fi
    echo "⏳ 等待进程退出... ($i/10)"
    sleep 1
done

# 如果优雅退出失败，强制杀死进程
echo "⚠️ 优雅退出超时，强制停止进程..."
kill -KILL $PID

sleep 1

if ! ps -p $PID > /dev/null 2>&1; then
    echo "✅ 历史数据采集器已强制停止"
    rm -f "$PID_FILE"
else
    echo "❌ 无法停止历史数据采集器进程"
    exit 1
fi