#!/bin/bash

# 历史数据补全采集器停止脚本

if [ ! -f "backfill_collector.pid" ]; then
    echo "❌ 未找到PID文件，补全采集器可能未运行"
    exit 1
fi

PID=$(cat backfill_collector.pid)

# 检查进程是否存在
if ! ps -p $PID > /dev/null 2>&1; then
    echo "❌ 进程 $PID 不存在，清理PID文件"
    rm -f backfill_collector.pid
    exit 1
fi

echo "🛑 正在停止历史数据补全采集器 (PID: $PID)..."

# 发送SIGTERM信号
kill -TERM $PID

# 等待进程退出
for i in {1..10}; do
    if ! ps -p $PID > /dev/null 2>&1; then
        echo "✅ 历史数据补全采集器已优雅停止"
        rm -f backfill_collector.pid
        exit 0
    fi
    echo "⏳ 等待进程退出... ($i/10)"
    sleep 1
done

# 强制停止
echo "⚠️ 优雅退出超时，强制停止进程..."
kill -KILL $PID
sleep 1

if ! ps -p $PID > /dev/null 2>&1; then
    echo "✅ 历史数据补全采集器已强制停止"
    rm -f backfill_collector.pid
else
    echo "❌ 无法停止进程"
    exit 1
fi