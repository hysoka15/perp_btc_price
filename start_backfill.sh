#!/bin/bash

# 历史数据补全采集器启动脚本

echo "🔍 检查运行环境..."

# 检查Python依赖
python3 -c "import asyncio, sqlite3, requests" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ Python依赖检查失败"
    exit 1
fi

echo "✅ 环境检查通过"

# 检查是否已有进程在运行
if [ -f "backfill_collector.pid" ]; then
    PID=$(cat backfill_collector.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "❌ 补全采集器已在运行 (PID: $PID)"
        echo "请先运行 ./stop_backfill.sh 停止现有进程"
        exit 1
    else
        echo "🧹 清理过期的PID文件"
        rm -f backfill_collector.pid
    fi
fi

echo "🚀 启动历史数据补全采集器..."

# 后台启动补全采集器
nohup python3 backfill_collector.py > backfill_collector.log 2>&1 &
PID=$!

# 保存PID
echo $PID > backfill_collector.pid

echo "✅ 历史数据补全采集器启动成功 (PID: $PID)"
echo ""
echo "📋 管理命令:"
echo "  查看日志: tail -f backfill_collector.log"
echo "  查看状态: ./status_backfill.sh"
echo "  停止服务: ./stop_backfill.sh"
echo ""
echo "💡 补全采集器会自动检查过去30天的数据并填充缺失的分钟数据"
echo "   每3秒采集一次，完成后自动停止"