#!/bin/bash

# 历史数据补全采集器状态检查脚本

echo "📊 === 历史数据补全采集器状态检查 ==="
echo "🕒 检查时间: $(date)"
echo ""

# 检查PID文件和进程
echo "🔍 进程状态:"
if [ -f "backfill_collector.pid" ]; then
    PID=$(cat backfill_collector.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "   ✅ 运行中 (PID: $PID)"
        echo "   📋 进程信息: "
        ps -p $PID -o pid,ppid,cmd,etime
    else
        echo "   ❌ 进程不存在 (PID文件过期)"
        rm -f backfill_collector.pid
    fi
else
    echo "   ❌ 未运行 (无PID文件)"
fi

echo ""

# 检查日志文件
echo "📄 日志文件状态:"
if [ -f "backfill_collector.log" ]; then
    LOG_SIZE=$(du -h backfill_collector.log | cut -f1)
    LOG_LINES=$(wc -l < backfill_collector.log)
    LOG_MODIFIED=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" backfill_collector.log 2>/dev/null || stat -c "%y" backfill_collector.log 2>/dev/null | cut -d. -f1)
    
    echo "   📁 文件大小: $LOG_SIZE"
    echo "   📝 行数:     $LOG_LINES"
    echo "   🕐 最后修改: $LOG_MODIFIED"
else
    echo "   ❌ 日志文件不存在"
fi

echo ""

# 显示最近的日志
echo "📋 最近10行日志:"
if [ -f "backfill_collector.log" ]; then
    tail -10 backfill_collector.log | sed 's/^/      /'
else
    echo "      无日志文件"
fi

echo ""

# 检查数据库状态
echo "💾 数据库状态:"
if [ -f "prices.db" ]; then
    DB_SIZE=$(du -h prices.db | cut -f1)
    echo "   📁 数据库大小:   $DB_SIZE"
    
    # 简单的数据统计
    echo "   📊 数据统计:"
    python3 -c "
try:
    from database import get_database
    db = get_database()
    stats = db.get_statistics()
    print(f'      总记录数: {stats.get(\"total_records\", 0)}')
    print(f'      最新时间: {stats.get(\"latest_time\", \"未知\")}')
    print(f'      活跃交易所: {stats.get(\"active_exchanges\", 0)}')
except Exception as e:
    print(f'      ❌ 查询失败: {e}')
" 2>/dev/null
else
    echo "   ❌ 数据库文件不存在"
fi

echo ""

# 检查磁盘空间
echo "💿 磁盘空间:"
DISK_USAGE=$(df -h . | tail -1 | awk '{print $4 " 可用 (使用率: " $5 ")"}')
echo "   📊 可用空间: $DISK_USAGE"

echo ""

# 管理命令提示
echo "🛠️ 管理命令:"
echo "   启动: ./start_backfill.sh"
echo "   停止: ./stop_backfill.sh"
echo "   查看实时日志: tail -f backfill_collector.log"
echo "   清理日志: > backfill_collector.log"

echo ""
echo "==================================="