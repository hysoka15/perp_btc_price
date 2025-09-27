#!/bin/bash
# 多交易所BTC价格历史数据采集器 - 状态查看脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="historical_collector.pid"
LOG_FILE="historical_collector.log"
DB_FILE="prices.db"

echo "📊 === 历史数据采集器状态检查 ==="
echo "🕒 检查时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 检查进程状态
echo "🔍 进程状态:"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo "   ✅ 运行中 (PID: $PID)"
        
        # 获取进程信息
        PROCESS_INFO=$(ps -p $PID -o pid,ppid,etime,pcpu,pmem,cmd --no-headers)
        echo "   📋 进程信息: $PROCESS_INFO"
        
        # 获取运行时间
        START_TIME=$(ps -p $PID -o lstart --no-headers)
        echo "   ⏰ 启动时间: $START_TIME"
    else
        echo "   ❌ 进程不存在 (PID: $PID)"
        echo "   🧹 建议清理PID文件: rm $PID_FILE"
    fi
else
    echo "   ❌ 未运行 (找不到PID文件)"
fi

echo ""

# 检查日志文件
echo "📄 日志文件状态:"
if [ -f "$LOG_FILE" ]; then
    LOG_SIZE=$(du -h "$LOG_FILE" | cut -f1)
    LOG_LINES=$(wc -l < "$LOG_FILE")
    LAST_MODIFIED=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" "$LOG_FILE" 2>/dev/null || stat -c "%y" "$LOG_FILE" 2>/dev/null | cut -d' ' -f1-2)
    
    echo "   📁 文件大小: $LOG_SIZE"
    echo "   📝 行数: $LOG_LINES"
    echo "   🕐 最后修改: $LAST_MODIFIED"
    
    echo ""
    echo "📋 最近10行日志:"
    tail -10 "$LOG_FILE" | sed 's/^/      /'
else
    echo "   ❌ 日志文件不存在"
fi

echo ""

# 检查数据库状态
echo "💾 数据库状态:"
if [ -f "$DB_FILE" ]; then
    DB_SIZE=$(du -h "$DB_FILE" | cut -f1)
    echo "   📁 数据库大小: $DB_SIZE"
    
    # 使用Python查询数据库统计
    echo "   📊 数据统计:"
    python3 -c "
try:
    from database import get_database
    import sqlite3
    from datetime import datetime, timedelta
    
    db = get_database()
    stats = db.get_statistics()
    
    print(f'      总记录数: {stats.get(\"total_records\", 0)}')
    
    # 查询最近24小时的记录
    conn = sqlite3.connect('$DB_FILE')
    cursor = conn.cursor()
    
    # 最新记录时间
    cursor.execute('SELECT MAX(timestamp) FROM price_data')
    latest = cursor.fetchone()[0]
    if latest:
        print(f'      最新数据: {latest}')
    
    # 最近24小时记录数
    yesterday = (datetime.now() - timedelta(days=1)).isoformat()
    cursor.execute('SELECT COUNT(*) FROM price_data WHERE timestamp > ?', (yesterday,))
    recent_count = cursor.fetchone()[0]
    print(f'      24h记录数: {recent_count}')
    
    # 各交易所记录数
    cursor.execute('SELECT exchange, COUNT(*) FROM price_data GROUP BY exchange ORDER BY COUNT(*) DESC')
    exchanges = cursor.fetchall()
    print('      交易所记录:')
    for exchange, count in exchanges:
        print(f'        {exchange}: {count}')
    
    conn.close()
    
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

# 提供管理建议
echo "🛠️ 管理命令:"
echo "   启动: ./start_historical_collector.sh"
echo "   停止: ./stop_historical_collector.sh"
echo "   查看实时日志: tail -f $LOG_FILE"
echo "   清理日志: > $LOG_FILE"

echo ""
echo "==================================="