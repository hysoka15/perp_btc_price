#!/bin/bash
# 多交易所BTC价格历史数据采集器 - 后台启动脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE="historical_collector.pid"
LOG_FILE="historical_collector.log"

# 检查是否已经在运行
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo "❌ 历史数据采集器已在运行 (PID: $PID)"
        echo "   如需重启，请先运行: ./stop_historical_collector.sh"
        exit 1
    else
        echo "🧹 清理过时的PID文件..."
        rm -f "$PID_FILE"
    fi
fi

# 检查Python依赖
echo "🔍 检查运行环境..."
python3 -c "
try:
    from binance_client import BinanceClient
    from lighter_client import get_lighter_btc_price
    from edgex_client import get_edgex_btc_price
    from aster_client import get_aster_btc_price
    from database import get_database
    print('✅ 所有依赖模块检查通过')
except ImportError as e:
    print(f'❌ 依赖模块缺失: {e}')
    exit(1)
" || exit 1

# 创建日志目录
mkdir -p logs

echo "🚀 启动多交易所BTC价格历史数据采集器..."
echo "📅 采集间隔: 每3秒"
echo "🏦 交易所: Binance, Lighter, EdgeX, Aster"
echo "📂 日志文件: $LOG_FILE"
echo "📊 数据库: prices.db"

# 后台运行
nohup python3 historical_data_collector.py > "$LOG_FILE" 2>&1 &
PID=$!

# 保存PID
echo $PID > "$PID_FILE"

# 等待一下确保启动成功
sleep 3

if ps -p $PID > /dev/null 2>&1; then
    echo "✅ 历史数据采集器启动成功 (PID: $PID)"
    echo ""
    echo "📋 管理命令:"
    echo "   查看日志: tail -f $LOG_FILE"
    echo "   查看状态: ./status_historical_collector.sh"
    echo "   停止服务: ./stop_historical_collector.sh"
    echo ""
    echo "💡 建议: 添加到系统启动项以实现开机自启"
else
    echo "❌ 历史数据采集器启动失败"
    rm -f "$PID_FILE"
    echo "📋 请检查日志文件: $LOG_FILE"
    exit 1
fi