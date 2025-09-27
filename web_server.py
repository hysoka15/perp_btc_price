#!/usr/bin/env python3
"""
多交易所BTC价格对比Web服务器
提供Web界面展示各交易所与币安的价差K线图
"""

import json
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

from database import get_database

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 获取数据库实例
db = get_database()

@app.route('/')
def index():
    """主页面 - 显示价差K线图"""
    return render_template('index.html')

@app.route('/api/price_data')
def get_price_data():
    """获取价格数据API"""
    try:
        # 获取查询参数
        hours = request.args.get('hours', 24, type=int)
        exchange = request.args.get('exchange', None)
        
        if exchange:
            # 获取特定交易所的价格历史
            data = db.get_price_history(exchange, hours)
        else:
            # 获取所有交易所的价格对比数据
            data = db.get_price_comparison_data(hours)
        
        return jsonify({
            'success': True,
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取价格数据失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/exchange_status')
def get_exchange_status():
    """获取交易所状态API"""
    try:
        status_data = db.get_exchange_status()
        return jsonify({
            'success': True,
            'data': status_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取交易所状态失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/statistics')
def get_statistics():
    """获取统计信息API"""
    try:
        stats = db.get_statistics()
        return jsonify({
            'success': True,
            'data': stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取统计信息失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/latest_prices')
def get_latest_prices():
    """获取最新价格API"""
    try:
        limit = request.args.get('limit', 100, type=int)
        latest_data = db.get_latest_prices(limit)
        
        return jsonify({
            'success': True,
            'data': latest_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取最新价格失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/chart_data')
def get_chart_data():
    """获取图表数据API - 格式化为Chart.js所需的格式"""
    try:
        hours = request.args.get('hours', 24, type=int)
        
        # 获取所有交易所的价格对比数据
        raw_data = db.get_price_comparison_data(hours)
        
        # 转换为Chart.js格式
        chart_data = {
            'labels': [],
            'datasets': []
        }
        
        # 定义交易所颜色
        exchange_colors = {
            'binance': '#F0B90B',  # 币安黄色
            'lighter': '#007bff',  # 蓝色
            'edgex': '#28a745',    # 绿色
            'aster': '#dc3545'     # 红色
        }
        
        # 按分钟聚合数据，解决重复时间标签问题
        def format_time_minute(timestamp_str):
            try:
                if ' ' in timestamp_str:
                    time_part = timestamp_str.split(' ')[1]
                else:
                    time_part = timestamp_str
                
                if ':' in time_part:
                    parts = time_part.split(':')
                    if len(parts) >= 2:
                        return f"{parts[0]}:{parts[1]}"
                
                return time_part[:5]
            except:
                return timestamp_str[-8:]
        
        # 按分钟聚合数据
        minute_data = {}
        
        for exchange, records in raw_data.items():
            for record in records:
                minute_key = format_time_minute(record['timestamp'])
                
                if minute_key not in minute_data:
                    minute_data[minute_key] = {}
                
                # 对同一分钟内的多条数据，取最新的一条
                if exchange not in minute_data[minute_key]:
                    minute_data[minute_key][exchange] = record
                else:
                    # 比较时间戳，保留最新的
                    if record['timestamp'] > minute_data[minute_key][exchange]['timestamp']:
                        minute_data[minute_key][exchange] = record
        
        # 按时间排序
        sorted_minutes = sorted(minute_data.keys())
        chart_data['labels'] = sorted_minutes
        
        # 为每个交易所创建数据集（排除test_exchange）
        excluded_exchanges = {'binance', 'test_exchange'}
        
        # 获取所有交易所（除了被排除的）
        all_exchanges = set()
        for minute_records in minute_data.values():
            all_exchanges.update(minute_records.keys())
        
        # 首先构建所有时间点的币安价格数组
        binance_prices = []
        for minute in sorted_minutes:
            if 'binance' in minute_data[minute]:
                binance_price = minute_data[minute]['binance']['binance_base_price']
                binance_prices.append(binance_price)
            else:
                binance_prices.append(None)
        
        for exchange in all_exchanges:
            if exchange in excluded_exchanges:
                continue  # 跳过币安（基准）和测试交易所
            
            # 构建每分钟的数据点
            data_points = []
            for minute in sorted_minutes:
                if exchange in minute_data[minute]:
                    price_diff = minute_data[minute][exchange]['price_diff']
                    data_points.append(price_diff)
                else:
                    data_points.append(None)  # 没有数据的时间点
            
            # 如果这个交易所没有任何有效数据，跳过
            if not any(x is not None for x in data_points):
                continue
            
            # 添加到数据集
            dataset = {
                'label': f'{exchange.upper()} 价差',
                'data': data_points,
                'borderColor': exchange_colors.get(exchange, '#666666'),
                'backgroundColor': exchange_colors.get(exchange, '#666666') + '20',  # 添加透明度
                'fill': False,
                'tension': 0.1,
                'pointRadius': 2,
                'pointHoverRadius': 4,
                'binancePrices': binance_prices  # 添加对应时间点的币安价格
            }
            chart_data['datasets'].append(dataset)
        
        return jsonify({
            'success': True,
            'data': chart_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取图表数据失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    logger.info("启动Web服务器")
    app.run(host='0.0.0.0', port=8080, debug=True)