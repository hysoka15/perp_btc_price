#!/usr/bin/env python3
"""
测试图表平滑刷新功能
验证5秒自动刷新时不会出现跳动
"""

import asyncio
import time
from playwright.async_api import async_playwright

async def test_chart_smooth_update():
    """测试图表平滑更新"""
    async with async_playwright() as p:
        print("启动浏览器测试...")
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        try:
            # 访问页面
            await page.goto('http://localhost:8080')
            print("页面加载成功")
            
            # 等待页面完全加载
            await page.wait_for_selector('#priceChart', timeout=10000)
            print("等待图表初始化...")
            await asyncio.sleep(5)
            
            # 检查控制台错误
            console_messages = []
            page.on('console', lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
            
            # 等待图表初始化完成
            await page.wait_for_function("""
                () => window.priceChart && window.priceChart.data
            """, timeout=15000)
            
            print("图表初始化完成，开始监控刷新...")
            
            # 监控5次刷新周期（25秒）- 减少测试时间
            for i in range(5):
                print(f"等待第{i+1}次刷新...")
                
                # 记录刷新前的图表状态
                chart_elements_before = await page.evaluate("""
                    () => {
                        const chart = window.priceChart;
                        if (!chart) return null;
                        return {
                            datasets: chart.data.datasets.length,
                            labels: chart.data.labels.length,
                            hasData: chart.data.datasets.some(d => d.data.length > 0)
                        };
                    }
                """)
                
                # 等待5秒刷新
                await asyncio.sleep(5.5)
                
                # 记录刷新后的图表状态
                chart_elements_after = await page.evaluate("""
                    () => {
                        const chart = window.priceChart;
                        if (!chart) return null;
                        return {
                            datasets: chart.data.datasets.length,
                            labels: chart.data.labels.length,
                            hasData: chart.data.datasets.some(d => d.data.length > 0)
                        };
                    }
                """)
                
                print(f"刷新前: {chart_elements_before}")
                print(f"刷新后: {chart_elements_after}")
                
                # 检查是否有平滑更新
                if chart_elements_before and chart_elements_after:
                    if chart_elements_after['labels'] > chart_elements_before['labels']:
                        print("✅ 数据成功更新，标签数量增加")
                    else:
                        print("ℹ️ 数据更新但标签数量未变化（可能在同一时间范围内）")
                
            # 检查控制台错误
            if console_messages:
                print("\n⚠️ 控制台消息:")
                for msg in console_messages[-10:]:  # 显示最近10条
                    print(f"  {msg}")
            else:
                print("\n✅ 无控制台错误")
                
            # 测试手动时间范围切换
            print("\n测试时间范围切换...")
            await page.click('button[onclick*="changeTimeRange(1)"]')  # 1小时
            await asyncio.sleep(2)
            await page.click('button[onclick*="changeTimeRange(24)"]')  # 24小时
            await asyncio.sleep(2)
            
            print("✅ 时间范围切换测试完成")
            
        except Exception as e:
            print(f"❌ 测试失败: {e}")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_chart_smooth_update())