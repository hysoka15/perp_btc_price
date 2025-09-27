#!/usr/bin/env python3
"""
简单测试图表功能
"""

import asyncio
from playwright.async_api import async_playwright

async def simple_test():
    async with async_playwright() as p:
        print("启动浏览器...")
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        try:
            await page.goto('http://localhost:8080')
            print("页面加载成功")
            
            # 等待图表加载
            await page.wait_for_selector('#priceChart', timeout=10000)
            await asyncio.sleep(3)
            
            # 检查图表是否初始化
            chart_exists = await page.evaluate("() => !!window.priceChart")
            print(f"图表对象存在: {chart_exists}")
            
            if chart_exists:
                # 获取图表基本信息
                chart_info = await page.evaluate("""
                    () => {
                        const chart = window.priceChart;
                        return {
                            datasets: chart.data.datasets.length,
                            labels: chart.data.labels.length,
                            hasData: chart.data.datasets.some(d => d.data && d.data.length > 0)
                        };
                    }
                """)
                print(f"图表信息: {chart_info}")
                
                # 等待一次自动刷新
                print("等待5秒自动刷新...")
                await asyncio.sleep(6)
                
                # 再次检查图表信息
                chart_info_after = await page.evaluate("""
                    () => {
                        const chart = window.priceChart;
                        return {
                            datasets: chart.data.datasets.length,
                            labels: chart.data.labels.length,
                            hasData: chart.data.datasets.some(d => d.data && d.data.length > 0)
                        };
                    }
                """)
                print(f"刷新后图表信息: {chart_info_after}")
                
                # 检查控制台错误
                errors = await page.evaluate("""
                    () => {
                        // 获取任何控制台错误
                        return window.console_errors || [];
                    }
                """)
                if errors:
                    print(f"控制台错误: {errors}")
                else:
                    print("✅ 无控制台错误")
            
            print("等待用户观察... (10秒后自动关闭)")
            await asyncio.sleep(10)
                
        except Exception as e:
            print(f"测试出错: {e}")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(simple_test())