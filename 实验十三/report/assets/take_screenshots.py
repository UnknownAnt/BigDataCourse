"""
自动化截图脚本：使用 Playwright 对实验十三看板进行截图
需要先启动服务器: uvicorn server:app --port 8000
"""
import asyncio
import io
import os
import sys
from pathlib import Path

# Fix Windows console encoding
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 确保输出目录存在
OUTPUT_DIR = Path(__file__).parent
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

async def take_screenshots():
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = await browser.new_context(
            viewport={'width': 1440, 'height': 900},
            device_scale_factor=2  # 2x 高清截图
        )
        page = await context.new_page()

        try:
            # ================================================
            # 截图 1: 完整看板初始视图（全量数据）
            # ================================================
            print("[1/5] 加载看板初始页面...")
            await page.goto('http://127.0.0.1:8000', wait_until='commit', timeout=30000)
            # 等待 ECharts 图表渲染完成（给足时间加载 CDN 资源 + 图表动画）
            await page.wait_for_timeout(10000)

            # 滚动页面确保所有内容可见，然后截全页
            full_page = await page.screenshot(full_page=True)
            OUTPUT_DIR.joinpath('dashboard_full_initial.png').write_bytes(full_page)
            print("   ✓ 初始全貌截图已保存")

            # ================================================
            # 截图 2: 双向联动 —— 设置 sentiment="负面"
            # ================================================
            print("[2/5] 设置 sentiment='负面'，触发双向联动...")
            await page.evaluate("""
                dashboardState.sentiment = '负面';
                dashboardState.category = null;
                dashboardState.drilldownActive = false;
                dashboardState.currentDrillCat = null;
                refreshAllCharts();
            """)
            # 等待 API 请求完成 + 图表重绘
            await page.wait_for_timeout(5000)

            full_page = await page.screenshot(full_page=True)
            OUTPUT_DIR.joinpath('bidirectional_linkage_negative.png').write_bytes(full_page)
            print("   ✓ 双向联动（负面过滤）截图已保存")

            # ================================================
            # 截图 3: 下钻前 —— 品类分布视图（无过滤）
            # ================================================
            print("[3/5] 恢复全量数据，截取下钻前的品类视图...")
            await page.evaluate("""
                dashboardState.sentiment = '';
                dashboardState.category = null;
                dashboardState.drilldownActive = false;
                dashboardState.currentDrillCat = null;
                dashboardState.searchQuery = '';
                document.getElementById('searchInput').value = '';
                refreshAllCharts();
            """)
            await page.wait_for_timeout(6000)

            # 先截完整页面
            full_page = await page.screenshot(full_page=True)
            OUTPUT_DIR.joinpath('drilldown_before.png').write_bytes(full_page)
            print("   ✓ 下钻前视图截图已保存")

            # ================================================
            # 截图 4: 下钻后 —— 点击"平板"，显示子维度 + 返回按钮
            # ================================================
            print("[4/5] 触发下钻到'平板'品类的子维度视图...")
            await page.evaluate("""
                dashboardState.drilldownActive = true;
                dashboardState.currentDrillCat = '平板';
                dashboardState.category = '平板';
                dashboardState.sentiment = '';
                dashboardState.searchQuery = '';
                document.getElementById('searchInput').value = '';
                lastReviewCat = null;
                refreshAllCharts();
            """)
            await page.wait_for_timeout(5000)
            btn_visible = await page.evaluate(
                "document.getElementById('drillBackBtn').classList.contains('visible')"
            )
            print(f"   返回按钮可见: {btn_visible}")

            full_page = await page.screenshot(full_page=True)
            OUTPUT_DIR.joinpath('drilldown_after.png').write_bytes(full_page)
            print("   ✓ 下钻后（平板子维度）截图已保存")

            # ================================================
            # 截图 5: 词云联动 —— 点击词云中的词后看板刷新
            # ================================================
            print("[5/5] 演示词云联动：点击词语'屏幕'后刷新看板...")
            # 先回到全量数据
            await page.evaluate("""
                dashboardState.drilldownActive = false;
                dashboardState.currentDrillCat = null;
                dashboardState.category = null;
                dashboardState.sentiment = '';
                dashboardState.searchQuery = '屏幕';
                document.getElementById('searchInput').value = '屏幕';
                lastReviewCat = null;
                refreshAllCharts();
            """)
            await page.wait_for_timeout(6000)

            full_page = await page.screenshot(full_page=True)
            OUTPUT_DIR.joinpath('wordcloud_search_filter.png').write_bytes(full_page)
            print("   ✓ 词云联动截图已保存")

            print("\n===== 全部截图完成 =====")
            for f in sorted(OUTPUT_DIR.glob('*.png')):
                size_kb = f.stat().st_size / 1024
                print(f"  {f.name} ({size_kb:.0f} KB)")

        except Exception as e:
            print(f"截图过程出错: {e}")
            # 尝试保存当前页面状态用于调试
            try:
                debug = await page.screenshot(full_page=True)
                OUTPUT_DIR.joinpath('_debug_error.png').write_bytes(debug)
                print("   (已保存错误时的页面截图)")
            except:
                pass
            raise
        finally:
            await browser.close()

if __name__ == '__main__':
    asyncio.run(take_screenshots())
