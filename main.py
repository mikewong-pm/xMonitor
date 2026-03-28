import os
import json
import time
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Bot
from telegram.constants import ParseMode

load_dotenv()

# ================== 配置 ==================
LUNARCRUSH_TOKEN = os.getenv("LUNARCRUSH_TOKEN")
GROK_API_KEY = os.getenv("GROK_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

BASE_URL = "https://lunarcrush.com/api4/public"

bot = Bot(token=TELEGRAM_TOKEN)

# 初始化历史记录
conn = sqlite3.connect('history.db')
conn.execute('''CREATE TABLE IF NOT EXISTS stats 
                (timestamp TEXT PRIMARY KEY, volume INTEGER, engagement INTEGER)''')
conn.commit()

def get_last_stats():
    row = conn.execute("SELECT volume, engagement FROM stats ORDER BY timestamp DESC LIMIT 1").fetchone()
    return row if row else (0, 0)

def save_stats(volume, engagement):
    ts = datetime.now().isoformat()
    conn.execute("INSERT INTO stats VALUES (?, ?, ?)", (ts, volume, engagement))
    conn.commit()

async def send_telegram_alert(alert_text):
    await bot.send_message(chat_id=CHAT_ID, text=alert_text, parse_mode=ParseMode.MARKDOWN)

def fetch_lunarcrush_time_series(topic="crypto"):
    """获取最近5小时社交量（用于计算增长）"""
    end = datetime.utcnow()
    start = end - timedelta(hours=6)
    params = {
        "interval": "hourly",
        "limit": 6,
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z"
    }
    headers = {"Authorization": f"Bearer {LUNARCRUSH_TOKEN}"}
    resp = requests.get(f"{BASE_URL}/topic/{topic}/time-series/v2", headers=headers, params=params)
    if resp.status_code != 200:
        print(f"LunarCrush 时间序列错误: {resp.text}")
        return []
    return resp.json().get("data", [])  # 假设返回 data 数组

def fetch_lunarcrush_posts(topic="crypto", limit=20):
    """获取最近高互动推文"""
    end = datetime.utcnow()
    start = end - timedelta(hours=1)
    params = {
        "limit": limit,
        "sort": "interactions_desc",
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z"
    }
    headers = {"Authorization": f"Bearer {LUNARCRUSH_TOKEN}"}
    resp = requests.get(f"{BASE_URL}/topic/{topic}/posts/v1", headers=headers, params=params)
    return resp.json().get("data", []) if resp.status_code == 200 else []

def fetch_lunarcrush_creators(topic="crypto", limit=5):
    """获取顶级KOL"""
    headers = {"Authorization": f"Bearer {LUNARCRUSH_TOKEN}"}
    resp = requests.get(f"{BASE_URL}/topic/{topic}/creators/v1", headers=headers, params={"limit": limit, "period": "day"})
    return resp.json().get("data", []) if resp.status_code == 200 else []

def run_monitor():
    print(f"[{datetime.now()}] LunarCrush 扫描开始...")
    
    # 使用主要话题 "crypto"（覆盖金融+Crypto全领域）
    time_data = fetch_lunarcrush_time_series("crypto")
    posts = fetch_lunarcrush_posts("crypto", 15)
    creators = fetch_lunarcrush_creators("crypto", 5)
    
    if not time_data or len(time_data) < 2:
        print("数据不足，跳过本次扫描")
        return
    
    # 热度计算（严格按提示词）
    current_volume = time_data[-1].get("social_volume", 0)  # 最近1小时
    prev_volume = sum(item.get("social_volume", 0) for item in time_data[:-1]) // 4  # 前4小时平均
    growth = ((current_volume - prev_volume) / prev_volume * 100) if prev_volume > 0 else 999
    
    total_engagement = sum(p.get("interactions", 0) for p in posts)
    
    last_volume, _ = get_last_stats()
    save_stats(current_volume, total_engagement)
    
    # 触发条件（任一满足即报警）
    if growth >= 300 or total_engagement >= 5000 or current_volume >= prev_volume * 3:
        print("🚨 检测到热点爆发！生成警报...")
        
        # 构造给 Grok 的提示（完全按你原始格式）
        grok_prompt = f"""你是一个专业的金融&Crypto热点监控AI。请严格按照以下格式输出（只输出Markdown内容，不要多余解释）：

【🚨 金融/Crypto 热点爆发警报】
**话题**：一句话总结核心事件
**热度指标**：过去1小时讨论量增长{growth:.1f}%，当前相关推文量约{len(posts)}条
**触发时间**：{datetime.now().strftime('%Y-%m-%d %H:%M')}（北京时间）

1. 核心推文（Top 3-5条最具代表性）
...

2. 关键KOL（Top 3-4位）
...

3. 热门互动与市场情绪
...

附加信息（可选）
...

立即查看详情 → [X搜索链接]
本警报由Grok热点监控AI自动生成，如需调整阈值请回复指令。

请基于以下真实数据生成（优先原创高互动内容）：
推文数据：{json.dumps([{"author": p.get("author", {}).get("screen_name", "unknown"), "text": p.get("text", ""), "interactions": p.get("interactions", 0), "link": f"https://x.com/{p.get('author', {}).get('screen_name','')}/status/{p.get('post_id','')}"} for p in posts[:12]], ensure_ascii=False, indent=2)}
KOL数据：{json.dumps([{"screen_name": c.get("screen_name", ""), "followers": c.get("followers", 0), "influence": c.get("influence_score", 0)} for c in creators], ensure_ascii=False)}
"""

        # 调用 Grok API 生成警报
        grok_resp = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "grok-4.1-fast-reasoning",
                "messages": [{"role": "user", "content": grok_prompt}],
                "temperature": 0.3
            },
            timeout=30
        )
        
        if grok_resp.status_code == 200:
            alert_text = grok_resp.json()["choices"][0]["message"]["content"]
            import asyncio
            asyncio.run(send_telegram_alert(alert_text))
            print("✅ 警报已成功推送到Telegram！")
        else:
            print(f"Grok API错误: {grok_resp.text}")

# ================== 定时任务 ==================
scheduler = BackgroundScheduler()
scheduler.add_job(run_monitor, 'interval', minutes=15)
scheduler.start()

print("🚀 LunarCrush 24/7 监控系统已启动！每15分钟自动扫描...")
# ================== 启动立即测试推送 + 错误捕获 ==================
print("🔧 启动测试：立即尝试推送测试警报...")
try:
    test_alert = f"""【🚨 测试警报 - 系统启动成功】
**话题**：Railway部署测试
**热度指标**：测试推送
**触发时间**：{datetime.now().strftime('%Y-%m-%d %H:%M')}（北京时间）

✅ 你的24/7监控机器人已成功上线！
如果看到这条消息，说明Telegram推送完全正常，后续只会推送真实热点。

本消息为启动测试推送。"""
    import asyncio
    asyncio.run(send_telegram_alert(test_alert))
    print("✅ 测试警报推送成功！请检查Telegram群组")
except Exception as e:
    print(f"❌ Telegram推送失败，错误信息：{str(e)}")
try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    print("系统已停止")
