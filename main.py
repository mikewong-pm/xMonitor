import os
import json
import time
import sqlite3
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import requests
import asyncio
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Bot
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest   # 解决 PoolTimeout

load_dotenv()

# ================== 配置 ==================
LUNARCRUSH_TOKEN = os.getenv("LUNARCRUSH_TOKEN")
GROK_API_KEY = os.getenv("GROK_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

# ================== Telegram Bot（大连接池）==================
request = HTTPXRequest(
    connection_pool_size=32,
    read_timeout=30,
    write_timeout=30,
    connect_timeout=30
)
bot = Bot(token=TELEGRAM_TOKEN, request=request)

# ================== SQLite 跨线程安全 ==================
def get_db_connection():
    conn = sqlite3.connect('history.db', check_same_thread=False)
    conn.execute('''CREATE TABLE IF NOT EXISTS stats 
                    (timestamp TEXT PRIMARY KEY, volume INTEGER, engagement INTEGER)''')
    return conn

def get_last_stats():
    conn = get_db_connection()
    row = conn.execute("SELECT volume, engagement FROM stats ORDER BY timestamp DESC LIMIT 1").fetchone()
    conn.close()
    return row if row else (0, 0)

def save_stats(volume, engagement):
    conn = get_db_connection()
    ts = datetime.now(timezone.utc).isoformat()
    conn.execute("INSERT INTO stats VALUES (?, ?, ?)", (ts, volume, engagement))
    conn.commit()
    conn.close()

async def send_telegram_alert(alert_text):
    for attempt in range(3):
        try:
            await bot.send_message(chat_id=CHAT_ID, text=alert_text, parse_mode=ParseMode.MARKDOWN)
            print("✅ 警报已成功推送到Telegram群组！")
            return
        except Exception as e:
            print(f"❌ 发送失败（尝试 {attempt+1}/3）：{e}")
            await asyncio.sleep(2 ** attempt)
    print("❌ 3次重试后仍失败")

# ================== LunarCrush 函数 ==================
def fetch_lunarcrush_time_series(topic="crypto"):
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=6)
    params = {"interval": "hourly", "limit": 6, "start": start.isoformat(), "end": end.isoformat()}
    headers = {"Authorization": f"Bearer {LUNARCRUSH_TOKEN}"}
    resp = requests.get(f"https://lunarcrush.com/api4/public/topic/{topic}/time-series/v2", headers=headers, params=params)
    if resp.status_code != 200:
        print(f"LunarCrush 时间序列错误 {topic}: {resp.text}")
        return []
    return resp.json().get("data", [])

def fetch_lunarcrush_posts(topic="crypto", limit=8):
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=1)
    params = {"limit": limit, "sort": "interactions_desc", "start": start.isoformat(), "end": end.isoformat()}
    headers = {"Authorization": f"Bearer {LUNARCRUSH_TOKEN}"}
    resp = requests.get(f"https://lunarcrush.com/api4/public/topic/{topic}/posts/v1", headers=headers, params=params)
    return resp.json().get("data", []) if resp.status_code == 200 else []

def fetch_lunarcrush_creators(topic="crypto", limit=3):
    headers = {"Authorization": f"Bearer {LUNARCRUSH_TOKEN}"}
    resp = requests.get(f"https://lunarcrush.com/api4/public/topic/{topic}/creators/v1", headers=headers, params={"limit": limit, "period": "day"})
    return resp.json().get("data", []) if resp.status_code == 200 else []

# ================== 核心监控函数 ==================
def run_monitor():
    print(f"[{datetime.now()}] LunarCrush 多话题扫描开始...（已扩展9大类金融话题 + 测试模式）")
    
    TOPICS = ["crypto", "bitcoin", "ethereum", "stocks", "equities", "market", "macro", "economy", "fed", "geopolitics", "politics", "institutions", "rwa", "commodities", "energy", "oil", "forex", "bonds", "treasury"]
    
    total_volume = 0
    all_posts = []
    all_creators = []
    
    for topic in TOPICS:
        try:
            time_data = fetch_lunarcrush_time_series(topic)
            if time_data:
                current = time_data[-1].get("social_volume", 0)
                total_volume += current
            posts = fetch_lunarcrush_posts(topic, 8)
            all_posts.extend(posts)
            creators = fetch_lunarcrush_creators(topic, 3)
            all_creators.extend(creators)
        except Exception as e:
            print(f"  └─ 话题 {topic} 跳过: {e}")
            continue
    
    all_posts = sorted(all_posts, key=lambda x: x.get("interactions", 0), reverse=True)[:20]
    all_creators = sorted(all_creators, key=lambda x: x.get("influence_score", 0), reverse=True)[:8]
    
    current_volume = total_volume
    last_volume, _ = get_last_stats()
    growth = ((current_volume - last_volume) / last_volume * 100) if last_volume > 0 else 999
    total_engagement = sum(p.get("interactions", 0) for p in all_posts)
    
    save_stats(current_volume, total_engagement)
    
    if growth >= 50 or total_engagement >= 1000 or current_volume >= last_volume * 1.5:
        print("🚨【测试模式】检测到热点！生成警报...")
        
        grok_prompt = f"""你是一个专业的金融&Crypto热点监控AI。请严格按照以下格式输出（只输出Markdown内容，不要多余解释）：

【🚨 金融/Crypto 热点爆发警报】
**话题**：一句话总结核心事件
**热度指标**：过去1小时讨论量增长{growth:.1f}%，当前相关推文量约{len(all_posts)}条
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

请基于以下真实数据生成（已覆盖 antseer.ai 全部9大类金融话题）：
推文数据：{json.dumps([{"author": p.get("author", {}).get("screen_name", "unknown"), "text": p.get("text", ""), "interactions": p.get("interactions", 0), "link": f"https://x.com/{p.get('author', {}).get('screen_name','')}/status/{p.get('post_id','')}"} for p in all_posts[:15]], ensure_ascii=False, indent=2)}
KOL数据：{json.dumps([{"screen_name": c.get("screen_name", ""), "followers": c.get("followers", 0), "influence": c.get("influence_score", 0)} for c in all_creators], ensure_ascii=False)}
"""

        grok_resp = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "grok-beta",          # ← 已修正为当前有效模型
                "messages": [{"role": "user", "content": grok_prompt}],
                "temperature": 0.3
            },
            timeout=30
        )
        
        if grok_resp.status_code == 200:
            alert_text = grok_resp.json()["choices"][0]["message"]["content"]
            asyncio.run(send_telegram_alert(alert_text))
        else:
            print(f"Grok API错误: {grok_resp.text}")
    else:
        print(f"  当前总热度增长 {growth:.1f}%（未达测试阈值，继续监控）")

# ================== 启动 ==================
scheduler = BackgroundScheduler()
scheduler.add_job(run_monitor, 'interval', minutes=15)
scheduler.start()

print("🚀 LunarCrush 24/7 监控系统已启动！每15分钟自动扫描...")

# 启动测试推送
print("🔧 启动测试：立即尝试推送测试警报...")
try:
    test_alert = f"""【🚨 测试警报 - 系统启动成功】
**话题**：Railway部署测试
**热度指标**：测试推送
**触发时间**：{datetime.now().strftime('%Y-%m-%d %H:%M')}（北京时间）

✅ 你的24/7监控机器人已成功上线！
本消息为启动测试推送。"""
    asyncio.run(send_telegram_alert(test_alert))
except Exception as e:
    print(f"❌ 启动测试推送失败：{e}")

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    print("系统已停止")
