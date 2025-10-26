# -*- coding: utf-8 -*-
"""
scrape_icook_keywords_to_firestore.py
- 針對常見食材關鍵字，在 iCook 搜尋頁抓取前 N 筆食譜
- 解析 JSON-LD 取得：標題、主圖、食材、步驟(純文字)、時間、幾人份、連結
- 儲存本地 JSON、累積歷史、並嘗試寫入 Firestore（優先從 GitHub Secret 讀金鑰）
"""

import os, re, json, time, random
from datetime import datetime
from typing import List, Tuple, Optional, Any
from urllib.parse import urljoin, quote

# ---------- 設定 ----------
LOG_FILE = "crawler.log"
SAMPLE_FILE = "icook_keywords_sample.json"
HISTORY_FILE = "icook_keywords_history.json"
LIMIT_PER_ING = 20    # 每個關鍵字抓幾筆
RATE_DELAY = 1.2      # 基本等待（秒）

COMMON_INGREDIENTS = [
    "雞肉","豬肉","牛肉","羊肉",
    "魚","蝦","蛤蜊","花枝",
    "高麗菜","青江菜","菠菜","空心菜",
    "洋蔥","青椒","胡蘿蔔","番茄",
    "香菇","杏鮑菇","金針菇",
    "雞蛋","豆腐","白飯","麵條"
]

# ---------- Log ----------
def log(msg: str):
    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(text)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except Exception:
        # 若寫 log 失敗也不要中斷
        pass

# ---------- Firestore 初始化：先從環境變數 SERVICE_ACCOUNT_KEY（GitHub Secret）讀，再 fallback 本地檔 ----------
db = None
key_json = os.environ.get("SERVICE_ACCOUNT_KEY")
if key_json:
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
        creds_dict = json.loads(key_json)
        if not firebase_admin._apps:
            cred = credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        log("✅ 已連線 Firestore (from SERVICE_ACCOUNT_KEY)")
    except Exception as e:
        log(f"⚠️ Firestore 初始化 (from secret) 失敗：{e}")
else:
    KEY_FILE = "serviceAccountKey.json"
    if os.path.exists(KEY_FILE):
        try:
            import firebase_admin
            from firebase_admin import credentials, firestore
            if not firebase_admin._apps:
                cred = credentials.Certificate(KEY_FILE)
                firebase_admin.initialize_app(cred)
            db = firestore.client()
            log("✅ 已連線 Firestore (from local file)")
        except Exception as e:
            log(f"⚠️ Firestore 初始化 (from local file) 失敗：{e}")
    else:
        log("ℹ️ 未找到 SERVICE_ACCOUNT_KEY 與本地金鑰，僅輸出到本地 JSON")

# ---------- Playwright (會在 runtime import) ----------
from playwright.sync_api import sync_playwright

# ---------- 工具函式 ----------
def upsert_firestore(doc: dict):
    assert "link" in doc
    doc_id = doc["link"].replace("https://", "").replace("http://", "").replace("/", "_")
    db.collection("recipes").document(doc_id).set(doc)
    return doc_id

def iso8601_duration_to_text(s: str) -> Optional[str]:
    if not s: return None
    if not s.startswith("PT"): return s
    hours = minutes = 0
    m_h = re.search(r"PT(\d+)H", s)
    m_m = re.search(r"(\d+)M", s)
    if m_h: hours = int(m_h.group(1))
    if m_m: minutes = int(m_m.group(1))
    if hours == 0 and minutes == 0: return None
    if hours and minutes: return f"{hours} 小時 {minutes} 分鐘"
    if hours: return f"{hours} 小時"
    return f"{minutes} 分鐘"

def ensure_str(x: Any) -> Optional[str]:
    return str(x).strip() if x is not None else None

def extract_image_url_from_ld(obj: Any) -> Optional[str]:
    if not obj: return None
    if isinstance(obj, str): return obj
    if isinstance(obj, list) and obj:
        if isinstance(obj[0], str): return obj[0]
        if isinstance(obj[0], dict): return ensure_str(obj[0].get("url"))
    if isinstance(obj, dict): return ensure_str(obj.get("url"))
    return None

def extract_steps_from_ld(obj: Any) -> List[str]:
    steps: List[str] = []
    def pick_from_list(items: List[Any]):
        for it in items:
            if isinstance(it, str):
                t = it.strip()
                if t: steps.append(t)
            elif isinstance(it, dict):
                t = it.get("text") or it.get("name")
                if t and str(t).strip():
                    steps.append(str(t).strip())
    if isinstance(obj, list): pick_from_list(obj)
    elif isinstance(obj, dict):
        if obj.get("@type") == "HowToSection" and isinstance(obj.get("itemListElement"), list):
            pick_from_list(obj["itemListElement"])
        else:
            t = obj.get("text") or obj.get("name")
            if t and str(t).strip():
                steps.append(str(t).strip())
    return [s for s in (s.strip() for s in steps) if s]

def parse_ld_json(ld_texts: List[str]) -> Tuple[Optional[str], List[str], Optional[str], Optional[str], List[str], Optional[str]]:
    title = None
    ingredients: List[str] = []
    time_text = None
    yield_info = None
    steps: List[str] = []
    image_url = None

    for raw in ld_texts:
        try:
            data = json.loads(raw)
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            nodes: List[dict] = []
            if isinstance(obj, dict):
                if "@graph" in obj and isinstance(obj["@graph"], list):
                    nodes.extend([g for g in obj["@graph"] if isinstance(g, dict)])
                else:
                    nodes.append(obj)
            elif isinstance(obj, list):
                nodes.extend([x for x in obj if isinstance(x, dict)])
            else:
                continue

            for node in nodes:
                types = node.get("@type")
                is_recipe = (types == "Recipe") or (isinstance(types, list) and "Recipe" in types)
                if not is_recipe:
                    continue

                if not title:
                    title = ensure_str(node.get("name"))

                if not ingredients:
                    ing = node.get("recipeIngredient")
                    if isinstance(ing, list):
                        ingredients = [str(x).strip() for x in ing if str(x).strip()]

                if not time_text:
                    t = ensure_str(node.get("totalTime")) or ensure_str(node.get("cookTime"))
                    if t:
                        time_text = iso8601_duration_to_text(t) or t

                if not yield_info:
                    y = node.get("recipeYield")
                    if isinstance(y, list) and y:
                        yield_info = ensure_str(y[0])
                    else:
                        yield_info = ensure_str(y)

                if not steps:
                    inst = node.get("recipeInstructions")
                    st = extract_steps_from_ld(inst)
                    if st:
                        steps = st

                if not image_url:
                    image_url = extract_image_url_from_ld(node.get("image"))

    return title, ingredients, time_text, yield_info, steps, image_url

# ---------- 抓單一關鍵字的邏輯 ----------
def scrape_keyword(keyword: str, page) -> List[dict]:
    search_url = f"https://icook.tw/recipes/search?q={quote(keyword)}"
    log(f"🔎 搜尋關鍵字：{keyword} -> {search_url}")
    page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1500)

    anchors = page.locator("a[href^='/recipes/']").all()
    links = []
    for a in anchors:
        href = a.get_attribute("href")
        if href and re.fullmatch(r"/recipes/\d+", href):
            links.append(urljoin("https://icook.tw", href))
    links = list(dict.fromkeys(links))[:LIMIT_PER_ING]
    log(f"👉 找到 {len(links)} 筆")

    saved: List[dict] = []
    for i, url in enumerate(links, 1):
        log(f"  [{i}/{len(links)}] 抓取：{url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(800)
            scripts = page.locator("script[type='application/ld+json']").all()
        except Exception as e:
            log(f"  ⚠️ 開啟頁面失敗：{e}")
            continue

        ld_texts = []
        for s in scripts:
            try:
                txt = s.inner_text()
                if txt and "Recipe" in txt: ld_texts.append(txt)
            except Exception:
                pass

        title, ingredients, time_text, yield_info, steps, image_url = parse_ld_json(ld_texts)

        # fallback: title / image / steps / ingredients
        if not title:
            try:
                title = page.locator("h1").first.inner_text().strip()
            except Exception:
                title = "(未找到標題)"
        if not image_url:
            try:
                og = page.locator("meta[property='og:image']").first.get_attribute("content")
                if og and og.startswith("http"):
                    image_url = og
            except Exception:
                pass
        if not steps:
            try:
                candidates = page.locator("li[class*=step], .step, [class*=instruction] li").all()
                tmp = [(c.inner_text() or "").strip() for c in candidates if (c.inner_text() or "").strip()]
                if tmp:
                    steps = tmp
            except Exception:
                pass
        if not ingredients:
            try:
                items = page.locator("li[class*='ingredient'], .ingredients li, [data-ingredient-name]").all()
                tmp = [(it.inner_text() or "").strip() for it in items if (it.inner_text() or "").strip()]
                if tmp:
                    ingredients = tmp
            except Exception:
                pass

        doc = {
            "title": title or "",
            "ingredients": ingredients,
            "steps": steps,
            "time": time_text,
            "yield": yield_info,
            "link": url,
            "imageUrl": image_url,
            "source": "icook"
        }

        if ingredients:
            if db:
                try:
                    doc_id = upsert_firestore(doc)
                    log(f"📝 已寫入 Firestore：recipes/{doc_id}")
                except Exception as e:
                    log(f"⚠️ 寫入 Firestore 失敗：{e}")
            saved.append(doc)
        else:
            log("  ⚠️ 此頁未抓到食材，略過")

        # polite delay + small random jitter between requests
        page.wait_for_timeout(int(RATE_DELAY * 1000 + random.uniform(200, 800)))
    return saved

# ---------- 主程式 ----------
def main():
    # start jitter：避免所有排程完全同時發起
    start_jitter = random.uniform(5, 45)  # seconds
    log(f"⏱ 起始隨機延遲 {start_jitter:.1f} 秒以降低被偵測風險")
    time.sleep(start_jitter)

    log("🚀 爬蟲開始執行")
    all_saved: List[dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="zh-TW",
        )
        page = ctx.new_page()

        # 可進一步把 COMMON_INGREDIENTS 隨機打散，減少固定行為特徵
        kws = COMMON_INGREDIENTS.copy()
        random.shuffle(kws)
        for kw in kws:
            docs = scrape_keyword(kw, page)
            all_saved.extend(docs)
            # 每抓完一個 keyword，加個較長的隨機等待，降低頻率特徵
            sec = random.uniform(2.0, 6.0)
            log(f"  ✋ keyword 間隙等待 {sec:.1f} 秒")
            time.sleep(sec)

        ctx.close()
        browser.close()

    # 輸出 sample json（覆蓋）
    try:
        with open(SAMPLE_FILE, "w", encoding="utf-8") as f:
            json.dump(all_saved, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"⚠️ 寫入 {SAMPLE_FILE} 失敗：{e}")

    # 累積寫歷史
    history = []
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = []
    history.extend(all_saved)
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"⚠️ 寫入 {HISTORY_FILE} 失敗：{e}")

    log(f"✅ 完成！本次處理 {len(all_saved)} 筆；已輸出 {SAMPLE_FILE} 並累積到 {HISTORY_FILE}")

    # Firestore 統計 & 紀錄
    if db:
        try:
            docs = db.collection("recipes").stream()
            total = sum(1 for _ in docs)
            log(f"📊 Firestore 目前 recipes 總筆數：{total}")
            # 新增 crawler_logs 紀錄
            log_doc = {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "status": "success",
                "count": len(all_saved),
                "recipes_total": total
            }
            db.collection("crawler_logs").add(log_doc)
            log("📝 已寫入 crawler_logs")
        except Exception as e:
            log(f"⚠️ Firestore 統計或紀錄失敗：{e}")

    log("🏁 爬蟲結束")

if __name__ == "__main__":
    main()

