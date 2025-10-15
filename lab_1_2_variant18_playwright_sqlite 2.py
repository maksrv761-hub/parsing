# lab_1_2_variant18_playwright_sqlite.py
# !pip install playwright pandas matplotlib
# !playwright install

import sqlite3, re
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
# from playwright.sync_api import sync_playwright   # раскомментировать при реальном запуске

DB = Path('lab_variant18.db')
OUT = Path('lab_variant18_outputs'); OUT.mkdir(exist_ok=True)

XPATHS = {
    'title': "//h1[contains(@class,'product__title') or contains(@class,'product-title')]",
    'price': "//span[contains(@class,'price') and (contains(@class,'__current') or contains(@class,'-current'))]",
    'availability': "//*[contains(translate(.,'ВНАЛИЧИЕТ','вналичиет'),'в наличии') or contains(.,'нет в наличии')]",
    'format': "//*[contains(.,'твёрдая') or contains(.,'твердая') or contains(.,'мягкая')]"
}

def normalize_price(s):
    if not isinstance(s,str): return None
    s = re.sub(r'[^0-9.,]','', s).replace(',', '.')
    try: return float(s)
    except: return None

# def scrape(url: str):
#     with sync_playwright() as p:
#         br = p.chromium.launch(headless=True)
#         pg = br.new_page()
#         pg.goto(url, wait_until='domcontentloaded', timeout=60000)
#         pg.wait_for_timeout(1500)
#         def txt(xp):
#             try: return pg.locator(f'xpath={xp}').first.inner_text().strip()
#             except: return ''
#         data = {k: txt(xp) for k,xp in XPATHS.items()}
#         data['price'] = normalize_price(data.get('price'))
#         br.close()
#         return data

# Демоданные (если парсинг не запущен)
records = [
    {'title':'Книга A','price':599.0,'availability':'В наличии','format':'твёрдая'},
    {'title':'Книга B','price':449.0,'availability':'Нет в наличии','format':'мягкая'},
    {'title':'Книга C','price':899.0,'availability':'В наличии','format':'твёрдая'},
    {'title':'Книга D','price':399.0,'availability':'В наличии','format':'мягкая'},
    {'title':'Книга E','price':999.0,'availability':'В наличии','format':'твёрдая'},
]

conn = sqlite3.connect(DB)
pd.DataFrame(records).to_sql('books', conn, if_exists='replace', index=False)
# Примеры SQL
df_top = pd.read_sql_query("SELECT title, price FROM books ORDER BY price DESC LIMIT 5;", conn)
df_fmt = pd.read_sql_query("SELECT format, COUNT(*) as cnt FROM books GROUP BY format;", conn)
df_av  = pd.read_sql_query("SELECT availability, COUNT(*) as cnt FROM books GROUP BY availability;", conn)
conn.close()

# Визуализации
plt.figure(figsize=(6,4))
plt.barh(df_top['title'], df_top['price'])
plt.gca().invert_yaxis()
plt.title('Топ-5 по цене')
plt.xlabel('Цена')
plt.tight_layout()
plt.savefig(OUT/'top5_price.png', dpi=200)
plt.close()

plt.figure(figsize=(5,4))
plt.bar(df_fmt['format'], df_fmt['cnt'])
plt.title('Распределение по формату')
plt.ylabel('Кол-во')
plt.tight_layout()
plt.savefig(OUT/'formats_count.png', dpi=200)
plt.close()

plt.figure(figsize=(5,5))
plt.pie(df_av['cnt'], labels=df_av['availability'], autopct='%1.1f%%')
plt.title('Наличие')
plt.tight_layout()
plt.savefig(OUT/'availability_share.png', dpi=200)
plt.close()
