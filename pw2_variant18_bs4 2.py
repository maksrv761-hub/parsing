# pw2_variant18_bs4.py
import requests, time, re
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from pathlib import Path

BASE_URL = 'https://www.sports.ru/football/'
HEADERS = {'User-Agent':'Mozilla/5.0'}
OUT = Path('pw2_variant18_outputs'); OUT.mkdir(exist_ok=True)

def fetch_page(page=1):
    url = BASE_URL if page==1 else f'{BASE_URL}news/page{page}.html'
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.text

def parse_list(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = []
    for block in soup.select('.news-item, .news, .material-item'):
        a = block.select_one('a[href]')
        if not a: continue
        title = a.get_text(strip=True)
        href = a.get('href') or ''
        if href.startswith('/'):
            href = 'https://www.sports.ru' + href
        comments = 0
        c = block.find(attrs={'class': re.compile('comments|comment|icon_comments')})
        if c:
            m = re.search(r'(\\d+)', c.get_text(' ', strip=True))
            if m: comments = int(m.group(1))
        items.append({'title':title,'url':href,'comments':comments})
    return items

def crawl(max_pages=5, delay=0.5):
    out = []
    for p in range(1, max_pages+1):
        try:
            html = fetch_page(p); out += parse_list(html)
        except Exception as e:
            print("Ошибка страницы", p, e)
        time.sleep(delay)
    df = pd.DataFrame(out)
    if not df.empty: df['comments'] = pd.to_numeric(df['comments'], errors='coerce').fillna(0).astype(int)
    return df

try:
    df = crawl(5)
except Exception as e:
    print("Сеть недоступна:", e)
    df = pd.DataFrame({'title':[f'Новость {i}' for i in range(1,31)],
                       'url':['']*30,
                       'comments': [*range(200,185,-1), *range(120,105,-1), *range(60,45,-1)]})

# Топ-15 обсуждаемых
top = df.sort_values('comments', ascending=False).head(15)
plt.figure(figsize=(10,6))
plt.barh(range(len(top)), top['comments'])
plt.yticks(range(len(top)), top['title'].str.slice(0,60))
plt.gca().invert_yaxis()
plt.title('Топ-15 обсуждаемых новостей')
plt.xlabel('Комментарии')
plt.tight_layout()
plt.savefig(OUT/'top15_comments.png', dpi=200)
plt.close()

# Грубая тематическая группировка
TOPICS = {'Zenit':['зенит'],'Spartak':['спартак'],'CSKA':['цска'],'Real Madrid':['реал'],'Barcelona':['барселона']}
def detect(t):
    t = (t or '').lower()
    for topic,keys in TOPICS.items():
        if any(k in t for k in keys): return topic
    return 'Other'

df['topic'] = df['title'].apply(detect)
agg = df.groupby('topic', as_index=False)['comments'].sum().sort_values('comments', ascending=False)
plt.figure(figsize=(8,5))
plt.bar(agg['topic'], agg['comments'])
plt.title('Сумма комментариев по темам')
plt.ylabel('Комментарии')
plt.tight_layout()
plt.savefig(OUT/'comments_by_topic.png', dpi=200)
plt.close()
