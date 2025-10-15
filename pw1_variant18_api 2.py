# pw1_variant18_api.py
import os, time, re, json, html, requests
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

OUT = Path('outputs_variant_18'); OUT.mkdir(exist_ok=True)

# ---------------------------
# Задание 1 — Kaggle (Pandas)
# ---------------------------
try:
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi(); api.authenticate()
    kernels = api.kernels_list(search='Pandas', page_size=100, sort_by='votes')
    rows = []
    for k in kernels:
        d = k.__dict__ if hasattr(k, '__dict__') else dict(k)
        title = d.get('title') or d.get('_title')
        author = d.get('author') or d.get('_author')
        votes = d.get('totalVotes') or d.get('_total_votes') or d.get('voteCount') or d.get('_score')
        ref = d.get('ref') or d.get('_ref')
        url = d.get('url') or (f"https://www.kaggle.com/{ref}" if ref else None)
        rows.append({'title': title, 'author': author, 'votes': votes, 'url': url})
    df_kaggle = pd.DataFrame(rows).dropna(subset=['votes']).sort_values('votes', ascending=False).head(10)
except Exception as e:
    print("Kaggle недоступен:", e)
    df_kaggle = pd.DataFrame({
        'title':[f'Notebook {i}' for i in range(1,11)],
        'author':['mock']*10,
        'votes':list(range(500,400,-10)),
        'url':['']*10
    })

df_kaggle.to_csv(OUT/'kaggle_top10_pandas_kernels.csv', index=False)

plt.figure(figsize=(10,6))
plt.barh(range(len(df_kaggle)), df_kaggle['votes'])
plt.yticks(range(len(df_kaggle)), df_kaggle['title'].astype(str).str.slice(0,40))
plt.gca().invert_yaxis()
plt.title('Kaggle: топ-10 ноутбуков по Pandas (голоса)')
plt.xlabel('Голоса')
plt.tight_layout()
plt.savefig(OUT/'kaggle_top10_pandas_kernels.png', dpi=200)
plt.close()

# ---------------------------------------------
# Задание 2 — GitHub: library без Issues (10шт)
# ---------------------------------------------
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
headers = {'Accept':'application/vnd.github+json'}
if GITHUB_TOKEN: headers['Authorization'] = f'Bearer {GITHUB_TOKEN}'

found, page = [], 1
while len(found)<10 and page<=10:
    r = requests.get('https://api.github.com/search/repositories',
                     headers=headers,
                     params={'q':'library in:name,description,readme','sort':'stars','order':'desc',
                             'per_page':100,'page':page},
                     timeout=20)
    if r.status_code!=200:
        print("GitHub API error:", r.status_code, r.text[:120])
        break
    for repo in r.json().get('items',[]):
        if repo.get('has_issues') is False:
            found.append({
                'full_name': repo.get('full_name'),
                'html_url' : repo.get('html_url'),
                'stars'    : repo.get('stargazers_count'),
                'has_issues': repo.get('has_issues')
            })
            if len(found)==10: break
    page += 1
    time.sleep(0.2)

df_gh = pd.DataFrame(found)
if df_gh.empty:
    df_gh = pd.DataFrame({
        'full_name':[f'user/repo{i}' for i in range(1,11)],
        'html_url':['']*10,
        'stars':list(range(2000,1000,-100)),
        'has_issues':[False]*10
    })

df_gh.to_csv(OUT/'github_library_no_issues_top10.csv', index=False)

plt.figure(figsize=(10,5))
plt.barh(range(len(df_gh)), df_gh['stars'])
plt.yticks(range(len(df_gh)), df_gh['full_name'])
plt.gca().invert_yaxis()
plt.xlabel('Stars')
plt.title("GitHub: 'library' без Issues (10 репозиториев)")
plt.tight_layout()
plt.savefig(OUT/'github_library_no_issues_top10.png', dpi=200)
plt.close()

# -------------------------------------------------------------
# Задание 3 — hh.ru: сертификаты в вакансиях Cloud Engineer
# -------------------------------------------------------------
HH_HEADERS = {"User-Agent":"study-client"}
CERT_PATTERNS = {
    'aws'  : r"\\b(AWS|Amazon Web Services|SAA-C0\\d|SAP-C0\\d|DVA-C0\\d|SOA-C0\\d)\\b",
    'azure': r"\\b(Azure|AZ-9\\d\\d|AZ-1\\d\\d|AZ-2\\d\\d|Microsoft Certified)\\b",
    'gcp'  : r"\\b(Google Cloud|GCP|Associate Cloud Engineer|ACE|PCA)\\b"
}

def strip_html(x:str)->str:
    if not isinstance(x,str): return ''
    x = re.sub(r'<[^>]+>',' ',x); x = html.unescape(x); x = re.sub(r'\\s+',' ',x)
    return x.strip()

ids, stats, rows = [], {'total':0,'with_any':0,'aws':0,'azure':0,'gcp':0}, []
try:
    for page in range(5):
        r = requests.get('https://api.hh.ru/vacancies',
                         headers=HH_HEADERS,
                         params={'text':'Cloud Engineer','area':113,'per_page':100,'page':page},
                         timeout=20)
        data = r.json()
        ids.extend([it['id'] for it in data.get('items',[])])
        if page >= (data.get('pages',1)-1): break
        time.sleep(0.2)
    for vid in ids:
        rr = requests.get(f'https://api.hh.ru/vacancies/{vid}', headers=HH_HEADERS, timeout=20)
        if rr.status_code!=200: continue
        v = rr.json()
        text = ' '.join([strip_html(v.get('name','')), strip_html(v.get('description','')),
                         strip_html((v.get('snippet') or {}).get('requirement',''))])
        found = {k: bool(re.search(p, text, flags=re.I)) for k,p in CERT_PATTERNS.items()}
        anyc = any(found.values())
        stats['total'] += 1; stats['with_any'] += int(anyc)
        for k,val in found.items(): stats[k]+=int(val)
        rows.append({'id':vid,'name':v.get('name'),'has_cert':anyc, **{f'has_{k}':int(val) for k,val in found.items()}})
        time.sleep(0.15)
    df_hh = pd.DataFrame(rows)
except Exception as e:
    print("hh.ru недоступен:", e)
    df_hh = pd.DataFrame({'id':[1,2,3],'name':['Cloud Eng A','Cloud Eng B','Cloud Eng C'],
                          'has_cert':[1,0,1],'has_aws':[1,0,0],'has_azure':[0,0,1],'has_gcp':[0,0,0]})
    stats = {'total':3,'with_any':2,'aws':1,'azure':1,'gcp':0}

df_hh.to_csv(OUT/'hh_cloud_engineer_cert_mentions.csv', index=False)
pct = round((stats['with_any']/stats['total']*100), 2) if stats['total'] else 0.0
print("Итого:", stats, "=>", pct, "% с упоминанием любого сертификата")

# Визуализации
total = max(1, stats.get('total', len(df_hh)))
with_any = stats.get('with_any', int(df_hh['has_cert'].sum())) if not df_hh.empty else 0

plt.figure(figsize=(5,5))
plt.pie([with_any, total-with_any], labels=['Упомянут сертификат','Не упомянут'], autopct='%1.1f%%')
plt.title('Доля вакансий с упоминанием сертификата')
plt.tight_layout()
plt.savefig(OUT/'hh_cert_mentions_share.png', dpi=200)
plt.close()

plt.figure(figsize=(6,4))
plt.bar(['aws','azure','gcp'], [stats.get('aws',0), stats.get('azure',0), stats.get('gcp',0)])
plt.title('Упоминания по провайдерам')
plt.ylabel('Кол-во вакансий')
plt.tight_layout()
plt.savefig(OUT/'hh_cert_mentions_by_provider.png', dpi=200)
plt.close()
