#!/usr/bin/env python3
"""cedwyn-graphrag ingest: pull every non-datro Cedwyn webapp repo + the public legal wiki, extract statements,
deduplicate near-identical ones, count distinct supporting sources, and write a small static statement graph.

Deterministic, zero model spend. Streams repos (no clones on disk), reads text files only.
Usage:  python3 tools/ingest.py [--out data] [--repos repos.txt] [--max-files 1500] [--only cedwyn-wiki,...]
Env:    GITHUB_TOKEN (optional; raises API limits in Actions)
Outputs (each kept under 400 KB): data/stats.json, data/statements-top.json, data/graph.json, data/noise.json, data/sources.json
Method notes:
  * document mirrors: a file whose normalised text was already seen (any repo) is skipped and counted as a mirror;
  * statements: sentences of 8-60 words with real content; near-duplicates merged (exact normalised key, then MinHash
    over word 3-shingles, Jaccard >= 0.6);
  * support = number of DISTINCT SOURCES (a source = one webapp repo, the catalogue, or the legal wiki) carrying the
    statement; repetition inside one source counts once; template text repeated across most pages of a source is noise;
  * public-text filter: statements with personal details, contact data or the excluded words are left out.
— Sion's Tab agent"""
import io, os, re, sys, json, time, tarfile, hashlib, zlib, html, argparse, collections, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from html.parser import HTMLParser
try:
    import numpy as np
except Exception:
    np = None
OWNER = 'unclehowell'
TOKEN = os.environ.get('GITHUB_TOKEN', '')
UA = {'User-Agent': 'cedwyn-graphrag'}
TEXT_EXT = ('.md', '.markdown', '.html', '.htm', '.txt', '.json', '.csv', '.yml', '.yaml', '.xml')
SKIP_DIR = re.compile(r'(^|/)(node_modules|vendor|\.git|\.github|dist/assets|assets/(js|css|fonts?)|lib|libs|fonts?|_next/static|three|draco)(/|$)', re.I)
SKIP_FILE = re.compile(r'(\.min\.|package(-lock)?\.json$|manifest\.webmanifest$|tsconfig|\.map$|lock\.json$|sitemap.*\.xml$|feed\.xml$|search[-_]?index|lunr|fuse)', re.I)
BIG_REPO_KB = 30000
MIRROR_SHARE = 0.6   # a document whose statements are >= 60% already carried by other sources is a copy: it adds no support
SHARED_FILES = {'central.json', 'central.min.json'}   # the central DB copied into every webapp: read once, as its own source
MAX_FILE = 400_000
WIKI = 'greathousefarmwiki.wordpress.com'
EXCLUDE_REPOS = {'cedwyn'}   # the old monorepo: its webapp content now lives in the split repos read here

def get(url, timeout=60, api=False):
    h = dict(UA)
    if api and TOKEN: h['Authorization'] = 'Bearer ' + TOKEN
    for a in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
                return r.read()
        except Exception as e:
            if getattr(e, 'code', None) in (404, 451): return None
            time.sleep(3 * (a + 1))
    return None

# ---------------- text extraction ----------------
class _H(HTMLParser):
    def __init__(s): super().__init__(convert_charrefs=True); s.out = []; s.skip = 0
    def handle_starttag(s, t, a):
        if t in ('script', 'style', 'noscript', 'svg', 'nav', 'footer', 'header', 'template'): s.skip += 1
        if t in ('p', 'br', 'li', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'td', 'tr', 'section', 'article', 'blockquote', 'dd', 'dt'): s.out.append('\n')
    def handle_endtag(s, t):
        if t in ('script', 'style', 'noscript', 'svg', 'nav', 'footer', 'header', 'template') and s.skip: s.skip -= 1
        if t in ('p', 'li', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'td', 'tr', 'blockquote'): s.out.append('\n')
    def handle_data(s, d):
        if not s.skip: s.out.append(d)
def html_text(t):
    p = _H()
    try: p.feed(t)
    except Exception: return re.sub(r'<[^>]+>', ' ', t)
    return ''.join(p.out)
def md_text(t):
    t = re.sub(r'```.*?```', ' ', t, flags=re.S)
    t = re.sub(r'!\[[^\]]*\]\([^)]*\)', ' ', t)
    t = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', t)
    t = re.sub(r'<[^>]+>', ' ', t)
    t = re.sub(r'^[#>\-\*\+\s|]+', '', t, flags=re.M)
    return t.replace('**', '').replace('__', '').replace('`', '')
def json_text(t):
    try: o = json.loads(t)
    except Exception: return ''
    out = []
    def walk(x, d=0):
        if d > 40 or len(out) > 20000: return
        if isinstance(x, str):
            if len(x) > 40 and ' ' in x: out.append(html_text(x) if '<' in x else x)
        elif isinstance(x, dict):
            for v in x.values(): walk(v, d + 1)
        elif isinstance(x, list):
            for v in x: walk(v, d + 1)
    walk(o)
    return '\n'.join(out)
def to_text(path, raw):
    try: t = raw.decode('utf-8')
    except Exception: t = raw.decode('latin-1', 'ignore')
    p = path.lower()
    if p.endswith(('.html', '.htm', '.xml')): return html_text(t)
    if p.endswith('.json'): return json_text(t)
    if p.endswith(('.md', '.markdown')): return md_text(t)
    if p.endswith(('.csv', '.yml', '.yaml')): return '\n'.join(c for c in re.split(r'[,\n]\s*(?=[^"]*(?:"[^"]*"[^"]*)*$)|\n', t) if len(c) > 40)
    return t

# ---------------- statements ----------------
SPLIT = re.compile(r'(?<=[.!?])["\u201d\u2019)]?\s+(?=[A-Z\u0174\u0176"\u201c(])|\n+')
STOP = set('a an the and or of to in on at by for from with as is was were be been are it its this that these those which who whom whose his her their our we you he she they them there here not no but if then than so such into onto over under after before about also has have had do did does done can could may might shall should will would'.split())
UI = re.compile(r'(cookie|javascript|click here|tap here|sign in|log in|subscribe|newsletter|menu|toggle|copyright|all rights reserved|loading|back to top|read more|learn more|share this|privacy policy|terms of use|\bnav\b|button|dropdown|\bmodal\b|\bwebapp\b|navigator|\bgithub\b|\bpull request\b|\bcommit\b|\bbranch\b|\bworkflow\b|semver|changelog|v\d+\.\d+\.\d+|\bagent\b|\bagents\b|\bTab\b|flywheel|hubspot|\bAPI\b|\bJSON\b|localhost|\.js\b|\.css\b|\bPR\b|\.py\b|by hand|read-only|£\d|\bprice\b|checkout|stripe|gumroad|buy now|e-?book)', re.I)
PRIVATE = re.compile(r'(\bsion\b|\bhywel\b|branwen|unclehowell|@[a-z0-9-]+\.|\b0\d{3,4}\s?\d{3}\s?\d{3,4}\b|\+44|\b[A-Z]{1,2}\d{1,2}[A-Z]?\s\d[A-Z]{2}\b|passw|medical|\b1547\b|\b1552\b)', re.I)
EXCL = re.compile(r'\b(says|claims|according to|hearsay|unverified|alleged|fraud|stolen)\b', re.I)
def norm(s):
    s = s.lower().replace('\u0175', 'w').replace('\u0177', 'y')
    return [w for w in re.findall(r'[a-z0-9]+', s) if w not in STOP]
def statements(text):
    for raw in SPLIT.split(text):
        s = re.sub(r'\s+', ' ', raw).strip(' \t-•*|:;,')
        s = re.sub(r'^(?:subject|re|fwd?|aw|ynglŷn)\s*:\s*(?:(?:re|fwd?|subject)\s*:\s*)*', '', s, flags=re.I)
        if not s or not (s[0].isupper() or s[0].isdigit() or s[0] in '"\u201c\u2018\'(['): continue   # a fragment cut mid-sentence
        if re.match(r'^[A-Z][a-z]+\)', s) or re.search(r'automatic reply|out of office|undeliverable', s, re.I): continue
        if not (40 <= len(s) <= 420): continue
        words = s.split()
        if not (8 <= len(words) <= 60): continue
        letters = sum(c.isalpha() for c in s)
        if letters < 0.6 * len(s): continue
        if s.count('http') or s.count('/') > 4 or s.count('{') or s.count('=') > 1: continue
        yield s

# ---------------- entities ----------------
GAZ = [('BP Properties', r'\bBP\b|BP Properties|BP Pension'), ('HM Land Registry', r'Land Registry|\bHMLR\b'), ('Bute', r'\bBute\b'),
       ('Williams family', r'\bWilliams\b'), ('Mary (defendant, d.1983)', r'\bMary\b'), ('Western Ground Rents', r'Western Ground Rents'),
       ('Linklaters', r'Linklaters'), ('Court of Appeal', r'Court of Appeal'), ('Tŷ Mawr / Great House Farm', r'T[ŷy] ?Mawr|Great House'),
       ('Llandough', r'Llandough|Llandochau'), ('Leckwith', r'Leckwith|Lecwydd'), ('Cedwyn / Cydfin', r'Cedwyn|Cydfin|Cedfin'),
       ('Vale of Glamorgan Council', r'Vale of Glamorgan'), ('Cadw', r'\bCadw\b'), ('National Archives', r'National Archives|\bTNA\b'),
       ('Glamorgan Archives', r'Glamorgan Archives'), ('National Library of Wales', r'National Library of Wales|\bNLW\b'),
       ('South Wales Police', r'South Wales Police|\bSWP\b'), ('Limitation Act', r'Limitation Act'), ('Land Registration Act', r'Land Registration Act'),
       ('adverse possession', r'adverse possession'), ('licence / permission', r'\blicen[cs]e\b|\bpermission\b'), ('possession order', r'possession order|possession action'),
       ('tithe', r'\btithe\b'), ('conveyance', r'\bconvey(ance|ed)\b'), ('first registration', r'first registration|registered (?:in|as) (?:the )?proprietor'),
       ('eviction', r'\bevict'), ('Penarth', r'\bPenarth\b'), ('Cardiff', r'\bCardiff\b'), ('Welsh Office', r'Welsh Office'), ('Ordnance Survey', r'Ordnance Survey|\bOS\b map')]
GAZ_RX = [(n, re.compile(r, re.I if n in ('adverse possession', 'licence / permission', 'possession order', 'tithe', 'conveyance', 'first registration', 'eviction') else 0)) for n, r in GAZ]
ID_RX = re.compile(r'\b(EV-\d{3,4}|MR-\d{2,4}|WA\d{6}|CYM\d{5,6}|D ?\d{3}(?:/\d+)?)\b')
YEAR_RX = re.compile(r'\b(1[0-9]{3}|20[0-2][0-9])\b')
def entities(s):
    e = {n for n, rx in GAZ_RX if rx.search(s)}
    e |= {'id:' + m.replace(' ', '') for m in ID_RX.findall(s)}
    e |= {'year:' + y for y in YEAR_RX.findall(s) if 1000 <= int(y) <= 2026}
    return e

# ---------------- sources ----------------
def repo_list(args):
    """-> {name: (size_kb, default_branch)} from the owner's public repo listing (1-2 API calls)."""
    info, page = {}, 1
    while True:
        b = get(f'https://api.github.com/users/{OWNER}/repos?per_page=100&page={page}', api=True)
        js = json.loads(b) if b else []
        for x in js:
            if not x.get('fork') and not x.get('private'): info[x['name']] = (x.get('size', 0), x.get('default_branch', 'main'))
        if len(js) < 100: break
        page += 1
    if args.repos and os.path.exists(args.repos):
        names = [l.strip() for l in open(args.repos) if l.strip() and not l.startswith('#')]
    else:
        names = [n for n in info if n.startswith('cedwyn')]
    names = [n for n in names if n in info and n not in EXCLUDE_REPOS and 'datro' not in n.lower() and n != 'cedwyn-graphrag']
    if args.only: names = [n for n in names if n in args.only.split(',')]
    return {n: info[n] for n in sorted(set(names))}
def repo_meta(name):
    b = get(f'https://api.github.com/repos/{OWNER}/{name}', api=True)
    return json.loads(b) if b else {}
def want(path, size=0):
    return path.lower().endswith(TEXT_EXT) and not SKIP_DIR.search(path) and not SKIP_FILE.search(path) and size <= MAX_FILE
def read_repo(name, max_files, size_kb, branch='main'):
    """yield (path, raw bytes): tarball stream for small repos; file list + raw reads for big ones."""
    if size_kb < BIG_REPO_KB:
        u = f'https://codeload.github.com/{OWNER}/{name}/tar.gz/refs/heads/{branch}'
        h = dict(UA)
        try:
            r = urllib.request.urlopen(urllib.request.Request(u, headers=h), timeout=300)
            n = 0
            with tarfile.open(fileobj=r, mode='r|gz') as tf:
                for m in tf:
                    if not m.isfile(): continue
                    p = m.name.split('/', 1)[1] if '/' in m.name else m.name
                    if not want(p, m.size): continue
                    f = tf.extractfile(m)
                    if f: yield p, f.read(); n += 1
                    if n >= max_files: break
            return
        except Exception as e:
            print('  tar failed', name, str(e)[:80], file=sys.stderr)
    b = get(f'https://api.github.com/repos/{OWNER}/{name}/git/trees/{branch}?recursive=1', timeout=120, api=True)
    if not b: return
    tree = json.loads(b).get('tree', [])
    paths = [t['path'] for t in tree if t.get('type') == 'blob' and want(t['path'], t.get('size', 0))][:max_files]
    def fetch(p): return p, get(f'https://raw.githubusercontent.com/{OWNER}/{name}/{branch}/' + urllib.parse.quote(p), timeout=60)
    with ThreadPoolExecutor(12) as ex:
        for p, raw in ex.map(fetch, paths):
            if raw: yield p, raw
def read_wiki():
    for kind in ('pages', 'posts'):
        page = 1
        while True:
            u = f'https://public-api.wordpress.com/wp/v2/sites/{WIKI}/{kind}?per_page=100&page={page}&_fields=id,link,title,content'
            b = get(u, timeout=120)
            if not b: break
            try: js = json.loads(b)
            except Exception: break
            if not isinstance(js, list) or not js: break
            for x in js:
                t = html.unescape(re.sub(r'<[^>]+>', ' ', x.get('title', {}).get('rendered', '')))
                yield x.get('link', ''), t + '.\n' + html_text(x.get('content', {}).get('rendered', ''))
            if len(js) < 100: break
            page += 1

CATALOGUE = 'https://cedwyn.bucklerfamily.estate/api/catalogue/'
def read_catalogue():
    off, lim = 0, 50
    while True:
        b = get(f'{CATALOGUE}?limit={lim}&offset={off}', timeout=60)
        if not b: break
        try: js = json.loads(b)
        except Exception: break
        items = js.get('items') or []
        for x in items:
            t = '. '.join(str(x.get(k) or '') for k in ('title', 'summary', 'body') if x.get(k))
            pg = x.get('page') or ''
            url = pg if str(pg).startswith('http') else ('https://cedwyn.bucklerfamily.estate' + pg if pg else f'https://cedwyn.bucklerfamily.estate/catalogue/#{x.get("id")}')
            yield str(x.get('id')), url, (html_text(t) if '<' in t else t)
        off += len(items)
        if not items or off >= (js.get('total') or 0): break

# ---------------- dedupe ----------------
NPERM, BANDS = 32, 8
def shingles(ws):
    return {zlib.crc32(' '.join(ws[i:i + 3]).encode()) for i in range(max(1, len(ws) - 2))}
class UF:
    def __init__(s, n): s.p = list(range(n))
    def f(s, x):
        while s.p[x] != x: s.p[x] = s.p[s.p[x]]; x = s.p[x]
        return x
    def u(s, a, b):
        a, b = s.f(a), s.f(b)
        if a != b: s.p[max(a, b)] = min(a, b)
def cluster(keys, sh):
    n = len(keys); uf = UF(n)
    if np is None: return uf
    rng = np.random.RandomState(7); A = rng.randint(1, 2**31 - 1, NPERM, dtype=np.int64); B = rng.randint(0, 2**31 - 1, NPERM, dtype=np.int64)
    P = (1 << 61) - 1
    buckets = collections.defaultdict(list)
    for i, s in enumerate(sh):
        a = np.fromiter(s, dtype=np.int64, count=len(s)) & 0x7FFFFFFF
        mh = ((a[:, None] * A[None, :] + B[None, :]) % 2147483647).min(0)
        r = NPERM // BANDS
        for b in range(BANDS): buckets[(b, mh[b * r:(b + 1) * r].tobytes())].append(i)
    for v in buckets.values():
        if 1 < len(v) < 200:
            for j in v[1:]:
                a, b = sh[v[0]], sh[j]
                if len(a & b) / max(1, len(a | b)) >= 0.6: uf.u(v[0], j)
    return uf

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='data'); ap.add_argument('--repos'); ap.add_argument('--max-files', type=int, default=3000)
    ap.add_argument('--only'); ap.add_argument('--no-wiki', action='store_true'); ap.add_argument('--top', type=int, default=500)
    args = ap.parse_args(); os.makedirs(args.out, exist_ok=True)
    t0 = time.time()
    names = repo_list(args)
    seen_docs = {}
    occ = collections.defaultdict(lambda: collections.defaultdict(set))   # key -> source -> set(doc ids)
    text_of = {}; docs_per_source = collections.Counter(); src_info = {}; mirrors = 0; files = 0
    excluded = collections.Counter(); docs = []; first_doc = {}; near_mirrors = collections.Counter(); shared_done = {}
    def add_doc(source, path, url, text):
        nonlocal mirrors, files
        files += 1
        h = hashlib.sha1(re.sub(r'\W+', '', text.lower()).encode()).hexdigest()
        if len(text) < 80: return
        if h in seen_docs: mirrors += 1; return
        cand = []
        for s in statements(text):
            if PRIVATE.search(s): excluded['personal or contact detail'] += 1; continue
            if EXCL.search(s): excluded['excluded word'] += 1; continue
            if UI.search(s): excluded['site/UI or commerce text'] += 1; continue
            ws = norm(s)
            if len(ws) < 5: continue
            cand.append((' '.join(ws), s))
        if not cand: return
        keyset = {k for k, _ in cand}
        # near-mirror: most of this document's statements already sit in ONE earlier document of another source
        # (mostly a copy of another source: it adds its new statements, but no support to the copied ones)
        mirror_of = None
        if len(keyset) >= 3:
            other = sum(1 for k in keyset if k in first_doc and docs[first_doc[k]][0] != source)
            if other >= MIRROR_SHARE * len(keyset): mirror_of = True; near_mirrors[source] += 1
        did = len(docs); seen_docs[h] = did; docs.append((source, path, url)); docs_per_source[source] += 1
        for k, s in cand:
            if mirror_of and k in first_doc: continue
            first_doc.setdefault(k, did)
            occ[k][source].add(did)
            if k not in text_of or (len(s) < len(text_of[k]) and len(s) > 60): text_of[k] = s
    n0 = files
    for cid, url, text in read_catalogue():
        add_doc('catalogue', cid, url, text)
    src_info['catalogue'] = {'kind': 'central evidence catalogue (live API)', 'files_read': files - n0, 'url': 'https://cedwyn.bucklerfamily.estate/catalogue/'}
    print(f'catalogue: {files-n0} records, {len(occ)} keys', flush=True)
    if not args.no_wiki:
        n0 = files
        for link, text in read_wiki():
            add_doc('legal-wiki', link.replace('https://' + WIKI, ''), link, text)
        src_info['legal-wiki'] = {'kind': 'legal wiki (public pages and posts)', 'files_read': files - n0, 'url': 'https://' + WIKI + '/'}
        print(f'wiki: {files-n0} pages, {len(occ)} keys', flush=True)
    PRIMARY = ['cedwyn-research', 'cedwyn-catalogue', 'cedwyn-records', 'cedwyn-ebooks']
    order = [n for n in PRIMARY if n in names] + [n for n in names if n not in PRIMARY]
    for i, name in enumerate(order):
        size_kb, br = names[name]
        n0 = files
        for p, raw in read_repo(name, args.max_files, size_kb, br):
            u = f'https://github.com/{OWNER}/{name}/blob/{br}/{p}'
            if p.rsplit('/', 1)[-1] in SHARED_FILES:
                if shared_done.get(p.rsplit('/', 1)[-1]): mirrors += 1; continue
                shared_done[p.rsplit('/', 1)[-1]] = u
                add_doc('central-db', p, u, to_text(p, raw)); continue
            add_doc(name, p, u, to_text(p, raw))
        src_info[name] = {'kind': 'webapp repo', 'files_read': files - n0, 'documents_kept': docs_per_source[name], 'near_mirror_documents': near_mirrors[name], 'url': f'https://github.com/{OWNER}/{name}'}
        print(f'[{i+1}/{len(names)}] {name}: {files-n0} files, {len(occ)} keys, {time.time()-t0:.0f}s', flush=True)
    if shared_done: src_info['central-db'] = {'kind': 'central database (data/central.json, copied into the webapps; read once)', 'documents_kept': docs_per_source['central-db'], 'url': list(shared_done.values())[0]}
    keys = list(occ)
    sh = [shingles(k.split()) for k in keys]
    uf = cluster(keys, sh)
    groups = collections.defaultdict(list)
    for i in range(len(keys)): groups[uf.f(i)].append(i)
    out = []; noise = []
    for g in groups.values():
        srcs = collections.defaultdict(set); variants = 0
        for i in g:
            variants += 1
            for s, d in occ[keys[i]].items(): srcs[s] |= d
        ndocs = sum(len(v) for v in srcs.values())
        rep = max(g, key=lambda i: (sum(len(v) for v in occ[keys[i]].values()), -abs(len(text_of[keys[i]]) - 160)))
        text = text_of[keys[rep]]
        # template text: present on most documents of the sources carrying it
        tmpl = [s for s, d in srcs.items() if docs_per_source[s] >= 8 and len(d) / docs_per_source[s] >= 0.5]
        item = {'text': text, 'support': len(srcs), 'docs': ndocs, 'variants': variants,
                'entities': sorted(entities(text)),
                'sources': [{'source': s, 'docs': len(d), 'url': docs[min(d)][2]} for s, d in sorted(srcs.items(), key=lambda x: -len(x[1]))][:12]}
        if tmpl and len(tmpl) >= max(1, len(srcs) // 2): item['noise'] = 'template text repeated on most pages'; noise.append(item); continue
        out.append(item)
    out.sort(key=lambda x: (-x['support'], -x['docs'], len(x['text'])))
    noise.sort(key=lambda x: (-x['docs']))
    for n, x in enumerate(out): x['id'] = f'S{n+1:05d}'
    # graph: entities <-> statements, entity co-occurrence weighted by support
    ent_w = collections.Counter(); co = collections.Counter()
    for x in out:
        es = [e for e in x['entities'] if not e.startswith('year:')] + [e for e in x['entities'] if e.startswith('year:')][:3]
        for e in es: ent_w[e] += x['support']
        for a in es:
            for b in es:
                if a < b: co[(a, b)] += x['support']
    top_ents = [e for e, _ in ent_w.most_common(250)]; te = set(top_ents)
    top = out[:args.top]
    graph = {'nodes': [{'id': e, 'kind': 'year' if e.startswith('year:') else 'record' if e.startswith('id:') else 'entity', 'weight': ent_w[e]} for e in top_ents] +
                      [{'id': x['id'], 'kind': 'statement', 'weight': x['support']} for x in top[:300]],
             'edges': [{'a': a, 'b': b, 'w': w, 'rel': 'co-occurs'} for (a, b), w in co.most_common(1500) if a in te and b in te] +
                      [{'a': x['id'], 'b': e, 'w': x['support'], 'rel': 'mentions'} for x in top[:300] for e in x['entities'] if e in te]}
    hist = collections.Counter(min(x['support'], 10) for x in out)
    stats = {'generated_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'runtime_s': round(time.time() - t0),
             'sources': len(src_info), 'repos': len(names), 'files_read': files, 'documents_unique': len(docs) - sum(near_mirrors.values()), 'documents_mirrored_skipped': mirrors + sum(near_mirrors.values()), 'documents_exact_mirrors': mirrors, 'documents_near_mirrors': sum(near_mirrors.values()),
             'statement_variants': len(keys), 'statement_clusters': len(out) + len(noise), 'statements_public': len(out), 'noise_template': len(noise),
             'excluded_by_filter': dict(excluded), 'support_histogram': {('10+' if k == 10 else str(k)): v for k, v in sorted(hist.items())},
             'method': 'support = distinct sources (webapp repo, evidence catalogue, legal wiki) carrying a near-identical statement. A document whose statements are mostly (60% or more) already carried by other sources is a copy (the central database in each webapp, a mirrored wiki page): it adds its new statements but no support to the copied ones. Repetition inside one source counts once. Deterministic, no model calls.'}
    def dump(name, obj, cap=390_000):
        s = json.dumps(obj, ensure_ascii=False, separators=(',', ':'))
        while len(s.encode()) > cap and isinstance(obj, list) and len(obj) > 50:
            obj = obj[:int(len(obj) * 0.85)]; s = json.dumps(obj, ensure_ascii=False, separators=(',', ':'))
        open(os.path.join(args.out, name), 'w').write(s); return len(obj) if isinstance(obj, list) else None
    for x in top: x['sources'] = x['sources'][:8]
    stats['statements_in_dashboard'] = dump('statements-top.json', top)
    dump('noise.json', [{'text': x['text'], 'docs': x['docs'], 'support': x['support']} for x in noise[:300]], 150_000)
    dump('graph.json', graph)
    dump('sources.json', src_info, 150_000)
    dump('stats.json', stats)
    print(json.dumps(stats, indent=1))
if __name__ == '__main__': main()
