"""Generează docs/DOCUMENTATION.html din docs/DOCUMENTATION.md (și protocol.html din protocol.md).

Rulare:  pip install markdown && python docs/build_html.py
Diagramele Mermaid se randează în browser (mermaid de pe jsDelivr); fără internet se afișează sursa diagramei.
"""
import html
import re
from pathlib import Path

import markdown

DOCS = Path(__file__).parent

CSS = """
:root{--bg:#fff;--fg:#1f2328;--muted:#59636e;--line:#d1d9e0;--accent:#0b6bcb;--code:#f3f5f7;--card:#f8f9fb;--quote:#e8f1fb}
@media (prefers-color-scheme:dark){:root{--bg:#0f1318;--fg:#e6e9ee;--muted:#9aa4b2;--line:#2b333d;--accent:#6cb2ff;--code:#1a2029;--card:#151b23;--quote:#14283d}}
:root[data-theme=light]{--bg:#fff;--fg:#1f2328;--muted:#59636e;--line:#d1d9e0;--accent:#0b6bcb;--code:#f3f5f7;--card:#f8f9fb;--quote:#e8f1fb}
:root[data-theme=dark]{--bg:#0f1318;--fg:#e6e9ee;--muted:#9aa4b2;--line:#2b333d;--accent:#6cb2ff;--code:#1a2029;--card:#151b23;--quote:#14283d}
*{box-sizing:border-box}html{scroll-behavior:smooth;scroll-padding-top:16px}
body{margin:0;background:var(--bg);color:var(--fg);font:16px/1.65 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.layout{display:flex;max-width:1280px;margin:0 auto}
nav{position:sticky;top:0;align-self:flex-start;height:100vh;overflow:auto;width:290px;flex:none;padding:24px 16px;border-right:1px solid var(--line);font-size:14px}
nav h2{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin:0 0 10px}
nav a{display:block;color:var(--fg);text-decoration:none;padding:5px 8px;border-radius:6px;line-height:1.35}
nav a:hover{background:var(--card)}nav a.active{color:var(--accent);background:var(--card);font-weight:600}
main{flex:1;min-width:0;padding:32px 40px 80px;max-width:920px}
h1{font-size:2rem;line-height:1.25;margin:.2em 0 .4em}
h2{font-size:1.5rem;margin:2.2em 0 .6em;padding-top:.8em;border-top:1px solid var(--line)}
h3{font-size:1.15rem;margin:1.6em 0 .4em}
a{color:var(--accent)}
p,li{max-width:75ch}
blockquote{margin:1em 0;padding:.6em 1em;background:var(--quote);border-left:4px solid var(--accent);border-radius:0 8px 8px 0;color:var(--fg)}
blockquote p{margin:.3em 0}
code{background:var(--code);padding:.15em .4em;border-radius:5px;font:.88em ui-monospace,SFMono-Regular,Consolas,monospace}
pre{background:var(--code);padding:14px 16px;border-radius:10px;overflow:auto;border:1px solid var(--line);line-height:1.5}
pre code{background:none;padding:0;font-size:.85em}
.tablewrap{overflow-x:auto;margin:1em 0}
table{border-collapse:collapse;width:100%;font-size:.93rem}
th,td{border:1px solid var(--line);padding:8px 10px;text-align:left;vertical-align:top}
th{background:var(--card);font-weight:600}
tr:nth-child(even) td{background:color-mix(in srgb,var(--card) 55%,transparent)}
hr{border:none;border-top:1px solid var(--line);margin:2em 0}
pre.mermaid{background:var(--card);text-align:center;padding:18px 10px}
pre.mermaid[data-processed]{white-space:normal}
.theme{position:fixed;right:14px;top:12px;z-index:5;background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:8px;padding:6px 10px;cursor:pointer;font:inherit;font-size:13px}
.menu{display:none}
@media (max-width:900px){
  nav{display:none;position:fixed;inset:0 auto 0 0;z-index:4;background:var(--bg);width:min(85vw,320px)}
  body.navopen nav{display:block}
  .menu{display:block;position:fixed;left:14px;top:12px;z-index:5;background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:8px;padding:6px 10px;font:inherit;font-size:13px}
  main{padding:56px 16px 60px}
}
@media print{nav,.theme,.menu{display:none}.layout{display:block}main{max-width:none;padding:0}h2{break-after:avoid}pre,table{break-inside:avoid}}
"""

JS = """
const root=document.documentElement;
function dark(){const t=root.dataset.theme;return t?t==='dark':matchMedia('(prefers-color-scheme: dark)').matches}
document.getElementById('theme').onclick=()=>{try{root.dataset.theme=dark()?'light':'dark';localStorage.setItem('theme',root.dataset.theme)}catch(e){root.dataset.theme=dark()?'light':'dark'};location.reload()};
try{const s=localStorage.getItem('theme');if(s)root.dataset.theme=s}catch(e){}
document.querySelector('.menu').onclick=()=>document.body.classList.toggle('navopen');
document.querySelectorAll('nav a').forEach(a=>a.addEventListener('click',()=>document.body.classList.remove('navopen')));
const links=[...document.querySelectorAll('nav a')],secs=links.map(a=>document.getElementById(a.getAttribute('href').slice(1)));
const io=new IntersectionObserver(es=>{es.forEach(e=>{if(e.isIntersecting){links.forEach(l=>l.classList.remove('active'));const l=links[secs.indexOf(e.target)];if(l)l.classList.add('active')}})},{rootMargin:'0px 0px -75% 0px'});
secs.forEach(s=>s&&io.observe(s));
import('https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs').then(m=>{
  m.default.initialize({startOnLoad:false,theme:dark()?'dark':'default',securityLevel:'loose'});
  m.default.run({querySelector:'pre.mermaid'});
}).catch(()=>{});
"""


def convert(md_text: str):
    # blocurile ```mermaid se scot înainte de conversie, ca să nu fie tratate ca cod obișnuit
    diagrams = []

    def stash(m):
        diagrams.append(m.group(1))
        return f"\n\nMERMAIDPLACEHOLDER{len(diagrams) - 1}\n\n"

    md_text = re.sub(r"```mermaid\n(.*?)```", stash, md_text, flags=re.S)
    md = markdown.Markdown(extensions=["tables", "fenced_code", "toc", "sane_lists"],
                           extension_configs={"toc": {"permalink": False}})
    body = md.convert(md_text)
    for i, d in enumerate(diagrams):
        body = body.replace(f"<p>MERMAIDPLACEHOLDER{i}</p>", f'<pre class="mermaid">{html.escape(d)}</pre>')
    body = re.sub(r"<table>", '<div class="tablewrap"><table>', body)
    body = body.replace("</table>", "</table></div>")
    title = re.search(r"<h1[^>]*>(.*?)</h1>", body, flags=re.S)
    title = re.sub(r"<[^>]+>", "", title.group(1)) if title else "Documentație"
    toc = "".join(
        f'<a href="#{t["id"]}">{html.escape(t["name"])}</a>'
        for t in md.toc_tokens[0]["children"]
    ) if md.toc_tokens and md.toc_tokens[0].get("children") else "".join(
        f'<a href="#{t["id"]}">{html.escape(t["name"])}</a>' for t in md.toc_tokens if t["level"] == 2)
    return title, body, toc


def page(title, body, toc):
    return f"""<!doctype html>
<html lang="ro">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title>
<style>{CSS}</style>
</head>
<body>
<button class="menu" aria-label="Cuprins">☰ Cuprins</button>
<button class="theme" id="theme" aria-label="Schimbă tema">◐ Temă</button>
<div class="layout">
<nav><h2>Cuprins</h2>{toc}</nav>
<main>
{body}
</main>
</div>
<script type="module">{JS}</script>
</body>
</html>
"""


def main():
    for name in ("DOCUMENTATION", "protocol"):
        src = DOCS / f"{name}.md"
        title, body, toc = convert(src.read_text(encoding="utf-8"))
        # linkurile între documente .md -> .html
        body = body.replace('href="protocol.md"', 'href="protocol.html"').replace(
            'href="DOCUMENTATION.md"', 'href="DOCUMENTATION.html"')
        (DOCS / f"{name}.html").write_text(page(title, body, toc), encoding="utf-8")
        print(f"scris docs/{name}.html")


if __name__ == "__main__":
    main()
