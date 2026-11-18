// Small enhancements for the static site: theme toggle, copy buttons, docs search, the mobile
// docs menu and the active section in "On this page". Everything works without it.
(() => {
  const root = document.documentElement;

  document.querySelector('.theme')?.addEventListener('click', () => {
    const dark = root.dataset.theme ? root.dataset.theme === 'dark' : matchMedia('(prefers-color-scheme: dark)').matches;
    root.dataset.theme = dark ? 'light' : 'dark';
    try { localStorage.setItem('bz-theme', root.dataset.theme); } catch {}
  });

  for (const button of document.querySelectorAll('.copy')) {
    button.addEventListener('click', async () => {
      const code = button.parentElement?.querySelector('code')?.innerText ?? '';
      try {
        await navigator.clipboard.writeText(code);
        button.textContent = 'Copied';
      } catch {
        button.textContent = 'Press Ctrl+C';
      }
      setTimeout(() => (button.textContent = 'Copy'), 1500);
    });
  }

  const side = document.querySelector('.side');
  side?.querySelector('.side-toggle')?.addEventListener('click', (e) => {
    const open = side.classList.toggle('open');
    e.currentTarget.setAttribute('aria-expanded', String(open));
  });

  // Search: page titles and section headings first, then text; at most 12 results.
  const input = document.querySelector('.search input');
  const list = document.querySelector('.results');
  let index;
  const esc = (s) => s.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
  input?.addEventListener('focus', async () => {
    index ??= await fetch('/docs/search.json').then((r) => r.json()).catch(() => []);
  });
  input?.addEventListener('input', () => {
    const q = input.value.trim().toLowerCase();
    if (!q || !index) { list.hidden = true; return; }
    const hits = [];
    for (const page of index) {
      if (page.title.toLowerCase().includes(q)) hits.push({ score: 3, url: page.url, title: page.title, where: 'Page' });
      for (const h of page.headings) {
        if (h.text.toLowerCase().includes(q)) hits.push({ score: 2, url: `${page.url}#${h.id}`, title: h.text, where: page.title });
      }
      if (!page.title.toLowerCase().includes(q) && page.text.toLowerCase().includes(q)) hits.push({ score: 1, url: page.url, title: page.title, where: 'Mentioned on this page' });
    }
    hits.sort((a, b) => b.score - a.score);
    list.innerHTML = hits.slice(0, 12).map((h) => `<li><a href="${h.url}">${esc(h.title)}<span>${esc(h.where)}</span></a></li>`).join('')
      || '<li><a>No results</a></li>';
    list.hidden = false;
  });
  input?.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') list.querySelector('a[href]')?.click();
    if (e.key === 'Escape') { input.value = ''; list.hidden = true; }
  });
  document.addEventListener('click', (e) => { if (list && !e.target.closest('.search')) list.hidden = true; });
  document.addEventListener('keydown', (e) => {
    if (e.key === '/' && input && document.activeElement !== input && !e.target.closest('input, textarea')) { e.preventDefault(); input.focus(); }
  });

  // "On this page": highlight the section in view.
  const links = [...document.querySelectorAll('.toc a')];
  if (links.length) {
    const targets = links.map((a) => document.getElementById(decodeURIComponent(a.hash.slice(1)))).filter(Boolean);
    const observer = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        for (const a of links) a.classList.toggle('active', a.hash === `#${entry.target.id}`);
      }
    }, { rootMargin: '-80px 0px -70% 0px' });
    for (const t of targets) observer.observe(t);
  }
})();
