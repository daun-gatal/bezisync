import type { Page } from '@playwright/test';

/** The type scale (px at the 16 px root). Every rendered font size must be one of these, scaled by
 * the root: wide screens step the root up (styles.css) and every size follows. */
export const TYPE_SCALE = [12, 13, 14, 16, 20, 26];

/**
 * Layout problems on the current page, as readable strings:
 * - overflow: an element wider than its box that is not a declared scroll area
 *   (`data-scroll-x`) and does not clip on purpose (overflow hidden, ellipsis);
 * - page-scroll: the page itself scrolls sideways;
 * - font: a font size outside the type scale;
 * - cramped: text that wraps inside a box narrower than 90 px (a squeezed column);
 * - sizes: a card header that isn't 56 px, or a table row shorter than 48 px (scaled like fonts; the
 *   layout sizes that keep cards and rows on one rhythm), or a search box taller than its input
 *   (a toolbar item stretched by a column).
 */
export async function layoutProblems(page: Page): Promise<string[]> {
  return page.evaluate((base) => {
    const out = new Set<string>();
    const k = parseFloat(getComputedStyle(document.documentElement).fontSize) / 16;
    const onScale = (size: number) => base.some((s) => Math.abs(s * k - size) < 0.05);
    const name = (el: Element) => {
      const h = el as HTMLElement;
      const id = h.dataset?.testid ?? h.getAttribute('aria-label') ?? '';
      const text = (h.innerText ?? h.textContent ?? '').trim().replace(/\s+/g, ' ').slice(0, 40);
      return `${el.tagName.toLowerCase()}${id ? `[${id}]` : ''} "${text}"`;
    };
    const visible = (el: Element) => {
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0 && getComputedStyle(el).visibility !== 'hidden';
    };
    const root = document.scrollingElement ?? document.documentElement;
    if (root.scrollWidth > window.innerWidth + 1) out.add(`page-scroll: ${root.scrollWidth}px wide in a ${window.innerWidth}px window`);
    for (const el of document.querySelectorAll<HTMLElement>('body *')) {
      if (!visible(el) || el.closest('[data-scroll-x], svg, canvas, .uplot, [data-sonner-toaster]')) continue;
      const style = getComputedStyle(el);
      if (el.scrollWidth > el.clientWidth + 1 && style.overflowX === 'visible' && el.clientWidth > 0 && style.display !== 'inline') {
        out.add(`overflow: ${name(el)} (${el.scrollWidth}px in ${el.clientWidth}px)`);
      }
      const ownText = [...el.childNodes].some((n) => n.nodeType === Node.TEXT_NODE && (n.textContent ?? '').trim().length > 0);
      if (!ownText) continue;
      const size = Math.round(parseFloat(style.fontSize) * 100) / 100;
      if (!onScale(size)) out.add(`font: ${size}px on ${name(el)}`);
      const r = el.getBoundingClientRect();
      const line = parseFloat(style.lineHeight) || size * 1.4;
      if (r.width < 90 && r.height > line * 1.6 && style.whiteSpace !== 'nowrap' && (el.textContent ?? '').trim().length > 12) {
        out.add(`cramped: ${name(el)} wraps in ${Math.round(r.width)}px`);
      }
    }
    for (const head of document.querySelectorAll<HTMLElement>('[data-card-head]')) {
      const h = head.getBoundingClientRect().height;
      if (h && Math.abs(h - 56 * k) > 0.5) out.add(`sizes: card header ${name(head)} is ${Math.round(h)}px, not ${Math.round(56 * k)}px`);
    }
    for (const row of document.querySelectorAll<HTMLElement>('table[data-table] > tbody > tr')) {
      const h = row.getBoundingClientRect().height;
      if (h && h < 48 * k - 0.5) out.add(`sizes: table row ${name(row)} is ${Math.round(h)}px, under ${Math.round(48 * k)}px`);
    }
    for (const input of document.querySelectorAll<HTMLElement>('input[type=search]')) {
      const box = input.parentElement;
      const h = box?.getBoundingClientRect().height ?? 0;
      if (box && h > input.getBoundingClientRect().height + 1) out.add(`sizes: search box ${name(input)} is ${Math.round(h)}px tall around its input`);
    }
    return [...out];
  }, TYPE_SCALE);
}
