import { useEffect, useState } from 'react';
import { readStored, writeStored } from '@/components/ui/hooks';

/**
 * The colour theme, chosen per browser: `system` follows the computer's setting, `light` and
 * `dark` force one by setting `data-theme` on <html> (styles.css). `public/theme.js` applies the
 * saved choice before the first paint, so pages never flash the other theme.
 */
export type Theme = 'system' | 'light' | 'dark';

export const THEME_KEY = 'bz.theme';
export const THEMES: { value: Theme; label: string }[] = [
  { value: 'system', label: 'System' },
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
];
const CHANGED = 'bz:theme';

export function readTheme(): Theme {
  const t = readStored(THEME_KEY);
  return t === 'light' || t === 'dark' ? t : 'system';
}

export function applyTheme(theme: Theme): void {
  const root = document.documentElement;
  if (theme === 'system') root.removeAttribute('data-theme');
  else root.setAttribute('data-theme', theme);
}

export function setTheme(theme: Theme): void {
  writeStored(THEME_KEY, theme === 'system' ? null : theme);
  applyTheme(theme);
  window.dispatchEvent(new Event(CHANGED));
}

/** The current choice, kept in step across components and other tabs. */
export function useTheme(): [Theme, (t: Theme) => void] {
  const [theme, set] = useState(readTheme);
  useEffect(() => {
    const sync = () => {
      const t = readTheme();
      applyTheme(t);
      set(t);
    };
    const onStorage = (e: StorageEvent) => {
      if (e.key === THEME_KEY) sync();
    };
    window.addEventListener(CHANGED, sync);
    window.addEventListener('storage', onStorage);
    return () => {
      window.removeEventListener(CHANGED, sync);
      window.removeEventListener('storage', onStorage);
    };
  }, []);
  return [theme, setTheme];
}
