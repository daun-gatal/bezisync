import { afterEach, describe, expect, it } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { THEME_KEY, applyTheme, readTheme, setTheme, useTheme } from '../theme';

afterEach(() => {
  window.localStorage.clear();
  document.documentElement.removeAttribute('data-theme');
});

describe('theme', () => {
  it('defaults to the system theme and ignores unknown values', () => {
    expect(readTheme()).toBe('system');
    window.localStorage.setItem(THEME_KEY, 'sepia');
    expect(readTheme()).toBe('system');
  });

  it('forces light or dark with data-theme and forgets the choice for system', () => {
    setTheme('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    expect(window.localStorage.getItem(THEME_KEY)).toBe('dark');
    setTheme('system');
    expect(document.documentElement.hasAttribute('data-theme')).toBe(false);
    expect(window.localStorage.getItem(THEME_KEY)).toBeNull();
  });

  it('keeps every hook in step', () => {
    const a = renderHook(() => useTheme());
    const b = renderHook(() => useTheme());
    act(() => a.result.current[1]('light'));
    expect(b.result.current[0]).toBe('light');
    applyTheme('system');
  });
});
