import { describeBrowser, feedbackKind } from '../feedback';

describe('describeBrowser', () => {
  it.each([
    ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', 'Chrome 131, macOS'],
    ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0', 'Edge 130, Windows'],
    ['Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0', 'Firefox 132, Linux'],
    ['Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1', 'Safari 18, iOS'],
    ['Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36', 'Chrome 131, Android'],
    ['curl/8.5.0', 'Unknown browser'],
  ])('%s', (ua, expected) => {
    expect(describeBrowser(ua)).toBe(expected);
  });
});

it('unknown kinds fall back to Other', () => {
  expect(feedbackKind('praise').short).toBe('Other');
  expect(feedbackKind('security').short).toBe('Security');
});
