// Applies the saved colour theme before the first paint (src/lib/theme.ts owns the choice).
try {
  var t = window.localStorage.getItem('bz.theme');
  if (t === 'light' || t === 'dark') document.documentElement.setAttribute('data-theme', t);
} catch (e) {
  // Storage blocked: follow the system theme.
}
