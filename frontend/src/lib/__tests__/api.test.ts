import { ApiError, problemMessage, readCookie, unwrap } from '../api';

describe('api helpers', () => {
  it('reads cookies by name', () => {
    document.cookie = 'bz_csrf=abc123';
    expect(readCookie('bz_csrf')).toBe('abc123');
    expect(readCookie('missing')).toBeUndefined();
  });

  it('turns problem responses into ApiError with field details', async () => {
    const response = new Response(null, { status: 422 });
    const error = { code: 'validation_failed', detail: 'Some fields are invalid.', errors: [{ loc: ['body', 'slug'], message: 'bad' }] };
    await expect(unwrap(Promise.resolve({ error, response }))).rejects.toBeInstanceOf(ApiError);
    try {
      await unwrap(Promise.resolve({ error, response }));
    } catch (e) {
      expect(problemMessage(e)).toBe('Some fields are invalid. slug: bad');
      expect((e as ApiError).status).toBe(422);
    }
  });

  it('maps network failures to a clear message', async () => {
    await expect(unwrap(Promise.reject(new TypeError('fetch failed')))).rejects.toMatchObject({ code: 'network_error' });
  });
});

describe('session expiry', () => {
  it('opens sign-in for any 401 but a failed sign-in or the session probe', async () => {
    const { quietUnauthorized } = await import('../api');
    const at = (method: string, url: string) => new Request(`http://localhost${url}`, { method });
    expect(quietUnauthorized(at('POST', '/api/v1/auth/login'))).toBe(true);
    expect(quietUnauthorized(at('GET', '/api/v1/auth/me'))).toBe(true);
    expect(quietUnauthorized(at('GET', '/api/v1/auth/me?x=1'))).toBe(true);
    // Saving your profile after the session expired is an expired session (Known issue 16).
    expect(quietUnauthorized(at('PATCH', '/api/v1/auth/me'))).toBe(false);
    expect(quietUnauthorized(at('GET', '/api/v1/projects'))).toBe(false);
  });
});
