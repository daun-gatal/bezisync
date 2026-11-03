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
