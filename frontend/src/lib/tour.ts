const SEEN_KEY = 'bz.tour.seen';

/** Whether this viewer opened (or dismissed) the Tour; a per-browser convenience only. */
export function readTourSeen(): boolean {
  try {
    return localStorage.getItem(SEEN_KEY) === '1';
  } catch {
    return false;
  }
}

export function writeTourSeen(): void {
  try {
    localStorage.setItem(SEEN_KEY, '1');
  } catch {
    // Storage blocked: the nudge simply shows again next time.
  }
}
