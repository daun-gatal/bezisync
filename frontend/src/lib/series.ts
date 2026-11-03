/** Time series data for TimeSeries, and the pure parts of its readout (testable without uPlot). */

export interface Series {
  label: string;
  values: (number | null)[];
  /** A design token, e.g. "--bz-accent": resolved when drawn, so the chart follows the theme. */
  color: string;
  /** Format for axis ticks and the readout. */
  format: (v: number) => string;
  dashed?: boolean;
}

/** The readout's values: at the hovered point, or the last point (null = unknown there). */
export function readoutAt(series: Series[], times: number[], idx: number | null): { at: number | null; values: (number | null)[] } {
  const i = idx ?? times.length - 1;
  if (i < 0 || i >= times.length) return { at: null, values: series.map(() => null) };
  return { at: times[i] ?? null, values: series.map((s) => s.values[i] ?? null) };
}

/** The largest known value of each series, or null when it has none. */
export function peaks(series: Series[]): (number | null)[] {
  return series.map((s) => s.values.reduce<number | null>((m, v) => (v !== null && Number.isFinite(v) && (m === null || v > m) ? v : m), null));
}
