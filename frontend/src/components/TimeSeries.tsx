import { useEffect, useRef, useState } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';
import { peaks, readoutAt, type Series } from '@/lib/series';

export type { Series };

const hourMinute = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit' });
const dayHourMinute = new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });

/** A colour token resolved to a concrete colour (light-dark() is evaluated by the browser). */
function cssVar(name: string): string {
  if (!name.startsWith('--')) return name;
  const probe = document.createElement('span');
  probe.style.color = `var(${name})`;
  probe.style.display = 'none';
  document.body.appendChild(probe);
  const color = getComputedStyle(probe).color;
  probe.remove();
  return color || '#888';
}

/** Changes whenever the effective colour scheme changes (system setting or data-theme). */
function useThemeVersion(): number {
  const [version, setVersion] = useState(0);
  useEffect(() => {
    const bump = () => setVersion((v) => v + 1);
    const media = window.matchMedia?.('(prefers-color-scheme: dark)');
    media?.addEventListener('change', bump);
    const observer = new MutationObserver(bump);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme', 'class', 'style'] });
    return () => {
      media?.removeEventListener('change', bump);
      observer.disconnect();
    };
  }, []);
  return version;
}

/**
 * A small, dependency-light time-series chart (uPlot). Two series max, each on its own axis.
 * Above it, a readout shows each series' value at the hovered point, or at the last point, and
 * the peak over the range (`peakLabel`). `summary` is the text alternative for screen readers.
 */
export function TimeSeries({
  times,
  series,
  summary,
  height = 200,
  peakLabel,
}: {
  times: number[];
  series: Series[];
  summary: string;
  height?: number;
  peakLabel?: string;
}) {
  const el = useRef<HTMLDivElement>(null);
  const plot = useRef<uPlot | null>(null);
  const theme = useThemeVersion();
  const [hover, setHover] = useState<number | null>(null);

  useEffect(() => {
    const node = el.current;
    if (!node) return;
    const ink = cssVar('--bz-muted');
    const grid = cssVar('--bz-line');
    const scales = series.map((_, i) => `y${i}`);
    const opts: uPlot.Options = {
      width: node.clientWidth || 600,
      height,
      legend: { show: false },
      cursor: { drag: { x: false, y: false } },
      hooks: { setCursor: [(u) => setHover(u.cursor.idx ?? null)] },
      scales: Object.fromEntries(scales.map((s) => [s, { auto: true, range: (_u, min, max) => [Math.min(0, min), Math.max(max, 1) * 1.1] }])),
      axes: [
        { stroke: ink, space: 80, grid: { stroke: grid, width: 1 }, ticks: { stroke: grid } },
        ...series.map((s, i) => ({
          scale: scales[i],
          side: i === 0 ? 3 : 1,
          stroke: cssVar(s.color),
          grid: { show: i === 0, stroke: grid, width: 1 },
          values: (_u: uPlot, vals: number[]) => vals.map((v) => s.format(v)),
          space: 36,
          size: 64,
        })),
      ],
      series: [
        {},
        ...series.map((s, i) => ({
          label: s.label,
          scale: scales[i],
          stroke: cssVar(s.color),
          width: 2,
          dash: s.dashed ? [6, 4] : undefined,
          spanGaps: false,
        })),
      ],
    };
    plot.current = new uPlot(opts, [times, ...series.map((s) => s.values)] as uPlot.AlignedData, node);
    const ro = new ResizeObserver(() => plot.current?.setSize({ width: node.clientWidth, height }));
    ro.observe(node);
    return () => {
      ro.disconnect();
      plot.current?.destroy();
      plot.current = null;
    };
    // Re-create when the shape changes; data updates go through setData below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [series.length, height, theme]);

  useEffect(() => {
    plot.current?.setData([times, ...series.map((s) => s.values)] as uPlot.AlignedData);
  }, [times, series]);

  const shown = readoutAt(series, times, hover);
  const top = peaks(series);
  const span = times.length > 1 ? times[times.length - 1]! - times[0]! : 0;
  const when = shown.at === null ? null : (span > 86_400 ? dayHourMinute : hourMinute).format(shown.at * 1000);
  return (
    <figure className="m-0 flex min-w-0 flex-col gap-3">
      <div className="flex min-w-0 flex-wrap items-end gap-x-8 gap-y-2" aria-hidden>
        {series.map((s, i) => {
          const v = shown.values[i];
          return (
            <div key={s.label} className="flex min-w-0 flex-col gap-0.5">
              <span className="flex items-center gap-2 text-xs text-muted">
                <span className="inline-block w-3 border-t-2" style={{ borderColor: `var(${s.color})`, borderTopStyle: s.dashed ? 'dashed' : 'solid' }} />
                {s.label}
              </span>
              <span className="font-mono text-xl font-medium tabular-nums">{v === null || v === undefined ? '—' : s.format(v)}</span>
            </div>
          );
        })}
        <div className="ml-auto flex min-w-0 flex-col items-end gap-0.5 text-right">
          <span className="text-xs text-muted">{hover === null ? `Latest${when ? ` · ${when}` : ''}` : when}</span>
          {peakLabel ? (
            <span className="font-mono text-sm text-ink-2 tabular-nums">
              {peakLabel}: {top.map((p, i) => (p === null ? '—' : series[i]!.format(p))).join(' · ')}
            </span>
          ) : null}
        </div>
      </div>
      <div ref={el} role="img" aria-label={summary} className="w-full" />
      <figcaption className="sr-only">{summary}</figcaption>
    </figure>
  );
}
