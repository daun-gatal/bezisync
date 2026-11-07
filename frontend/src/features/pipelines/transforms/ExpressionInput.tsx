import { useRef } from 'react';
import { Textarea } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { useDebounced } from '@/components/ui/hooks';
import { useExpressionCheck } from '@/lib/transforms';

/** Back-quotes a column name for an expression (a back-quote doubled), as BeziSync stores it. */
export function quoteColumn(name: string): string {
  return '`' + name.replaceAll('`', '``') + '`';
}

/** The kind of an expression's value, in words. */
const KIND_WORDS: Record<string, string> = {
  text: 'text',
  int: 'a whole number',
  decimal: 'a decimal number',
  float: 'a floating-point number',
  bool: 'true or false',
  date: 'a date',
  time: 'a time of day',
  timestamp: 'a timestamp',
  timestamptz: 'a timestamp with a time zone',
  unknown: 'decided by the columns',
};

/**
 * A Flink CDC expression (ADR 0008): typed, checked by the server as it is typed (its grammar and
 * function list live there only), and replaced by BeziSync's canonical text when the field is left,
 * so what is saved is what is shown. Columns are inserted from the table, never guessed.
 */
export function ExpressionInput({
  id,
  label,
  value,
  use,
  projectId,
  pipelineId,
  table,
  columns,
  onChange,
}: {
  id: string;
  label: string;
  value: string;
  use: 'column' | 'filter';
  projectId: string;
  pipelineId: string;
  table: string;
  columns: { name: string; type: string }[];
  onChange: (v: string) => void;
}) {
  const ref = useRef<HTMLTextAreaElement>(null);
  const typed = useDebounced(value, 250);
  const body = table && typed.trim() ? { use, table, expression: typed, columns: columns.length ? columns : null } : null;
  const check = useExpressionCheck(projectId, pipelineId, body);
  const current = typed === value ? check.data : undefined;
  const error = current?.error ?? null;
  const ok = current && !error ? current : null;

  function insert(name: string) {
    const el = ref.current;
    const at = el?.selectionStart ?? value.length;
    const text = quoteColumn(name);
    onChange(value.slice(0, at) + text + value.slice(el?.selectionEnd ?? at));
    requestAnimationFrame(() => {
      el?.focus();
      el?.setSelectionRange(at + text.length, at + text.length);
    });
  }

  return (
    <div className="flex min-w-0 flex-col gap-2">
      <Textarea
        ref={ref}
        id={id}
        mono
        rows={3}
        maxLength={1000}
        aria-label={label}
        aria-invalid={Boolean(error) || undefined}
        aria-describedby={`${id}-status`}
        spellCheck={false}
        value={value}
        placeholder={use === 'filter' ? "e.g. `total` > 100 AND `status` <> 'test'" : "e.g. UPPER(`city`) || ' ' || `country`"}
        onChange={(e) => onChange(e.target.value)}
        onBlur={() => {
          // Adopt the canonical text: the server stores it, so the step shows what is saved.
          if (ok?.canonical && ok.canonical !== value) onChange(ok.canonical);
        }}
      />
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        {columns.length ? (
          <div className="w-56 max-w-full">
            <Select label={`${label}: insert a column`} value="" placeholder="Insert a column" options={columns.map((c) => ({ value: c.name, label: c.name, description: c.type }))} onChange={insert} />
          </div>
        ) : null}
        <Meta>Flink CDC functions, checked as you type. Text in single quotes, column names in back-quotes.</Meta>
      </div>
      <div id={`${id}-status`} aria-live="polite" className="min-w-0 text-xs">
        {!value.trim() ? null : check.isFetching || typed !== value ? (
          <span className="text-muted">Checking…</span>
        ) : error ? (
          <span role="alert" className="text-bad">
            {error.message}
            {error.offset < value.length ? ` (at “${value.slice(error.offset, error.offset + Math.max(error.length, 1))}”, character ${error.offset + 1})` : ''}
          </span>
        ) : ok ? (
          <span className="wrap-anywhere text-muted">
            Saved as <code className="font-mono text-ink">{ok.canonical}</code>
            {ok.kind ? ` · ${KIND_WORDS[ok.kind] ?? ok.kind}` : ''}
            {ok.deterministic === false ? ' · its value is when the row is written' : ''}
          </span>
        ) : check.isError ? (
          <span className="text-muted">Couldn’t check it now; the final checks will.</span>
        ) : null}
      </div>
    </div>
  );
}
