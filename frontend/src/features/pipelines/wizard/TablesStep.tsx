import { useEffect, useImperativeHandle, useMemo, useState, type Ref } from 'react';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Search } from '@/components/ui/form';
import { Card, QueryState, Toolbar } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { DataTable } from '@/components/ui/table';
import { Meta, plural, Short } from '@/components/ui/text';
import { useConnectorTypes, useDiscovery } from '@/lib/connections';
import { formatBytes, formatCount } from '@/lib/format';
import type { Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { fullSpec, pruneToTables, type FullSpec } from '../spec';
import { useSaveSpec, type StepHandle } from '../useSaveSpec';
import { PersonalDataCard } from './PersonalDataCard';
import { canMask as canMaskType, defaultsFor, personalProblem, personalRows, rulesToSave, setChoice, type ColumnRulesMap } from './personal';
import { StepHeader } from './shared';

/**
 * Step 2: which tables, and what to do with each column that looks like personal data.
 * Nothing is selected by default. A table picked for the first time protects its personal-data
 * columns (masked when they can be hashed, removed otherwise); saved choices are never changed.
 */
export function TablesStep({
  pipeline,
  handle,
  onReady,
  onBack,
  onNext,
}: {
  pipeline: Pipeline;
  handle: Ref<StepHandle>;
  onReady: (ready: boolean) => void;
  onBack: () => void;
  onNext: () => void;
}) {
  const { project, expert } = useProject();
  const discovery = useDiscovery(project.id, pipeline.source.id);
  const spec = useMemo(() => fullSpec(pipeline.spec), [pipeline.spec]);
  const [selected, setSelected] = useState<Set<string>>(new Set(spec.source.tables.include));
  const [query, setQuery] = useState('');
  const [schema, setSchema] = useState('');
  // Choices per table, also for tables unticked in this visit (ticking one again restores them).
  const [rules, setRules] = useState<ColumnRulesMap>(spec.source.columns);
  const connectorTypes = useConnectorTypes(project.id);
  const sourceType = connectorTypes.data?.find((t) => t.kind === 'source' && t.type === pipeline.source.type);
  const caps = sourceType?.transforms as { maskable_types?: string[] } | undefined;
  const maskable = sourceType?.supports_masking === false ? [] : caps?.maskable_types;
  const { save, pending, error } = useSaveSpec(pipeline);
  const tables = useMemo(() => discovery.data?.tables ?? [], [discovery.data]);
  const schemas = [...new Set(tables.map((t) => t.schema))].sort();
  const visible = tables.filter((t) => (!schema || t.schema === schema) && (!query || `${t.schema}.${t.name}`.toLowerCase().includes(query.toLowerCase())));
  const columnsOf = (id: string) => tables.find((t) => `${t.schema}.${t.name}` === id)?.columns ?? undefined;
  const include = [...selected].sort();
  const rows = personalRows(include, columnsOf, rules, maskable);
  const ruled = new Set(rows.map((r) => `${r.table}.${r.column}`));
  const candidates = include.flatMap((id) =>
    (columnsOf(id) ?? [])
      .filter((c) => !ruled.has(`${id}.${c.name}`))
      .map((c) => ({ table: id, column: c.name, type: c.type, maskable: canMaskType(maskable, c.type) })),
  );

  function pick(next: Set<string>) {
    // A table picked for the first time protects its personal data; known choices stay.
    const added = [...next].filter((id) => !selected.has(id) && !(id in rules));
    if (added.length) {
      setRules((r) => {
        const out = { ...r };
        for (const id of added) out[id] = defaultsFor(columnsOf(id) ?? [], maskable);
        return out;
      });
    }
    setSelected(next);
  }
  const choose = (table: string, column: string, choice: Parameters<typeof setChoice>[2]) =>
    setRules((r) => ({ ...r, [table]: setChoice(r[table], column, choice) }));

  function nextSpec(): FullSpec {
    const columns = rulesToSave(rules, include, spec.source.columns);
    const { keys, ...rest } = { ...spec, ...pruneToTables(spec, include) };
    return { ...rest, ...(keys ? { keys } : {}), source: { tables: { include }, columns } };
  }
  const dirty = () => discovery.isSuccess && JSON.stringify(nextSpec()) !== JSON.stringify(spec);
  useImperativeHandle(handle, () => ({
    dirty,
    commit: async () => (selected.size === 0 ? false : dirty() ? save({ spec: nextSpec() }) : true),
  }));
  const blocked = personalProblem(rows)?.blocking ?? false;
  const isReady = selected.size > 0 && !blocked;
  useEffect(() => onReady(isReady), [isReady, onReady]);

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <StepHeader title="Which tables?" description="Pick the tables to capture. Nothing is selected by default." />
      <Card pad={false}>
        <div className="border-b border-line px-card py-3">
          <Toolbar>
            <Search label="Search tables" placeholder={`Search ${tables.length.toLocaleString()} tables…`} value={query} onChange={setQuery} />
            {schemas.length > 1 ? (
              <div className="min-w-0 flex-[0_1_220px]">
                <label htmlFor="tables-schema" className="sr-only">
                  Schema
                </label>
                <Select
                  id="tables-schema"
                  value={schema}
                  onChange={setSchema}
                  placeholder="All schemas"
                  options={[{ value: '', label: 'All schemas' }, ...schemas.map((s) => ({ value: s, label: s }))]}
                />
              </div>
            ) : null}
          </Toolbar>
        </div>
        <QueryState query={discovery} doing="Couldn’t read the source’s tables">
          {() => (
            <DataTable
              label="Tables"
              rows={visible}
              rowKey={(t) => `${t.schema}.${t.name}`}
              checkLabel={(t) => `${t.schema}.${t.name}`}
              noun="tables"
              selection={{
                selected,
                onChange: pick,
                why: (t) => (!t.selectable && !selected.has(`${t.schema}.${t.name}`) ? (t.warnings?.[0] ?? 'BeziSync can’t capture this table.') : null),
              }}
              empty={
                <Empty title={query || schema ? 'No table matches' : 'No tables found'}>
                  {query || schema ? 'Clear the search or the schema.' : 'The source has no tables BeziSync can see.'}
                </Empty>
              }
              columns={[
                { id: 'table', header: 'Table', min: 200, cell: (t) => <Short max={48} mono>{`${t.schema}.${t.name}`}</Short> },
                {
                  id: 'rows',
                  header: 'Rows (est.)',
                  fit: true,
                  align: 'right',
                  cell: (t) => (
                    <span className="font-mono text-sm">
                      {formatCount(t.rows_estimate)}
                      {t.bytes_estimate ? <Meta className="block">{formatBytes(t.bytes_estimate)}</Meta> : null}
                    </span>
                  ),
                },
                {
                  id: 'key',
                  header: 'Key',
                  priority: 2,
                  min: 120,
                  cell: (t) =>
                    t.primary_key?.length ? <span className="text-sm wrap-anywhere text-ok">✔ {t.primary_key.join(', ')}</span> : <Pill tone="warn">none</Pill>,
                },
                {
                  id: 'notes',
                  header: 'Notes',
                  priority: 3,
                  min: 160,
                  wide: true,
                  cell: (t) => {
                    const pii = (t.columns ?? []).filter((c) => c.pii).map((c) => c.name);
                    return (
                      <div className="flex min-w-0 flex-col gap-0.5 text-sm">
                        {(t.warnings ?? []).map((w) => (
                          <span key={w} className="text-warn">
                            ▲ {w}
                          </span>
                        ))}
                        {pii.length ? <span className="wrap-anywhere text-ink-2">Personal data: {pii.join(', ')}</span> : null}
                      </div>
                    );
                  },
                },
              ]}
            />
          )}
        </QueryState>
        <div className="flex flex-wrap items-center gap-x-3 border-t border-line px-card py-3 text-sm text-muted">
          <span aria-live="polite">{plural(selected.size, 'table')} chosen</span>
          <span>
            · showing {Math.min(visible.length, 100).toLocaleString()} of {tables.length.toLocaleString()}
          </span>
        </div>
      </Card>
      {discovery.data?.truncated ? <Notice tone="warn">The source has more than 10,000 tables; only the first 10,000 are listed.</Notice> : null}
      {discovery.isSuccess && selected.size ? (
        <PersonalDataCard
          rows={rows}
          candidates={candidates}
          sourceCanMask={Boolean(maskable?.length)}
          sourceTitle={sourceType?.title ?? pipeline.source.type}
          expert={expert}
          onChoose={choose}
        />
      ) : null}
      <InlineError error={error} doing="Couldn’t save the tables" />
      <div className="flex flex-wrap justify-between gap-3">
        <Button onClick={onBack}>← Back</Button>
        <Button
          variant="primary"
          disabled={!isReady}
          why={selected.size === 0 ? 'Choose at least one table.' : 'Choose Remove or Keep as is for the columns that can’t be masked.'}
          loading={pending}
          onClick={onNext}
        >
          Continue to destination →
        </Button>
      </div>
    </div>
  );
}
