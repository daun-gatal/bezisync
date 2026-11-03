import { useRef, type ReactNode } from 'react';
import { ChipsInput, Field, Input, Segmented, Switch, Textarea } from '@/components/ui/form';
import { Select } from '@/components/ui/select';

/** The subset of JSON Schema that connector models produce (Pydantic v2). */
export interface JsonSchemaProp {
  type?: string;
  title?: string;
  description?: string;
  default?: unknown;
  enum?: string[];
  anyOf?: JsonSchemaProp[];
  minimum?: number;
  maximum?: number;
  ui?: {
    group?: string;
    expert?: boolean;
    placeholder?: string;
    help?: string;
    multiline?: boolean;
    /** Display names (and one-line help) of enum values. */
    labels?: Record<string, string>;
    descriptions?: Record<string, string>;
    /** Shown only while each named field has one of the listed values. */
    when?: Record<string, unknown[]>;
    /** Kept in the values but never rendered (the wizard fills it). */
    hidden?: boolean;
    /** What fills a hidden field: "table_keys" maps each captured table to its first key column. */
    from?: string;
    /** The field holds another connection of the project, of this "kind/type" (e.g. "sink/kafka"). */
    connection?: string;
  };
}

export interface JsonSchema {
  properties?: Record<string, JsonSchemaProp>;
  required?: string[];
}

export type FormValues = Record<string, unknown>;

/** What a secret field's custom control gets (e.g. a secret-store picker around the plain input). */
export interface SecretFieldProps {
  /** The id the field's label points at: put it on the control that is shown. */
  id: string;
  name: string;
  title: string;
  value: unknown;
  onChange: (value: string) => void;
  /** The saved secret-store reference, if the field holds one. */
  savedRef?: string;
  /** A value is saved: blank keeps it. */
  isSet: boolean;
  /** The plain input, for typing the value. */
  input: ReactNode;
  hint?: ReactNode;
}

/** Connections a linked field can point at, by "kind/type". */
export type LinkOptions = Record<string, { value: string; label: string }[]>;

interface FieldDef {
  name: string;
  prop: JsonSchemaProp;
  /** `list` is an array of strings, edited one value at a time as chips. */
  kind: 'string' | 'integer' | 'number' | 'boolean' | 'enum' | 'list';
  options: string[];
  nullable: boolean;
  required: boolean;
}

export function fieldsOf(schema: JsonSchema): FieldDef[] {
  const required = new Set(schema.required ?? []);
  return Object.entries(schema.properties ?? {}).map(([name, prop]) => {
    const variants = prop.anyOf ?? [prop];
    const nonNull = variants.find((v) => v.type !== 'null') ?? prop;
    const nullable = variants.some((v) => v.type === 'null');
    const options = nonNull.enum ?? [];
    const raw = nonNull.type as string | undefined;
    const type: FieldDef['kind'] = options.length ? 'enum' : raw === 'array' ? 'list' : ((raw as FieldDef['kind'] | undefined) ?? 'string');
    return { name, prop, kind: type, options, nullable, required: required.has(name) };
  });
}

/**
 * Whether a field applies, given the other fields (its `ui.when`; unset fields read as their default).
 * `context` holds the values of the sibling form: a secret field can depend on a config field.
 */
export function isVisible(prop: JsonSchemaProp, values: FormValues, schema: JsonSchema, context: FormValues = {}): boolean {
  const when = prop.ui?.when;
  if (!when) return true;
  return Object.entries(when).every(([name, allowed]) => allowed.includes(values[name] ?? context[name] ?? schema.properties?.[name]?.default));
}

/** Defaults from the schema, overlaid with a provider preset's defaults. */
export function defaultsOf(schema: JsonSchema, overrides: FormValues = {}): FormValues {
  const out: FormValues = {};
  for (const f of fieldsOf(schema)) {
    if (f.prop.default !== undefined && f.prop.default !== null) out[f.name] = f.prop.default;
  }
  return { ...out, ...overrides };
}

function label(name: string, prop: JsonSchemaProp): string {
  return prop.title ?? name.replace(/_/g, ' ').replace(/^\w/, (c) => c.toUpperCase());
}

/** Drops empty optional values, and fields that don't apply, so the API applies its own defaults. */
export function cleanValues(schema: JsonSchema, values: FormValues, context: FormValues = {}): FormValues {
  const out: FormValues = {};
  for (const f of fieldsOf(schema)) {
    if (!isVisible(f.prop, values, schema, context)) continue;
    const v = values[f.name];
    if (v === '' || v === undefined || (v === null && f.nullable)) continue;
    if (f.kind === 'list') {
      out[f.name] = (Array.isArray(v) ? v : String(v).split(',')).map((x) => String(x).trim()).filter(Boolean);
      continue;
    }
    out[f.name] = f.kind === 'integer' || f.kind === 'number' ? Number(v) : v;
  }
  return out;
}

export function SchemaForm({
  schema,
  values,
  onChange,
  expert,
  secret,
  setSecrets = [],
  secretRefs = {},
  errors = {},
  idPrefix,
  links = {},
  context = {},
  renderSecret,
}: {
  schema: JsonSchema;
  values: FormValues;
  onChange: (next: FormValues) => void;
  expert: boolean;
  /** Render as write-only secret inputs. */
  secret?: boolean;
  /** Secret field names already stored: blank input keeps them. */
  setSecrets?: string[];
  /** Secret fields stored as secret-store references (field → ref+store://path#key). */
  secretRefs?: Record<string, string>;
  errors?: Record<string, string>;
  idPrefix: string;
  /** Choices of fields that point at another connection (ui.connection). */
  links?: LinkOptions;
  /** The sibling form's values, for `ui.when` rules that name its fields. */
  context?: FormValues;
  /** Wraps each secret field's input (it owns the hint then). */
  renderSecret?: (props: SecretFieldProps) => ReactNode;
}) {
  const fields = fieldsOf(schema).filter((f) => !f.prop.ui?.hidden && isVisible(f.prop, values, schema, context)).filter((f) => expert || !f.prop.ui?.expert || (values[f.name] !== undefined && values[f.name] !== f.prop.default));
  const groups = new Map<string, FieldDef[]>();
  for (const f of fields) {
    const g = f.prop.ui?.group ?? 'Settings';
    groups.set(g, [...(groups.get(g) ?? []), f]);
  }
  const set = (name: string, value: unknown) => onChange({ ...values, [name]: value });

  return (
    <div className="flex min-w-0 flex-col gap-5">
      {[...groups.entries()].map(([group, items]) => (
        <fieldset key={group} className="m-0 grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,240px),1fr))] gap-4 border-0 p-0">
          <legend className="mb-2 text-xs font-semibold tracking-wide text-muted uppercase">{group}</legend>
          {items.map((f) => {
            const id = `${idPrefix}-${f.name}`;
            const value = values[f.name];
            const hint = f.prop.ui?.help ?? f.prop.description;
            const isSet = setSecrets.includes(f.name);
            const ref = secretRefs[f.name];
            // A reference (ref+store://path#key) is not secret: show it as text.
            const isRef = secret && typeof value === 'string' && value.startsWith('ref+');
            const keepHint = ref ? `From ${ref}. Leave blank to keep it.` : 'Stored. Leave blank to keep the current value.';
            // A boolean with labels for both values reads as a choice, not a switch.
            const choice = f.prop.ui?.labels;
            if (f.kind === 'boolean' && choice?.true && choice.false) {
              return (
                <Field key={f.name} label={label(f.name, f.prop)} hint={hint} group className="col-span-full">
                  {() => (
                    <Segmented
                      label={label(f.name, f.prop)}
                      value={value === undefined ? String(Boolean(f.prop.default)) : String(Boolean(value))}
                      onChange={(v) => set(f.name, v === 'true')}
                      options={[
                        { value: 'true', label: choice.true },
                        { value: 'false', label: choice.false },
                      ]}
                    />
                  )}
                </Field>
              );
            }
            if (f.kind === 'boolean') {
              return (
                <div key={f.name} className="col-span-full">
                  <Switch id={id} checked={Boolean(value)} onChange={(on) => set(f.name, on)} label={label(f.name, f.prop)} description={hint} />
                </div>
              );
            }
            const wide = f.prop.ui?.multiline || f.kind === 'list' || Boolean(secret && renderSecret);
            const fieldHint = secret && isSet ? keepHint : hint;
            const control = (fid: string) =>
                  f.prop.ui?.connection ? (
                    <Select
                      id={fid}
                      value={String(value ?? '')}
                      onChange={(v) => set(f.name, v || null)}
                      placeholder={(links[f.prop.ui.connection] ?? []).length ? 'Choose a connection' : 'None in this project yet'}
                      options={links[f.prop.ui.connection] ?? []}
                    />
                  ) : f.kind === 'enum' ? (
                    <Select
                      id={fid}
                      value={String(value ?? '')}
                      onChange={(v) => set(f.name, v || null)}
                      placeholder="Not set"
                      options={[
                        ...(f.nullable ? [{ value: '', label: 'Not set' }] : []),
                        ...f.options.map((o) => ({ value: o, label: f.prop.ui?.labels?.[o] ?? o, description: f.prop.ui?.descriptions?.[o] })),
                      ]}
                    />
                  ) : f.kind === 'list' ? (
                    <ChipsInput
                      id={fid}
                      values={
                        Array.isArray(value)
                          ? value.map(String)
                          : typeof value === 'string' && value
                            ? value
                                .split(',')
                                .map((x) => x.trim())
                                .filter(Boolean)
                            : []
                      }
                      onChange={(v) => set(f.name, v)}
                      placeholder={f.prop.ui?.placeholder}
                    />
                  ) : f.prop.ui?.multiline ? (
                    <div className="flex min-w-0 flex-col gap-1.5">
                      <Textarea
                        id={fid}
                        mono
                        value={String(value ?? '')}
                        placeholder={secret && isSet ? '••••••' : f.prop.ui?.placeholder}
                        onChange={(e) => set(f.name, e.target.value)}
                      />
                      <FileLoader label={`Load ${label(f.name, f.prop).toLowerCase()} from a file`} onLoad={(text) => set(f.name, text)} />
                    </div>
                  ) : (
                    <Input
                      id={fid}
                      type={secret && !isRef ? 'password' : 'text'}
                      inputMode={f.kind === 'integer' || f.kind === 'number' ? 'numeric' : undefined}
                      autoComplete={secret ? 'new-password' : 'off'}
                      spellCheck={false}
                      value={value === undefined || value === null ? '' : Array.isArray(value) ? value.join(', ') : String(value)}
                      placeholder={secret && isSet ? '••••••' : f.prop.ui?.placeholder}
                      required={f.required && !(secret && isSet)}
                      onChange={(e) => set(f.name, f.kind === 'integer' || f.kind === 'number' ? e.target.value.replace(/[^\d.-]/g, '') : e.target.value)}
                    />
                  );
            return (
              <Field
                key={f.name}
                label={label(f.name, f.prop)}
                optional={!f.required && !secret ? f.nullable : !f.required && secret}
                hint={secret && renderSecret ? undefined : fieldHint}
                error={errors[f.name]}
                className={wide ? 'col-span-full' : undefined}
              >
                {(fid) =>
                  secret && renderSecret
                    ? renderSecret({
                        id: fid,
                        name: f.name,
                        title: label(f.name, f.prop),
                        value,
                        onChange: (v) => set(f.name, v),
                        savedRef: ref,
                        isSet,
                        input: control(fid),
                        hint: fieldHint,
                      })
                    : control(fid)
                }
              </Field>
            );
          })}
        </fieldset>
      ))}
    </div>
  );
}

/** Reads a small text file (a kubeconfig, a key, a certificate) into a field; nothing is uploaded. */
function FileLoader({ label, onLoad }: { label: string; onLoad: (text: string) => void }) {
  const input = useRef<HTMLInputElement>(null);
  return (
    <div>
      <button type="button" className="text-sm font-medium text-accent hover:underline" onClick={() => input.current?.click()}>
        Load from a file
      </button>
      <input
        ref={input}
        type="file"
        aria-label={label}
        className="sr-only"
        tabIndex={-1}
        onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = '';
          if (file && file.size <= 1_000_000) void file.text().then(onLoad);
        }}
      />
    </div>
  );
}
