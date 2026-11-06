# ui

## Overview
The design system: every primitive screens are built from (text, buttons, feedback, form controls, selects, layout, tables, tabs, overlays, paging). It is the only place allowed to set its own sizes and break the layout rules that `src/test/source-rules.test.ts` enforces on everything else; features and `components/` compose these instead of styling raw elements.

## Interface
- Text (`text.tsx`): `[text.tsx:Title]` (wraps and clamps, full text as tooltip), `[text.tsx:Short]` (shortens in the middle via `[text.tsx:shorten]`), `[text.tsx:Ident]` (mono identifiers on detail pages), `[text.tsx:Meta]`, `[text.tsx:plural]`.
- Buttons (`button.tsx`): `[button.tsx:Button]` (`[button.tsx:ButtonProps]`, variants `[button.tsx:ButtonVariant]`, `loading`, `why` for disabled), `[button.tsx:ButtonLink]` / `[button.tsx:ButtonAnchor]` (links that look like buttons), `[button.tsx:buttonClass]`.
- Feedback (`feedback.tsx`): `[feedback.tsx:Pill]` (status: glyph + text, `[feedback.tsx:Tone]`, with `[feedback.tsx:TONE_BG]` / `[feedback.tsx:TONE_TEXT]` classes), `[feedback.tsx:InlineError]`, `[feedback.tsx:Notice]`, `[feedback.tsx:Skeleton]`, `[feedback.tsx:Empty]`, `[feedback.tsx:Meter]`.
- Forms (`form.tsx`): `[form.tsx:Field]`, `[form.tsx:controlClass]`, `[form.tsx:Input]`, `[form.tsx:Textarea]`, `[form.tsx:Search]`, `[form.tsx:SlugInput]` (with `[form.tsx:slugError]`), `[form.tsx:Stepper]`, `[form.tsx:Switch]`, `[form.tsx:Check]`, `[form.tsx:RowCheck]`, `[form.tsx:RadioCards]`, `[form.tsx:CardOption]`, `[form.tsx:Segmented]`, `[form.tsx:ChipsInput]`.
- Selects (`select.tsx`): `[select.tsx:Select]`, `[select.tsx:Combobox]`, `[select.tsx:TimeRange]`, over `[select.tsx:Option]` items.
- Layout (`layout.tsx`): `[layout.tsx:Page]`, `[layout.tsx:PageHeader]`, `[layout.tsx:Card]`, `[layout.tsx:Split]`, `[layout.tsx:Stack]`, `[layout.tsx:KeyValue]`, `[layout.tsx:Metric]`, `[layout.tsx:Toolbar]`, `[layout.tsx:Crumb]`, `[layout.tsx:QueryState]` (loading / error with retry / data).
- Tables (`table.tsx`): `[table.tsx:DataTable]` (`[table.tsx:Column]` with `min` and `priority`, `[table.tsx:Selection]`, paging), `[table.tsx:Cell]`, `[table.tsx:List]` / `[table.tsx:ListRow]`.
- Tabs (`tabs.tsx`): `[tabs.tsx:Tabs]` (`[tabs.tsx:TabItem]`), `[tabs.tsx:TabPanel]`.
- Overlays (`overlays.tsx`): `[overlays.tsx:Modal]`, `[overlays.tsx:Confirm]`, `[overlays.tsx:OneTimeSecret]`, `[overlays.tsx:CodeBlock]`, `[overlays.tsx:Menu]` / `[overlays.tsx:MenuEntry]`.
- Paging (`pager.tsx`): `[pager.tsx:Pager]`, `[pager.tsx:CompactPager]`, `[pager.tsx:CursorPager]`, `[pager.tsx:PagedBox]`, `[pager.tsx:usePageSize]`, `[pager.tsx:usePaged]`, `[pager.tsx:useScrollTopOn]`, `[pager.tsx:scrollBox]`, and the pure `[pager.tsx:pageCount]`, `[pager.tsx:clampPage]`, `[pager.tsx:pageItems]`, `[pager.tsx:rangeLabel]`.
- Hooks (`hooks.ts`): `[hooks.ts:useElementWidth]`, `[hooks.ts:useDebounced]`, `[hooks.ts:readStored]` / `[hooks.ts:writeStored]` (the one `localStorage` wrapper).

## Invariants
- `[hooks.ts:readStored]` and `[hooks.ts:writeStored]` swallow storage errors (private windows): reads return null, writes are skipped (`null` removes the key); other modules use them rather than `localStorage` directly, except `lib/tour.ts`, which wraps its own access the same way.
- Page size is one of `[pager.tsx:PAGE_SIZES]` (10, 20, 50, 100), default `[pager.tsx:DEFAULT_PAGE_SIZE]` 20, stored under `[pager.tsx:PAGE_SIZE_KEY]` (`bz.pageSize`) and shared by every list; `[pager.tsx:readPageSize]` falls back to the default for any other stored value.
- `[table.tsx:DataTable]` hides columns by `priority` as it narrows and turns rows into cards below 560 px (`CARDS_BELOW`).
- `[overlays.tsx:Modal]` with `dirty` asks before discarding on Esc, outside click or ×; with `closable={false}` Esc and outside clicks do nothing.
- `[overlays.tsx:Confirm]` with `typeToConfirm` keeps its action disabled until the exact text is typed and clears the typed text every time it opens; it cannot be closed while `pending`.
- `[overlays.tsx:OneTimeSecret]` is not closable and its Done stays disabled until the "I've copied it" box is ticked (reset on every open).
- `[button.tsx:Button]` disabled with a `why` shows the reason as its tooltip (`title`) and stays focusable (`aria-disabled` instead of `disabled`, clicks ignored) so the reason can be hovered and read; a `loading` button ignores clicks too.
- `[layout.tsx:QueryState]` renders `[feedback.tsx:Skeleton]` while pending and `[feedback.tsx:InlineError]` with Try again on error; errors are shown inline, never as toasts.
- `[form.tsx:slugError]` rejects a name with no letter or digit and a slug already in `taken`.

## Side Effects
- `localStorage` through `[hooks.ts:writeStored]`; `[pager.tsx:usePageSize]` writes `bz.pageSize` and dispatches a `bz:page-size` window event so every list follows.
- `[overlays.tsx:CodeBlock]`'s copy button (also inside `[overlays.tsx:OneTimeSecret]`) writes to the clipboard (`navigator.clipboard.writeText`) and starts a 1.5 s timer to reset its icon.
- `[hooks.ts:useElementWidth]` attaches a `ResizeObserver` and disconnects it on unmount.

## Verification
- Run: `cd frontend && bun run test src/components/ui`
