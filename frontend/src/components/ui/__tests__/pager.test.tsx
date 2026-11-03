import { afterEach, describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { CompactPager, CursorPager, DEFAULT_PAGE_SIZE, Pager, clampPage, pageItems, rangeLabel, readPageSize } from '../pager';

afterEach(() => window.localStorage.clear());

describe('page arithmetic', () => {
  it('shows at most seven page buttons around the current page', () => {
    expect(pageItems(1, 5)).toEqual([1, 2, 3, 4, 5]);
    expect(pageItems(1, 12)).toEqual([1, 2, 3, 4, 5, 'gap', 12]);
    expect(pageItems(6, 12)).toEqual([1, 'gap', 5, 6, 7, 'gap', 12]);
    expect(pageItems(12, 12)).toEqual([1, 'gap', 8, 9, 10, 11, 12]);
    for (let p = 1; p <= 30; p++) expect(pageItems(p, 30).length).toBeLessThanOrEqual(7);
  });

  it('clamps the page when the list shrinks', () => {
    expect(clampPage(5, 30, 20)).toBe(2);
    expect(clampPage(0, 30, 20)).toBe(1);
    expect(clampPage(3, 0, 20)).toBe(1);
    expect(rangeLabel(2, 20, 312)).toBe('21–40 of 312');
  });

  it('uses 20 rows unless the viewer picked one of the offered sizes', () => {
    expect(readPageSize()).toBe(DEFAULT_PAGE_SIZE);
    window.localStorage.setItem('bz.pageSize', '7');
    expect(readPageSize()).toBe(20);
    window.localStorage.setItem('bz.pageSize', '50');
    expect(readPageSize()).toBe(50);
  });
});

describe('pagers', () => {
  it('the full pager counts the rows and moves between pages', async () => {
    function Harness() {
      const [page, setPage] = useState(1);
      return <Pager total={45} page={page} onPage={setPage} noun="pipelines" />;
    }
    render(<Harness />);
    expect(screen.getByText('1–20 of 45 pipelines')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: /Next/ }));
    expect(screen.getByText('21–40 of 45 pipelines')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Page 2' })).toHaveAttribute('aria-current', 'page');
  });

  it('the full pager shows only the count when everything fits', () => {
    render(<Pager total={3} page={1} onPage={() => {}} noun="sources" />);
    expect(screen.getByText('3 sources')).toBeInTheDocument();
    expect(screen.queryByRole('navigation')).not.toBeInTheDocument();
  });

  it('the compact pager disappears when everything fits', () => {
    const { container } = render(<CompactPager total={20} page={1} onPage={() => {}} noun="versions" />);
    expect(container).toBeEmptyDOMElement();
  });

  it('the compact pager pages a cursor list without a total', async () => {
    let asked = 0;
    render(<CompactPager total={20} page={1} hasOlder onPage={(p) => (asked = p)} noun="events" />);
    expect(screen.getByText('Newest 20')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Newer events' })).toBeDisabled();
    await userEvent.click(screen.getByRole('button', { name: 'Older events' }));
    expect(asked).toBe(2);
  });

  it('the cursor pager says which rows are shown', () => {
    render(<CursorPager page={3} count={20} hasOlder={false} onNewer={() => {}} onOlder={() => {}} noun="changes" />);
    expect(screen.getByText('41–60 changes')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Older/ })).toBeDisabled();
  });
});
