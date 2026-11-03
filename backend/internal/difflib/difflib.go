// Package difflib is a faithful port of Python's difflib.SequenceMatcher and
// unified_diff for lists of lines, so diffs shown to users are identical.
package difflib

import (
	"fmt"
	"sort"
)

type match struct{ a, b, size int }

type opcode struct {
	tag            byte // 'r'eplace, 'd'elete, 'i'nsert, 'e'qual
	i1, i2, j1, j2 int
}

type matcher struct {
	a, b []string
	b2j  map[string][]int
}

func newMatcher(a, b []string) *matcher {
	m := &matcher{a: a, b: b, b2j: map[string][]int{}}
	for i, elt := range b {
		m.b2j[elt] = append(m.b2j[elt], i)
	}
	// autojunk: purge popular elements of long sequences.
	if n := len(b); n >= 200 {
		ntest := n/100 + 1
		for elt, idxs := range m.b2j {
			if len(idxs) > ntest {
				delete(m.b2j, elt)
			}
		}
	}
	return m
}

func (m *matcher) findLongestMatch(alo, ahi, blo, bhi int) match {
	besti, bestj, bestsize := alo, blo, 0
	j2len := map[int]int{}
	for i := alo; i < ahi; i++ {
		newj2len := map[int]int{}
		for _, j := range m.b2j[m.a[i]] {
			if j < blo {
				continue
			}
			if j >= bhi {
				break
			}
			k := j2len[j-1] + 1
			newj2len[j] = k
			if k > bestsize {
				besti, bestj, bestsize = i-k+1, j-k+1, k
			}
		}
		j2len = newj2len
	}
	// No junk (isjunk is None), so only popular elements were purged: extend with equal
	// elements on both sides (the "not isbjunk" loops; the junk loops never apply).
	for besti > alo && bestj > blo && m.a[besti-1] == m.b[bestj-1] {
		besti, bestj, bestsize = besti-1, bestj-1, bestsize+1
	}
	for besti+bestsize < ahi && bestj+bestsize < bhi && m.a[besti+bestsize] == m.b[bestj+bestsize] {
		bestsize++
	}
	return match{besti, bestj, bestsize}
}

func (m *matcher) matchingBlocks() []match {
	la, lb := len(m.a), len(m.b)
	queue := [][4]int{{0, la, 0, lb}}
	var blocks []match
	for len(queue) > 0 {
		q := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		alo, ahi, blo, bhi := q[0], q[1], q[2], q[3]
		x := m.findLongestMatch(alo, ahi, blo, bhi)
		if x.size > 0 {
			blocks = append(blocks, x)
			if alo < x.a && blo < x.b {
				queue = append(queue, [4]int{alo, x.a, blo, x.b})
			}
			if x.a+x.size < ahi && x.b+x.size < bhi {
				queue = append(queue, [4]int{x.a + x.size, ahi, x.b + x.size, bhi})
			}
		}
	}
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].a != blocks[j].a {
			return blocks[i].a < blocks[j].a
		}
		if blocks[i].b != blocks[j].b {
			return blocks[i].b < blocks[j].b
		}
		return blocks[i].size < blocks[j].size
	})
	var out []match
	i1, j1, k1 := 0, 0, 0
	for _, blk := range blocks {
		if i1+k1 == blk.a && j1+k1 == blk.b {
			k1 += blk.size
		} else {
			if k1 > 0 {
				out = append(out, match{i1, j1, k1})
			}
			i1, j1, k1 = blk.a, blk.b, blk.size
		}
	}
	if k1 > 0 {
		out = append(out, match{i1, j1, k1})
	}
	return append(out, match{la, lb, 0})
}

func (m *matcher) opcodes() []opcode {
	i, j := 0, 0
	var out []opcode
	for _, blk := range m.matchingBlocks() {
		var tag byte
		switch {
		case i < blk.a && j < blk.b:
			tag = 'r'
		case i < blk.a:
			tag = 'd'
		case j < blk.b:
			tag = 'i'
		}
		if tag != 0 {
			out = append(out, opcode{tag, i, blk.a, j, blk.b})
		}
		i, j = blk.a+blk.size, blk.b+blk.size
		if blk.size > 0 {
			out = append(out, opcode{'e', blk.a, i, blk.b, j})
		}
	}
	return out
}

func (m *matcher) groupedOpcodes(n int) [][]opcode {
	codes := m.opcodes()
	if len(codes) == 0 {
		codes = []opcode{{'e', 0, 1, 0, 1}}
	}
	if c := codes[0]; c.tag == 'e' {
		codes[0] = opcode{'e', max(c.i1, c.i2-n), c.i2, max(c.j1, c.j2-n), c.j2}
	}
	if c := codes[len(codes)-1]; c.tag == 'e' {
		codes[len(codes)-1] = opcode{'e', c.i1, min(c.i2, c.i1+n), c.j1, min(c.j2, c.j1+n)}
	}
	nn := n + n
	var groups [][]opcode
	var group []opcode
	for _, c := range codes {
		i1, i2, j1, j2 := c.i1, c.i2, c.j1, c.j2
		if c.tag == 'e' && i2-i1 > nn {
			group = append(group, opcode{'e', i1, min(i2, i1+n), j1, min(j2, j1+n)})
			groups = append(groups, group)
			group = nil
			i1, j1 = max(i1, i2-n), max(j1, j2-n)
		}
		group = append(group, opcode{c.tag, i1, i2, j1, j2})
	}
	if len(group) > 0 && !(len(group) == 1 && group[0].tag == 'e') {
		groups = append(groups, group)
	}
	return groups
}

func formatRange(start, stop int) string {
	beginning := start + 1
	length := stop - start
	if length == 1 {
		return fmt.Sprintf("%d", beginning)
	}
	if length == 0 {
		beginning--
	}
	return fmt.Sprintf("%d,%d", beginning, length)
}

// UnifiedDiff is difflib.unified_diff(a, b, fromfile, tofile, lineterm="", n=n).
func UnifiedDiff(a, b []string, fromfile, tofile string, n int) []string {
	var out []string
	started := false
	for _, group := range newMatcher(a, b).groupedOpcodes(n) {
		if !started {
			started = true
			out = append(out, "--- "+fromfile, "+++ "+tofile)
		}
		first, last := group[0], group[len(group)-1]
		out = append(out, fmt.Sprintf("@@ -%s +%s @@", formatRange(first.i1, last.i2), formatRange(first.j1, last.j2)))
		for _, c := range group {
			if c.tag == 'e' {
				for _, line := range a[c.i1:c.i2] {
					out = append(out, " "+line)
				}
				continue
			}
			if c.tag == 'r' || c.tag == 'd' {
				for _, line := range a[c.i1:c.i2] {
					out = append(out, "-"+line)
				}
			}
			if c.tag == 'r' || c.tag == 'i' {
				for _, line := range b[c.j1:c.j2] {
					out = append(out, "+"+line)
				}
			}
		}
	}
	if out == nil {
		out = []string{}
	}
	return out
}
