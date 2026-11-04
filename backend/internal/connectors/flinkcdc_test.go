package connectors

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// Flink CDC reads server ids as signed 32-bit numbers; a range never crosses a block of 8.
func TestMyServerIDRange(t *testing.T) {
	for i := range 2000 {
		r := MyServerIDRange(fmt.Sprintf("pipeline-%d", i), 8)
		lo, hi, _ := strings.Cut(r, "-")
		a, _ := strconv.ParseInt(lo, 10, 64)
		b, _ := strconv.ParseInt(hi, 10, 64)
		if a < 1_000_000_000 || b > 1<<31-1 || b-a != 7 || a%8 != 0 {
			t.Fatalf("%s", r)
		}
	}
	if a, b := MyServerIDRange("p", 2), MyServerIDRange("p", 2); a != b {
		t.Fatal("not stable")
	}
}

func TestFlinkTableNames(t *testing.T) {
	if err := FlinkTableNames([]string{"shop.orders", "Shop_2.Order_lines"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"shop.order-lines", "shop.orders$", "a.b.c", "orders", "shop.ord.ers"} {
		if FlinkTableNames([]string{bad}) == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}
