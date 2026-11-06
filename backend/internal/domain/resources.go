package domain

import (
	"fmt"
	"regexp"
	"strconv"
)

// A Debezium Server pipeline runs at a preset size or a custom one (policies.resources "custom"
// with policies.custom_resources). Sizes in numbers, for limits and checks; compiler.Resources
// holds the presets as the container asks for them.

// ResourceAmount is a size: CPU in thousandths and memory in MiB.
type ResourceAmount struct{ CPUMilli, MemoryMiB int }

// ResourcePresets are the preset sizes.
var ResourcePresets = map[string]ResourceAmount{
	"small":  {CPUMilli: 500, MemoryMiB: 768},
	"medium": {CPUMilli: 1000, MemoryMiB: 1536},
	"large":  {CPUMilli: 2000, MemoryMiB: 3072},
}

// CustomResources is a custom size, in canonical form: CPU in quarters ("0.25" … "8"), memory in
// steps of 256 MiB ("512Mi" … "16Gi", whole GiB written as Gi).
type CustomResources struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// Custom size bounds.
const (
	MinCustomCPUMilli = 250
	MaxCustomCPUMilli = 8000
	MinCustomMemory   = 512
	MaxCustomMemory   = 16384
)

var (
	cpuRe    = regexp.MustCompile(`^(\d{1,2})(\.(25|5|75))?$`)
	memoryRe = regexp.MustCompile(`^([1-9]\d{0,5})(Mi|Gi)$`)
)

// ParseCPU reads a custom CPU ("1.5" → 1500); false unless canonical and in bounds.
func ParseCPU(s string) (int, bool) {
	milli, ok := ParseCPUAny(s)
	if !ok || milli < MinCustomCPUMilli || milli > MaxCustomCPUMilli {
		return 0, false
	}
	return milli, true
}

// ParseCPUAny reads a canonical CPU of any size (quarters; no leading zero).
func ParseCPUAny(s string) (int, bool) {
	m := cpuRe.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	whole, _ := strconv.Atoi(m[1])
	milli := whole * 1000
	switch m[3] {
	case "25":
		milli += 250
	case "5":
		milli += 500
	case "75":
		milli += 750
	}
	if (whole > 0 && m[1][0] == '0') || milli == 0 {
		return 0, false
	}
	return milli, true
}

// FormatCPU writes CPU thousandths the way a spec stores them (1500 → "1.5").
func FormatCPU(milli int) string {
	whole, frac := milli/1000, milli%1000
	switch frac {
	case 250:
		return fmt.Sprintf("%d.25", whole)
	case 500:
		return fmt.Sprintf("%d.5", whole)
	case 750:
		return fmt.Sprintf("%d.75", whole)
	}
	return strconv.Itoa(whole)
}

// ParseMemory reads a custom memory ("1536Mi" → 1536, "2Gi" → 2048); false unless canonical and in
// bounds.
func ParseMemory(s string) (int, bool) {
	mib, ok := ParseMemoryAny(s)
	if !ok || mib < MinCustomMemory || mib > MaxCustomMemory {
		return 0, false
	}
	return mib, true
}

// ParseMemoryAny reads a canonical memory of any size (steps of 256 MiB).
func ParseMemoryAny(s string) (int, bool) {
	m := memoryRe.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	n, _ := strconv.Atoi(m[1])
	mib := n
	if m[2] == "Gi" {
		mib = n * 1024
	} else if n%1024 == 0 {
		return 0, false // whole GiB are written as Gi
	}
	if mib%256 != 0 {
		return 0, false
	}
	return mib, true
}

// FormatMemory writes MiB the way a spec stores them (2048 → "2Gi", 1536 → "1536Mi").
func FormatMemory(mib int) string {
	if mib%1024 == 0 {
		return strconv.Itoa(mib/1024) + "Gi"
	}
	return strconv.Itoa(mib) + "Mi"
}

// HeapMiB is the Java heap of a container with mem MiB: 70%, leaving at least 256 MiB to the JVM.
func HeapMiB(mem int) int {
	return min(mem*7/10, mem-256)
}

// Amount is the size a spec asks for (preset or custom).
func (p Policies) Amount() ResourceAmount {
	if p.Resources == "custom" && p.Custom != nil {
		cpu, _ := ParseCPU(p.Custom.CPU)
		mem, _ := ParseMemory(p.Custom.Memory)
		return ResourceAmount{CPUMilli: cpu, MemoryMiB: mem}
	}
	return ResourcePresets[p.Resources]
}

// Within reports whether a fits inside limit (CPU and memory).
func (a ResourceAmount) Within(limit ResourceAmount) bool {
	return a.CPUMilli <= limit.CPUMilli && a.MemoryMiB <= limit.MemoryMiB
}

// String is a size in words ("1.5 CPU · 2 GiB").
func (a ResourceAmount) String() string {
	mem := strconv.Itoa(a.MemoryMiB) + " MiB"
	if a.MemoryMiB%1024 == 0 {
		mem = strconv.Itoa(a.MemoryMiB/1024) + " GiB"
	}
	return FormatCPU(a.CPUMilli) + " CPU · " + mem
}
