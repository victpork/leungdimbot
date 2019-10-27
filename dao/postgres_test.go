package dao

import (
	"testing"
)
func BenchmarkNeighouring(b *testing.B) {
	for i := 0; i < b.N; i++ {
        area("wecpk8t", "150m")
    }
}