package dao

import (
	"testing"
	"fmt"
)

const (
	connStr = "host=localhost port=32768 user=shopfinder password=shoppasswd dbname=shop_db sslmode=disable"
)
func BenchmarkNeighouring(b *testing.B) {
	for i := 0; i < b.N; i++ {
        area("wecpk8t", "150m")
    }
}

func TestSuggestKeywords(t *testing.T) {
	db, err := NewPostGISBackend(fmt.Sprint(connStr))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := db.SuggestKeyword("珈啡")

	if err != nil {
		t.Fatal(err)
	}

	if len(s) == 0 {
		t.Fatal("result is 0")
	}
	if s[0] != "咖啡" {
		t.Fatalf("Expected: 咖啡, actual: %s", s[0])
	}
}

func TestShopMissingInfo(t *testing.T) {
	db, err := NewPostGISBackend(fmt.Sprint(connStr))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := db.ShopMissingInfo()

	if err != nil {
		t.Fatal(err)
	}

	for i := range s {
		if s[i].Type == "網店" || s[i].Type == "已結業" {
			t.Error("shop with unwanted type selected")
		} 
	}
}