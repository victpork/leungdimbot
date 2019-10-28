package dao

import (
	"fmt"
	"testing"
	"github.com/blevesearch/bleve"
)

func prepareDataset() (bleve.Index, error) {
	idx, err := bleve.NewMemOnly(newShopIndexMapping())
	if err != nil {
		return nil, fmt.Errorf("Cannot create store file %w", err)
	}

	idx.Index("1", Shop{
		ID: 1,
		Name: "水門泰式雞飯專門店",
		Address: "Hong Kong, Sham Shui Po, Yen Chow St, 37號, Dragon Centre, 美食廣場8樓55號鋪",
		Type: "泰國菜",
		District: "上環",
		Geohash: "wecpjc2b27ev",	
	})
	idx.Index("2", Shop{
		ID: 2,
		Name: "留白",
		Address: "荃灣荃昌中心昌寧商場地下, 12號舖 Tsuen Wan, Hong Kong",
		Type: "咖啡",
		District: "荃灣",
		Geohash: "wecpkbeddsmf",		
	})
	idx.Index("3", Shop{
		ID: 3,
		Name: "阿土伯鹽水雞",
		Address: "觀塘成業街7號東廣場地下20號舖",
		Type: "台灣菜",
		District: "觀塘",
		Geohash: "wecnzm94b80h",		
	})
	idx.Index("4", Shop{
		ID: 4,
		Name: "白宮咖啡廳",
		Address: "號, 24 Heung Wo St, Hong Kong",
		Type: "咖啡",
		District: "荃灣",
		Geohash: "wecpk8tne453",		
	})
	idx.Index("5", Shop{
		ID: 5,
		Name: "樂林小鍋米線",
		Address: "Hong Kong, 鰂魚涌濱海街11號號地下",
		Type: "米線",
		District: "鰂魚涌",
		Geohash: "wecnycjgz1u3",		
	})
	idx.Index("6", Shop{
		ID: 6,
		Name: "大一海洋火鍋",
		Address: "Hong Kong, 尖沙咀金馬倫道38-40號金龍中心3樓",
		Type: "火鍋",
		District: "尖沙咀",
		Geohash: "wecny57t09cu",		
	})
	idx.Index("7", Shop{
		ID: 7,
		Name: "齊柏林熱狗店",
		Address: " 80 Hau Tei Square, Tsuen Wan, Hong Kong",
		Type: "熱狗",
		District: "荃灣",
		Geohash: "wecpkb80s09t",
	})
	idx.Index("8", Shop{
		ID: 8,
		Name: "御品·千之味",
		Address: "Shop C, 390 Lai Chi Kok Rd, Sham Shui Po, Hong Kong",
		Type: "日本菜",
		District: "深水埗",
		Geohash: "wecpjbbru3dj",	
	})
	idx.Index("9", Shop{
		ID: 9,
		Name: "侘寂珈琲 WabiSabi",
		Address: "觀塘觀塘道396號毅力工業中心4樓C室",
		Type: "咖啡",
		District: "觀塘",
		Geohash: "wecnznn0hzr1",
	})
	idx.Index("10", Shop{
		ID: 10,
		Name: "Explorer Fusion Restaurant",
		Address: "沙田石門安群街3號京瑞廣場第一期地下G10號舖",
		Type: "西式",
		District: "石門",
		Geohash: "wecpqgeu2uzw",	
	})

	return idx, nil
}

func TestIDSearch(t *testing.T) {
	idx, err := prepareDataset()
	if err != nil {
		t.Fatal(err)
	}
	q := bleve.NewDocIDQuery([]string{"3"})
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"*"}
	sr, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Total != 1 {
		t.Errorf("Size expected: 1, actual %d", sr.Total)
	}
	if sr.Hits[0].Fields["Name"] != "阿土伯鹽水雞" {
		t.Errorf("Name expected: 阿土伯鹽水雞, actual %s", sr.Hits[0].Fields["Name"])
	}
}

func TestSearchByKeyword(t *testing.T) {
	idx, err := prepareDataset()
	if err != nil {
		t.Fatal(err)
	}
	q := bleve.NewMatchQuery("咖啡")
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"*"}
	req.SortBy([]string{"_id"})
	sr, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Total != 3 {
		t.Errorf("Size expected: 3, actual %d", sr.Total)
	}
	if sr.Hits[0].ID != "2" || sr.Hits[1].ID != "4" || sr.Hits[2].ID != "9" {
		t.Errorf("Result expected: {2,4,9}, actual {%s,%s,%s}", sr.Hits[0].ID, sr.Hits[1].ID, sr.Hits[2].ID)
	}
}

func TestUpdateIndex(t *testing.T) {
	idx, _ := prepareDataset()
	idx.Index("8", Shop{
		ID: 8,
		Name: "御品·千之味",
		Address: "Shop C, 390 Lai Chi Kok Rd, Sham Shui Po, Hong Kong",
		Type: "日本菜",
		District: "長沙灣",
		Geohash: "wecpqgeu2uzw",		
	})
	q := bleve.NewDocIDQuery([]string{"8"})
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"District"}
	sr, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Total != 1 {
		t.Errorf("Size expected: 1, actual %d", sr.Total)
	}
	if sr.Hits[0].Fields["District"] != "長沙灣" {
		t.Errorf("District expected: 長沙灣, actual %s", sr.Hits[0].Fields["District"])
	}
	q2 := bleve.NewMatchQuery("千之味")
	req = bleve.NewSearchRequest(q2)
	req.Fields = []string{"*"}
	sr, err = idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Total != 1 {
		t.Errorf("Size expected: 1, actual %d", sr.Total)
	}
	if sr.Hits[0].Fields["District"] != "長沙灣" {
		t.Errorf("District expected: 長沙灣, actual %s", sr.Hits[0].Fields["District"])
	}
	t.Log(sr.Hits[0].Fields["Location"])
}

func TestGeoSearch(t *testing.T) {
	idx, err := prepareDataset()
	if err != nil {
		t.Fatal(err)
	}
	lat, lon  := 22.371154, 114.112603
	q := bleve.NewGeoDistanceQuery(lon, lat, "10km")
	q.SetField("Location")
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"*"}
	req.SortBy([]string{"_id"})
	sr, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if sr.Total != 3 {
		t.Errorf("Size expected: 3, actual %d", sr.Total)
	}
	if sr.Hits[0].Fields["District"] != "荃灣" {
		t.Errorf("District expected: 荃灣, actual %s", sr.Hits[0].Fields["District"])
	}
}