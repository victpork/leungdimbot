package wongdim

import (
	"log"
	"equa.link/wongdim/dao"
	lru "github.com/hashicorp/golang-lru"
	ghash "github.com/mmcloughlin/geohash"
)

var (
	cache *lru.TwoQueueCache
)

func init() {
	var err error
	cache, err = lru.New2Q(128)
	if err != nil {
		log.Fatal("[ERR] Error initializing cache: ", err)
	}
}

func (s *ServeBot) shopWithGeohash(lat, long float64) ([]dao.Shop, error) {
	var err error
	geohash := ghash.EncodeWithPrecision(lat, long, GeohashPrecision)
	v, ok := cache.Get(geohash)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = s.da.NearestShops(lat, long, "700m")
		if err != nil {
			log.Println("DB err:", err)
			return nil, err
		}
		cache.Add(geohash, shops)
	}

	return shops, nil
}

func (s *ServeBot) shopWithTags(keywords string) ([]dao.Shop, error) {
	var err error
	v, ok := cache.Get(keywords)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = s.da.ShopsWithKeyword(keywords)
		if err != nil {
			log.Println("DB err:", err)
			return nil, err
		}
		cache.Add(keywords, shops)
	}

	return shops, nil
}