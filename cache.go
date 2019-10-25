package wongdim

import (
	"log"
	"equa.link/wongdim/dao"
	lru "github.com/hashicorp/golang-lru"
	"strings"
	ghash "github.com/mmcloughlin/geohash"
)

var (
	cache *lru.TwoQueueCache 
	da dao.Backend
)

func init() {
	var err error
	cache, err = lru.New2Q(128)
	if err != nil {
		log.Fatal("[ERR] Error initializing cache: ", err)
	}
}

func shopWithGeohash(geohash string) ([]dao.Shop, error) {
	var err error
	lat, long := ghash.Decode(geohash)
	v, ok := cache.Get(geohash)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = da.NearestShops(lat, long, "1km")
		if err != nil {
			log.Println("DB err:", err)
			return nil, err
		}
		cache.Add(geohash, shops)
	}

	return shops, nil
}

func shopWithTags(keywords []string) ([]dao.Shop, error) {
	var err error
	cacheKey := strings.Join(keywords, "||")
	v, ok := cache.Get(cacheKey)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = da.ShopsWithKeyword(keywords)
		if err != nil {
			log.Println("DB err:", err)
			return nil, err
		}
		cache.Add(cacheKey, shops)
	}

	return shops, nil
}