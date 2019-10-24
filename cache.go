package wongdim

import (
	"log"
	"equa.link/wongdim/dao"
	lru "github.com/hashicorp/golang-lru"
	"strings"
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

func shopWithGeohash(geohash string) ([]dao.Shop, error) {
	var err error
	v, ok := cache.Get(geohash)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = dao.NearestShops(geohash)
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
		shops, err = dao.ShopsWithTags(keywords)
		if err != nil {
			log.Println("DB err:", err)
			return nil, err
		}
		cache.Add(cacheKey, shops)
	}

	return shops, nil
}