package wongdim

import (
	"log"
	"equa.link/wongdim/dao"
	gcache "github.com/patrickmn/go-cache"
	ghash "github.com/mmcloughlin/geohash"
	"time"
)

var (
	cache *gcache.Cache
)

func init() {
	cache = gcache.New(10*time.Minute, 20*time.Minute)
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
		cache.SetDefault(geohash, shops)
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
		cache.SetDefault(keywords, shops)
	}

	return shops, nil
}