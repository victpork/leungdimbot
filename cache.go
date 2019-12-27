package wongdim

import (
	log "github.com/sirupsen/logrus"
	"equa.link/wongdim/dao"
	gcache "github.com/patrickmn/go-cache"
	ghash "github.com/mmcloughlin/geohash"
	"time"
)

var (
	cache *gcache.Cache
	districts map[string]struct{}
)

func init() {
	cache = gcache.New(10*time.Minute, 20*time.Minute)
	districts = make(map[string]struct{})
}

func (s *ServeBot) shopWithGeohash(geohash, distance string) ([]dao.Shop, error) {
	var shops []dao.Shop
	var err error

	v, ok := cache.Get("<G>" + geohash)
	if ok {
		shops = v.([]dao.Shop)
	} else {
		lat, long := ghash.DecodeCenter(geohash)
		shops, err = s.da.NearestShops(lat, long, distance)
		if err != nil {
			log.WithError(err).Error("Database error")
			return nil, err
		}
		cache.SetDefault("<G>" + geohash, shops)
	}

	return shops, nil
}

func (s *ServeBot) shopWithCoord(lat, long float64, distance string) ([]dao.Shop, error) {
	var err error
	geohash := ghash.EncodeWithPrecision(lat, long, GeohashPrecision)
	v, ok := cache.Get("<G>" + geohash)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = s.da.NearestShops(lat, long, distance)
		if err != nil {
			log.WithError(err).Error("Database error")
			return nil, err
		}
		cache.SetDefault("<G>" + geohash, shops)
	}

	return shops, nil
}

func (s *ServeBot) shopWithTags(keywords string) ([]dao.Shop, error) {
	var err error
	v, ok := cache.Get("<S>" + keywords)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = s.da.ShopsWithKeyword(keywords)
		if err != nil {
			log.WithError(err).Error("Database error")
			return nil, err
		}
		cache.SetDefault("<S>" + keywords, shops)
	}

	return shops, nil
}

func (s *ServeBot) advSearch(query string) ([]dao.Shop, error) {
	var err error
	v, ok := cache.Get("<A>" + query)
	var shops []dao.Shop
	if ok {
		shops = v.([]dao.Shop)
	} else {
		shops, err = s.da.AdvQuery(query)
		if err != nil {
			log.WithError(err).Error("Database error")
			return nil, err
		}
		cache.SetDefault("<A>" + query, shops)
	}

	return shops, nil
}

func (s *ServeBot) isDistrict(d string) bool {
	if len(districts) == 0 {
		dList, err := s.da.Districts()
		if err != nil {
			return true
		}
		for i := range dList {
			districts[dList[i]] = struct{}{}
		}
	}
	_, ok := districts[d]
	if ok {
		return true
	}
	return false
}