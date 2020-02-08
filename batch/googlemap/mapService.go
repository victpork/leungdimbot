package googlemap

import (
	"context"
	"errors"
	"time"

	"equa.link/wongdim/dao"
	log "github.com/sirupsen/logrus"
	"googlemaps.github.io/maps"
)

const (
	//GeocodeAPITimeout is the timeout value for Google Geocode API timeout
	GeocodeAPITimeout time.Duration = 3 * time.Second
)

//GeocodeClient takes shop name and district, query Google Map Geocode API, and
//returns geohash
type GeocodeClient struct {
	c *maps.Client
}

//NewGMapClient creates a new Geocode client for querying
func NewGMapClient(apiKey string) (GeocodeClient, error) {
	t := GeocodeClient{}
	var err error
	t.c, err = maps.NewClient(maps.WithAPIKey(apiKey))

	return t, err
}

//FillGeocode fills Geocode and address for given shop
func (gc GeocodeClient) FillGeocode(ctx context.Context, shop dao.Shop) (dao.Shop, error) {
	geoReq := maps.GeocodingRequest{}
	useOriginalAddr := false
	if len(shop.Address) > 0 {
		geoReq.Address = shop.Address
		useOriginalAddr = true
	} else {
		geoReq.Address = shop.District + " " + shop.Name
	}
	cCtx, cancel := context.WithTimeout(ctx, GeocodeAPITimeout)
	defer cancel()
	res, err := gc.c.Geocode(cCtx, &geoReq)
	if err != nil {
		log.WithError(err).Error("Geocode request failed")
		return shop, err
	}
	if len(res) == 0 {
		log.WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  geoReq.Address,
		}).Error("No results found")
		return shop, errors.New("No results found")
	}
	if len(res) > 1 {
		log.WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  geoReq.Address,
		}).Warn("Multiple results returned from Google")
	}
	if !res[0].PartialMatch {
		shop.Position.Lat = res[0].Geometry.Location.Lat
		shop.Position.Long = res[0].Geometry.Location.Lng
		if len(shop.Address) == 0 {
			shop.Address = res[0].FormattedAddress
		}
	} else {
		log.WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  geoReq.Address,
		}).Warn("Partial address returned from Google")
		if useOriginalAddr {
			shop.Position.Lat = res[0].Geometry.Location.Lat
			shop.Position.Long = res[0].Geometry.Location.Lng
		} else {
			return shop, errors.New("Received partial address")
		}
	}
	return shop, nil
}
