package bingmap

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"time"

	"equa.link/wongdim/dao"
	log "github.com/sirupsen/logrus"
)

const (
	bingMapAPIURL = "https://dev.virtualearth.net/REST/v1/Locations?q=%s&o=json&culture=zh-Hant&key=%s"
	//GeocodeAPITimeout is the timeout value for Google Geocode API timeout
	GeocodeAPITimeout time.Duration = 3 * time.Second
)

//Service is a service to use Bing Map geocoding
type Service struct {
	apiKey string
	c      *http.Client
}

//Result is the Bing API return
type Result struct {
	AuthResult  string      `json:"authenticationResultCode"`
	ResourceSet []ResultSet `json:"resourceSets"`
	Status      int         `json:"statusCode"`
	StatusDesc  string      `json:"statusDescription"`
}

//ResultSet is the result
type ResultSet struct {
	EstTotal  int        `json:"estimatedTotal"`
	Resources []Resource `json:"resources"`
}

//Resource is a collection of result data set
type Resource struct {
	Point   Point   `json:"point"`
	Address Address `json:"address"`
}

//Point represent a location in coordinates format
type Point struct {
	Coord []float64 `json:"coordinates"`
}

//Location returns the Point Latitude and Longititude
func (p Point) Location() (lat, long float64) {
	return p.Coord[0], p.Coord[1]
}

//Address is address break down in various component
type Address struct {
	AddressLine      string `json:"addressLine"`
	AdminDistrict    string `json:"adminDistrict"`
	AdminDistrict2   string `json:"adminDistrict2"`
	CountryRegion    string `json:"countryRegion"`
	FormattedAddress string `json:"formattedAddress"`
	Locality         string `json:"locality"`
}

//NewBingMapClient returns a BingService client
func NewBingMapClient(key string) Service {
	return Service{apiKey: key, c: &http.Client{}}
}

//FillGeocode fills geocode using Bing Map services
func (s Service) FillGeocode(ctx context.Context, shop dao.Shop) (dao.Shop, error) {
	if len(shop.Address) == 0 {
		log.WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
		}).Error("No address")
		return shop, fmt.Errorf("No address")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf(bingMapAPIURL, shop.Address, s.apiKey), nil)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  shop.Address,
			"url":      fmt.Sprintf(bingMapAPIURL, shop.Address, s.apiKey),
		}).Error("Geocode create request failed")
		return shop, err
	}

	rsp, err := s.c.Do(req)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  shop.Address,
			"url":      fmt.Sprintf(bingMapAPIURL, shop.Address, s.apiKey),
		}).Error("Connection to bing failed")
		return shop, err
	}
	defer rsp.Body.Close()
	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  shop.Address,
		}).Error("Geocode request failed")
		return shop, err
	}
	var rspJSON Result
	err = json.Unmarshal(body, &rspJSON)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  shop.Address,
			"url":      fmt.Sprintf(bingMapAPIURL, shop.Address, s.apiKey),
			"json":     body,
		}).Error("Result parse fail")
		return shop, err
	}
	if rspJSON.Status != 200 {
		return shop, fmt.Errorf("Error respond code %d", rspJSON.Status)
	}
	if len(rspJSON.ResourceSet) == 0 || len(rspJSON.ResourceSet[0].Resources) == 0 {
		log.WithError(err).WithFields(log.Fields{
			"shopID":   shop.ID,
			"shopName": shop.Name,
			"address":  shop.Address,
		}).Error("No result returned")
		return shop, err
	}
	lat, lng := rspJSON.ResourceSet[0].Resources[0].Point.Location()
	if strings.Contains(rspJSON.ResourceSet[0].Resources[0].Address.CountryRegion, "香港") {
		//Do address translation
		lat, lng = GCJtoWGS(lat, lng)
	}

	shop.Position = dao.Coord{Lat: lat, Long: lng}
	return shop, nil
}
