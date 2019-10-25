package dao

import (
	"fmt"
)

//Shop is a struct for storing shop info
type Shop struct {
	ID       int    //Internal ID
	Name     string //Shop name 
	Address  string //Shop address
	Geohash  string //Geohash code for lat/long coordinates
	Type 	 string //Shop type
	District string  //Where is the shop?
	URL      string  //URL, currently unused
	Tags     []string //Tags used
}

func (s Shop) String() string {
	return fmt.Sprintf("%s (%s)\n%s", s.Name, s.Type, s.Address)
}

//BleveType fulfills bleveType interface
func (s Shop) BleveType() string {
	return "Shop"
}
//Backend represents an adstract data backend, which can have different
//implementation underlying
type Backend interface {
	NearestShops(lat, long float64, distance string) ([]Shop, error)
	ShopsWithKeyword(keywords []string) ([]Shop, error)
	ShopCount() (int, error)
	ShopByID(shopID int) (Shop, error)
	UpdateShopInfo(shops []Shop) error
	ShopMissingInfo() ([]Shop, error)
}