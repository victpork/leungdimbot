package dao

import (
	"fmt"

	ghash "github.com/mmcloughlin/geohash"
)

const (
	//Type for non-physical (network) store
	nonPhyStore = "網店"
	closedStore = "已結業"
)

//Shop is a struct for storing shop info
type Shop struct {
	ID       int      //Internal ID
	Name     string   //Shop name
	Address  string   //Shop address
	Geohash  string   //Geohash code for lat/long coordinates
	Position Coord    //Position is the numeric representation of the shop coordinates
	Type     string   //Shop type
	District string   //Where is the shop?
	URL      string   //URL, currently unused
	Tags     []string //Tags used
	Notes    string   //Notes for the shop
	Distance int      //Distance in metres
}

//Coord represents a point on Earth
type Coord struct {
	Lat  float64
	Long float64
}

func (s Shop) String() string {
	return fmt.Sprintf("%s (%s)\n%s", s.Name, s.Type, s.Address)
}

//BleveType fulfills bleveType interface
func (s Shop) BleveType() string {
	return "Shop"
}

//ToGeohash returns geohash representation of shop's location
func (s Shop) ToGeohash() string {
	if s.Geohash != "" {
		return s.Geohash
	} else if s.Position != (Coord{}) {
		return ghash.Encode(s.Position.Lat, s.Position.Long)
	}
	return ""
}

//ToCoord returns coordinte representation (lat, long) of shop's location
func (s Shop) ToCoord() (lat, long float64) {
	if s.Position != (Coord{}) {
		return s.Position.Lat, s.Position.Long
	} else if s.Geohash != "" {
		return ghash.DecodeCenter(s.Geohash)
	}
	return 0, 0
}

//HasPhyLoc returns true if the shop has a physical location, i.e. either Geohash or coordinates
func (s Shop) HasPhyLoc() bool {
	return s.Geohash != "" || s.Position != (Coord{})
}

//Backend represents an adstract data backend, which can have different
//implementation underlying
type Backend interface {
	AdvQuery(query string) ([]Shop, error)
	ShopsWithKeyword(keywords string) ([]Shop, error)
	ShopCount() (int, error)
	ShopByID(shopID int) (Shop, error)
	UpdateShopInfo(shops []Shop) error
	NearestShops(lat, long float64, distance string) ([]Shop, error)
	ShopMissingInfo() ([]Shop, error)
	SuggestKeyword(key string) ([]string, error)
	Districts() ([]string, error)
	ShopsWithKeywordSortByDist(keywords string, lat, long float64) ([]Shop, error)
	Close()
}

//TaggedBackend are datasources with separate function to update tags after input
type TaggedBackend interface {
	Backend
	UpdateTags() (int, error)
	RefreshKeywords() (int, error)
}

//Exporter is for backend to export all data
type Exporter interface {
	AllShops() ([]Shop, error)
	Close()
}
