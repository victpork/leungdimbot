package batch

import (
	"context"
	"equa.link/wongdim/dao"
	"errors"
	ghash "github.com/TomiHiltunen/geohash-golang"
	"github.com/jackc/pgx/v4"
	"golang.org/x/sync/errgroup"
	"googlemaps.github.io/maps"
	"log"
	"time"
)

var (
	//GeocodeAPITimeout is the timeout value for Google Geocode API timeout
	GeocodeAPITimeout time.Duration = 3 * time.Second
	gCodeFunc         Processor
	conn              *pgx.Conn
)

//GeocodeClient takes shop name and district, query Google Map Geocode API, and
//returns geohash
type GeocodeClient struct {
	c *maps.Client
}

// Processor is a function on processing Shop info
type Processor func(context.Context, dao.Shop) (dao.Shop, error)

//NewGeocodeClient creates a new Geocode client for querying
func NewGeocodeClient(apiKey string) (GeocodeClient, error) {
	t := GeocodeClient{}
	var err error
	t.c, err = maps.NewClient(maps.WithAPIKey(apiKey))

	return t, err
}

//FillGeocode fills Geocode and address for given shop
func (gc GeocodeClient) FillGeocode(ctx context.Context, shop dao.Shop) (dao.Shop, error) {
	geoReq := maps.GeocodingRequest{}
	if len(shop.Address) > 0 {
		geoReq.Address = shop.Address
	} else {
		geoReq.Address = shop.District + " " + shop.Name
	}
	cCtx, cancel := context.WithTimeout(ctx, GeocodeAPITimeout)
	defer cancel()
	res, err := gc.c.Geocode(cCtx, &geoReq)
	if err != nil {
		log.Println("fatal error", err)
		return shop, err
	}
	if len(res) == 0 {
		log.Printf("[ERR] No results found: Search keyword:[%s] - ID: %d", geoReq.Address, shop.ID)
		return shop, errors.New("No results found")
	}
	if len(res) > 1 {
		log.Printf("[WRN] multiple results: Search keyword:[%s] - ID: %d", geoReq.Address, shop.ID)
	}
	if !res[0].PartialMatch {
		shop.Geohash = ghash.Encode(res[0].Geometry.Location.Lat, res[0].Geometry.Location.Lng)
		if len(shop.Address) == 0 {
			shop.Address = res[0].FormattedAddress
		}
	} else {
		log.Printf("Partial address: Search keyword:[%s] - ID: %d", geoReq.Address, shop.ID)
		return shop, errors.New("Received partial address")
	}
	return shop, nil
}

//Run is a batch function that fill missing geohash, addresses, tags into shop info and save to DB
func Run(ctx context.Context, dbConn *pgx.Conn, geoCodeAPI Processor) <-chan error {
	gCodeFunc = geoCodeAPI
	conn = dbConn
	errCh := make(chan error)
	go batchController(ctx, errCh)
	return errCh
}

func batchController(ctx context.Context, errCh chan<- error) {
	grp, ctx := errgroup.WithContext(ctx)
	inCh := make(chan dao.Shop)
	resultCh := make(chan dao.Shop)
	shopList, err := dao.ShopMissingInfo()
	if err != nil {
		errCh <- err
	}

	grp.Go(func() error {
		defer close(inCh)
		for i := range shopList {
			select {
			case inCh <- shopList[i]:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	for i := 0; i < 5; i++ {
		grp.Go(func() error {
			//Take from inChannel and process
			for s0 := range inCh {
				var s1 dao.Shop
				var err error
				if s0.Geohash == "" {
					s1, err = gCodeFunc(ctx, s0)
					log.Printf("Updated %s with address %s and geohash %s", s1.Name, s1.Address, s1.Geohash)
				}
				if err != nil {
					// Since returning error would kill the group, we push error to the
					// channel instead. Also we skip processing
					select {
					case errCh <- err:
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					select {
					case resultCh <- s1:
					case <-ctx.Done():
						return ctx.Err()
					}
				}

			}
			return nil
		})
	}

	go func() {
		grp.Wait()
		close(resultCh)
		log.Print("All channels closed")
	}()
	resultList := make([]dao.Shop, 0)
	for shop := range resultCh {
		resultList = append(resultList, shop)
	}
	log.Printf("Writing %d shops info into database...", len(resultList))
	err = dao.UpdateShopInfo(resultList)
	if err != nil {
		errCh <- err
	}
	res, err := dao.UpdateTags()
	if err != nil {
		errCh <- err
	}
	log.Printf("%d rows updated tags", res)
	close(errCh)
}
