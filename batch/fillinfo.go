package batch

import (
	"context"

	"equa.link/wongdim/dao"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	gCodeFunc Processor
	da        dao.Backend
)

// Processor is a function on processing Shop info
type Processor func(context.Context, dao.Shop) (dao.Shop, error)

//Run is a batch function that fill missing geohash, addresses, tags into shop info and save to DB
func Run(ctx context.Context, backend dao.Backend, geoCodeAPI Processor) <-chan error {
	gCodeFunc = geoCodeAPI
	da = backend
	errCh := make(chan error)
	go batchController(ctx, errCh)
	return errCh
}

func batchController(ctx context.Context, errCh chan<- error) {
	grp, ctx := errgroup.WithContext(ctx)
	inCh := make(chan dao.Shop)
	resultCh := make(chan dao.Shop)
	shopList, err := da.ShopMissingInfo()
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
				if !s0.HasPhyLoc() {
					s1, err = gCodeFunc(ctx, s0)
					lat, long := s1.ToCoord()
					log.WithFields(log.Fields{
						"shopName": s1.Name,
						"address":  s1.Address,
						"lat":      lat,
						"long":     long,
					}).Info("Updated shop addresses and location")
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
		log.Debug("All channels closed")
	}()
	resultList := make([]dao.Shop, 0)
	for shop := range resultCh {
		resultList = append(resultList, shop)
	}
	log.WithField("affectedRows", len(resultList)).Info("Updated shops info into database")
	err = da.UpdateShopInfo(resultList)
	if err != nil {
		errCh <- err
	}
	pg, ok := da.(dao.TaggedBackend)
	if ok {
		res, err := pg.UpdateTags()
		if err != nil {
			errCh <- err
		} else {
			log.WithField("affectedRows", res).Printf("Updating rows with tags")
		}
		res, err = pg.RefreshKeywords()
		if err != nil {
			errCh <- err
		} else {
			log.WithField("affectedRows", res).Printf("Updating keyword table")
		}
	}

	close(errCh)
}
