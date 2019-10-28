package dao

import (
	"strconv"
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
)

// BleveBackend is the data backend powered by Bleve
type BleveBackend struct {
	index bleve.Index
}

// NewBleveBackend returns a bleve-based backend
func NewBleveBackend(path string) (*BleveBackend, error) {
	idx, err := bleve.Open(path)
	if err == bleve.ErrorIndexPathDoesNotExist {
		idx, err = bleve.New(path, newShopIndexMapping())
		if err != nil {
			return nil, fmt.Errorf("Cannot create store file %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("Cannot open store file %w", err)
	}

	b := BleveBackend{idx}
	return &b, nil
} 

//ShopByID returns shop with provided ID
func (b *BleveBackend) ShopByID(shopID int) (Shop, error) {
	q := bleve.NewDocIDQuery([]string{strconv.Itoa(shopID)})
	req := bleve.NewSearchRequest(q)
	req.Fields = []string{"*"}
	result, err := b.index.Search(req)
	if err != nil {
		return Shop{}, err
	}
	if result.Size() == 0 {
		return Shop{}, fmt.Errorf("Shop with %d not found", shopID)
	}
	return convertSearchResultToShop(*result.Hits[0]), nil
}

//NearestShops retrieves nearest shops with provided current location and distance
func (b *BleveBackend) NearestShops(lat, long float64, dist string) ([]Shop, error) {
	q := bleve.NewGeoDistanceQuery(long, lat, dist)
	
	return b.queryIndex(q)
}

func newShopIndexMapping() mapping.IndexMapping {
	mapping := bleve.NewIndexMapping()
	shopMapping := bleve.NewDocumentMapping()

	//Fields
	shopNameMap := bleve.NewTextFieldMapping()
	kwordMap := bleve.NewTextFieldMapping()
	kwordMap.Analyzer = keyword.Name
	shopMapping.AddFieldMappingsAt("Name", shopNameMap)
	shopMapping.AddFieldMappingsAt("District", kwordMap)
	shopMapping.AddFieldMappingsAt("Type", kwordMap)
	
	shopMapping.AddFieldMappingsAt("Geohash", bleve.NewGeoPointFieldMapping())
	
	noSearchMap := bleve.NewTextFieldMapping()
	noSearchMap.Index = false
	shopMapping.AddFieldMappingsAt("Address", noSearchMap)
	shopMapping.AddFieldMappingsAt("URL", noSearchMap)

	mapping.AddDocumentMapping("Shop", shopMapping)
	mapping.TypeField = "DocType"
	return mapping
}

//ShopCount returns total number of shops in system
func (b *BleveBackend) ShopCount() (int, error) {
	c, err := b.index.DocCount()
	if err != nil {
		return -1, err
	}
	return int(c), nil
}

func convertSearchResultToShop(docMatch search.DocumentMatch) Shop {
	id, _ := strconv.Atoi(docMatch.ID)
	s := Shop{
		ID: id,
		Name: docMatch.Fields["Name"].(string),
		Type: docMatch.Fields["Type"].(string),
		District: docMatch.Fields["District"].(string),
		Address: docMatch.Fields["Address"].(string),

	}

	return s
}

// ShopsWithKeyword returns shops based on keywords
func (b *BleveBackend) ShopsWithKeyword(keyword string) ([]Shop, error) {
	q := bleve.NewMatchPhraseQuery(keyword)
	return b.queryIndex(q)
}


// ShopMissingInfo returns shops with missing location or addresses
func (b *BleveBackend) ShopMissingInfo() ([]Shop, error) {
	q := bleve.NewBoolFieldQuery(false)
	q.SetField("AddressFilled")
	return b.queryIndex(q)
}

func (b *BleveBackend) queryIndex(q query.Query) ([]Shop, error) {
	req := bleve.NewSearchRequest(q)
	res, err := b.index.Search(req)
	if err != nil {
		return nil, err
	}
	shops := make([]Shop, len(res.Hits))
	for i := range res.Hits {
		shops[i] = convertSearchResultToShop(*res.Hits[i])
	}

	return shops, nil
}

// UpdateShopInfo fills shops into index 
func (b *BleveBackend) UpdateShopInfo(shops []Shop) error {
	batch := b.index.NewBatch()
	for i:= range shops {
		batch.Index(strconv.Itoa(shops[i].ID), shops[i])
	}
	err := b.index.Batch(batch)
	if err != nil {
		return err
	}
	return nil
}

// Close Bleve index
func (b *BleveBackend) Close() error {
	return b.index.Close()
}