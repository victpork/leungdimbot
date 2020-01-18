package dao

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
	"strconv"
)

const (
	//Bleve is the type name for Bleve search engine
	Bleve = "bleve"
)

// BleveBackend is the data backend powered by Bleve
type BleveBackend struct {
	index bleve.Index
}

// NewBleveBackend returns a bleve-based backend
func NewBleveBackend(path string) (*BleveBackend, error) {
	idx, err := bleve.Open(path)
	if err == bleve.ErrorIndexPathDoesNotExist {
		idx, err = bleve.NewUsing(path, newShopIndexMapping(), "scorch", "scorch", nil)
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
	q.SetField("Location")
	sr := bleve.NewSearchRequest(q)
	s, err := b.index.Search(sr)
	if err != nil {
		return nil, err
	}
	gSort, err := search.NewSortGeoDistance("Location", "m", long, lat, true)
	if err != nil {
		return nil, err
	}
	s.Request.SortByCustom(search.SortOrder{gSort})

	res := make([]Shop, s.Total)
	for i := range s.Hits {
		res[i] = convertSearchResultToShop(*s.Hits[i])
	}
	return res, nil
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

	shopMapping.AddFieldMappingsAt("Location", bleve.NewGeoPointFieldMapping())

	noSearchMap := bleve.NewTextFieldMapping()
	noSearchMap.Index = false
	shopMapping.AddFieldMappingsAt("Address", noSearchMap)
	shopMapping.AddFieldMappingsAt("URL", noSearchMap)
	shopMapping.AddFieldMappingsAt("Tags", kwordMap)
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
		ID:       id,
		Name:     docMatch.Fields["Name"].(string),
		Type:     docMatch.Fields["Type"].(string),
		District: docMatch.Fields["District"].(string),
		Address:  docMatch.Fields["Address"].(string),
		//Position: docMatch.Locations["Location"],
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
	req.IncludeLocations = true
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
	for i := range shops {
		batch.Index(strconv.Itoa(shops[i].ID), shops[i])
	}
	err := b.index.Batch(batch)
	if err != nil {
		return err
	}
	return nil
}

//AdvQuery accepts query string syntax (in Bleve format) and returns result
func (b *BleveBackend) AdvQuery(query string) ([]Shop, error) {
	q := bleve.NewQueryStringQuery(query)
	return b.queryIndex(q)
}

// Close Bleve index
func (b *BleveBackend) Close() {
	b.index.Close()
}

//ShopsWithKeywordSortByDist sort position by distance
func (b *BleveBackend) ShopsWithKeywordSortByDist(keywords string, lat, long float64) ([]Shop, error) {
	q := bleve.NewMatchPhraseQuery(keywords)
	req := bleve.NewSearchRequest(q)
	gs, err := search.NewSortGeoDistance("location", "m", long, lat, true)
	if err != nil {
		return nil, err
	}
	req.SortByCustom(search.SortOrder{gs})
	req.IncludeLocations = true
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

//SuggestKeyword will take provided keyword to look into the keyword db and search
//with edit distance <= len(key) - 1
func (b *BleveBackend) SuggestKeyword(key string) ([]string, error) {
	q := bleve.NewFuzzyQuery(key)
	q.SetFuzziness(len(key) - 1)
	sr := bleve.NewSearchRequest(q)
	fr := bleve.NewFacetRequest("Tags", 4)
	sr.AddFacet("shopType", fr)
	res, err := b.index.Search(sr)
	if err != nil {
		return nil, err
	}
	terms := make([]string, len(res.Facets["shopType"].Terms))
	for i := range res.Facets["shopType"].Terms {
		terms[i] = res.Facets["shopType"].Terms[i].Term
	}
	return terms, nil
}

//Districts returns a list of districts
func (b *BleveBackend) Districts() ([]string, error) {
	dict, err := b.index.FieldDict("District")
	if err != nil {
		return nil, err
	}
	defer dict.Close()

	dc := make([]string, 0)
	for {
		ety, err := dict.Next()
		if err != nil {
			break
		}
		dc = append(dc, ety.Term)
	}
	return dc, nil
}
