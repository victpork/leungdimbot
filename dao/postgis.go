package dao

import (
	"log"
	"context"
	"strings"
	"strconv"
)

const (
	//PostGIS is a PostgreSQL database with PostGIS module installed
	PostGIS = "postgis"
)

//PostGISBackend is a PostGIS-enabled PostgreSQL database
type PostGISBackend struct {
	*PostgresBackend
}

//CreateTable create necessary table for storing shop records
func (pg *PostGISBackend) CreateTable() error {
	_, err := pg.conn.Exec(context.Background(), `CREATE TABLE public.shops
	(
		shop_id SERIAL NOT NULL,
		name TEXT NOT NULL,
		address TEXT,
		geog geography,
		type TEXT NOT NULL,
		url TEXT,
		district TEXT,
		search_text TEXT,
		CONSTRAINT shops_pkey PRIMARY KEY (shop_id)
	)`)

	return err
}

//NewPostGISBackend returns new PostGIS backend
func NewPostGISBackend(connStr string) (*PostGISBackend, error) {
	db, err := NewPostgresBackend(connStr)
	return &PostGISBackend{db}, err
}

//ShopMissingInfo get data with missing info
func (pg *PostGISBackend) ShopMissingInfo() ([]Shop, error) {
	rows, err := pg.conn.Query(context.Background(),
		`SELECT shop_id, name, district, coalesce(address, ''), 
		 type FROM shops WHERE geog IS NULL and district <> $1`, nonPhyStore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		rows.Scan(&shop.ID, &shop.Name, &shop.District, &shop.Address, &shop.Type)
		shoplist = append(shoplist, shop)
	}
	return shoplist, nil
}

//UpdateShopInfo fill missing info into shops
func (pg *PostGISBackend) UpdateShopInfo(shops []Shop) error {
	tx, err := pg.conn.Begin(context.Background())
	if err != nil {
		return err
	}
	var rowsAffected int64 = 0
	for _, shop := range shops {
		lat, long := shop.ToCoord()
		cmdTag, err := pg.conn.Exec(context.Background(),
			"UPDATE shops SET address = $1, geog = ST_MakePoint($2, $3)::geography WHERE shop_id = $4",
			shop.Address, long, lat, shop.ID)
		if err != nil {
			log.Printf("[ERR] %e", err)
			return err
		}
		rowsAffected += cmdTag.RowsAffected()
	}

	tx.Commit(context.Background())
	return err
}

//NearestShops returns nearby shops
func (pg *PostGISBackend) NearestShops(lat, long float64, distance string) ([]Shop, error) {
	d, err := disToInt(distance)
	if err != nil {
		return nil, err
	}
	
	rows, err := pg.conn.Query(context.Background(),
		`SELECT shop_id, name, type, coalesce(address, ''), 
		coalesce(url, ''), district, ST_X(geog::geometry) long, ST_Y(geog::geometry) lat,
		round(ST_Distance(geog, ST_MakePoint($1, $2)::geography, false)) as dist
		FROM shops
		WHERE ST_DWithin(geog, ST_MakePoint($1, $2), $3, false) order by dist`,
		long, lat, d)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		rows.Scan(&shop.ID, &shop.Name, &shop.Type, &shop.Address, 
			&shop.URL, &shop.District, &shop.Position.Long, &shop.Position.Lat, &shop.Distance)
		
		shoplist = append(shoplist, shop)
	}

	return shoplist, nil
}

//ShopByID returns shop by internal ID
func (pg *PostGISBackend) ShopByID(shopID int) (Shop, error) {
	r := pg.conn.QueryRow(context.Background(),
		`SELECT name, type, coalesce(address, ''), coalesce(url,''), coalesce(ST_X(geog::geometry), 0) long, 
		coalesce(ST_Y(geog::geometry), 0) lat, district, coalesce(notes, '') FROM shops WHERE shop_id = $1`, shopID)
	shop := Shop{}
	err := r.Scan(&shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Position.Long, 
		&shop.Position.Lat, &shop.District, &shop.Notes)
	if err != nil {
		return shop, err
	}
	return shop, nil
}

//ShopsWithKeyword returns shops with tags provided
func (pg *PostGISBackend) ShopsWithKeyword(keywords string) ([]Shop, error) {
	rows, err := pg.conn.Query(context.Background(), 
	`SELECT shop_id, name, type, coalesce(address, ''), 
	coalesce(url,''), coalesce(ST_X(geog::geometry), 0) long, coalesce(ST_Y(geog::geometry), 0) lat, district, coalesce(notes, '') 
	FROM shops WHERE (to_tsvector('cuisine', search_text) @@ plainto_tsquery('cuisine_syn', $1) OR name ILIKE '%'||$1||'%') 
	and (address IS NOT NULL OR url IS NOT NULL) order by random()`,
		keywords)
	
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		err := rows.Scan(&shop.ID, 
			&shop.Name, 
			&shop.Type, 
			&shop.Address, 
			&shop.URL, 
			&shop.Position.Long,
			&shop.Position.Lat,
			&shop.District,
			&shop.Notes,
		)
		if err != nil {
			log.Println(err)
		}
		shoplist = append(shoplist, shop)
	}
	return shoplist, nil
}


func disToInt(distance string) (int, error) {
	var multiplier int
	var t string
	if strings.HasSuffix(distance, "km") {
		t = strings.TrimSuffix(distance, "km")
		multiplier = 1000
	} else if strings.HasSuffix(distance, "m") {
		t = strings.TrimSuffix(distance, "m")
		multiplier = 1
	}
	n, err := strconv.Atoi(t)
	if err != nil {
		return -1, err
	}
	return n * multiplier, nil
}