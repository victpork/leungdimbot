package dao

import (
	"context"
	pgx "github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	ghash "github.com/mmcloughlin/geohash"
	"strings"
	"fmt"
)

const (
	//PostgreSQL is the type name for PostgreSQL DB
	PostgreSQL = "pgsql"
)

//PostgresBackend is the data backend supported by PostgresSQL database
type PostgresBackend struct {
	//Conn is the database connection
	conn *pgx.Conn
}

//NewPostgresBackend creates and return a backend backed by PostgresSQL
func NewPostgresBackend(connStr string) (*PostgresBackend, error) {
	db, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		return nil, err
	}
	return &PostgresBackend{db}, nil
}

//CreateTable create necessary table for storing shop records
func (pg *PostgresBackend) CreateTable() error {
	_, err := pg.conn.Exec(context.Background(), `CREATE TABLE public.shops
	(
		shop_id SERIAL NOT NULL,
		name TEXT NOT NULL,
		address TEXT,
		geohash character varying(12),
		type TEXT NOT NULL,
		url TEXT,
		district TEXT,
		search_text TEXT,
		CONSTRAINT shops_pkey PRIMARY KEY (shop_id)
	)`)

	return err
}

//ShopMissingInfo get data with missing info
func (pg *PostgresBackend) ShopMissingInfo() ([]Shop, error) {
	rows, err := pg.conn.Query(context.Background(),
		`SELECT shop_id, name, district, coalesce(address, ''), coalesce(geohash, ''),
		 type FROM shops WHERE geohash IS NULL and district <> $1`, nonPhyStore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		rows.Scan(&shop.ID, &shop.Name, &shop.District, &shop.Address, &shop.Geohash, &shop.Type)
		shoplist = append(shoplist, shop)
	}
	return shoplist, nil
}

//UpdateShopInfo fill missing info into shops
func (pg *PostgresBackend) UpdateShopInfo(shops []Shop) error {
	tx, err := pg.conn.Begin(context.Background())
	if err != nil {
		return err
	}
	var rowsAffected int64 = 0
	for _, shop := range shops {
		cmdTag, err := pg.conn.Exec(context.Background(),
			"UPDATE shops SET address = $1, geohash = $2 WHERE shop_id = $3",
			shop.Address, shop.ToGeohash(), shop.ID)
		if err != nil {
			log.WithError(err).Error("Update shop info error")
			return err
		}
		rowsAffected += cmdTag.RowsAffected()
	}

	tx.Commit(context.Background())
	return err
}

//NearestShops retrieves nearest shops with provided geohash
func (pg *PostgresBackend) NearestShops(lat, long float64, distance string) ([]Shop, error) {
	gHashArr := area(ghash.EncodeWithPrecision(lat, long, 7), distance)
	rows, err := pg.conn.Query(context.Background(),
		"SELECT shop_id, name, type, coalesce(address, ''), coalesce(url,''), geohash, district FROM shops WHERE LEFT(geohash, 7) = ANY($1) order by random()",
		gHashArr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		rows.Scan(&shop.ID, &shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Geohash, &shop.District)
		shoplist = append(shoplist, shop)
	}
	return shoplist, nil
}

//ShopsWithKeyword returns shops with tags provided
func (pg *PostgresBackend) ShopsWithKeyword(keywords string) ([]Shop, error) {
	rows, err := pg.conn.Query(context.Background(), 
	`SELECT shop_id, name, type, coalesce(address, ''), 
	coalesce(url,''), coalesce(geohash, ''), district, coalesce(notes, '') 
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
		rows.Scan(&shop.ID, 
			&shop.Name, 
			&shop.Type, 
			&shop.Address, 
			&shop.URL, 
			&shop.Geohash, 
			&shop.District,
			&shop.Notes,
		)
		shoplist = append(shoplist, shop)
	}
	return shoplist, nil
}

//ShopCount returns the number of shops stored in system
func (pg *PostgresBackend) ShopCount() (int, error) {
	r := pg.conn.QueryRow(context.Background(), "SELECT count(*) FROM shops")
	var cnt int
	err := r.Scan(&cnt)
	if err != nil {
		return -1, err
	}
	return cnt, nil
}

//UpdateTags set keywords for searching for the shops
func (pg *PostgresBackend) UpdateTags() (int, error) {
	ctag, err := pg.conn.Exec(context.Background(), "update shops set search_text = district || ' ' || type where coalesce(TRIM(search_text), '') = ''")
	if err != nil {
		return -1, err
	}
	return int(ctag.RowsAffected()), nil
}

//ShopByID returns shop by internal ID
func (pg *PostgresBackend) ShopByID(shopID int) (Shop, error) {
	r := pg.conn.QueryRow(context.Background(),
		"SELECT name, type, coalesce(address, ''), coalesce(url,''), coalesce(geohash, ''), district, coalesce(notes, '') FROM shops WHERE shop_id = $1", shopID)
	shop := Shop{}
	err := r.Scan(&shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Geohash, &shop.District, &shop.Notes)
	if err != nil {
		return shop, err
	}
	return shop, nil
}

//Close close DB connection
func (pg *PostgresBackend) Close() error {
	return pg.conn.Close(context.Background())
}

// AllShops returns all records from the database
func (pg *PostgresBackend) AllShops() ([]Shop, error) {
	rows, err := pg.conn.Query(context.Background(),
		"SELECT shop_id, name, type, coalesce(address, ''), coalesce(url,''), coalesce(geohash, ''), district FROM shops")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		err := rows.Scan(&shop.ID, &shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Geohash, &shop.District)
		if err != nil {
			return nil, err
		}
		shoplist = append(shoplist, shop)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return shoplist, nil
}

//AdvQuery accepts web search query from user
func (pg *PostgresBackend) AdvQuery(query string) ([]Shop, error) {
	//Filter out to avoid returning every entry
	words := strings.Split(query, " ")
	onlyHasNeg := true
	for i:= range words {
		if !strings.HasPrefix(words[i], "-") && strings.ToLower(words[i]) != "or" {
			onlyHasNeg = false
		}
	}
	if onlyHasNeg {
		return nil, fmt.Errorf("%s returns too many results", query)
	}
	rows, err := pg.conn.Query(context.Background(),
		`SELECT shop_id, name, type, coalesce(address, ''), 
		coalesce(url,''), coalesce(geohash, ''), district, coalesce(notes, '') from shops 
	    where to_tsvector('cuisine', search_text) @@ websearch_to_tsquery('cuisine_syn', $1)`, query)

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		err := rows.Scan(&shop.ID, &shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Geohash, &shop.District, &shop.Notes)
		if err != nil {
			return nil, err
		}
		shoplist = append(shoplist, shop)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return shoplist, nil
}

func area(hash, distance string) []string {
	var result []string
	switch distance {
	case "70m":
		result = []string{hash}
	case "150m":
		result = append(ghash.Neighbors(hash), hash)
	case "1km":
	default:
		result = ghash.Neighbors(hash)
		extended := []string{
			ghash.Neighbor(result[0], ghash.North),
			ghash.Neighbor(result[1], ghash.North),
			ghash.Neighbor(result[1], ghash.NorthEast),
			ghash.Neighbor(result[1], ghash.East),
			ghash.Neighbor(result[2], ghash.East),
			ghash.Neighbor(result[3], ghash.East),
			ghash.Neighbor(result[3], ghash.SouthEast),
			ghash.Neighbor(result[3], ghash.South),
			ghash.Neighbor(result[4], ghash.South),
			ghash.Neighbor(result[5], ghash.South),
			ghash.Neighbor(result[5], ghash.SouthWest),
			ghash.Neighbor(result[5], ghash.West),
			ghash.Neighbor(result[6], ghash.West),
			ghash.Neighbor(result[7], ghash.West),
			ghash.Neighbor(result[7], ghash.NorthWest),
			ghash.Neighbor(result[7], ghash.North),
		}
		result = append([]string{hash}, result...)
		result = append(result, extended...)
	}
	return result
}