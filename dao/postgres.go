package dao

import (
	"context"
	pgx "github.com/jackc/pgx/v4"
	"log"
	ghash "github.com/mmcloughlin/geohash"
	"strings"
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
		name character varying(50) NOT NULL,
		address character varying(200),
		geohash character varying(12),
		type character varying(7) NOT NULL,
		url character varying(200),
		district character varying(10),
		tags character varying[],
		CONSTRAINT shops_pkey PRIMARY KEY (shop_id)
	)`)

	return err
}

//ShopMissingInfo get data with missing info
func (pg *PostgresBackend) ShopMissingInfo() ([]Shop, error) {
	rows, err := pg.conn.Query(context.Background(),
		"SELECT shop_id, name, district, coalesce(address, ''), coalesce(geohash, ''), type FROM shops WHERE geohash IS NULL OR tags IS NULL")
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
			shop.Address, shop.Geohash, shop.ID)
		if err != nil {
			log.Printf("[ERR] %e", err)
			return err
		}
		rowsAffected += cmdTag.RowsAffected()
	}

	tx.Commit(context.Background())
	return err
}

//NearestShops retrieves nearest shops with provided geohash
func (pg *PostgresBackend) NearestShops(lat, long float64, distance string) ([]Shop, error) {
	rows, err := pg.conn.Query(context.Background(),
		"SELECT shop_id, name, type, coalesce(address, ''), coalesce(url,''), geohash, district FROM shops WHERE LEFT(geohash, 6) = $1 ORDER BY geohash",
		ghash.EncodeWithPrecision(lat, long, 6))
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
	var rows pgx.Rows
	var err error
	tags := strings.Split(keywords, " ")
	if len(tags) == 1 {
		rows, err = pg.conn.Query(context.Background(), `SELECT shop_id, name, type, address, 
		coalesce(url,''), geohash, district 
		FROM shops WHERE (tags @> $1 OR name ILIKE '%'||$2||'%') and address IS NOT NULL`,
			tags, tags[0])
	} else {
		rows, err = pg.conn.Query(context.Background(), `SELECT shop_id, name, type, address, 
	coalesce(url,''), geohash, district 
	FROM shops WHERE tags @> $1 and address IS NOT NULL`,
		tags)
	}
	
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		err := rows.Scan(&shop.ID, &shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Geohash, &shop.District)
		if err != nil {
			log.Println(err)
		}
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
	ctag, err := pg.conn.Exec(context.Background(), "update shops set tags = array[district,type] where tags is null")
	if err != nil {
		return -1, err
	}
	return int(ctag.RowsAffected()), nil
}

//ShopByID returns shop by internal ID
func (pg *PostgresBackend) ShopByID(shopID int) (Shop, error) {
	r := pg.conn.QueryRow(context.Background(),
		"SELECT name, type, address, coalesce(url,''), geohash, district FROM shops WHERE shop_id = $1", shopID)
	shop := Shop{}
	err := r.Scan(&shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Geohash, &shop.District)
	if err != nil {
		return shop, err
	}
	return shop, nil
}

//Close close DB connection
func (pg *PostgresBackend) Close() error {
	return pg.conn.Close(context.Background())
}