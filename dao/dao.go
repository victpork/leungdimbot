package dao

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"log"
)

//Shop is a struct for storing shop info
type Shop struct {
	ID       int    //Internal ID
	Name     string //Shop name
	Address  string //Shop address
	Geohash  string //Geohash code for lat/long coordinates
	Type     string
	District string
	URL      string
	Tags     []string
}

var (
	//DB is the database connection
	DB *sql.DB
)

func (s Shop) String() string {
	return fmt.Sprintf("%s (%s)\n%s", s.Name, s.Type, s.Address)
}

//CreateTable create necessary table for storing shop records
func CreateTable() error {
	_, err := DB.Exec(`CREATE TABLE public.shops
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
func ShopMissingInfo() ([]Shop, error) {
	rows, err := DB.Query("SELECT shop_id, name, district, coalesce(address, ''), coalesce(geohash, ''), type FROM shops WHERE geohash IS NULL OR tags IS NULL")
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
func UpdateShopInfo(shops []Shop) error {
	tx, err := DB.Begin()
	if err != nil {
		return err
	}
	stmt, err := DB.Prepare("UPDATE shops SET address = $1, geohash = $2 WHERE shop_id = $3")
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, shop := range shops {
		_, err = stmt.Exec(shop.Address, shop.Geohash, shop.ID)
		if err != nil {
			log.Printf("[ERR] %e", err)
			return err
		}
	}
	err = tx.Commit()
	return err
}

//NearestShops retrieves nearest shops with provided geohash
func NearestShops(geohash string) ([]Shop, error) {
	rows, err := DB.Query("SELECT shop_id, name, type, coalesce(address, ''), coalesce(url,''), tags, geohash, district FROM shops WHERE LEFT(geohash, 6) = $1 ORDER BY geohash",
		geohash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shoplist := make([]Shop, 0)
	for rows.Next() {
		shop := Shop{}
		rows.Scan(&shop.ID, &shop.Name, &shop.Type, &shop.Address, &shop.URL, pq.Array(&shop.Tags), &shop.Geohash, &shop.District)
		shoplist = append(shoplist, shop)
	}
	return shoplist, nil
}

//ShopsWithTags returns shops with tags provided
func ShopsWithTags(tags []string) ([]Shop, error) {
	rows, err := DB.Query(`SELECT shop_id, name, type, address, coalesce(url,''), geohash, district 
	FROM shops WHERE (tags @> $1 OR name ILIKE '%'||$2||'%') and address IS NOT NULL`,
		pq.Array(tags), tags[0])
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
func ShopCount() (int, error) {
	r := DB.QueryRow("SELECT count(*) FROM shops")
	var cnt int
	err := r.Scan(&cnt)
	if err != nil {
		return -1, err
	}
	return cnt, nil
}

//UpdateTags set keywords for searching for the shops
func UpdateTags() error {
	stmt, err := DB.Prepare("update shops set tags = array[district,type] where tags is null")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}

//ShopByID returns shop by internal ID
func ShopByID(shopID int) (Shop, error) {
	r := DB.QueryRow("SELECT name, type, address, coalesce(url,''), geohash, district FROM shops WHERE shop_id = $1", shopID)
	shop := Shop{}
	err := r.Scan(&shop.Name, &shop.Type, &shop.Address, &shop.URL, &shop.Geohash, &shop.District)
	if err != nil {
		return shop, err
	}
	return shop, nil
}
