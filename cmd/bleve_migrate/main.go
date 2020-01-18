package main

import (
	"equa.link/wongdim/dao"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	viper.SetDefault("backendType", dao.PostgreSQL)

	viper.SetDefault("db.host", "0.0.0.0")
	viper.SetDefault("db.port", "6543")
	viper.SetDefault("db.user", "wongdim")
	viper.SetDefault("db.password", "wongdimpassword")
	viper.SetDefault("db.db", "wongdim")

	viper.SetDefault("bleve.path", "/wongdim/datastore")
}

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/wongdim/")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvPrefix("WDIM")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.WithError(err).Error("Config file not found")
	}

	dbConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		viper.Get("db.host"),
		viper.Get("db.port"),
		viper.Get("db.user"),
		viper.Get("db.password"),
		viper.Get("db.db"))
	db, err := dao.NewPostgresBackend(dbConnStr)
	if err != nil {
		log.WithError(err).Fatal("Could not connect to database")
	}
	defer db.Close()
	log.Info("Database connected")

	blevebe, err := dao.NewBleveBackend(viper.GetString("bleve.path"))
	if err != nil {
		log.WithError(err).Fatal("Could not create index")
	}
	defer blevebe.Close()
	shops, err := db.AllShops()
	if err != nil {
		log.WithError(err).Fatal("Could not extract shops from database")
	}
	log.Printf("%d rows extracted", len(shops))
	err = blevebe.UpdateShopInfo(shops)
	if err != nil {
		log.WithError(err).Fatal("Could not import shops into bleve store")
	}
	log.Info("Done")
}
