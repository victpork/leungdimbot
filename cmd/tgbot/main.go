package main

import (
	"equa.link/wongdim"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/viper"
	"log"
	"fmt"
	"context"
)

func init() {
	viper.SetDefault("tg.debug", false)
}

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/wongdim/")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvPrefix("WDIM")
	
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil { // Handle errors reading the config file
		log.Printf("Config file not detected: %s \n", err)
	}
	//Connect to DB
	dbConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", 
		viper.Get("db.host"),
		viper.Get("db.port"),
		viper.Get("db.user"),
		viper.Get("db.password"),
		viper.Get("db.db"))
	db, err := pgx.Connect(context.Background(), dbConnStr)
	if err != nil {
		log.Fatal("Could not connect to database", err)
	}
	log.Print("Database connected")
	bot, err := wongdim.New(
		wongdim.WithDatabase(db),
		wongdim.WithTelegramAPIKey(viper.GetString("tg.key"), viper.GetBool("tg.debug")),
		wongdim.WithWebhookURL(viper.GetString("tg.serveURL")),
		wongdim.WithMapAPIKey(viper.GetString("googlemap.key")),
	)
	if err != nil {
		log.Fatal("Could not create bot, ", err)
	}
	bot.Listen()
}
