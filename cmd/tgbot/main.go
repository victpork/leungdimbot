package main

import (
	"equa.link/wongdim"
	"equa.link/wongdim/dao"
	"fmt"
	"github.com/orandin/lumberjackrus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io/ioutil"
)

const (
	tgWebhook  = "H"
	tgLongPoll = "P"
)

func init() {
	viper.SetDefault("tg.debug", false)
	viper.SetDefault("tg.connectType", tgLongPoll)
	viper.SetDefault("tg.serveURL", "localhost")
	viper.SetDefault("backendType", dao.PostgreSQL)

	viper.SetDefault("db.host", "0.0.0.0")
	viper.SetDefault("db.port", 6543)
	viper.SetDefault("db.user", "wongdim")
	viper.SetDefault("db.password", "wongdimpassword")
	viper.SetDefault("db.db", "wongdim")

	viper.SetDefault("bleve.path", "/wongdim/datastore")

	viper.SetDefault("helpfile", "/wongdim/help.txt")

	hook, err := lumberjackrus.NewHook(
		&lumberjackrus.LogFile{
			Filename:   "/wongdim/log/access.log",
			MaxSize:    100,
			MaxBackups: 10,
			MaxAge:     1,
			Compress:   false,
			LocalTime:  false,
		},
		log.InfoLevel,
		&log.JSONFormatter{},
		&lumberjackrus.LogFileOpts{},
	)

	if err != nil {
		log.WithError(err).Error("Cannot create file log hook")
	} else {
		log.AddHook(hook)
	}
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

	var beOptCfg wongdim.Option
	beType := viper.Get("backendType")
	switch beType {
	case dao.PostgreSQL:
		//Connect to DB
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
		beOptCfg = wongdim.WithBackend(db)
	case dao.PostGIS:
		dbConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			viper.Get("db.host"),
			viper.Get("db.port"),
			viper.Get("db.user"),
			viper.Get("db.password"),
			viper.Get("db.db"))
		db, err := dao.NewPostGISBackend(dbConnStr)
		if err != nil {
			log.WithError(err).Fatal("Could not connect to database")
		}
		defer db.Close()
		log.Info("Database connected")
		beOptCfg = wongdim.WithBackend(db)
	case dao.Bleve:
		//Use Bleve-based storgage
		blevebe, err := dao.NewBleveBackend(viper.GetString("bleve.path"))
		if err != nil {
			log.WithError(err).Fatal("Could not create index")
		}
		beOptCfg = wongdim.WithBackend(blevebe)
	}
	helpContent, err := ioutil.ReadFile(viper.GetString("helpfile"))
	if err != nil {
		log.WithError(err).Fatal("Cannot read help file")
	}

	bot, err := wongdim.New(
		beOptCfg,
		wongdim.WithTelegramAPIKey(viper.GetString("tg.key"), viper.GetBool("tg.debug")),
		wongdim.WithWebhookURL(viper.GetString("tg.serveURL")),
		wongdim.WithMapAPIKey(viper.GetString("googlemap.key")),
		wongdim.WithHelpMsg(string(helpContent)),
	)
	if err != nil {
		log.WithError(err).Fatal("Could not create TG bot")
	}
	switch viper.GetString("tg.connectType") {
	case tgWebhook:
		bot.Listen()
	case tgLongPoll:
		bot.Connect()
	}
}
