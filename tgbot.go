package wongdim

import (
	"equa.link/wongdim/dao"
	"equa.link/wongdim/batch"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"log"
	"net/http"
	"strings"
	"fmt"
	"context"
	"strconv"
	ghash "github.com/mmcloughlin/geohash"
)

// ServeBot is the bot construct for serving shops info
type ServeBot struct {
	bot *tgbotapi.BotAPI
	url string
	mapClient batch.GeocodeClient
	keyFile string
	certFile string
	da dao.Backend
}

// Option is a constructor argument for Retrievr
type Option func(r *ServeBot) error

const (
	//EntriesPerPage is number of entries per display in single message
	EntriesPerPage = 10
	//GeohashPrecision is the no. of characters used to represent a coordinates
	GeohashPrecision = 6
)

// New return new instance of ServeBot
func New(options ...Option) (r *ServeBot, err error) {
	r = &ServeBot{}
	for f := range options {
		err = options[f](r)
		if err != nil {
			return nil, err
		}
	}
	if r.da == nil {
		return nil, fmt.Errorf("Datastore undefined")
	}
	shopCnt, err := r.da.ShopCount()
	log.Printf("Database loaded with %d shop(s)", shopCnt) 
	log.Printf("Authorized on account %s", r.bot.Self.UserName)
	return r, nil
}

// WithWebhookURL configures bot with Telegram Webhook URL
func WithWebhookURL(url string) Option {
	return func(s *ServeBot) error {
		s.url = url
		return nil
	}
}

// WithMapAPIKey configures bot with Google Maps API key
func WithMapAPIKey(key string) Option {
	return func(s *ServeBot) error {
		var err error
		s.mapClient, err = batch.NewGeocodeClient(key)
		return err
	}
}
// WithTelegramAPIKey configures bot with Telegram API key
func WithTelegramAPIKey(key string, debug bool) Option {
	return func(s *ServeBot) error {
		var err error
		s.bot, err = tgbotapi.NewBotAPI(key)
		s.bot.Debug = debug
		return err
	}
}

// WithBackend configures bot with backend database
func WithBackend(backend dao.Backend) Option {
	return func(s *ServeBot) error {
		s.da = backend
		return nil
	}
}

// WithCert configure to use own cert for HTTPS communication
func WithCert(certFile, keyFile string) Option {
	return func(s *ServeBot) error {
		s.keyFile = keyFile
		s.certFile = certFile
		return nil
	}
}

//Listen start the bot to listen to request
func (r *ServeBot) Listen() error {
	log.Printf("URL: %s/%s", r.url, r.bot.Token)
	_, err := r.bot.SetWebhook(tgbotapi.NewWebhook(r.url +"/"+ r.bot.Token))
	if err != nil {
		return err
	}
	info, err := r.bot.GetWebhookInfo()
	if err != nil {
		log.Fatal(err)
	}
	if info.LastErrorDate != 0 {
		log.Printf("Telegram callback failed: %s", info.LastErrorMessage)
	}
	updates := r.bot.ListenForWebhook("/" + r.bot.Token)
	for i := 0; i < 5; i++ {
		go r.process(updates)
	}
	//Create a URL for triggering fillInfo batch
	http.HandleFunc("/fillInfo", func(writer http.ResponseWriter, req *http.Request) {
		ctx := context.Background()
		
		errCh := batch.Run(ctx, r.da, r.mapClient.FillGeocode)

		go func() {
			for e := range errCh {
				log.Printf("[ERR] %s", e)
			}
		}()
		
		cache.Purge()
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("OK"))
	})
	if len(r.certFile) > 0 {
		http.ListenAndServeTLS("0.0.0.0:443", r.certFile, r.keyFile, nil)
	} else {
		http.ListenAndServe("0.0.0.0:80", nil)
	}

	return nil
}

func (r *ServeBot) process(updates tgbotapi.UpdatesChannel) {
	for update := range updates {

		switch {
		case update.InlineQuery != nil:
			// Inline query
			shops, err := r.shopWithTags(strings.TrimSpace(update.InlineQuery.Query))
			if err != nil {
				log.Printf("[ERR] Database error: %v", err)
				continue
			}
			result := make([]interface{}, 0, len(shops))
			for i := range shops {
				box:= ghash.BoundingBox(shops[i].Geohash)
				lat, long := box.Center()
				r := tgbotapi.NewInlineQueryResultLocation(
					update.InlineQuery.Query + strconv.Itoa(shops[i].ID), shops[i].String(), lat, long)
				r.InputMessageContent = tgbotapi.InputVenueMessageContent{
					Latitude: lat,
					Longitude: long,
					Title: shops[i].Name,
					Address: shops[i].Address,
				}
				result = append(result, r)
			}
			_, err = r.bot.AnswerInlineQuery(tgbotapi.InlineConfig{
				InlineQueryID: update.InlineQuery.ID,
				IsPersonal: true,
				Results: result,
				//NextOffset: ghash + "||",
			})
		case update.CallbackQuery != nil:
			//When user click one of the inline button in message in direct chat
			if update.CallbackQuery.Message != nil {
				if update.CallbackQuery.Data[0] == 'P' {
					//Jump to another page
					pageInfo := strings.Split(update.CallbackQuery.Data[1:], "||")
					var shops []dao.Shop
					offset, err := strconv.Atoi(pageInfo[0])
					if strings.HasPrefix(pageInfo[1], "<G>") {
						shops, err = r.shopWithGeohash(strings.TrimPrefix(pageInfo[1], "<G>"))
					} else {
						shops, err = r.shopWithTags(pageInfo[1])
					}
					if err != nil {
						log.Print(err)
					}

					err = r.RefreshList(update.CallbackQuery.Message.Chat.ID, 
						update.CallbackQuery.Message.MessageID,
						shops,
						strings.Join(pageInfo[1:], "||"),
						EntriesPerPage, offset,
					)
					if err != nil {
						log.Print(err)
					}
				} else {
					//Pick an item and post its detail, behaves same as picking 
					//single item
					itemID, err := strconv.Atoi(update.CallbackQuery.Data)
					if err != nil {
						log.Printf("Unexpected data: %s, %v", update.CallbackQuery.Data, err)		
					} else {
						result, err := r.da.ShopByID(itemID)
						if err != nil {
							r.SendMsg(update.CallbackQuery.Message.Chat.ID, "Database error!")
							log.Printf("Shop not found: %d, %v", itemID, err)
						} else {
							r.SendSingleShop(update.CallbackQuery.Message.Chat.ID, result)
						}
					}
				}
				r.bot.AnswerCallbackQuery(tgbotapi.NewCallback(update.CallbackQuery.ID, update.CallbackQuery.Data))
			}
		case update.Message != nil:
			//Direct chat
			switch {
			case update.Message.Location != nil:
				//Posting location
				//Get geohash
				geoHashStr := ghash.EncodeWithPrecision(update.Message.Location.Latitude, update.Message.Location.Longitude, GeohashPrecision)
				log.Println("Geohash submitted: ", geoHashStr)
				shops, err := r.shopWithGeohash(geoHashStr)
				if err != nil {
					r.SendMsg(update.Message.Chat.ID, "Database error! Please try again later")
				}

				switch (len(shops)) {
				case 0:
					err = r.SendMsg(update.Message.Chat.ID, "No shops found nearby!")
				case 1:
					err = r.SendSingleShop(update.Message.Chat.ID, shops[0])
				default:
					err = r.SendList(update.Message.Chat.ID, shops, "<G>" + geoHashStr, EntriesPerPage, 0)
				}
				if err != nil {
					log.Print(err)
				}
				
			case len(update.Message.Text) > 0:
				//Text search
				msgBody := strings.Builder{}
				shops, err := r.shopWithTags(update.Message.Text)
				if err != nil {
					msgBody.WriteString("Error! DB error!")
					log.Println("DB err:", err)
				}
				switch (len(shops)) {
				case 0:
					err = r.SendMsg(update.Message.Chat.ID, "No shops found with keywords!")
				case 1:
					err = r.SendSingleShop(update.Message.Chat.ID, shops[0])
				default:
					err = r.SendList(update.Message.Chat.ID, shops, update.Message.Text, EntriesPerPage, 0)
				}
				if err != nil {
					log.Print(err)
				}
			}
		}
	}
}

// SendMsg sends simple telegram message back to user
func (r ServeBot) SendMsg(chatID int64, text string) error {
	msg := tgbotapi.NewMessage(chatID, text)
	_, err := r.bot.Send(msg)
	if err != nil {
		return err
	}
	return nil
}

// RefreshList edit an already sent message to refresh shops list when 
// user request next/prev page
func (r ServeBot) RefreshList(chatID int64, messageID int, shops []dao.Shop, key string, limit, offset int) error {
	msgBody, buttons := shopListMessage(shops, key, limit, offset)
	editMsg := tgbotapi.NewEditMessageText(chatID, messageID, msgBody)
	editMsg.ParseMode = tgbotapi.ModeMarkdown
	_, err := r.bot.Send(editMsg)
	if err != nil {
		err = fmt.Errorf("Error editing message: %w", err)
	}
	
	_, err = r.bot.Send(tgbotapi.NewEditMessageReplyMarkup(chatID, messageID, buttons))
	if err != nil {
		err = fmt.Errorf("Error updating buttons: %w", err)
	}
	return err
}

//SendList sends a restaurant list along with callback inline btns
func (r ServeBot) SendList(chatID int64, shops []dao.Shop, key string, limit, offset int) error {
	msgBody, buttons := shopListMessage(shops, key, limit, offset)
	msg := tgbotapi.NewMessage(chatID, msgBody)
	msg.ParseMode = tgbotapi.ModeMarkdown
	msg.ReplyMarkup = buttons
	_, err := r.bot.Send(msg)
	return err
}

func shopListMessage(shops []dao.Shop, key string, limit, offset int) (string, tgbotapi.InlineKeyboardMarkup) {
	msgBody := strings.Builder{}
	// Do paging
	pagedShop := shops[offset:min(len(shops), offset+limit)]
	btns := make([]tgbotapi.InlineKeyboardButton, 0, len(pagedShop))
	// Generate message body and nav buttons
	for i := range pagedShop {
		msgBody.WriteString(fmt.Sprintf("(%d) *%s* (%s) - %s\n", i+1, pagedShop[i].Name, pagedShop[i].Type, pagedShop[i].District))
		btns = append(btns, tgbotapi.NewInlineKeyboardButtonData(strconv.Itoa(i+1), strconv.Itoa(pagedShop[i].ID)))
	}
	fullInlineKb := make([][]tgbotapi.InlineKeyboardButton, 0)
	if len(btns) > 5 {
		//Split into 2 rows if more than 5 btns
		fullInlineKb = append(fullInlineKb, btns[:5], btns[5:])
	} else {
		fullInlineKb = append(fullInlineKb, btns)
	}
	//Add prev/next btn on second row
	pageControl := make([]tgbotapi.InlineKeyboardButton, 0)
	if offset > 0 {
		pageControl = append(pageControl, tgbotapi.NewInlineKeyboardButtonData("⏮️", fmt.Sprintf("P%d||%s", min(0, offset-limit), key)))
	}
	if offset + EntriesPerPage < len(shops)-1 {
		pageControl = append(pageControl, tgbotapi.NewInlineKeyboardButtonData("⏭️", fmt.Sprintf("P%d||%s", min(len(shops), offset+limit), key)))
	}
	if len(pageControl) > 0 {
		fullInlineKb = append(fullInlineKb, pageControl)
	}

	return msgBody.String(), tgbotapi.NewInlineKeyboardMarkup(fullInlineKb...)
}

//SendSingleShop sends single shop data to Chat, along with 
// coordinates
func (r ServeBot) SendSingleShop(chatID int64, shop dao.Shop) error {
	box := ghash.BoundingBox(shop.Geohash)
	lat, long := box.Center()
	venue := tgbotapi.NewVenue(chatID, shop.Name, shop.Address, lat, long)
	_, err := r.bot.Send(venue)
	if err != nil {
		return fmt.Errorf("ChatID %v cannot be sent: %v", chatID, err)
	}
	return nil
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}