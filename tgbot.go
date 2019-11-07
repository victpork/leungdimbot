package wongdim

import (
	"context"
	"equa.link/wongdim/batch"
	"equa.link/wongdim/dao"
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	ghash "github.com/mmcloughlin/geohash"
	"log"
	"net/http"
	"strconv"
	"strings"
	"net/url"
)

// ServeBot is the bot construct for serving shops info
type ServeBot struct {
	bot       *tgbotapi.BotAPI
	url       string
	mapClient batch.GeocodeClient
	keyFile   string
	certFile  string
	da        dao.Backend
}

// Option is a constructor argument for Retrievr
type Option func(r *ServeBot) error

const (
	//EntriesPerPage is number of entries per display in single message
	EntriesPerPage = 10
	//GeohashPrecision is the no. of characters used to represent a coordinates
	GeohashPrecision = 7

	geoSearchPrefix = "<G>"
	simpleSearchPrefix = "<S>"
	advSearchPrefix = "<A>"
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
	log.Printf("[LOG] Database loaded with %d shop(s)", shopCnt)
	log.Printf("[LOG] Authorized on account %s", r.bot.Self.UserName)
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
	log.Printf("[LOG] Listening on URL: %s/%s", r.url, r.bot.Token)
	_, err := r.bot.SetWebhook(tgbotapi.NewWebhook(r.url + "/" + r.bot.Token))
	if err != nil {
		return err
	}
	info, err := r.bot.GetWebhookInfo()
	if err != nil {
		log.Fatal(err)
	}
	if info.LastErrorDate != 0 {
		log.Printf("[ERR] Telegram callback failed: %s", info.LastErrorMessage)
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
				log.Printf("[ERR] Batch error: %s", e)
			}
		}()

		cache.Flush()
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
			offset := 0
			if update.InlineQuery.Offset != "" {
				var err error
				offset, err = strconv.Atoi(update.InlineQuery.Offset)
				if err != nil {
					offset = 50
				}
			}
			// Skip empty queries
			if strings.TrimSpace(update.InlineQuery.Query) == "" {
				continue
			}
			log.Printf("[LOG] Inline query: %s", strings.TrimSpace(update.InlineQuery.Query))
			shops, err := r.shopWithTags(strings.TrimSpace(update.InlineQuery.Query))
			log.Printf("[LOG] %d result(s) returned", len(shops))
			if err != nil {
				log.Printf("[ERR] Database error: %v", err)
				continue
			}
			orgLen := len(shops)
			if orgLen > 50 {
				//Paging, telegram does not support over 50 inline results
				shops = shops[offset:min(orgLen, offset+50)]
			} 
			result := make([]interface{}, len(shops))
			for i := range shops {
				if shops[i].Geohash != "" {
					box := ghash.BoundingBox(shops[i].Geohash)
					lat, long := box.Center()
					r := tgbotapi.NewInlineQueryResultLocation(
						update.InlineQuery.Query+strconv.Itoa(shops[i].ID), shops[i].String(), lat, long)
					r.InputMessageContent = tgbotapi.InputVenueMessageContent{
						Latitude:  lat,
						Longitude: long,
						Title:     shops[i].Name,
						Address:   shops[i].Address,
					}
					var t tgbotapi.InlineKeyboardButton
					if shops[i].URL != "" {
						t = tgbotapi.NewInlineKeyboardButtonURL("🏠店舖網站", shops[i].URL)
					} else {
						t = tgbotapi.NewInlineKeyboardButtonURL("🔍Google 店名", "https://google.com/search?q="+url.PathEscape(shops[i].Name))
					}
					l := tgbotapi.NewInlineKeyboardMarkup(tgbotapi.NewInlineKeyboardRow(t))
					r.ReplyMarkup = &l
					result[i] = r
				} else {
					r := tgbotapi.NewInlineQueryResultArticleMarkdown(
						update.InlineQuery.Query+strconv.Itoa(shops[i].ID), 
						fmt.Sprintf("%s - (%s)", shops[i].String(), shops[i].District),
						fmt.Sprintf("%s - (%s)", shops[i].String(), shops[i].District) + shops[i].URL, 
					)
					r.URL = shops[i].URL
					result[i] = r
				}
				
			}

			inlineCfg := tgbotapi.InlineConfig{
				InlineQueryID: update.InlineQuery.ID,
				IsPersonal:    true,
				Results:       result,
			}
			if offset+50 < orgLen { 
				inlineCfg.NextOffset = strconv.Itoa(offset+50)
			}
			_, err = r.bot.AnswerInlineQuery(inlineCfg)
		case update.CallbackQuery != nil:
			//When user click one of the inline button in message in direct chat
			if update.CallbackQuery.Message != nil {
				if update.CallbackQuery.Data == "---" {
					r.bot.AnswerCallbackQuery(tgbotapi.NewCallback(update.CallbackQuery.ID, update.CallbackQuery.Data))
					continue
				}
				if update.CallbackQuery.Data[0] == 'P' {
					//Jump to another page
					pageInfo := strings.Split(update.CallbackQuery.Data[1:], "||")
					var shops []dao.Shop
					offset, err := strconv.Atoi(pageInfo[0])
					if strings.HasPrefix(pageInfo[1], geoSearchPrefix) {
						shops, err = r.shopWithGeohash(ghash.Decode(strings.TrimPrefix(pageInfo[1], geoSearchPrefix)))
					} else if strings.HasPrefix(pageInfo[1], advSearchPrefix) {
						shops, err = r.advSearch(strings.TrimPrefix(pageInfo[1], advSearchPrefix))
					} else {
						shops, err = r.shopWithTags(strings.TrimPrefix(pageInfo[1], simpleSearchPrefix))
					}
					if err != nil {
						log.Print(err)
					}
					if len(shops) == 0 {
						log.Print("Cache hit failed, search string: ", pageInfo[1])
						r.SendMsg(update.CallbackQuery.Message.Chat.ID, "系統錯誤，請稍後重試")
						r.bot.AnswerCallbackQuery(tgbotapi.NewCallback(update.CallbackQuery.ID, update.CallbackQuery.Data))
						continue
					}
					err = r.RefreshList(update.CallbackQuery.Message.Chat.ID,
						update.CallbackQuery.Message.MessageID,
						shops,
						strings.Join(pageInfo[1:], "||"),
						EntriesPerPage, offset,
					)
					if err != nil {
						log.Print("[ERR] Telegram error: ", err)
					}
				} else {
					//Pick an item and post its detail, behaves same as picking
					//single item
					itemID, err := strconv.Atoi(update.CallbackQuery.Data)
					if err != nil {
						log.Printf("[ERR] Unexpected data: %s, %v", update.CallbackQuery.Data, err)
					} else {
						result, err := r.da.ShopByID(itemID)
						log.Printf("[LOG] Single shop %d selected: (%s)", itemID, result.Name)
						if err != nil {
							r.SendMsg(update.CallbackQuery.Message.Chat.ID, "資料庫錯誤! 找不到店舖")
							log.Printf("[LOG] Shop not found: %d, %v", itemID, err)
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
				//geoHashStr := ghash.EncodeWithPrecision(update.Message.Location.Latitude, update.Message.Location.Longitude, GeohashPrecision)
				shops, err := r.shopWithGeohash(update.Message.Location.Latitude, update.Message.Location.Longitude)
				if err != nil {
					r.SendMsg(update.Message.Chat.ID, "資料庫錯誤！請稍後再試")
					log.Print("Database error: ", err)
				}
				log.Printf("Location search, %d result(s) returned", len(shops))
				switch len(shops) {
				case 0:
					err = r.SendMsg(update.Message.Chat.ID, "附近找不到店舖！")
				case 1:
					err = r.SendSingleShop(update.Message.Chat.ID, shops[0])
				default:
					geoHashStr := ghash.EncodeWithPrecision(update.Message.Location.Latitude, update.Message.Location.Longitude, GeohashPrecision)
					err = r.SendList(update.Message.Chat.ID, shops, geoSearchPrefix+geoHashStr, EntriesPerPage, 0)
				}
				if err != nil {
					log.Print("[ERR] Telegram error: ", err)
				}

			case len(update.Message.Text) > 0:
				if update.Message.Text == "/start" || update.Message.Text == "/help" {
					msgBody := strings.Builder{}
					msgBody.WriteString("🍙直接輸入關鍵字(以空格分隔例如「中環 咖啡」) 或店名一部份搜尋\n\n")
					msgBody.WriteString("🍙輸入「網店」作關鍵字可搜尋沒實體店面的商戶\n\n")
					msgBody.WriteString("🍙可直接提供座標 (📎>Location) 搜尋座標附近店舖\n\n")
					msgBody.WriteString("🍙利用內嵌功能(在其他對話中輸入 @WongDimBot 再加上關鍵字)搜尋及分享店舖")
					r.SendMsg(update.Message.Chat.ID, msgBody.String())
					if update.Message.Text == "/start" {
						log.Print("[LOG] New joiner")
					}
				} else {
					var shops []dao.Shop
					var err error
					if strings.HasPrefix(update.Message.Text, "/query") { 
						queryStr := strings.TrimPrefix(update.Message.Text, "/query ")
						shops, err = r.advSearch(strings.TrimSpace(queryStr))
						if err != nil {
							r.SendMsg(update.Message.Chat.ID, "資料庫錯誤")
							log.Println("[ERR] Database error: ", err)
						}
						log.Printf("Advance search: \"%s\", %d results returned", queryStr, len(shops))
					} else {					
						//Text search
						shops, err = r.shopWithTags(strings.TrimSpace(update.Message.Text))
						if err != nil {
							r.SendMsg(update.Message.Chat.ID, "資料庫錯誤")
							log.Println("[ERR] Database error: ", err)
						}
						log.Printf("Simple search: \"%s\", %d results returned", update.Message.Text, len(shops))
					}
					switch len(shops) {
					case 0:
						err = r.SendMsg(update.Message.Chat.ID, "關鍵字找不到任何結果\n可嘗試直接提供座標 (📎>Location) 搜尋座標附近店舖")
					case 1:
						err = r.SendSingleShop(update.Message.Chat.ID, shops[0])
					default:
						if strings.HasPrefix(update.Message.Text, "/query") {
							err = r.SendList(update.Message.Chat.ID, shops, advSearchPrefix + strings.TrimPrefix(update.Message.Text, "/query "), EntriesPerPage, 0)
						} else {
							err = r.SendList(update.Message.Chat.ID, shops, simpleSearchPrefix + update.Message.Text, EntriesPerPage, 0)
						}
					}
					if err != nil {
						log.Print(err)
					}
				}
			}
		}
	}
}

// SendMsg sends simple telegram message back to user
func (r ServeBot) SendMsg(chatID int64, text string) error {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = tgbotapi.ModeMarkdown
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
	editMsg.DisableWebPagePreview = true
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
	msg.DisableWebPagePreview = true
	msg.ReplyMarkup = buttons
	_, err := r.bot.Send(msg)
	return err
}

func shopListMessage(shops []dao.Shop, key string, limit, offset int) (string, tgbotapi.InlineKeyboardMarkup) {
	msgBody := strings.Builder{}
	// Do paging
	pageInd := fmt.Sprintf("%d/%d", offset/EntriesPerPage+1, (len(shops) + EntriesPerPage - 1) / EntriesPerPage)
	pagedShop := shops[offset:min(len(shops), offset+limit)]
	btns := make([]tgbotapi.InlineKeyboardButton, 0, len(pagedShop))
	// Generate message body and nav buttons
	for i := range pagedShop {
		msgBody.WriteString(fmt.Sprintf("(%d) *%s* (%s) - %s", i+1, pagedShop[i].Name, pagedShop[i].Type, pagedShop[i].District))
		if pagedShop[i].URL != "" {
			msgBody.WriteString(fmt.Sprintf(" [連結](%s)", pagedShop[i].URL))
		}
		if pagedShop[i].Notes != "" {
			msgBody.WriteString(fmt.Sprintf("\n📝%s", pagedShop[i].Notes))
		}
		msgBody.WriteString("\n")
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
		pageControl = append(pageControl, tgbotapi.NewInlineKeyboardButtonData("⏮️", fmt.Sprintf("P%d||%s", max(0, offset-limit), key)))
		//Insert page number
		pageControl = append(pageControl, tgbotapi.NewInlineKeyboardButtonData(pageInd, "---"))
	}
	if offset+EntriesPerPage < len(shops) {
		if len(pageControl) == 0 {
			//Insert page number
			pageControl = append(pageControl, tgbotapi.NewInlineKeyboardButtonData(pageInd, "---"))
		}
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
	if shop.Geohash != "" {
		box := ghash.BoundingBox(shop.Geohash)
		lat, long := box.Center()
		venue := tgbotapi.NewVenue(chatID, fmt.Sprintf("%s (%s)", shop.Name, shop.Type), shop.Address, lat, long)
		
		var t tgbotapi.InlineKeyboardButton
		if shop.URL != "" {
			t = tgbotapi.NewInlineKeyboardButtonURL("🏠店舖網站", shop.URL)
		} else {
			t = tgbotapi.NewInlineKeyboardButtonURL("🔍Google 店名", "https://google.com/search?q="+url.PathEscape(shop.Name))
		}
		row := tgbotapi.NewInlineKeyboardRow(t)
		venue.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(row)
		_, err := r.bot.Send(venue)
		if err != nil {
			return fmt.Errorf("ChatID %v cannot be sent: %v", chatID, err)
		}
	} else {
		//non-physical store
		r.SendMsg(chatID, fmt.Sprintf("*%s* (%s) - \n[連結](%s)", shop.Name, shop.Type, shop.URL))
	}
	if shop.Notes != "" {
		r.SendMsg(chatID, fmt.Sprintf("📝備註: %s", shop.Notes))
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
