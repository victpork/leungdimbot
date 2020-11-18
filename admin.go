package wongdim

import (
	"fmt"
	"strconv"
	"strings"

	"equa.link/wongdim/dao"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	newShop actionCode = iota
	newShopName
	newShopType
	newShopDistrict
	newShopAddress
	newShopLocation
	newShopURL
	newShopKeywords
	newShopPreview
	editSearch
)

const (
	newShopCallback  = "newShop"
	editShopCallback = "searchShop"
	logoutCallback   = "logout"
	editName         = "EName"
	editType         = "EType"
	editDistrict     = "EDist"
	editAddress      = "EAddr"
	editLocation     = "ELoc"
	editURL          = "EURL"
	editKeywords     = "EKey"
	editStatus       = "EStatus"
)

type actionCode int

type state struct {
	Action       actionCode
	SelectedShop dao.Shop
}

func (r ServeBot) adminChallenge(u tgbotapi.Update) {
	//Admin mode
	userList := viper.GetStringSlice("tg.admin")
	for i := range userList {
		if userList[i] == strconv.Itoa(u.Message.From.ID) {
			r.enterAdminMode(u.Message.Chat.ID, state{})
			mainMenu := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("新商戶", newShopCallback)),
				tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("搜尋商戶", editShopCallback)),
				tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("離開", logoutCallback)),
			)
			log.WithFields(log.Fields{
				"userID": u.Message.From.ID,
				"chatID": u.Message.Chat.ID,
			}).Info("User entered admin mode")
			msg := tgbotapi.NewMessage(u.Message.Chat.ID, "管理選單")
			msg.ReplyMarkup = mainMenu
			r.bot.Send(msg)
			return
		}
	}
	//Missed
	r.SendMsg(u.Message.Chat.ID, "Access denied")
	log.WithFields(log.Fields{
		"userID":   u.Message.From.ID,
		"username": u.Message.From.UserName,
	}).Info("Unauthorized user trying to access admin mode")
}

//SendSingleShopEdit is the edit interface for a single shop search result
func (r ServeBot) SendSingleShopEdit(chatID int64, shop dao.Shop) error {
	if shop.HasPhyLoc() {
		lat, long := shop.ToCoord()
		p := tgbotapi.NewVenue(chatID, fmt.Sprintf("%s-%s (%s)", shop.Name, shop.District, shop.Type), shop.Address, lat, long)
		row := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("更改名字", editName+strconv.Itoa(shop.ID)),
			tgbotapi.NewInlineKeyboardButtonData("更改類型", editType+strconv.Itoa(shop.ID)),
			tgbotapi.NewInlineKeyboardButtonData("更改地區", editDistrict+strconv.Itoa(shop.ID)),
		)
		row2 := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("更改地址", editAddress+strconv.Itoa(shop.ID)),
			tgbotapi.NewInlineKeyboardButtonData("結業", editStatus+strconv.Itoa(shop.ID)),
			tgbotapi.NewInlineKeyboardButtonData("更改座標", editLocation+strconv.Itoa(shop.ID)),
		)
		row3 := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("關鍵字", editKeywords+strconv.Itoa(shop.ID)),
		)
		p.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(row, row2, row3)
		_, err := r.bot.Send(p)
		if err != nil {
			return err
		}
	} else {
		p := tgbotapi.NewMessage(chatID, fmt.Sprintf("%s-%s (%s)\n%s\n📝%s\n關鍵字:%s", shop.Name, shop.District, shop.Type, shop.URL, shop.Notes, shop.Tags))
		row := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("修改", "E"+strconv.Itoa(shop.ID)),
			tgbotapi.NewInlineKeyboardButtonData("刪除", "D"+strconv.Itoa(shop.ID)),
		)
		p.ReplyMarkup = tgbotapi.NewInlineKeyboardMarkup(row)

		_, err := r.bot.Send(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ServeBot) handleAdminFunc(u tgbotapi.Update) {
	if u.CallbackQuery != nil {
		chatID := u.CallbackQuery.Message.Chat.ID
		switch u.CallbackQuery.Data {
		case newShopCallback:
			r.SendMsg(chatID, "請輸入店舖名字")
			r.enterAdminMode(chatID, state{Action: newShopName})
		case editShopCallback:
			r.SendMsg(chatID, "請輸入店舖關鍵字")
			r.enterAdminMode(chatID, state{Action: editSearch})
		case logoutCallback:
			r.SendMsg(chatID, "已登出")
			r.exitAdminMode(chatID)
			log.WithFields(log.Fields{
				"userID":   u.Message.From.ID,
				"username": u.Message.From.UserName,
			}).Print("User logout")

		case editStatus:
			//Switch shop status
		default:
			lastState, err := r.adminModeLastState(chatID)
			if err != nil {
				log.WithError(err).Print("Error when trying to retrieve last state")
				return
			}
			switch lastState.Action {
			case editSearch:
				//Handle pagination request
				if u.CallbackQuery.Message != nil {
					err = r.handleCallbackData(u.CallbackQuery)
					if err != nil {
						return
					}
				}
			}
		}
		r.bot.AnswerCallbackQuery(tgbotapi.NewCallback(u.CallbackQuery.ID, u.CallbackQuery.Data))
	} else if u.Message != nil && u.Message.Text == "/logout" {
		//Logout action
		r.SendMsg(u.Message.Chat.ID, "已登出")
		r.exitAdminMode(u.Message.Chat.ID)
	} else {
		//Get last state, text input
		chatID := u.Message.Chat.ID
		lastState, err := r.adminModeLastState(chatID)
		if err != nil {
			log.WithError(err).Print("Error when trying to retrieve last state")
			return
		}
		switch lastState.Action {
		case editSearch:
			shops, err := r.da.ShopsWithKeyword(u.Message.Text)
			if err != nil {
				r.SendMsg(u.Message.Chat.ID, "Error")
				log.WithError(err).Print("Error when loading shops")
			}

			if len(shops) == 0 {
				r.SendMsg(u.Message.Chat.ID, "Not found")
			}

			err = r.SendList(u.Message.Chat.ID, shops, simpleSearchPrefix+u.Message.Text, EntriesPerPage, 0)
			if err != nil {
				log.WithError(err).Print("Cannot display shop list in admin mode")
			}
		case newShopName:
			lastState.SelectedShop.Name = u.Message.Text
			lastState.Action = newShopType
			r.enterAdminMode(u.Message.Chat.ID, lastState)
			r.SendMsg(u.Message.Chat.ID, "請輸入類型")
		case newShopType:
			lastState.SelectedShop.Type = u.Message.Text
			lastState.Action = newShopAddress
			r.enterAdminMode(u.Message.Chat.ID, lastState)
			r.SendMsg(u.Message.Chat.ID, "請輸入地址")
		case newShopAddress:
			lastState.SelectedShop.Address = u.Message.Text
			lastState.Action = newShopURL
			r.enterAdminMode(u.Message.Chat.ID, lastState)
			r.SendMsg(u.Message.Chat.ID, "請輸入URL")
		case newShopURL:
			lastState.SelectedShop.URL = u.Message.Text
			lastState.Action = newShopDistrict
			r.enterAdminMode(u.Message.Chat.ID, lastState)
			r.SendMsg(u.Message.Chat.ID, "請輸入地區")
		case newShopDistrict:
			lastState.SelectedShop.District = u.Message.Text
			if u.Message.Text != "網店" {
				lastState.Action = newShopLocation
				r.SendMsg(u.Message.Chat.ID, "座標(請使用TG Location 上傳功能)")
			} else {
				lastState.Action = newShopKeywords
				r.SendMsg(u.Message.Chat.ID, "關鍵字")
			}
			r.enterAdminMode(u.Message.Chat.ID, lastState)
		case newShopLocation:
			if u.Message.Location == nil {
				r.SendMsg(u.Message.Chat.ID, "座標(請使用TG Location 上傳功能)")
				return
			}
			lastState.SelectedShop.Position.Lat = u.Message.Location.Latitude
			lastState.SelectedShop.Position.Long = u.Message.Location.Longitude
			lastState.Action = newShopKeywords
			r.enterAdminMode(u.Message.Chat.ID, lastState)
			r.SendMsg(u.Message.Chat.ID, "關鍵字(以空格分隔)")
		case newShopKeywords:
			lastState.SelectedShop.Tags = strings.Split(u.Message.Text, " ")
			lastState.Action = newShopPreview
			r.enterAdminMode(u.Message.Chat.ID, lastState)
			r.SendSingleShopEdit(u.Message.Chat.ID, lastState.SelectedShop)
		}
	}

}
