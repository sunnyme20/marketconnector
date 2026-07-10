package main

import (
	"fmt"
	"time"

	"github.com/sunnyme20/marketconnector/brokers"
	models "github.com/sunnyme20/marketconnector/brokers/model"
)

func checkWebsocket(broker brokers.Broker) {
	ws, err := broker.GetWebSocket()
	if err != nil {
		fmt.Println("GetWebSocket error:", err)
		return
	}

	ws.OnConnect(func() {
		fmt.Println("WebSocket connected")

		// Subscribe to SBI (token 3045) on NSE CM in LTP mode
		err := ws.Subscribe(int(models.ModeLTP), []models.WSTokenGroup{
			{
				ExchangeType: int(models.WSExchangeNseCM),
				Tokens:       []string{"3045"},
			},
		})
		if err != nil {
			fmt.Println("Subscribe error:", err)
		}
	})

	ws.OnTick(func(tick models.MarketQuoteResponse) {
		fmt.Printf("Tick: %s | LTP: %.2f | Vol: %d | Time: %d\n",
			tick.SymbolToken, tick.LTP, tick.TradeVolume, tick.ExchangeTime)
	})

	ws.OnError(func(err error) {
		fmt.Println("WS Error:", err)
	})

	ws.OnReconnect(func(attempt int, delay time.Duration) {
		fmt.Printf("WS reconnecting attempt %d in %v\n", attempt, delay)
	})

	// Run the WebSocket in a goroutine so it doesn't block
	go ws.Serve()

	// Let it run for 30 seconds then stop
	time.Sleep(30 * time.Second)
	ws.Stop()
	fmt.Println("WebSocket stopped")
}

func main() {

	clientCode := "YOUR_CLIENT_CODE"
	apiKey := "YOUR_API_KEY"
	password := "YOUR_PASSWORD"
	totp := "YOUR_TOTP"

	broker, err := brokers.NewBroker("angelone")

	if err != nil {
		fmt.Println("error")
	}
	sess, err := broker.NewSession(clientCode, apiKey, password, totp)
	if err != nil {
		fmt.Println("Login error:", err)
		return
	}

	if sess.Success {
		fmt.Println("login successful")
		fmt.Println("Access token : ", sess.Data.AccessToken)
		fmt.Println("Feed token : ", sess.Data.FeedToken)
		broker.SetAccessToken(sess.Data.AccessToken)
		broker.SetFeedToken(sess.Data.FeedToken)
	}

	profile, err := broker.GetUserProfile()
	if err != nil {
		fmt.Println("Profile error:", err)
		return
	}
	fmt.Println("Profile:", profile)

	holdings, err := broker.GetHoldings()
	if err != nil {
		fmt.Println("Holdings error:", err)
		return
	}
	fmt.Println("Holdings:", holdings)

	quotes, err := broker.GetMarketQuote(models.QuoteModeFull, map[models.Exchange][]string{
		models.ExchangeNSE: {"3045"},
	})

	if err != nil {
		fmt.Println("Quotes error:", err)
	}
	fmt.Println("Quotes:", quotes)

	data, err := broker.GetHistoricalData(models.ExchangeNSE, "3045", models.Timeframe1Day, "2026-06-01 09:00", "2026-06-30 15:30")
	if err == nil {
		fmt.Println(data.Data.Candles)
		fmt.Println(data.Data.OI)
	}

	positions, err := broker.GetPositions()
	if err == nil {
		for _, p := range positions.Data {
			fmt.Printf("%s: Buy %d @ %.2f\n", p.TradingSymbol, p.BuyQty, p.BuyAvgPrice)
		}
	}

	// ---------- WebSocket Example (subscribe to SBI token 3045 on NSE) ----------
	checkWebsocket(broker)
}
