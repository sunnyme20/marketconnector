package angelone

import (
	"fmt"

	models "github.com/sunnyme20/marketconnector/brokers/model"
)

type Response struct {
	Status    bool   `json:"status"`
	Message   string `json:"message"`
	ErrorCode string `json:"errorcode"`
}

// ------------ login ------------------------------
type LoginRequest struct {
	ClientCode string `json:"clientcode"`
	Password   string `json:"password"`
	TOTP       string `json:"totp"`
	State      string `json:"state"`
}

type LoginResponse struct {
	Response
	Data struct {
		JwtToken     string `json:"jwtToken"`
		RefreshToken string `json:"refreshToken"`
		FeedToken    string `json:"feedToken"`
		State        string `json:"state"`
	} `json:"data"`
}

// ------------ user profile ------------------------
type Profile struct {
	Response
	Data struct {
		ClientCode string   `json:"clientcode"`
		Username   string   `json:"name"`
		Email      string   `json:"email"`
		Exchanges  []string `json:"exchanges"`
		Products   []string `json:"products"`
	} `json:"data"`
}

// ------------ funds & margin ------------------------
type Funds struct {
	Response
	Data struct {
		NetMargin     string `json:"net"`
		AvailableCash string `json:"availablecash"`
	}
}

// ------------- Holdings ------------------------------
type HoldingItem struct {
	TradingSymbol string  `json:"tradingsymbol"`
	Exchange      string  `json:"exchange"`
	T1Quantity    int32   `json:"t1quantity"`
	Quantity      int32   `json:"quantity"`
	Product       string  `json:"product"`
	AveragePrice  float64 `json:"averageprice"`
	LTP           float64 `json:"ltp"`
	Close         float64 `json:"close"`
	Pnl           float64 `json:"profitandloss"`
	PnlPct        float32 `json:"pnlpercentage"`
}

type Holdings struct {
	Response
	Data []HoldingItem `json:"data"`
}

// ------------- Positions -----------------------------
type PositionItem struct {
	Exchange      string `json:"exchange"`
	SymbolToken   string `json:"symboltoken"`
	ProductType   string `json:"producttype"`
	TradingSymbol string `json:"tradingsymbol"`
	SymbolName    string `json:"symbolname"`
	BuyQty        string `json:"buyqty"`
	SellQty       string `json:"sellqty"`
	BuyAmount     string `json:"buyamount"`
	SellAmount    string `json:"sellamount"`
	BuyAvgPrice   string `json:"buyavgprice"`
	SellAvgPrice  string `json:"sellavgprice"`
	AvgNetPrice   string `json:"avgnetprice"`
	NetValue      string `json:"netvalue"`
	CFBuyQty      string `json:"cfbuyqty"`
	CFSellQty     string `json:"cfsellqty"`
	CFBuyAmount   string `json:"cfbuyamount"`
	CFSellAmount  string `json:"cfsellamount"`
	LotSize       string `json:"lotsize"`
}

type PositionData struct {
	Response
	Data []PositionItem `json:"data"`
}

// ------------- Market Quote --------------------------
type QuoteRequest struct {
	Mode           string              `json:"mode"`
	ExchangeTokens map[string][]string `json:"exchangeTokens"`
}

type QuoteDepthLevel struct {
	Price    float64 `json:"price"`
	Quantity int32   `json:"quantity"`
	Orders   int32   `json:"orders"`
}

type QuoteDepth struct {
	Buy  []QuoteDepthLevel `json:"buy"`
	Sell []QuoteDepthLevel `json:"sell"`
}

type QuoteFetched struct {
	Exchange      string     `json:"exchange"`
	TradingSymbol string     `json:"tradingSymbol"`
	SymbolToken   string     `json:"symbolToken"`
	LTP           float64    `json:"ltp"`
	Open          float64    `json:"open"`
	High          float64    `json:"high"`
	Low           float64    `json:"low"`
	Close         float64    `json:"close"`
	LastTradeQty  int32      `json:"lastTradeQty"`
	ExchFeedTime  string     `json:"exchFeedTime"`
	ExchTradeTime string     `json:"exchTradeTime"`
	NetChange     float64    `json:"netChange"`
	PercentChange float64    `json:"percentChange"`
	AvgPrice      float64    `json:"avgPrice"`
	TradeVolume   int32      `json:"tradeVolume"`
	OpnInterest   int32      `json:"opnInterest"`
	UpperCircuit  float64    `json:"upperCircuit"`
	LowerCircuit  float64    `json:"lowerCircuit"`
	TotBuyQuan    int32      `json:"totBuyQuan"`
	TotSellQuan   int32      `json:"totSellQuan"`
	Week52High    float64    `json:"52WeekHigh"`
	Week52Low     float64    `json:"52WeekLow"`
	Depth         QuoteDepth `json:"depth"`
}

type QuoteResponse struct {
	Response
	Data struct {
		Fetched   []QuoteFetched `json:"fetched"`
		Unfetched []any          `json:"unfetched"`
	} `json:"data"`
}

// ------------- Historical Data ----------------------
type HistoricalRequest struct {
	Exchange    string `json:"exchange"`
	SymbolToken string `json:"symboltoken"`
	Interval    string `json:"interval"`
	FromDate    string `json:"fromdate"`
	ToDate      string `json:"todate"`
}

type HistoricalCandleData struct {
	Response
	Data [][]any `json:"data"`
}

type HistoricalOIData struct {
	Response
	Data []HistoricalOIItemRaw `json:"data"`
}

type HistoricalOIItemRaw struct {
	Time string `json:"time"`
	OI   int64  `json:"oi"`
}

// --------------- Time frame ------------------------
const (
	OneMin     = "ONE_MINUTE"
	ThreeMin   = "THREE_MINUTE"
	FiveMin    = "FIVE_MINUTE"
	TenMin     = "TEN_MINUTE"
	FifteenMin = "FIFTEEN_MINUTE"
	ThirtyMin  = "THIRTY_MINUTE"
	OneHr      = "ONE_HOUR"
	OneDay     = "ONE_DAY"
)

// MapExchange converts a broker-agnostic Exchange to AngelOne's API string.
func MapExchange(e models.Exchange) string {
	return string(e)
}

// MapTimeframe converts a broker-agnostic Timeframe to AngelOne's API string.
func MapTimeframe(tf models.Timeframe) string {
	switch tf {
	case models.Timeframe1Minute:
		return OneMin
	case models.Timeframe3Minutes:
		return ThreeMin
	case models.Timeframe5Minutes:
		return FiveMin
	case models.Timeframe10Minutes:
		return TenMin
	case models.Timeframe15Minutes:
		return FifteenMin
	case models.Timeframe30Minutes:
		return ThirtyMin
	case models.Timeframe1Hour:
		return OneHr
	case models.Timeframe1Day:
		return OneDay
	default:
		return OneDay
	}
}

// MapSubscriptionMode converts a broker-agnostic SubscriptionMode to AngelOne's API int.
func MapSubscriptionMode(mode models.SubscriptionMode) int {
	return int(mode)
}

// MapWSExchangeType converts a broker-agnostic WSExchangeType to AngelOne's API int.
func MapWSExchangeType(ex models.WSExchangeType) int {
	return int(ex)
}

// WSExchangeTypeFromExchange maps a common Exchange to the AngelOne WebSocket exchange type.
func WSExchangeTypeFromExchange(e models.Exchange) (models.WSExchangeType, error) {
	switch e {
	case models.ExchangeNSE:
		return models.WSExchangeNseCM, nil
	case models.ExchangeNFO:
		return models.WSExchangeNseFO, nil
	case models.ExchangeBSE:
		return models.WSExchangeBseCM, nil
	case models.ExchangeBFO:
		return models.WSExchangeBseFO, nil
	default:
		return 0, fmt.Errorf("unsupported exchange for WebSocket: %s", e)
	}
}
