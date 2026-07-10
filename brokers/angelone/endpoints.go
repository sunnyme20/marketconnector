package angelone

const rootEndpoint string = "https://apiconnect.angelone.in"

type Endpoint struct {
	Login        string
	Profile      string
	RMS          string
	Logout       string
	Brokerage    string
	Holding      string
	Position     string
	Margin       string
	Quote        string
	Historical   string
	HistoricalOI string
	Instruments  string
	Websocket    string
}

var Api = Endpoint{
	Login:        rootEndpoint + "/rest/auth/angelbroking/user/v1/loginByPassword",
	Profile:      rootEndpoint + "/rest/secure/angelbroking/user/v1/getProfile",
	RMS:          rootEndpoint + "/rest/secure/angelbroking/user/v1/getRMS",
	Logout:       rootEndpoint + "/rest/secure/angelbroking/user/v1/logout",
	Brokerage:    rootEndpoint + "/rest/secure/angelbroking/brokerage/v1/estimateCharges",
	Holding:      rootEndpoint + "/rest/secure/angelbroking/portfolio/v1/getHolding",
	Position:     rootEndpoint + "/rest/secure/angelbroking/order/v1/getPosition",
	Margin:       rootEndpoint + "/rest/secure/angelbroking/margin/v1/batch",
	Quote:        rootEndpoint + "/rest/secure/angelbroking/market/v1/quote/",
	Historical:   rootEndpoint + "/rest/secure/angelbroking/historical/v1/getCandleData",
	HistoricalOI: rootEndpoint + "/rest/secure/angelbroking/historical/v1/getOIData",
	Instruments:  "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
	Websocket:    "wss://smartapisocket.angelone.in/smart-stream",
}
