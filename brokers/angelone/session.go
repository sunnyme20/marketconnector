package angelone

import (
	"fmt"
	"strconv"

	models "github.com/sunnyme20/marketconnector/brokers/model"
)

type Angelone struct {
	ClientCode  string
	ApiKey      string
	AccessToken string
	FeedToken   string
}

func (a *Angelone) NewSession(clientcode, apikey, password, totp string) (*models.Response[models.LoginResponse], error) {
	a.ClientCode = clientcode
	a.ApiKey = apikey

	var resp *LoginResponse

	client := NewClient(a.ApiKey)
	req := LoginRequest{ClientCode: a.ClientCode, Password: password, TOTP: totp}
	err := client.Post(Api.Login, req, &resp)
	if err != nil {
		return nil, err
	}

	a.SetAccessToken(resp.Data.JwtToken)
	a.SetFeedToken(resp.Data.FeedToken)
	a.SetClientAccessToken(client, resp.Data.JwtToken)

	finalResp := models.Response[models.LoginResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data: models.LoginResponse{
			AccessToken: resp.Data.JwtToken,
			FeedToken:   resp.Data.FeedToken,
		},
	}
	return &finalResp, nil
}

func (a *Angelone) SetClientAccessToken(c *Client, accessToken string) {
	c.AccessToken = accessToken
}

func (a *Angelone) SetClientCode(clientcode string) {
	a.ClientCode = clientcode
}

func (a *Angelone) SetApiKey(apikey string) {
	a.ApiKey = apikey
}

func (a *Angelone) SetFeedToken(feedToken string) {
	a.FeedToken = feedToken
}

func (a *Angelone) SetAccessToken(accessToken string) {
	a.AccessToken = accessToken
}

func (a *Angelone) GetAccessToken() (string, error) {
	return a.AccessToken, nil
}

func (a *Angelone) GetWebSocket() (models.WebSocketTicker, error) {
	return NewWebSocket(a.ApiKey, a.ClientCode, a.AccessToken, a.FeedToken), nil
}

func (a *Angelone) GetUserProfile() (*models.Response[models.UserProfileResponse], error) {
	client := NewClient(a.ApiKey)
	client.AccessToken = a.AccessToken // set the token from the session
	var profile *Profile
	err := client.Get(Api.Profile, nil, &profile)
	if err != nil {
		return nil, err
	}

	finalResp := models.Response[models.UserProfileResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data: models.UserProfileResponse{
			ClientCode: profile.Data.ClientCode,
			Username:   profile.Data.Username,
			Email:      profile.Data.Email,
			Exchanges:  profile.Data.Exchanges,
			Products:   profile.Data.Products,
		},
	}
	return &finalResp, nil
}

func (a *Angelone) GetRMSData() (*models.Response[models.FundsResponse], error) {
	client := NewClient(a.ApiKey)
	client.AccessToken = a.AccessToken

	var funds *Funds
	err := client.Get(Api.RMS, nil, &funds)
	if err != nil {
		return nil, err
	}

	netMargin, _ := strconv.ParseFloat(funds.Data.NetMargin, 64)
	availableCash, _ := strconv.ParseFloat(funds.Data.AvailableCash, 64)

	finalResp := models.Response[models.FundsResponse]{
		Success: true,
		Message: "SUCCESS",
		Broker:  "angelone",
		Data: models.FundsResponse{
			NetMargin:     netMargin,
			AvailableCash: availableCash,
		},
	}
	return &finalResp, nil
}

func (a *Angelone) Logout() {
	fmt.Printf("Fetching RMS data for %s\n", a.ClientCode)
}
