package models

type LoginRequest struct {
	ClientCode string
	Password   string
	Totp       string
	State      string
}
