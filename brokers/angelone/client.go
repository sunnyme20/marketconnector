package angelone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type Client struct {
	httpClient  *http.Client
	AccessToken string
	ApiKey      string
}

func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func getMACAddress() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp != 0 && len(iface.HardwareAddr) > 0 {
			return iface.HardwareAddr.String()
		}
	}
	return ""
}

func (c *Client) addHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-UserType", "USER")
	req.Header.Set("X-SourceID", "WEB")

	clientIP := getLocalIP()
	req.Header.Set("X-ClientLocalIP", clientIP)
	req.Header.Set("X-ClientPublicIP", clientIP)
	req.Header.Set("X-MACaddress", getMACAddress())
	req.Header.Set("X-PrivateKey", c.ApiKey)

	if c.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AccessToken)
	}
}

func NewClient(apiKey string) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		ApiKey: apiKey,
	}
}

func (c *Client) Get(url string, body any, result any) error {

	var payload *bytes.Buffer
	if body == nil {
		payload = bytes.NewBuffer([]byte("{}"))
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewBuffer(data)
	}

	client := c.httpClient
	req, err := http.NewRequest("GET", url, payload)
	if err != nil {
		return err
	}
	c.addHeaders(req)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if len(bodyBytes) > 0 && bodyBytes[0] == '<' {
		return fmt.Errorf("server returned HTML instead of JSON (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	return json.Unmarshal(bodyBytes, result)
}

func (c *Client) Post(url string, body any, result any) error {

	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	payload := bytes.NewBuffer(data)
	client := c.httpClient
	req, err := http.NewRequest("POST", url, payload)
	if err != nil {
		return err
	}
	c.addHeaders(req)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if len(bodyBytes) > 0 && bodyBytes[0] == '<' {
		return fmt.Errorf("server returned HTML instead of JSON (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	return json.Unmarshal(bodyBytes, result)
}
