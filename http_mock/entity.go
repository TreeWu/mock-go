package http_mock

type MockConfig struct {
	Method   string                 `json:"method"`
	URL      string                 `json:"url"`
	Params   map[string]interface{} `json:"params"`
	Req      map[string]interface{} `json:"req"`
	Response Response               `json:"response"`
}

type Response struct {
	StatusCode int         `json:"status_code"`
	Body       interface{} `json:"body"`
}
