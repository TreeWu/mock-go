package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

// 定义常量，根据实际情况修改
const (
	baseURL        = "http://192.168.0.22:8080" // 修改为实际的IP和端口
	loginPath      = "/api-v1/login"
	deviceListPath = "/api-v1/deviceList"

	// 登录接口的用户名和密码，请根据实际情况修改
	loginUsername = "root"
	loginPassword = "Xbrother@136"
)

// LoginRequest 登录请求结构体
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse 登录响应结构体
type LoginResponse struct {
	Success bool `json:"success"`
	Result  struct {
		AccessToken string `json:"access_token"`
		// ExpireInSeconds int `json:"expire_in_secends"` // 可选字段，这里未使用
		RefreshToken string `json:"refresh_token"`
	} `json:"result"`
	Message string `json:"message"`
}

// Device 设备信息
type Device struct {
	SN     string `json:"sn"`
	Name   string `json:"name"`
	AreaID string `json:"areald"` // 注意：文档中是 areald，可能为笔误，按实际返回字段调整
	Area   string `json:"area"`
}

// DeviceListResponse 设备列表响应
type DeviceListResponse struct {
	Success bool `json:"success"`
	Result  struct {
		Current int      `json:"current"`
		Pages   int      `json:"pages"`
		Total   int      `json:"total"`
		Records []Device `json:"records"`
	} `json:"result"`
	Message string `json:"message"`
}

// 发送登录请求，返回 access_token
func login() (string, error) {
	loginURL := baseURL + loginPath

	loginReq := LoginRequest{
		Username: loginUsername,
		Password: loginPassword,
	}

	reqBody, err := json.Marshal(loginReq)
	if err != nil {
		return "", fmt.Errorf("marshal login request failed: %v", err)
	}

	resp, err := http.Post(loginURL, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return "", fmt.Errorf("login post failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read login response body failed: %v", err)
	}

	var loginResp LoginResponse
	if err := json.Unmarshal(body, &loginResp); err != nil {
		return "", fmt.Errorf("unmarshal login response failed: %v, body: %s", err, string(body))
	}

	if !loginResp.Success {
		return "", fmt.Errorf("login failed: %s", loginResp.Message)
	}

	token := loginResp.Result.AccessToken
	if token == "" {
		return "", fmt.Errorf("access_token is empty in login response")
	}

	return token, nil
}

// 获取设备列表（处理分页）
func getDeviceList(token string) ([]Device, error) {
	var allDevices []Device
	pageNo := 1
	pageSize := 100 // 每页最大数量，根据文档最大支持2000，这里用100足够

	for {
		deviceListURL := baseURL + deviceListPath + "?pageNo=" + fmt.Sprintf("%d", pageNo) + "&pageSize=" + fmt.Sprintf("%d", pageSize)

		req, err := http.NewRequest("GET", deviceListURL, nil)
		if err != nil {
			return nil, fmt.Errorf("create device list request failed: %v", err)
		}

		// 添加 Authorization Header
		req.Header.Add("X-Access-Token", token)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("device list request failed: %v", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read device list response body failed: %v", err)
		}

		var deviceListResp DeviceListResponse
		if err := json.Unmarshal(body, &deviceListResp); err != nil {
			return nil, fmt.Errorf("unmarshal device list response failed: %v, body: %s", err, string(body))
		}

		if !deviceListResp.Success {
			return nil, fmt.Errorf("device list api failed: %s", deviceListResp.Message)
		}

		devices := deviceListResp.Result.Records
		allDevices = append(allDevices, devices...)

		totalPages := deviceListResp.Result.Pages
		if pageNo >= totalPages {
			break
		}
		pageNo++
	}

	return allDevices, nil
}

func main() {
	// 1. 登录获取token
	token, err := login()
	if err != nil {
		log.Fatalf("Login error: %v", err)
	}
	fmt.Println("Login successful, got access token")

	// 2. 获取所有设备列表（自动处理分页）
	devices, err := getDeviceList(token)
	if err != nil {
		log.Fatalf("Get device list error: %v", err)
	}

	var bs bytes.Buffer
	// 3. 打印所有设备的 sn 和 name
	fmt.Println("\nDevice List:")
	for _, dev := range devices {
		bs.WriteString(fmt.Sprintf("SN: %s, Name: %s\n", dev.SN, dev.Name))
	}
	err = os.WriteFile("./device.txt", bs.Bytes(), os.ModePerm)
	if err != nil {
		log.Fatalf("WriteFile error: %v", err)
	}
	fmt.Printf("\nTotal devices fetched: %d\n", len(devices))
}
