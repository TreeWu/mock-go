package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// SSHConfig 包含SSH连接配置
type SSHConfig struct {
	Username string
	Password string
	Port     int
	Timeout  time.Duration
}

// RemoteServer 表示远程服务器信息
type RemoteServer struct {
	IP      string
	OSInfo  string
	Success bool
	Error   string
}

// 解析IP范围，支持第三、第四位都包含范围
func parseIPRange(ipRange string) ([]string, error) {
	parts := strings.Split(ipRange, ".")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid IP range format")
	}

	// 解析每个部分的范围
	var ranges [4][]int
	for i, part := range parts {
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return nil, fmt.Errorf("invalid range in part %d: %s", i, part)
			}

			start, err := strconv.Atoi(rangeParts[0])
			if err != nil {
				return nil, fmt.Errorf("invalid start value in part %d: %s", i, rangeParts[0])
			}

			end, err := strconv.Atoi(rangeParts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid end value in part %d: %s", i, rangeParts[1])
			}

			if start > end {
				return nil, fmt.Errorf("start cannot be greater than end in part %d", i)
			}

			for j := start; j <= end; j++ {
				ranges[i] = append(ranges[i], j)
			}
		} else {
			// 单个值
			value, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid value in part %d: %s", i, part)
			}
			ranges[i] = []int{value}
		}
	}

	// 生成所有IP地址组合
	var ips []string
	for _, a := range ranges[0] {
		for _, b := range ranges[1] {
			for _, c := range ranges[2] {
				for _, d := range ranges[3] {
					// 验证IP地址各部分的有效性
					if a >= 0 && a <= 255 && b >= 0 && b <= 255 &&
						c >= 0 && c <= 255 && d >= 0 && d <= 255 {
						ip := fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
						ips = append(ips, ip)
					} else {
						return nil, fmt.Errorf("invalid IP address: %d.%d.%d.%d", a, b, c, d)
					}
				}
			}
		}
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("no valid IP addresses generated")
	}

	return ips, nil
}

// 通过SSH执行命令
func executeSSHCommand(ip string, config SSHConfig, command string) (string, error) {
	sshConfig := &ssh.ClientConfig{
		User: config.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(config.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         config.Timeout,
	}

	address := fmt.Sprintf("%s:%d", ip, config.Port)
	client, err := ssh.Dial("tcp", address, sshConfig)
	if err != nil {
		return "", fmt.Errorf("failed to dial: %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("failed to create session: %v", err)
	}
	defer session.Close()

	output, err := session.Output(command)
	if err != nil {
		return "", fmt.Errorf("failed to execute command: %v", err)
	}

	return string(output), nil
}

// 获取远程服务器的OS信息
func getOSInfo(ip string, config SSHConfig, wg *sync.WaitGroup, results chan<- RemoteServer) {
	defer wg.Done()

	server := RemoteServer{IP: ip}

	// 尝试获取 /etc/os-release 内容
	output, err := executeSSHCommand(ip, config, "cat /etc/os-release")
	if err != nil {
		// 如果失败，尝试其他可能的位置或命令
		output, err = executeSSHCommand(ip, config, "cat /usr/lib/os-release")
		if err != nil {
			server.Success = false
			server.Error = err.Error()
			results <- server
			return
		}
	}

	server.Success = true
	server.OSInfo = strings.TrimSpace(output)
	results <- server
}

// 保存结果到文件，格式为 {ip:osinfo}
func saveResultsToFile(results []RemoteServer, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, server := range results {
		if server.Success {
			// 成功获取OS信息的格式：{ip:osinfo}
			line := fmt.Sprintf("{%s:%s}\n", server.IP, server.OSInfo)
			if _, err := writer.WriteString(line); err != nil {
				return err
			}
		} else {
			// 失败的格式：{ip:error message}
			line := fmt.Sprintf("{%s:%s}\n", server.IP, server.Error)
			if _, err := writer.WriteString(line); err != nil {
				return err
			}
		}
	}

	return nil
}

// 检查主机是否可达
func isHostReachable(ip string, port int, timeout time.Duration) bool {
	address := fmt.Sprintf("%s:%d", ip, port)
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func main() {
	// SSH配置
	config := SSHConfig{
		Username: "root",     // 修改为你的用户名
		Password: "password", // 修改为你的密码
		Port:     22,         // SSH端口
		Timeout:  10 * time.Second,
	}

	// 从命令行参数获取IP范围，如果没有则使用默认值
	var ipRange string
	if len(os.Args) > 1 {
		ipRange = os.Args[1]
	} else {
		ipRange = "192.168.33.1-245" // 默认IP范围
	}

	// 解析IP范围
	ips, err := parseIPRange(ipRange)
	if err != nil {
		fmt.Printf("Error parsing IP range: %v\n", err)
		return
	}

	fmt.Printf("Scanning %d IP addresses...\n", len(ips))

	var wg sync.WaitGroup
	results := make(chan RemoteServer, len(ips))

	// 限制并发数，避免过多连接
	maxConcurrent := 10
	semaphore := make(chan struct{}, maxConcurrent)

	successCount := 0
	failedCount := 0

	// 为每个IP启动goroutine
	for _, ip := range ips {
		wg.Add(1)
		semaphore <- struct{}{} // 获取信号量

		go func(ip string) {
			defer func() {
				<-semaphore // 释放信号量
			}()

			fmt.Printf("Checking %s...\n", ip)

			// 先检查主机是否可达
			if !isHostReachable(ip, config.Port, 3*time.Second) {
				results <- RemoteServer{
					IP:      ip,
					Success: false,
					Error:   "Host unreachable",
				}
				return
			}

			getOSInfo(ip, config, &wg, results)
		}(ip)
	}

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	var allResults []RemoteServer
	for server := range results {
		allResults = append(allResults, server)
		if server.Success {
			successCount++
			fmt.Printf("✓ Successfully retrieved OS info from %s\n", server.IP)
		} else {
			failedCount++
			fmt.Printf("✗ Failed to get OS info from %s: %s\n", server.IP, server.Error)
		}
	}

	// 保存结果到文件
	outputFile := "os-results.txt"
	if err := saveResultsToFile(allResults, outputFile); err != nil {
		fmt.Printf("Error saving results: %v\n", err)
		return
	}

	fmt.Printf("\nScan completed!\n")
	fmt.Printf("Successful: %d\n", successCount)
	fmt.Printf("Failed: %d\n", failedCount)
	fmt.Printf("Results saved to: %s\n", outputFile)
}
