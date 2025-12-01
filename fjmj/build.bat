@echo off
set GOOS=linux
set GOARCH=amd64
go build

if %ERRORLEVEL% equ 0 (
    echo ✅ 11编译成功！输出文件：%OUTPUT_NAME%
    echo ⚠️  12该文件是 Linux 程序，不能在 Windows 运行，请上传到 Linux 主机执行。
) else (
    echo ❌ 13编译失败，请检查代码或 Go 环境。
)
pause