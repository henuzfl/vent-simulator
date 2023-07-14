## 打包运行

> 使用控件：pyinstaller

1. 安装pyinstaller，`pip install pyinstaller`
2. 执行命令：`pyinstaller -F .\app.py`
3. 可在目录dist下找到`app.exe`

## docker运行

1. 执行生成docker image，`docker build -t vent_simulator:latest .`
2. 运行`docker run -d --name vent-simulator vent_simulator:latest`