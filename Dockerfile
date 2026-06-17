FROM python:3.9-slim

WORKDIR /app

# Nacos 全局环境变量
ENV NACOS_ADDR="nacos:8848"
ENV GROUP_NAME="DEFAULT_GROUP"
ENV NAMESPACE=""

# 当前采集服务配置
ENV SERVICE_NAME="iot-modbus-collector"
ENV SELF_PORT=8000
# 本机采集分组，多机器部署时可在 docker-compose 单独覆盖
ENV LOCAL_GROUP_ID="group-01"

# 1. 先升级pip + 设置国内pip源（关键修复）
#RUN pip install --upgrade pip && \
#    pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# 拷贝依赖文件，分层构建加速
COPY requirements.txt .
RUN pip install -r requirements.txt

# 拷贝剩余代码
COPY . .

## 启动入口
CMD ["python3", "main.py"]
## CMD bash -c "python3 /app/tools/opcua_sim.py & python3 /app/main.py"
#CMD ["/usr/bin/supervisord", "-n", "-c", "/app/supervisord.conf"]

## 赋予启动脚本执行权限
#RUN chmod +x /app/run_all.sh
#
## 入口脚本
#CMD ["/app/run_all.sh"]
