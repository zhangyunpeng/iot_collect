import start
import uvicorn

def main():
    start.run()

    # 启动 FastAPI 接口
    print("✅ FastAPI 接口服务启动")
    uvicorn.run(
        app="api.collect:app",
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )


if __name__ == "__main__":
    main()