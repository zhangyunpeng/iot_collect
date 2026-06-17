import os
import yaml

# 配置文件
CONFIG_FILE = os.path.join(os.getcwd(), "config.yaml")

class Settings:
    _instance = None
    raw_config = {}

    def __new__(cls):
        cls._instance = super(Settings, cls).__new__(cls)
        cls._instance.load_config()
        return cls._instance

    def load_config(self):
        """ 加载并解析 YAML 配置"""
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                self.raw_config = yaml.safe_load(f)

            if not isinstance(self.raw_config, dict):
                raise TypeError("配置文件解析结果不是字典，请加查 YAML 格式")

            print(f"✅ 配置加载成功: {self.raw_config}")
        except Exception as e:
            print(f"❌ 加载配置失败: {e}")
            self.raw_config = {}

    @property
    def mqtt(self) -> dict:
        return self.raw_config.get("mqtt", {})

    @property
    def influx(self) -> dict:
        return self.raw_config.get("influx", {})

    @property
    def modbus(self) -> dict:
        return self.raw_config.get("modbus", {})

    @property
    def opcua(self) -> dict:
        return self.raw_config.get("opcua", {})

# 全局唯一实例
settings = Settings()