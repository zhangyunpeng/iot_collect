import asyncio
from protocol.mqtt_client import MqttClient
from service.modbus_collect import run as modbus_collect_run
from core.sqlite_helper import init_sqlite
import threading
import time


mqtt_client = MqttClient()

def run():
    # init sqlite
    init_sqlite()

    # mqtt client pub
    # mqtt_client.connect_publisher()

    # modbus 采集
    # mod_thread = threading.Thread(target=modbus_collect_run, daemon=True)
    # mod_thread.start()
    # print("✅ Modbus 采集线程启动")
    asyncio.run(modbus_collect_run())
