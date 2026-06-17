import asyncio
import threading
import time
from typing import List, Dict
from core.modbus_long_client import ModbusLongClient
from core.data_queue import LocalDataQueue
from core.mqtt_publish import MqttPublisher
from core.sqlite_helper import modbus_to_sqlite, query_modbus_offline, batch_delete_modbus_offline
from config.setting import settings
import os
import queue
import json

mqtt_cfg = settings.mqtt

mqtt_pub = MqttPublisher(
    mqtt_cfg.get("host"),
    mqtt_cfg.get("port"),
    mqtt_cfg.get("topic"),
    os.environ.get("MQTT_CLIENT_ID", "modbus_collector"),
    60
)

# 全局内存缓冲队列
MAX_QUEUE_LEN = 3000
DATA_QUEUE = queue.Queue(maxsize=MAX_QUEUE_LEN)
# 全局单例队列
local_queue = LocalDataQueue()

class ModbusCollectService:
    def __init__(self, device_configs: List[Dict]):
        self.device_configs = device_configs
        self.devices: List[ModbusLongClient] = []

    async def init_queue_monitor(self):
        """初始化队列监控协程"""
        await local_queue.init_monitor()
        print("✅ 本地队列监控协程启动完成")

    async def init_devices(self):
        """批量初始化所有Modbus长连接设备"""
        for cfg in self.device_configs:
            dev = ModbusLongClient(
                host=cfg["host"],
                port=cfg["port"],
                slave_id=cfg["slave_id"],
                collect_interval=cfg.get("interval", 1.0)
            )
            self.devices.append(dev)
        print(f"✅ 已加载 {len(self.devices)} 台Modbus采集节点")

    async def single_device_collect_task(self, dev: ModbusLongClient):
        """单设备无限采集循环：只写入本地队列，不直接操作MQTT"""
        while not dev._closed:
            reg_data = await dev.read_holding(addr=0, count=10)
            if reg_data is not None:
                # 组装上报报文
                msg = {
                    "device_ip": dev.host,
                    "slave_id": dev.slave_id,
                    "reg_start": 0,
                    "data": reg_data,
                    "timestamp": int(time.time())
                }
                # 写入全局本地队列
                local_queue.enqueue(msg)
                print(f"queue insert : {msg}")
            await asyncio.sleep(dev.collect_interval)

    async def run_all_collect(self):
        """并发启动全部设备采集协程"""
        tasks = []
        for dev in self.devices:
            tasks.append(self.single_device_collect_task(dev))
        await asyncio.gather(*tasks)

    def mqtt_queue_consumer_thread(self):
        """独立线程：消费本地队列，调用MQTT发布"""
        print("🚀 MQTT队列消费线程启动")
        while True:
            msg = local_queue.dequeue()
            if msg is None:
                continue
            print(f"queue consumer: {msg}")
            # 发送失败重新入队重试
            ok = mqtt_pub.publish(msg)
            if not ok:
                modbus_to_sqlite(int(time.time()), json.dumps(msg))

    def start_mqtt_consumer(self):
        """启动MQTT消费后台线程"""
        t = threading.Thread(target=self.mqtt_queue_consumer_thread, daemon=True)
        t.start()

    def sqlite_consumer_thread(self):
        """独立线程：消费本地sqlite，调用MQTT发布"""
        print("🚀 SQLite消费线程启动")
        while True:
            try:
                data_list = query_modbus_offline()
                if not data_list:
                    continue

                delete_ids = []
                for row in data_list:
                    record_id = row["id"]
                    msg = json.loads(row['json_content'])
                    # 写入全局本地队列
                    ok = local_queue.enqueue(msg)
                    if not ok:
                        print(f"⚠️ 写入本地队列失败，暂不删除离线记录 id={record_id}")
                        continue
                    delete_ids.append(record_id)
                    # # 入队成功，删除sqlite本条缓存
                    # delete_modbus_offline(record_id)
                    # print(f"sqlite to local queue success, delete id={record_id}: {msg}")

                if delete_ids:
                    batch_delete_modbus_offline(delete_ids)
            except Exception as e:
                print(f"sqlite consumer write local queue : {e}")
            time.sleep(10)

    def start_sqlite_consumer(self):
        """启动SQLite消费后台线程"""
        t = threading.Thread(target=self.sqlite_consumer_thread, daemon=True)
        t.start()

    async def close_all_devices(self):
        """优雅关闭所有modbus链路"""
        for dev in self.devices:
            await dev.close()
        await local_queue.stop_monitor()



    # Modbus节点配置列表
DEVICE_LIST = [
    {"host": "host.docker.internal", "port": 5020, "slave_id": 1, "interval": 5.0},
]

async def run():
    # 1. 初始化并连接MQTT客户端
    mqtt_pub.connect_publisher()

    # 2. 初始化采集服务
    collect_srv = ModbusCollectService(device_configs=DEVICE_LIST)

    # 3. 初始化队列监控协程
    await collect_srv.init_queue_monitor()

    # 4. 批量创建所有Modbus长连接设备
    await collect_srv.init_devices()

    # 5. 启动独立MQTT消费线程（拉队列发mqtt）
    collect_srv.start_mqtt_consumer()

    # 6. 启动sqlite数据处理线程
    collect_srv.start_sqlite_consumer()


    print("\n===== 全部服务初始化完成，开始采集 =====")
    try:
        # 阻塞运行所有modbus采集协程
        await collect_srv.run_all_collect()
    except asyncio.CancelledError:
        print("\n===== 收到退出信号，执行关闭流程 =====")
        await collect_srv.close_all_devices()
        mqtt_pub.stop()