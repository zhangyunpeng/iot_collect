from config.setting import settings
import json
import os
import paho.mqtt.client as mqtt
import threading
import time
from typing import Dict

mqtt_cfg = settings.mqtt
MQTT_HOST = mqtt_cfg.get("host")
MQTT_PORT = mqtt_cfg.get("port")
MQTT_TOPIC = mqtt_cfg.get("topic")
MQTT_KEEPALIVE = 60
MQTT_CLIENT_ID_PUB = os.environ.get("MQTT_CLIENT_ID", "default_client_id")

class MqttClient:
    def __init__(self):
        self.pub_client = None
        self._pub_keepalive_thread = None
        self._pub_running = False
        # 重连锁，防止并发大量重连线程
        self._reconnect_lock = threading.Lock()

    def is_connected(self) -> bool:
        """判断发布客户端是否正常连接"""
        return self.pub_client is not None

    def connect_publisher(self) -> bool:
        if self.pub_client:
            return True
        self.pub_client = mqtt.Client(client_id=MQTT_CLIENT_ID_PUB, clean_session=True)
        try:
            self.pub_client.connect(MQTT_HOST, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
            self._pub_running = True
            if not self._pub_keepalive_thread or not self._pub_keepalive_thread.is_alive():
                self._pub_keepalive_thread = threading.Thread(
                    target=self._pub_keepalive_loop,
                    daemon=True
                )
                self._pub_keepalive_thread.start()
            print("✅ MQTT 发布端连接成功，已开启常驻保活")
            return True
        except Exception as e:
            print(f"❌ MQTT 发布端连接失败: {e}")
            self.pub_client = None
            return False

    def reconnect(self):
        """断线重连封装"""
        print("🔄 尝试重连MQTT...")
        self.stop()
        time.sleep(2)
        return self.connect_publisher()

    def _pub_keepalive_loop(self):
        while self._pub_running:
            try:
                if self.pub_client:
                    # loop 处理网络读写、心跳、断开检测
                    rc = self.pub_client.loop(timeout=1)
                    # rc !=0 代表连接异常断开
                    if rc != 0:
                        print(f"⚠️ MQTT 网络检测到断开，错误码:{rc}")
                        self.pub_client = None
            except Exception as e:
                print(f"⚠️ MQTT保活线程异常: {e}")
                self.pub_client = None
            time.sleep(2)

    def publish(self, data: Dict) -> bool:
        if not self.pub_client:
            print("⚠️ pub_client 为空，跳过发布")
            # 非阻塞锁，避免多线程疯狂创建重连线程
            if self._reconnect_lock.acquire(blocking=False):
                try:
                    threading.Thread(target=self.reconnect, daemon=True).start()
                finally:
                    self._reconnect_lock.release()
            return False

        # 1. 单独捕获JSON序列化错误
        try:
            payload = json.dumps(data, ensure_ascii=False)
        except Exception as e:
            print(f"❌ JSON序列化失败: {e}, data={data}")
            return False

        try:
            # 发布消息，返回消息回执对象
            msg_info = self.pub_client.publish(MQTT_TOPIC, payload=payload, qos=1)
            # 阻塞等待投递确认，超时2秒，QoS1必须等待ack
            msg_info.wait_for_publish(timeout=2)

            # rc=0 才代表消息成功投递broker
            if msg_info.rc != 0:
                print(f"❌ MQTT消息投递失败，错误码:{msg_info.rc}, data={data}")
                self.pub_client = None
                return False

            print(f"📤 MQTT 发布成功: {data}")
            return True

        except Exception as e:
            print(f"❌ MQTT 发布运行异常: {e}")
            self.pub_client = None
            return False

    def stop(self) -> None:
        self._pub_running = False
        if self.pub_client:
            self.pub_client.disconnect()
            self.pub_client = None
        print("🛑 MQTT 客户端已全部断开")