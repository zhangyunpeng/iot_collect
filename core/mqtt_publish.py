import json
import os
import threading
import time
from typing import Dict, Optional
import paho.mqtt.client as mqtt

class MqttPublisher:
    def __init__(
            self,
            host,
            port,
            topic,
            client_id,
            keepalive = 60):
        self.host = host
        self.port = port
        self.topic = topic
        self.client_id = client_id
        self.keepalive = keepalive
        self.pub_client: Optional[mqtt.Client] = None
        self._pub_keepalive_thread: Optional[threading.Thread] = None
        self._pub_running = False
        self._reconnect_lock = threading.Lock()

    def is_connected(self) -> bool:
        return self.pub_client is not None

    def connect_publisher(self) -> bool:
        if self.pub_client:
            return True
        self.pub_client = mqtt.Client(client_id=self.client_id, clean_session=True)
        try:
            self.pub_client.connect(self.host, self.port, keepalive=self.keepalive)
            self._pub_running = True
            if not self._pub_keepalive_thread or not self._pub_keepalive_thread.is_alive():
                self._pub_keepalive_thread = threading.Thread(
                    target=self._pub_keepalive_loop,
                    daemon=True
                )
                self._pub_keepalive_thread.start()
            print("✅ MQTT发布端连接成功，保活线程已启动")
            return True
        except Exception as e:
            print(f"❌ MQTT连接失败: {e}")
            self.pub_client = None
            return False

    def reconnect(self):
        print("🔄 MQTT触发重连")
        self.stop()
        time.sleep(2)
        return self.connect_publisher()

    def _pub_keepalive_loop(self):
        while self._pub_running:
            try:
                if self.pub_client:
                    rc = self.pub_client.loop(timeout=1)
                    if rc != 0:
                        print(f"⚠️ MQTT链路断开 rc={rc}")
                        self.pub_client = None
            except Exception as e:
                print(f"⚠️ MQTT保活异常: {e}")
                self.pub_client = None
            time.sleep(2)

    def publish(self, data: Dict) -> bool:
        if not self.pub_client:
            print("⚠️ MQTT客户端为空，跳过发布，后台重连")
            if self._reconnect_lock.acquire(blocking=False):
                try:
                    threading.Thread(target=self.reconnect, daemon=True).start()
                finally:
                    self._reconnect_lock.release()
            return False
        try:
            payload = json.dumps(data, ensure_ascii=False)
        except Exception as e:
            print(f"❌ JSON序列化失败: {e}, data={data}")
            return False
        try:
            msg_info = self.pub_client.publish(self.topic, payload=payload, qos=1)
            msg_info.wait_for_publish(timeout=2)
            if msg_info.rc != 0:
                print(f"❌ MQTT投递失败 rc={msg_info.rc}")
                self.pub_client = None
                return False
            print(f"📤 MQTT发布成功 {data}")
            return True
        except Exception as e:
            print(f"❌ MQTT发送异常: {e}")
            self.pub_client = None
            return False

    def stop(self):
        self._pub_running = False
        if self.pub_client:
            self.pub_client.disconnect()
            self.pub_client = None
        print("🛑 MQTT客户端已断开")

