import asyncio
import queue
import threading
from typing import Dict, Any

class LocalDataQueue:
    def __init__(self, max_size:int = 10000):
        self.queue = queue.Queue(maxsize=max_size)
        self.max_size = max_size
        self._monitor_task: asyncio.Task | None = None

    def enqueue(self, msg: Dict[str, Any]) -> bool:
        """同步写入队列，供modbus协程调用"""
        if self.queue.full():
            print(f"[队列告警] 队列已满{self.max_size}，丢弃最新数据")
            return False
        self.queue.put(msg)
        return True

    def dequeue(self, block: bool = False, timeout: float = 0.1) -> Any | None:
        """消费线程拉取数据"""
        try:
            return self.queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None

    def get_size(self) -> int:
        return self.queue.qsize()

    async def start_monitor(self):
        """异步监控线程：打印队列堆积状态"""
        while True:
            size = self.get_size()
            if size > self.max_size * 0.7:
                print(f"[队列监控] 堆积较高：{size}/{self.max_size}")
            await asyncio.sleep(5)

    async def init_monitor(self):
        self._monitor_task = asyncio.create_task(self.start_monitor())

    async def stop_monitor(self):
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
