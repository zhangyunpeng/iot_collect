from typing import List, Optional
from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

class ModbusLongClient:
    def __init__(self, host: str, port: int, slave_id: int, collect_interval: float = 1.0):
        # 设备标识
        self.host = host
        self.port = port
        self.slave_id = slave_id
        self.collect_interval = collect_interval

        # 长连接实例
        self.client = AsyncModbusTcpClient(host, port)
        self._closed = False

    async def connect(self) -> bool:
        """建立长连接，自动重连入口"""
        if self._closed:
            return False
        if self.client.connected:
            return True
        try:
            await self.client.connect()
            print(f"[Modbus连接成功] {self.host}:{self.port} slave={self.slave_id}")
            return True
        except ModbusException as e:
            print(f"[Modbus连接失败] {self.host} err: {str(e)}")
            self.client.close()
            return False

    async def read_holding(self, addr: int, count: int) -> Optional[List[int]]:
        """读取保持寄存器"""
        if not await self.connect():
            return None
        try:
            resp = await self.client.read_holding_registers(
                address=addr,
                count=count,
                slave=self.slave_id
            )
            if resp.isError():
                print(f"[Modbus读取异常] {self.host} resp:{resp}")
                self.client.close()
                return None
            return resp.registers
        except ModbusException as e:
            print(f"[Modbus通信故障] {self.host} err:{str(e)}")
            self.client.close()
            return None

    async def close(self):
        """关闭当前设备TCP链路"""
        self._closed = True
        self.client.close()
        print(f"[Modbus断开] {self.host}")