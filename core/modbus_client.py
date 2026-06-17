from pymodbus.client import ModbusTcpClient

def sync_read(host, port):
    """同步读取Modbus TCP数据，返回字典"""
    client = ModbusTcpClient(host=host, port=port)
    try:
        if not client.connect():
            print("❌ Modbus 连接失败")
            return None

        response = client.read_holding_registers(address=0, count=3, slave=1)
        if response.isError():
            print(f"❌ Modbus 读取异常: {response}")
            return None

        return response.registers
    except Exception as e:
        print(f"❌ Modbus 采集异常: {str(e)}")
        return None
    finally:
        client.close()

