import asyncio
from asyncua import Client
from config.setting import  settings

opcua_cfg = settings.opcua

OPCUA_URL = opcua_cfg.get("url", "opc.tcp://172.24.216.216:4840")

PLC_NODES = {
    "temp": "ns=2;i=1",
    "hum": "ns=2;i=2",
    "status": "ns=2;i=3",
}

async def read_data():
    res = {}
    client = None
    try:
        # 纯原生连接，不做任何安全配置
        client = Client(OPCUA_URL)
        await client.connect()

        for name, nid in PLC_NODES.items():
            node = client.get_node(nid)
            res[name] = await node.read_value()
        return res

    except Exception as e:
        print(f"❌ OPCUA 采集异常: {e}")
        return {}
    finally:
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass


def sync_read_plc():
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(read_data())
    finally:
        loop.close()
