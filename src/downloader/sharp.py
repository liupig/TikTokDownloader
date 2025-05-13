# -*- coding=utf-8
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, AsyncContextManager, Any, Callable, Awaitable
from yarl import URL
from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client
from redis.asyncio import ConnectionPool, Redis
from pydantic_settings import BaseSettings
import sys
import os
import logging

# 正常情况日志级别使用 INFO，需要定位时可以修改为 DEBUG，此时 SDK 会打印和服务端的通信信息
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

# 1. 设置用户属性, 包括 secret_id, secret_key, region等。Appid 已在 CosConfig 中移除，请在参数 Bucket 中带上 Appid。Bucket 由 BucketName-Appid 组成
secret_id = "" # os.environ['COS_SECRET_ID']     # 用户的 SecretId，建议使用子账号密钥，授权遵循最小权限指引，降低使用风险。子账号密钥获取可参见 https://cloud.tencent.com/document/product/598/37140
secret_key = "" #os.environ['COS_SECRET_KEY']   # 用户的 SecretKey，建议使用子账号密钥，授权遵循最小权限指引，降低使用风险。子账号密钥获取可参见 https://cloud.tencent.com/document/product/598/37140
region = 'ap-beijing'      # 替换为用户的 region，已创建桶归属的 region 可以在控制台查看，https://console.cloud.tencent.com/cos5/bucket
                           # COS 支持的所有 region 列表参见 https://cloud.tencent.com/document/product/436/6224
token = None               # 如果使用永久密钥不需要填入 token，如果使用临时密钥需要填入，临时密钥生成和使用指引参见 https://cloud.tencent.com/document/product/436/14048
scheme = 'https'           # 指定使用 http/https 协议来访问 COS，默认为 https，可不填

config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key, Token=token, Scheme=scheme)
client = CosS3Client(config)

class Settings(BaseSettings):
    redis_host: str = ""
    redis_port: int = 6379
    redis_user: Optional[str] = ""
    redis_pass: Optional[str] = r""
    redis_base: Optional[int] = 10

    @property
    def redis_url(self) -> URL:
        """
        Assemble REDIS URL from settings.

        :return: redis URL.
        """
        path = ""
        if self.redis_base is not None:
            path = f"/{self.redis_base}"
        return URL.build(
            scheme="redis",
            host=self.redis_host,
            port=self.redis_port,
            user=self.redis_user,
            password=self.redis_pass,
            path=path,
        )

settings = Settings()
redis_pool = ConnectionPool.from_url(
    str(settings.redis_url),
    decode_responses=True
)

@asynccontextmanager
async def acquire_redis_session() -> AsyncContextManager[Redis]:
    """Get a Redis session"""
    async with Redis(connection_pool=redis_pool) as redis:
        yield redis


def send_to_tos(file_name):
    try:
        # file_name = r'D:\MyTikTokDownloads\UID4160968570706796_奶呼呼_发布作品\2025-04-21 12.00.21-视频-奶呼呼-#元气少女 #甜妹 #笑起来很甜#企鹅防晒衣 #企鹅光合防晒衣.mp4'
        file_name = str(file_name).replace("\\", "/")
        key_name = "tiktok_china/"+file_name.split("MyTikTokDownloads")[-1].strip(r"\\/")
        with open(file_name, 'rb') as fp:
            response = client.put_object(
                # Bucket='musex-ai-1329532616',  # Bucket 由 BucketName-APPID 组成
                Bucket='musex-ai-public-1329532616',  # Bucket 由 BucketName-APPID 组成
                Body=fp,
                Key=key_name,
                StorageClass='STANDARD',
                # ContentType='text/html; charset=utf-8'
            )
            print(response['ETag'])
    except Exception as e:
        print("error", file_name)
        print(e)


class RedisPubSub:
    def __init__(self, redis: Redis):
        self.redis = redis
        self.pubsub = None

    async def publish(self, channel: str, message: Any) -> int:
        """
        Publish a message to a channel.

        :param channel: Channel name
        :param message: Message to publish
        :return: Number of subscribers that received the message
        """
        return await self.redis.publish(channel, message)

    async def subscribe(self, channel: str, callback: Callable[[str, str], Awaitable[None]]) -> None:
        """
        Subscribe to a channel and set up a message handler.

        :param channel: Channel name to subscribe to
        :param callback: Function to call when a message is received (channel, message)
        """
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(channel)

        async for message in self.pubsub.listen():
            if message['type'] == 'message':
                await callback(message['channel'], message['data'])

    async def unsubscribe(self, channel: str) -> None:
        """
        Unsubscribe from a channel.

        :param channel: Channel name to unsubscribe from
        """
        if self.pubsub:
            await self.pubsub.unsubscribe(channel)

    async def close(self) -> None:
        """
        Close the pubsub connection.
        """
        if self.pubsub:
            await self.pubsub.aclose()


# 使用示例
async def main():
    async with acquire_redis_session() as redis_session:
        pubsub = RedisPubSub(redis_session)

        # 定义消息处理回调
        async def handle_message(channel: str, message: str):
            print(f"Received message on channel {channel}: {message}")

        # 启动订阅任务
        subscribe_task = asyncio.create_task(pubsub.subscribe("my_channel", handle_message))

        # 发布一些消息
        await pubsub.publish("my_channel", "Hello, Redis!")
        await pubsub.publish("my_channel", "Another message")

        # 等待一会儿让消息被处理
        await asyncio.sleep(1)

        # 取消订阅并关闭
        await pubsub.unsubscribe("my_channel")
        await pubsub.close()
        # subscribe_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())