# -*- coding=utf-8
import json
import time
import uuid
import requests
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
from moviepy import VideoFileClip

# 正常情况日志级别使用 INFO，需要定位时可以修改为 DEBUG，此时 SDK 会打印和服务端的通信信息
logging.basicConfig(level=logging.INFO, stream=sys.stdout)


class Settings(BaseSettings):
    redis_host: str = "10.0.0.0"
    redis_port: int = 6379
    redis_user: Optional[str] = ""
    redis_pass: Optional[str] = r"xxx"
    redis_base: Optional[int] = 10

    # TOS
    tencent_secret_id: str = "1"
    tencent_secret_key: str = "1"
    tiktok_bucket: str = '1'

    # ASR
    asr_appid: str = '1'
    asr_token: str = '1'

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


class AsrTask(object):
    def submit_task(self, file_url):
        submit_url = "https://openspeech.bytedance.com/api/v3/auc/bigmodel/submit"

        task_id = str(uuid.uuid4())

        headers = {
            "X-Api-App-Key": settings.asr_appid,
            "X-Api-Access-Key": settings.asr_token,
            "X-Api-Resource-Id": "volc.bigasr.auc",
            "X-Api-Request-Id": task_id,
            "X-Api-Sequence": "-1"
        }

        request = {
            "user": {
                "uid": "fake_uid"
            },
            "audio": {
                "url": file_url,
                "format": "mp3",
                "codec": "raw",
                "rate": 16000,
                "bits": 16,
                "channel": 1
            },
            "request": {
                "model_name": "bigmodel",
                # "enable_itn": True,
                # "enable_punc": True,
                # "enable_ddc": True,
                "show_utterances": True,
                # "enable_channel_split": True,
                # "vad_segment": True,
                "enable_speaker_info": True,
                "corpus": {
                    # "boosting_table_name": "test",
                    "correct_table_name": "",
                    "context": ""
                }
            }
        }
        print(f'Submit task id: {task_id}')
        response = requests.post(submit_url, data=json.dumps(request), headers=headers)
        if 'X-Api-Status-Code' in response.headers and response.headers["X-Api-Status-Code"] == "20000000":
            print(f'Submit task response header X-Api-Status-Code: {response.headers["X-Api-Status-Code"]}')
            print(f'Submit task response header X-Api-Message: {response.headers["X-Api-Message"]}')
            x_tt_logid = response.headers.get("X-Tt-Logid", "")
            print(f'Submit task response header X-Tt-Logid: {response.headers["X-Tt-Logid"]}\n')
            return task_id, x_tt_logid
        else:
            print(f'Submit task failed and the response headers are: {response.headers}')

        return task_id

    def query_task(self, task_id, x_tt_logid):
        query_url = "https://openspeech.bytedance.com/api/v3/auc/bigmodel/query"

        headers = {
            "X-Api-App-Key": settings.asr_appid,
            "X-Api-Access-Key": settings.asr_token,
            "X-Api-Resource-Id": "volc.bigasr.auc",
            "X-Api-Request-Id": task_id,
            "X-Tt-Logid": x_tt_logid  # 固定传递 x-tt-logid
        }

        response = requests.post(query_url, json.dumps({}), headers=headers)

        if 'X-Api-Status-Code' in response.headers:
            print(f'Query task response header X-Api-Status-Code: {response.headers["X-Api-Status-Code"]}')
            print(f'Query task response header X-Api-Message: {response.headers["X-Api-Message"]}')
            print(f'Query task response header X-Tt-Logid: {response.headers["X-Tt-Logid"]}\n')
        else:
            print(f'Query task failed and the response headers are: {response.headers}')
            return
        return response

    def run_asr_task(self, file_url, asr_file_name):
        task_id, x_tt_logid = self.submit_task(file_url)
        while True:
            try:
                query_response = self.query_task(task_id, x_tt_logid)
                code = query_response.headers.get('X-Api-Status-Code', "")
                if code == '20000000':
                    print("SUCCESS!")
                    return query_response.json()
                elif code != '20000001' and code != '20000002':  # task failed
                    print("FAILED!")
                    break
                time.sleep(1)
            except Exception as e:
                print(e)
                break

    def extract_audio_from_video(self, video_path, audio_path):
        """
        从视频文件中提取音频并保存为 MP3。

        参数:
        video_path (str): 输入的 MP4 视频文件路径。
        audio_path (str): 输出的 MP3 音频文件路径。
        """
        try:
            # 载入视频文件
            video_clip = VideoFileClip(video_path)

            # 提取音频部分
            audio_clip = video_clip.audio

            # 将音频写入 MP3 文件
            # 可以指定比特率，例如 bitrate="192k"
            # codec="libmp3lame" 是常用的 MP3 编码器
            audio_clip.write_audiofile(audio_path, codec="libmp3lame")

            # 关闭剪辑以释放资源
            audio_clip.close()
            video_clip.close()

            print(f"音频已成功提取并保存到: {audio_path}")
            return True
        except Exception as e:
            print(f"提取音频时发生错误: {e}")
            return False


class CloudTos(object):
    region = 'ap-beijing'
    scheme = 'https'
    token = None

    def __init__(self, bucket):
        self.config = self.init_config()
        self.client = CosS3Client(self.config)
        self.bucket = bucket

    def init_config(self):
        self.config = CosConfig(Region=self.region, SecretId=settings.tencent_secret_id,
                                SecretKey=settings.tencent_secret_key, Token=self.token, Scheme=self.scheme)
        return self.config

    def save_local_file(self, file_name, key_name):
        with open(file_name, 'rb') as fp:
            response = self.client.put_object(
                Bucket=self.bucket,  # Bucket 由 BucketName-APPID 组成
                Body=fp,
                Key=key_name,
                StorageClass='STANDARD',
            )
            print(response['ETag'])

    def save_content(self, content, key_name):
        response = self.client.put_object(
            Bucket='musex-ai-public-1329532616',  # Bucket 由 BucketName-APPID 组成
            # Body=json.dumps(query_response.json(), ensure_ascii=False, indent=4),
            Body=content,
            Key=key_name,
            StorageClass='STANDARD',
            # ContentType='text/html; charset=utf-8'
        )

    def get_object_url(self, key_name):
        return self.client.get_object_url(
            Bucket=self.bucket,
            Key=key_name
        )

    def send_to_tos(self, file_name):
        try:
            # file_name = r'D:\MyTikTokDownloads\UID4160968570706796_奶呼呼_发布作品\2025-04-21 12.00.21-视频-奶呼呼-#元气少女 #甜妹 #笑起来很甜#企鹅防晒衣 #企鹅光合防晒衣.mp4'
            file_name = str(file_name).replace("\\", "/")
            if "MyTikTokDownloads" in file_name:
                split_flag = "MyTikTokDownloads"
            else:
                split_flag = "TikTokDownloader"
            key_name = "tiktok_china/tiktok_china_video/" + file_name.split(split_flag)[-1].strip(r"\\/")

            self.save_local_file(file_name, key_name)

            if "mp4" in file_name:
                audio_file_name = file_name.replace("mp4", "mp3")
                asr_task.extract_audio_from_video(file_name, audio_file_name)
                key_name = "tiktok_china/tiktok_china_audio/" + audio_file_name.split(split_flag)[-1].strip(r"\\/")
                self.save_local_file(file_name, key_name)

                url = self.get_object_url(key_name)

                asr_file_name = file_name.replace("mp4", "json")
                key_name = "tiktok_china/tiktok_china_asr/" + asr_file_name.split(split_flag)[-1].strip(r"\\/")
                asr_result = asr_task.run_asr_task(url, key_name)
                if asr_result:
                    self.save_content(json.dumps(asr_result, ensure_ascii=False, indent=4), key_name)
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


asr_task = AsrTask()
cloud_tos = CloudTos(settings.tiktok_bucket)
