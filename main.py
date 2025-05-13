import asyncio
from asyncio import CancelledError
from asyncio import run

from src.application import TikTokDownloader


async def main():
    async with TikTokDownloader() as downloader:
        try:
            await downloader.run()
        except (
                KeyboardInterrupt,
                CancelledError,
        ):
            return

async def main_sharp():
    async with TikTokDownloader() as downloader:
        try:
            loop = asyncio.get_event_loop()

            # 创建任务并运行
            task1 = loop.create_task(downloader.run_set_cookie())
            task2 = loop.create_task(downloader.run_account_detail_inquire())
            await task1  # 如果需要等待任务完成
            await task2  # 如果需要等待任务完成

        except (
                KeyboardInterrupt,
                CancelledError,
        ):
            return


if __name__ == "__main__":
    # run(main())
    asyncio.run(main_sharp())

