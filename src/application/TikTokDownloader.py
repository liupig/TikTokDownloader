import asyncio
from asyncio import run
from threading import Event, Thread
from time import sleep

from httpx import RequestError, get

from src.config import Parameter, Settings
from src.custom import (
    COOKIE_UPDATE_INTERVAL,
    DISCLAIMER_TEXT,
    DOCUMENTATION_URL,
    LICENCE,
    MASTER,
    PROJECT_NAME,
    PROJECT_ROOT,
    RELEASES,
    REPOSITORY,
    SERVER_HOST,
    SERVER_PORT,
    TEXT_REPLACEMENT,
    VERSION_BETA,
    VERSION_MAJOR,
    VERSION_MINOR,
)
from src.manager import Database, DownloadRecorder
from src.module import Cookie, Register
from src.record import BaseLogger, LoggerManager
from src.tools import (
    Browser,
    ColorfulConsole,
    TikTokDownloaderError,
    choose,
    remove_empty_directories,
    safe_pop,
)
from src.translation import _, switch_language

from .main_api import APIServer
from .main_complete import TikTok

# from typing import Type
# from webbrowser import open
# from flask import abort
# from flask import request

# from .main_server import Server
# from .main_web_UI import WebUI

__all__ = ["TikTokDownloader"]

from ..downloader.sharp import acquire_redis_session, RedisPubSub


def server_tip(function):
    async def inner(self, *args, **kwargs):
        self.console.print(_("正在启动服务器，如需关闭服务器，请按下 Ctrl + C"))
        return await function(self, *args, **kwargs)

    return inner


class TikTokDownloader:
    VERSION_MAJOR = VERSION_MAJOR
    VERSION_MINOR = VERSION_MINOR
    VERSION_BETA = VERSION_BETA
    PLATFORM = (
        "cookie",
        "cookie_tiktok",
    )
    NAME = PROJECT_NAME
    WIDTH = 50
    LINE = ">" * WIDTH

    def __init__(
        self,
    ):
        self.console = ColorfulConsole()
        self.logger = None
        self.recorder = None
        self.settings = Settings(PROJECT_ROOT, self.console)
        self.event = Event()
        self.cookie = Cookie(self.settings, self.console)
        self.params_task = None
        self.parameter = None
        self.running = True
        self.run_command = None
        self.database = Database()
        self.config = None
        self.option = None
        self.__function_menu = None
        self._task = None
        self._stop_event = asyncio.Event()
        self.pubsub = None

    async def read_config(self):
        self.config = self.__format_config(await self.database.read_config_data())
        self.option = self.__format_config(await self.database.read_option_data())
        self.set_language(self.option["Language"])

    @staticmethod
    def __format_config(config: list) -> dict:
        return {i["NAME"]: i["VALUE"] for i in config}

    @staticmethod
    def set_language(language: str) -> None:
        switch_language(language)

    async def __aenter__(self):
        await self.database.__aenter__()
        await self.read_config()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.database.__aexit__(exc_type, exc_val, exc_tb)
        if self.parameter:
            await self.parameter.close_client()
            self.close()

    def __update_menu(self):
        options = {
            1: _("禁用"),
            0: _("启用"),
        }
        self.__function_menu = (
            (_("复制粘贴写入 Cookie (抖音)"), self.write_cookie),
            (_("从浏览器获取 Cookie (抖音)"), self.browser_cookie),
            (_("扫码登录获取 Cookie (抖音)"), self.auto_cookie),
            (_("复制粘贴写入 Cookie (TikTok)"), self.write_cookie_tiktok),
            (_("从浏览器获取 Cookie (TikTok)"), self.browser_cookie_tiktok),
            (_("终端交互模式"), self.complete),
            (_("后台监测模式"), self.disable_function),
            (_("Web API 模式"), self.api_server),
            (_("Web UI 模式"), self.disable_function),
            (_("服务器部署模式"), self.disable_function),
            # (_("Web API 模式"), self.__api_object),
            # (_("Web UI 模式"), self.__web_ui_object),
            # (_("服务器部署模式"), self.__server_object),
            (
                _("{}作品下载记录").format(options[self.config["Record"]]),
                self.__modify_record,
            ),
            (_("删除作品下载记录"), self.delete_works_ids),
            (
                _("{}运行日志记录").format(options[self.config["Logger"]]),
                self.__modify_logging,
            ),
            (_("检查程序版本更新"), self.check_update),
            (_("切换语言"), self._switch_language),
        )

    async def disable_function(
        self,
        *args,
        **kwargs,
    ):
        self.console.warning(
            "该功能正在重构，未来开发完成重新开放！",
        )

    @server_tip
    async def api_server(self):
        try:
            await APIServer(
                self.parameter,
                self.database,
            ).run_server(
                SERVER_HOST,
                SERVER_PORT,
            )
        except KeyboardInterrupt:
            self.running = False

    # def __web_ui_object(self):
    #     self.server(WebUI, token=False)

    # def __server_object(self):
    #     self.server(Server)

    async def __modify_record(self):
        await self.change_config("Record")

    async def __modify_logging(self):
        await self.change_config("Logger")

    async def _switch_language(
        self,
    ):
        if self.option["Language"] == "zh_CN":
            language = "en_US"
        elif self.option["Language"] == "en_US":
            language = "zh_CN"
        else:
            raise TikTokDownloaderError
        await self._update_language(language)

    async def _update_language(self, language: str) -> None:
        self.option["Language"] = language
        await self.database.update_option_data("Language", language)
        self.set_language(language)

    async def disclaimer(self):
        if not self.config["Disclaimer"]:
            await self.__init_language()
            self.console.print(_(DISCLAIMER_TEXT), style=MASTER)
            if self.console.input(
                _("是否已仔细阅读上述免责声明(YES/NO): ")
            ).upper() not in ("Y", "YES"):
                return False
            await self.database.update_config_data("Disclaimer", 1)
            self.console.print()
        return True

    async def __init_language(self):
        languages = (
            (
                "简体中文",
                "zh_CN",
            ),
            (
                "English",
                "en_US",
            ),
        )
        language = choose(
            "请选择语言(Please Select Language)",
            [i[0] for i in languages],
            self.console,
        )
        try:
            language = languages[int(language) - 1][1]
            await self._update_language(language)
        except ValueError:
            await self.__init_language()

    def project_info(self):
        self.console.print(
            f"{self.LINE}\n\n\n{self.NAME.center(self.WIDTH)}\n\n\n{self.LINE}\n",
            style=MASTER,
        )
        self.console.print(_("项目地址: {}").format(REPOSITORY), style=MASTER)
        self.console.print(_("项目文档: {}").format(DOCUMENTATION_URL), style=MASTER)
        self.console.print(_("开源许可: {}\n").format(LICENCE), style=MASTER)

    def check_config(self):
        self.recorder = DownloadRecorder(
            self.database,
            self.config["Record"],
            self.console,
        )
        self.logger = {1: LoggerManager, 0: BaseLogger}[self.config["Logger"]]

    async def check_update(self):
        try:
            response = get(
                RELEASES,
                timeout=5,
                follow_redirects=True,
            )
            latest_major, latest_minor = map(
                int, str(response.url).split("/")[-1].split(".", 1)
            )
            if latest_major > self.VERSION_MAJOR or latest_minor > self.VERSION_MINOR:
                self.console.warning(
                    _("检测到新版本: {major}.{minor}").format(
                        major=latest_major, minor=latest_minor
                    ),
                )
                self.console.print(RELEASES)
            elif latest_minor == self.VERSION_MINOR and self.VERSION_BETA:
                self.console.warning(
                    _("当前版本为开发版, 可更新至正式版"),
                )
                self.console.print(RELEASES)
            elif self.VERSION_BETA:
                self.console.warning(
                    _("当前已是最新开发版"),
                )
            else:
                self.console.info(
                    _("当前已是最新正式版"),
                )
        except RequestError:
            self.console.error(
                _("检测新版本失败"),
            )

    async def main_menu(
        self,
        mode=None,
    ):
        """选择功能模式"""
        while self.running:
            self.__update_menu()
            if not mode:
                mode = choose(
                    _("TikTokDownloader 功能选项"),
                    [i for i, __ in self.__function_menu],
                    self.console,
                    separate=(
                        5,
                        10,
                    ),
                )
            await self.compatible(mode)
            mode = None

    async def complete(self):
        """终端交互模式"""
        example = TikTok(
            self.parameter,
            self.database,
        )
        try:
            await example.run(self.run_command)
            self.running = example.running
        except KeyboardInterrupt:
            self.running = False

    # def server(
    #         self,
    #         server: Type[APIServer | WebUI | Server],
    #         host="0.0.0.0",
    #         token=True):
    #     """
    #     服务器模式
    #     """
    #     self.console.print(
    #         "如果您看到 WARNING: This is a development server. 提示，这并不是异常错误！\n如需关闭服务器，可以在终端按下 Ctrl + C 快捷键！",
    #         style=INFO)
    #     master = server(self.parameter)
    #     app = master.run_server(Flask("__main__"))
    #     register(self.recorder.close)
    #     if token:
    #         app.before_request(self.verify_token)
    #     open(f"http://127.0.0.1:{SERVER_PORT}")
    #     app.run(host=host, port=SERVER_PORT)

    # @staticmethod
    # def verify_token():
    #     if request.method == "POST" and not verify_token(
    #             request.json.get("token")):
    #         return abort(403)

    async def change_config(
        self,
        key: str,
    ):
        self.config[key] = 0 if self.config[key] else 1
        await self.database.update_config_data(key, self.config[key])
        self.console.print(_("修改设置成功！"))
        self.check_config()
        await self.check_settings()

    async def write_cookie(self):
        await self.__write_cookie()

    async def write_cookie_tiktok(self):
        await self.__write_cookie(1)

    async def __write_cookie(self, index=0):
        self.console.print(
            _("Cookie 获取教程：")
            + "https://github.com/JoeanAmier/TikTokDownloader/blob/master/docs/Cookie%E8%8E%B7%E5%8F%96%E6"
            "%95%99%E7%A8%8B.md"
        )
        if self.cookie.run(self.PLATFORM[index], index):
            await self.check_settings()

    async def auto_cookie(self):
        self.console.error(
            _(
                "该功能为实验性功能，仅适用于学习和研究目的；目前仅支持抖音平台，建议使用其他方式获取 Cookie，未来可能会禁用或移除该功能！"
            ),
        )
        if self.console.input(_("是否返回上一级菜单(YES/NO)")).upper() != "NO":
            return
        if cookie := await Register(
            self.parameter,
            self.settings,
        ).run():
            self.cookie.extract(cookie)
            await self.check_settings()
        else:
            self.console.warning(
                _("扫码登录失败，未写入 Cookie！"),
            )

    async def compatible(self, mode: str):
        if mode in {"Q", "q", ""}:
            self.running = False
        try:
            n = int(mode) - 1
        except ValueError:
            return
        if n in range(len(self.__function_menu)):
            await self.__function_menu[n][1]()

    async def delete_works_ids(self):
        if not self.config["Record"]:
            self.console.warning(
                _("作品下载记录功能已禁用！"),
            )
            return
        await self.recorder.delete_ids(self.console.input("请输入需要删除的作品 ID："))
        self.console.info(
            "删除作品下载记录成功！",
        )

    async def check_settings(self, restart=True):
        if restart:
            await self.parameter.close_client()
        self.parameter = Parameter(
            self.settings,
            self.cookie,
            logger=self.logger,
            console=self.console,
            **self.settings.read(),
            recorder=self.recorder,
        )
        self.parameter.set_headers_cookie()
        # self.restart_cycle_task(
        #     restart,
        # )
        await self.parameter.update_params_beta()
        if not restart:
            self.run_command = self.parameter.run_command.copy()
        self.parameter.CLEANER.set_rule(TEXT_REPLACEMENT, True)

    async def run(self):
        self.project_info()
        self.check_config()
        await self.check_settings(
            False,
        )
        if await self.disclaimer():
            await self.main_menu(safe_pop(self.run_command))

    async def set_cookie(self):
        channel = "TIKTOK_SET_COOKIE"
        async with acquire_redis_session() as redis_session:
            self.pubsub = RedisPubSub(redis_session)

            async def handle_message(channel: str, message: str):
                print(f"Received message on channel {channel}: {message}")
                self.cookie.extract(message)

            self._task = asyncio.create_task(
                self.pubsub.subscribe(channel, handle_message)
            )

            await self._stop_event.wait()  # 等待停止信号

            # 清理
            await self.pubsub.unsubscribe(channel)
            await self.pubsub.close()
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def account_detail_inquire(self):
        channel = "TIKTOK_ACCOUNT_DETAIL_INQUIRE"
        example = TikTok(
            self.parameter,
            self.database,
        )
        async with acquire_redis_session() as redis_session:
            self.pubsub = RedisPubSub(redis_session)

            async def handle_message(channel: str, message: str):
                print(f"Received message on channel {channel}: {message}")
                try:
                    await example.account_detail_inquire_sharp(url=message)
                    self.running = example.running
                except KeyboardInterrupt:
                    self.running = False

            self._task = asyncio.create_task(
                self.pubsub.subscribe(channel, handle_message)
            )

            await self._stop_event.wait()  # 等待停止信号

            # 清理
            await self.pubsub.unsubscribe(channel)
            await self.pubsub.close()
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def run_set_cookie(self):
        # cookie-name:odin_tt
        self.project_info()
        self.check_config()
        await self.check_settings(
            False,
        )
        await self.set_cookie()

    async def run_account_detail_inquire(self):
        self.project_info()
        self.check_config()
        await self.check_settings(
            False,
        )
        await self.account_detail_inquire()

    def periodic_update_params(self):
        async def inner():
            while not self.event.is_set():
                await self.parameter.update_params()
                self.event.wait(COOKIE_UPDATE_INTERVAL)

        run(
            inner(),
        )

    def restart_cycle_task(
        self,
        restart=True,
    ):
        if restart:
            self.event.set()
            while self.params_task.is_alive():
                # print("等待子线程结束！")  # 调试代码
                sleep(1)
        self.params_task = Thread(target=self.periodic_update_params)
        self.event.clear()
        self.params_task.start()

    def close(self):
        self.event.set()
        if self.parameter.folder_mode:
            remove_empty_directories(self.parameter.ROOT)
            remove_empty_directories(self.parameter.root)
        self.parameter.logger.info(_("正在关闭程序"))

    async def browser_cookie(
        self,
    ):
        if Browser(self.parameter, self.cookie).run(
            select=safe_pop(self.run_command),
        ):
            await self.check_settings()

    async def browser_cookie_tiktok(
        self,
    ):
        if Browser(self.parameter, self.cookie).run(
            True,
            select=safe_pop(self.run_command),
        ):
            await self.check_settings()
