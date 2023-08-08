import concurrent.futures
import json
import random
import sys
import threading
import warnings
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils.web_scraping_tools import user_agent_list

CODE_DIGITS = 12


def ensure_endswith(string: str, ends: str) -> str:
    """
    确保一个字符串以某个字符串结尾。
    """
    return string if string.endswith(ends) else (string + ends)


def ensure_endswithout(string: str, ends: str) -> str:
    """
    确保一个字符串不以某个字符串结尾。
    """
    return string[:-len(ends)] if string.endswith(ends) else string


def isurl(string: str, protocols=('http', 'https')) -> bool:
    if not string or not isinstance(string, str):
        return False
    return any(string.startswith(protocol) for protocol in protocols)


class ProgressBar:
    """
    进度条函数
    """

    def __init__(self):
        self.bar_length = 30
        self.progress_data = {}
        self.progress_semaphore = threading.Semaphore()

    def update(self, thread_id: str, progress: int, data_length: int):
        """更新进度条

        :param thread_id: 字符串类型 -> 设置进度条名称
        :param progress: 整数类型 -> 设置进度
        :param data_length: 整数类型 -> 任务长度
        :return: None
        """
        progress = ((progress + 1) / data_length) * 100  # Ensure progress is between 0 and 100
        self.progress_semaphore.acquire()
        self.progress_data[thread_id] = progress
        self.display_progress()
        if self.progress_data[thread_id] == 100:
            del self.progress_data[thread_id]
        self.progress_semaphore.release()

    def display_progress(self):
        """显示进度

        :return: None
        """
        bars = "\r"
        for thread_id in sorted(self.progress_data.keys()):
            progress = self.progress_data[thread_id]
            bar = (f"{thread_id}:["
                   f"{'=' * int(self.bar_length * progress / 100)}"
                   f"{' ' * (self.bar_length - int(self.bar_length * progress / 100))}"
                   f"] {progress:.1f}%\t\t\t\n")
            bars += bar
        sys.stdout.write(bars)
        sys.stdout.flush()


class RetrievableProxy:

    @property
    def info(self):
        """
        代理信息。
        """
        return self._info

    def __init__(self, getter_url: str = None, deleter_url: str = None):
        """
        重获式代理对象。

        :param getter_url: 获取代理的请求地址。
        :param deleter_url: 释放代理的请求地址。
        """
        self._getting: Optional[str] = getter_url if isurl(getter_url) else None
        self._deletion: Optional[str] = deleter_url if isurl(deleter_url) else None
        self._info: dict = {}

    def __repr__(self):
        pkg = self.__class__.__module__
        cls = self.__class__.__qualname__
        return f'{pkg}.{cls}({self._getting}, {self._deletion})'

    def __bool__(self):
        return bool(self._getting and self._deletion)

    def request(self, raise_exception=False, close_first=False):
        """
        从网络代理池获取一个代理。

        :param raise_exception: 是否抛出异常。
        :param close_first: 开始获取前，释放已有代理。
        :raise ValueError: 代理池响应失败或响应内容错误。
        """
        if close_first:
            self.close()
        if not self._getting:
            return
        try:
            response = requests.get(self._getting).json()
        except Exception as e:
            if raise_exception:
                raise ValueError('代理池响应失败。') from e
            else:
                return

        if not isinstance(response, dict):
            if raise_exception:
                raise ValueError('代理池响应内容错误。')
            else:
                return

        protocol = 'https' if response.get("https") else 'http'
        self._deletion = response.get("proxy")
        self._info = {protocol: f'{protocol}://{self._deletion!s}'}

    def close(self, raise_exception=False):
        """
        向代理池告知释放当前的代理。

        :param raise_exception: 是否抛出异常。
        """
        self._info = None
        if not self._deletion:
            return

        session = requests.session()
        session.keep_alive = False
        try:
            requests.get(self._deletion, params={"proxy": self._deletion})
        except Exception as e:
            if raise_exception:
                raise ValueError('代理池响应失败。') from e


class Collector:

    def __init__(
            self,
            year: int,
            proxy: RetrievableProxy = None,
            save_to: str = "./",
            encoding: str = "utf-8",
            progress: ProgressBar = None,
            parsing_retries: int = 1,
    ):
        """
        采集器。

        :param year: 年份。
        :param proxy: 代理。
        :param save_to: 保存路径。默认在脚本所在目录或项目目录。
        :param encoding: 数据存取编码。
        :param progress: 最低层级的解析进度条。
        :param parsing_retries: 页面解析重试次数。若小于 1 则一直重试。
        """
        # 设置
        self._fp = Path(save_to or ".").absolute() / f"geo_code_{year}.json"
        self._proxy = proxy or RetrievableProxy()
        self._retries = parsing_retries
        self._progress = progress

        # 数据
        self._year = year
        self._data = {}

        # 缓存
        self._names = [''] * 5  # 遍历时，省、市、县、乡、村的名称，用于显示进度
        self._max_code = 0

        # ----
        self.load(encoding=encoding)

    def load(self, fp=None, encoding="utf-8"):
        """
        重新载入数据。

        :param fp: 文件路径。若留空则使用初始化时的文件。
        :param encoding: 数据存取编码。
        :raise ValueError: 文件内容异常或年份错误。
        """
        self._fp = Path(fp or self._fp).absolute()

        if not self._fp.exists() or not self._fp.stat().st_size:
            return
        try:
            content = json.load(self._fp.open('r', encoding=encoding))
        except Exception as e:
            raise ValueError("文件内容异常，请检查，无法转换为Json格式") from e
        if not isinstance(content, dict):
            raise ValueError("文件内容异常，请检查，无法转换为Json格式")
        if content.get('year') != self._year:
            raise ValueError("文件归属年度与参数不符")
        if not isinstance(data := content.get('data', {}), dict):
            raise ValueError("文件内容异常，请检查，无法转换为Json格式")

        self._data = data
        self._max_code = int(max(self._data.keys()))

    def dump(self, fp=None, encoding="utf-8", **fields):
        """
        以最小JSON格式导出数据。

        :param fp: 文件路径。若留空则使用初始化时的文件。
        :param encoding: 数据存取编码。
        :param fields: 其它需要添加到头部的字段。
        """
        self._fp = Path(fp or self._fp).absolute()
        data = {
            **fields,
            "title": f"全国统计用区划代码和城乡划分代码（国家统计局{self._year}年度）",
            "year": self._year,
            "data": self._data,
        }
        with self._fp.open(mode='w', encoding=encoding) as f:
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    def get_page_element(self, url: str) -> dict:
        """
        获取省份用于跳转的URL

        :param url: 网页链接
        :return: key值为省份，value值为链接
        """
        retries = self._retries
        while (retries > 0) if retries else True:
            try:
                return self._get_page_element(url)
            except (
                    ValueError,
                    ConnectionError,
                    requests.exceptions.ProxyError,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
            ):
                self._proxy.request()
                if retries:
                    retries -= 1
            except Exception as err:
                print("\n", "=" * 100)
                print(f"错误链接：{url}")
                print(f"由于发生未知错误，{self._year}年的爬虫提前退出：错误为：{err.args}")
                print("=" * 100, "\n")
                raise err

    def _get_page_element(self, url: str) -> dict:
        headers = {
            'Host': 'www.stats.gov.cn',
            'User-Agent': random.choice(user_agent_list),
            'Accept-Encoding': 'identity'
        }

        session = requests.session()
        session.keep_alive = False

        response = requests.get(
            url=url,
            headers=headers,
            verify=False,
            proxies=self._proxy.info,
            timeout=2
        )

        if response.status_code != 200:
            raise ConnectionError(f"数据获取错误，状态码为：{response.status_code}")
        with warnings.catch_warnings(record=True) as w:
            try:
                html_parser = BeautifulSoup(response.content.decode('gbk'), 'html.parser')
                if w and issubclass(
                        w[-1].category,
                        UserWarning
                ) and "MarkupResemblesLocatorWarning" in str(w[-1].message):
                    html_parser = BeautifulSoup(response.content.decode('gbk'), 'lxml')
            except UnicodeDecodeError:
                try:
                    html_parser = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
                    if w and issubclass(
                            w[-1].category,
                            UserWarning
                    ) and "MarkupResemblesLocatorWarning" in str(w[-1].message):
                        html_parser = BeautifulSoup(response.content.decode('utf-8'), 'lxml')
                except UnicodeDecodeError:
                    response.encoding = response.apparent_encoding
                    html_parser = BeautifulSoup(response.text, 'html.parser')
                    if w and issubclass(
                            w[-1].category,
                            UserWarning
                    ) and "MarkupResemblesLocatorWarning" in str(w[-1].message):
                        response.encoding = response.apparent_encoding
                        html_parser = BeautifulSoup(response.text, 'lxml')

        if html_parser.find("h1"):
            raise ConnectionError("数据获取错误，错误为：Please enable JavaScript and refresh the page.")
        if "认证失败，无法访问系统资源" in str(html_parser):
            raise ConnectionError("数据获取错误，错误为：401，无法访问系统资源.")
        if "cannot find token param." in str(html_parser):
            raise ConnectionError("数据获取错误，错误为：0x01900012, cannot find token param.")
        if "请输入验证码，以继续浏览" in str(html_parser):
            raise ConnectionError("数据获取错误，错误为：需要输入验证码")
        if not url.rstrip("/").endswith('html'):
            page_element = {
                i.get_text(): {
                    "code": a.attrs['href'].strip(".html").ljust(CODE_DIGITS, '0'),
                    "next_level_url": ensure_endswith(url, '/') + a.attrs['href'],
                } if (a := i.find('a')) else {
                    "code": None,
                    "next_level_url": None,
                }
                for x in html_parser.select('.provincetr') for i in x.findAll('td')
            }
            if isinstance(page_element, dict) and page_element:
                return page_element
            else:
                raise ValueError("未获取到数据")

        elif url.strip("/")[-4:] == "html":
            select_element = [".citytr", ".countytr", ".towntr", ".villagetr"]
            table = None
            for i in select_element:
                if html_parser.select(i):
                    table = html_parser.select(i)
                    break
                else:
                    continue

            if not table:
                return {}
            next_level_url = url.replace(url.split("/")[-1], "{href}")
            page_element = {
                (x.findAll('td')[1].get_text() if len(x.findAll('td')) == 2 else x.findAll('td')[
                    2].get_text()): {
                    "code": x.findAll('td')[0].get_text(),
                    "next_level_url": next_level_url.format(
                        href=x.findAll('td')[1].find('a').attrs['href']
                    ) if x.findAll('td')[1].find('a') else None
                } for x in table
            }

            if isinstance(page_element, dict) and page_element:
                return page_element
            else:
                raise ValueError("未获取到数据")

    def get_all_code(self):
        """
        获取某一年份所有区划代码并自动保存。
        """
        self._proxy.request(raise_exception=True)
        provinces = self.get_page_element(
            f"http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/{self._year}",
        )
        for province, province_v in provinces.items():
            self._names[0] = province
            self.parse_province(province, province_v)

    def parse_province(self, province, province_v):
        max_province_code = int(str(self._max_code)[0:2].ljust(CODE_DIGITS, '0'))
        if province_v["code"] and int(province_v["code"]) < max_province_code:
            return
        if not province_v["next_level_url"]:
            if not province_v["code"]:
                return
            self._data.update({province_v["code"]: province})
            return
        self._data.update({province_v["code"]: province})
        cities = self.get_page_element(province_v["next_level_url"])

        for city, city_v in cities.items():
            self._names[1] = city
            self.parse_city(city, city_v)

    def parse_city(self, city, city_v):
        max_province_code = int(str(self._max_code)[0:4].ljust(CODE_DIGITS, '0'))
        if int(city_v["code"]) < max_province_code:
            return
        if not city_v["next_level_url"]:
            self._data.update({city_v['code']: city})
            return
        self._data.update({city_v['code']: city})
        counties = self.get_page_element(city_v["next_level_url"])

        for county, county_v in counties.items():
            self._names[2] = county
            self.parse_county(county, county_v)

        self.dump()

    def parse_county(self, county, county_v):
        max_province_code = int(str(self._max_code)[0:6].ljust(CODE_DIGITS, '0'))
        if int(county_v["code"]) < max_province_code:
            return
        if not county_v["next_level_url"]:
            self._data.update({county_v['code']: county})
            return
        self._data.update({county_v['code']: county})
        towns = self.get_page_element(county_v["next_level_url"])

        for town_i, (town, town_v) in (
                enumerate(towns.items()) if self._progress else
                enumerate(tqdm(towns.items(), desc='-'.join(self._names[:3])))
        ):
            self._names[3] = town

            max_province_code = int(str(self._max_code)[0:9].ljust(CODE_DIGITS, '0'))
            if int(town_v["code"]) < max_province_code:
                continue
            if not town_v["next_level_url"]:
                self._data.update({town_v['code']: town})
                continue
            if self._progress:
                self._progress.update(
                    thread_id='-'.join(self._names[:4]),
                    progress=town_i,
                    data_length=len(towns)
                )
            self._data.update({town_v['code']: town})
            villages = self.get_page_element(town_v["next_level_url"])
            for village, village_v in villages.items():
                if int(village_v["code"]) < self._max_code:
                    continue
                self._data.update({village_v['code']: village})


def multithreading_get_all_code(
        years: list,
        save_path: str = None,
        proxy_pool_host: str = None,
        delete_proxy_ip_host: str = None,
        retry_num: int = None
):
    """多线程获取所有区划代码

    注意：该方法依赖代理池，因此需要自备代理池

    代理接口要求返回 json 格式：{"...": "...", "proxy": "xxx.xxx.xxx.xxx:xxxx", "https": true or false}

    若是要排除已用代理，则需要以提供

    使用方法：multithreading_get_all_code(
                years=[2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
            )

    项目采用开源免费代理池项目：https://github.com/jhao104/proxy_pool

    :param years: 列表类型 -> 需要获取的年份 -> 必需
    :param save_path: 字符串类型 -> 保存路径 -> 非必需
    :param proxy_pool_host: 字符串类型 -> 代理池地址 -> 非必需
    :param delete_proxy_ip_host: 字符串类型 -> 删除代理池ip地址 -> 非必需
    :param retry_num: 整数类型 -> 重试次数
    :return:
    """
    progress_bar = ProgressBar()

    with concurrent.futures.ThreadPoolExecutor() as executor:

        futures = []
        for year in years:
            collector = Collector(
                year=year,
                proxy=RetrievableProxy(
                    getter_url=proxy_pool_host,
                    deleter_url=delete_proxy_ip_host,
                ),
                save_to=save_path,
                progress=progress_bar,
                parsing_retries=retry_num,
            )
            futures.append(
                executor.submit(collector.get_all_code)
            )

        concurrent.futures.wait(futures)

        for i, f in enumerate(concurrent.futures.as_completed(futures)):
            try:
                _ = f.result()
            except Exception as e:
                raise Exception(f"第{i}个发生错误，错误为：{e}")
