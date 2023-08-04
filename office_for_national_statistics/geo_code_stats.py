__all__ = [
    "get_page_element", "ProgressBar", "delete_proxy", "get_all_code", "multithreading_get_all_code"
]

import sys
import json
import random
import warnings
import requests
import threading
import concurrent.futures
from tqdm import tqdm
from pathlib import Path
from bs4 import BeautifulSoup
from utils.web_scraping_tools import user_agent_list


class ProgressBar:
    """
    进度条函数
    """

    def __init__(self):
        self.bar_length = 30
        self.progress_data = {}
        self.progress_semaphore = threading.Semaphore()

    def update(self, thread_id: str, progress: int):
        """更新进度条

        :param thread_id: 字符串类型 -> 设置进度条名称
        :param progress: 整数类型 -> 设置进度
        :return: None
        """
        progress = min(max(progress, 0), 100)  # Ensure progress is between 0 and 100
        self.progress_semaphore.acquire()
        self.progress_data[thread_id] = progress
        if self.progress_data[thread_id] == 100:
            del self.progress_data[thread_id]
        self.display_progress()
        self.progress_semaphore.release()

    def display_progress(self):
        """显示进度

        :return: None
        """
        bars = "\r"
        for thread_id in sorted(self.progress_data.keys()):
            progress = self.progress_data[thread_id]
            bar = (f"Thread-{thread_id}:["
                   f"{'=' * int(self.bar_length * progress / 100)}"
                   f"{' ' * (self.bar_length - int(self.bar_length * progress / 100))}"
                   f"] {progress:.1f}%\t\t\t")
            bars += bar
        sys.stdout.write(bars)
        sys.stdout.flush()


def delete_proxy(delete_proxy_ip_host: str, proxy: str):
    """删除代理IP
    注意：可使用 https://github.com/jhao104/proxy_pool 项目获取免费代理 IP

    :return:
    """
    if delete_proxy_ip_host and proxy:
        requests.get(delete_proxy_ip_host, params={"proxy": proxy})
    else:
        pass


def get_page_element(url: str, proxy_pool_host: str = None, delete_proxy_ip_host: str = None) -> dict:
    """获取省份用于跳转的URL
    :param url: 字符串类型 -> 网页链接 -> 必需
    :param proxy_pool_host: 字符串类型 -> 代理池地址
    :param delete_proxy_ip_host: 字符串类型 -> 删除代理池ip地址
    :return province_url: 字典类型 -> key值为省份，value值为链接
    """
    retry_num = 0
    while True:
        try:
            headers = {
                'Host': 'www.stats.gov.cn',
                'User-Agent': random.choice(user_agent_list)
            }
            if proxy_pool_host:
                if "http" not in proxy_pool_host or "https" not in proxy_pool_host:
                    raise Exception("请填写完整的代理池IP，例如：https://127.0.0.1:8080/get/ip/")
                proxy_pool_response = requests.get(proxy_pool_host).json()
                proxy = proxy_pool_response.get("proxy")
                if not proxy_pool_response.get("https"):
                    proxies = {
                        "http": "http://{}".format(proxy)
                    }
                else:
                    proxies = {
                        "https": "https://{}".format(proxy)
                    }
            else:
                proxy = None
                proxies = None

            response = requests.get(
                url=url,
                headers=headers,
                verify=False,
                proxies=proxies,
                timeout=1
            )

            if response.status_code != 200:
                raise ConnectionError(f"数据获取错误，状态码为：{response.status_code}")

            try:
                with warnings.catch_warnings(record=True) as w:
                    html_parser = BeautifulSoup(response.content.decode('gbk'), 'html.parser')
                    if w and issubclass(
                            w[-1].category,
                            UserWarning
                    ) and "MarkupResemblesLocatorWarning" in str(w[-1].message):
                        html_parser = BeautifulSoup(response.content.decode('gbk'), 'lxml')
                if html_parser.find("h1"):
                    raise ConnectionError("数据获取错误，错误为：Please enable JavaScript and refresh the page.")
                if "认证失败，无法访问系统资源" in str(html_parser):
                    raise ConnectionError("数据获取错误，错误为：401，无法访问系统资源.")
            except UnicodeDecodeError:
                with warnings.catch_warnings(record=True) as w:
                    html_parser = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')
                    if w and issubclass(
                            w[-1].category,
                            UserWarning
                    ) and "MarkupResemblesLocatorWarning" in str(w[-1].message):
                        html_parser = BeautifulSoup(response.content.decode('utf-8'), 'lxml')
                if html_parser.find("h1"):
                    raise ConnectionError("数据获取错误，错误为：Please enable JavaScript and refresh the page.")
                if "认证失败，无法访问系统资源" in str(html_parser):
                    raise ConnectionError("数据获取错误，错误为：401，无法访问系统资源.")
            if url.strip("/")[-4:] != "html":
                page_element = {
                    i.get_text(): {
                        "code": i.find('a').attrs['href'].strip(".html") + "0000000000" if i.find('a') else None,
                        "next_level_url": (url + "/" if url[-1] != "/" else url) + i.find('a').attrs['href'] if i.find(
                            'a') else None
                    } for x in html_parser.select('.provincetr') for i in x.findAll('td')
                }
                return page_element

            elif url.strip("/")[-4:] == "html":
                select_element = {
                    2: ".citytr",
                    4: ".countytr",
                    6: ".towntr",
                    9: ".villagetr"
                }
                next_level_url = url.replace(url.split("/")[-1], "{href}")
                page_element = {
                    (x.findAll('td')[1].get_text() if len(x.findAll('td')) == 2 else x.findAll('td')[2].get_text()): {
                        "code": x.findAll('td')[0].get_text(),
                        "next_level_url": next_level_url.format(
                            href=x.findAll('td')[1].find('a').attrs['href']
                        ) if x.findAll('td')[1].find('a') else None
                    } for x in html_parser.select(select_element[len(url.split("/")[-1].strip(".html"))])
                }

                delete_proxy(delete_proxy_ip_host, proxy)
                return page_element
        except Exception as e:
            retry_num += 1
            if retry_num == 50000:
                raise e


def get_all_code(
        year: int,
        save_path: str = None,
        progress_bar: ProgressBar = None,
        proxy_pool_host: str = None,
        delete_proxy_ip_host: str = None
):
    """获取某一年份所有区划代码

    使用方法：get_all_code(years=2009)

    :param year: 整数类型 -> 年份，填入需要获取的那一年 -> 必需
    :param save_path: 字符串类型 -> 保存路径 -> 非必需，默认当前路径
    :param progress_bar: 进度条 -> ProgressBar函数
    :param proxy_pool_host: 字符串类型 -> 代理池地址
    :param delete_proxy_ip_host: 字符串类型 -> 删除代理池ip地址
    :return:
    """
    output = Path(save_path or ".") / f"geo_code_{year}.json"

    if output.exists() and output.stat().st_size:
        try:
            result = json.load(output.open('r', encoding="utf-8"))
            if result.get("data", None):
                max_code = max([int(x) for x in result["data"].keys()])
            else:
                result = {
                    "title": f"全国统计用区划代码和城乡划分代码（国家统计局{year}年度）",
                    "year": year,
                    "data": {},
                }
                max_code = 0
        except Exception:
            raise ValueError("文件内容异常，请检查，无法转换为Json格式")
    else:
        result = {
            "title": f"全国统计用区划代码和城乡划分代码（国家统计局{year}年度）",
            "year": year,
            "data": {},
        }
        max_code = 0
    print(f"开始获取{year}年区划代码...")
    provinces = get_page_element(
        url=f"http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/{year}",
        proxy_pool_host=proxy_pool_host,
        delete_proxy_ip_host=delete_proxy_ip_host
    )
    try:
        for province, province_v in provinces.items():
            if result.get("data", None):
                max_province_code = int(str(max_code)[0:2] + "0000000000")
                if province_v["code"] and int(province_v["code"]) < max_province_code:
                    continue

            if not province_v["next_level_url"]:
                if not province_v["code"]:
                    continue
                result["data"].update({province_v["code"]: province})
                continue
            result["data"].update({province_v["code"]: province})
            cities = get_page_element(
                url=province_v["next_level_url"],
                proxy_pool_host=proxy_pool_host,
                delete_proxy_ip_host=delete_proxy_ip_host
            )
            for city, city_v in cities.items():
                if result.get("data", None):
                    max_province_code = int(str(max_code)[0:4] + "00000000")
                    if int(city_v["code"]) < max_province_code:
                        continue
                if not city_v["next_level_url"]:
                    result["data"].update({city_v['code']: city})
                    continue
                result["data"].update({city_v['code']: city})
                counties = get_page_element(
                    url=city_v["next_level_url"],
                    proxy_pool_host=proxy_pool_host,
                    delete_proxy_ip_host=delete_proxy_ip_host
                )
                for county, county_v in counties.items():
                    if result.get("data", None):
                        max_province_code = int(str(max_code)[0:6] + "000000")
                        if int(county_v["code"]) < max_province_code:
                            continue
                    if not county_v["next_level_url"]:
                        result["data"].update({county_v['code']: county})
                        continue
                    result["data"].update({county_v['code']: county})
                    towns = get_page_element(
                        url=county_v["next_level_url"],
                        proxy_pool_host=proxy_pool_host,
                        delete_proxy_ip_host=delete_proxy_ip_host
                    )
                    for town_i, (town, town_v) in (
                            enumerate(tqdm(towns.items(), desc=f"{province}-{city}-{county}"))
                            if not progress_bar else enumerate(towns.items())
                    ):
                        if progress_bar:
                            progress_bar.update(thread_id=f"{year}-{province}-{city}-{county}-{town}", progress=town_i)
                        if result.get("data", None):
                            max_province_code = int(str(max_code)[0:9] + "000")
                            if int(town_v["code"]) < max_province_code:
                                continue
                        if not town_v["next_level_url"]:
                            result["data"].update({town_v['code']: town})
                            continue
                        result["data"].update({town_v['code']: town})
                        villages = get_page_element(
                            url=town_v["next_level_url"],
                            proxy_pool_host=proxy_pool_host,
                            delete_proxy_ip_host=delete_proxy_ip_host
                        )
                        for village, village_v in villages.items():
                            if result.get("data", None) and int(village_v["code"]) < max_code:
                                continue
                            result["data"].update({village_v['code']: village})

        with output.open(mode='w', encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)

        print(f"{year}年区划代码获取完成！")
    except Exception as err:
        raise Exception(f"程序异常，错误信息为:{str(err)}")
    finally:
        with output.open(mode='w', encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)


def multithreading_get_all_code(
        years: list,
        save_path: str = None,
        proxy_pool_host: str = None,
        delete_proxy_ip_host: str = None
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
    :return:
    """
    progress_bar = ProgressBar()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(get_all_code, years[i], save_path, progress_bar, proxy_pool_host, delete_proxy_ip_host) for
            i in range(len(years))
        ]

        concurrent.futures.wait(futures)

        for i, f in enumerate(concurrent.futures.as_completed(futures)):
            try:
                _ = f.result()
            except Exception as e:
                print(f"第{i}个发生错误，错误为：{e}")
