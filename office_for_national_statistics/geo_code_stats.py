__all__ = ["get_page_element", "get_all_code"]

import json
import random
import requests
from tqdm import tqdm
from pathlib import Path
from bs4 import BeautifulSoup
from retrying import retry
from utils.web_scraping_tools import user_agent_list


@retry(stop_max_attempt_number=10, wait_random_min=2000, wait_random_max=10000)
def get_page_element(url: str) -> dict:
    """获取省份用于跳转的URL
    :param url: 字符串类型 -> 网页链接 -> 必需
    :return province_url: 字典类型 -> key值为省份，value值为链接
    """
    headers = {
        'Host': 'www.stats.gov.cn',
        'User-Agent': random.choice(user_agent_list)
    }

    response = requests.get(
        url=url,
        headers=headers,
        verify=False
    )

    if response.status_code != 200:
        raise ConnectionError(f"数据获取错误，状态码为：{response.status_code}")
    try:
        html_parser = BeautifulSoup(response.content.decode('gbk'), 'html.parser')
    except UnicodeDecodeError:
        html_parser = BeautifulSoup(response.content.decode('utf-8'), 'html.parser')

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

        return page_element


def get_all_code(year: int, save_path: str = None):
    """获取某一年份所有区划代码

    :param year: 整数类型 -> 年份，填入需要获取的那一年 -> 必需
    :param save_path: 字符串类型 -> 保存路径 -> 非必需，默认当前路径
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
            "title": "全国统计用区划代码和城乡划分代码（国家统计局2009年度）",
            "year": 2009,
            "data": {},
        }
        max_code = 0

    provinces = get_page_element(url=f"http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/{year}")
    print("开始获取区划代码...")
    try:
        for province_i, (province, province_v) in enumerate(provinces.items()):
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
            cities = get_page_element(url=province_v["next_level_url"])
            for city, city_v in cities.items():
                if result.get("data", None):
                    max_province_code = int(str(max_code)[0:4] + "00000000")
                    if int(city_v["code"]) < max_province_code:
                        continue
                if not city_v["next_level_url"]:
                    result["data"].update({city_v['code']: city})
                    continue
                result["data"].update({city_v['code']: city})
                counties = get_page_element(url=city_v["next_level_url"])
                for county, county_v in counties.items():
                    if result.get("data", None):
                        max_province_code = int(str(max_code)[0:6] + "000000")
                        if int(province_v["code"]) < max_province_code:
                            continue
                    if not county_v["next_level_url"]:
                        result["data"].update({county_v['code']: county})
                        continue
                    result["data"].update({county_v['code']: county})
                    towns = get_page_element(url=county_v["next_level_url"])
                    for town, town_v in tqdm(towns.items(), desc=f"{province}-{city}-{county}"):
                        if result.get("data", None):
                            max_province_code = int(str(max_code)[0:9] + "000")
                            if int(province_v["code"]) < max_province_code:
                                continue
                        if not town_v["next_level_url"]:
                            result["data"].update({town_v['code']: town})
                            continue
                        result["data"].update({town_v['code']: town})
                        villages = get_page_element(url=town_v["next_level_url"])
                        for village_i, (village, village_v) in enumerate(villages.items()):
                            result["data"].update({village_v['code']: village})

        with output.open(mode='w', encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)

        print("区划代码获取完成！")
    except Exception as err:
        raise Exception(f"程序异常，错误信息为:{err}")
    finally:
        with output.open(mode='w', encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
