__all__ = ["get_page_element"]

import random
import os
import requests
from tqdm import tqdm
from pathlib import Path
from bs4 import BeautifulSoup
from retrying import retry
from utils.web_scraping_tools import user_agent_list


def req_error(exception):
    if isinstance(exception, ConnectionError):
        print("获取数据失败，正在重新尝试，共10次...")
    else:
        raise Exception(f"获取数据失败，原错误为：{str(exception)}")
    return isinstance(exception, ConnectionError)


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
    html_parser = BeautifulSoup(response.content.decode('gbk'), 'html.parser')

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
    file = str(Path(save_path).joinpath(f"geo_code_{year}.json")) if save_path else f"geo_code_{year}.json"

    if os.path.exists(file):
        raise FileExistsError(f"json文件已存在，请检查{file}")

    with open(file, 'w', encoding="utf-8") as f:
        f.write(
            """{{\n\t"title": "全国统计用区划代码和城乡划分代码（国家统计局{year}年度）",\n\t"year": {year},\n\t"data": {{\n"""
            .format(year=year))

        provinces = get_page_element(url=f"http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/{year}")
        print("开始获取区划代码...")
        try:
            for province_i, (province, province_v) in enumerate(provinces.items()):
                if not province_v["next_level_url"]:
                    f.write(f"\t\t\"{province_v['code']}\": \"{province}\",\n")
                    continue
                f.write(f"\t\t\"{province_v['code']}\": \"{province}\",\n")
                cities = get_page_element(url=province_v["next_level_url"])
                for city, city_v in cities.items():
                    if not city_v["next_level_url"]:
                        f.write(f"\t\t\"{city_v['code']}\": \"{city}\",\n")
                        continue
                    f.write(f"\t\t\"{city_v['code']}\": \"{city}\",\n")
                    counties = get_page_element(url=city_v["next_level_url"])
                    for county, county_v in counties.items():
                        if not county_v["next_level_url"]:
                            f.write(f"\t\t\"{county_v['code']}\": \"{county}\",\n")
                            continue
                        f.write(f"\t\t\"{county_v['code']}\": \"{county}\",\n")
                        towns = get_page_element(url=county_v["next_level_url"])
                        for town, town_v in tqdm(towns.items(), desc=f"{province}-{city}-{county}"):
                            if not town_v["next_level_url"]:
                                f.write(f"\t\t\"{town_v['code']}\": \"{town}\",\n")
                                continue
                            f.write(f"\t\t\"{town_v['code']}\": \"{town}\",\n")
                            villages = get_page_element(url=town_v["next_level_url"])
                            for village_i, (village, village_v) in enumerate(villages.items()):
                                if not village_v["next_level_url"]:
                                    if province_i == len(provinces) - 1 and village_i == len(villages) - 1:
                                        f.write(f"\t\t\"{village_v['code']}\": \"{village}\"\n")
                                    else:
                                        f.write(f"\t\t\"{village_v['code']}\": \"{village}\",\n")
                                    continue
                                if province_i == len(provinces) - 1 and village_i == len(villages) - 1:
                                    f.write(f"\t\t\"{village_v['code']}\": \"{village}\"\n")
                                else:
                                    f.write(f"\t\t\"{village_v['code']}\": \"{village}\",\n")
        except Exception as err:
            print(f"程序异常，错误信息为:{err}")
        finally:
            f.write("""\t}\n}""")
            f.close()

    print("区划代码获取完成！")
