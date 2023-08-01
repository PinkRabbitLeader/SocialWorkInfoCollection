__all__ = ["get_province_url"]

import random
import requests
from bs4 import BeautifulSoup
from utils.web_scraping_tools import user_agent_list


def get_province_url(year: int) -> dict:
    """获取省份用于跳转的URL

    :param year: 整数类型 -> 年份，填入需要获取的那一年 -> 必需
    :return province_url: 字典类型 -> key值为省份，value值为链接
    """
    headers = {
        'Host': 'www.stats.gov.cn',
        'User-Agent': random.choice(user_agent_list)
    }

    response = requests.get(
        url=f"http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/{year}/",
        headers=headers,
        verify=False
    )

    if response.status_code != 200:
        raise ConnectionError(f"数据获取错误，状态码为：{response.status_code}")

    html_parser = BeautifulSoup(response.content.decode('gbk'), 'html.parser')

    city_url = "http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/{year}/{html_name}"

    province_url = {
        i.get_text(): city_url.format(year=year, html_name=i.find('a').attrs['href'] if i.find('a') else None)
        for x in html_parser.select('.provincetr') for i in x.findAll('td')
    }

    return province_url

