__all__ = ["get_page_element"]

import random
import requests
from bs4 import BeautifulSoup
from utils.web_scraping_tools import user_agent_list


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
                "next_level_url": url + i.find('a').attrs['href'] if i.find('a') else None
            } for x in html_parser.select('.provincetr') for i in x.findAll('td')
        }
        return page_element

    elif url.strip("/")[-4:] == "html":
        next_level_url = url.replace(url.split("/")[-1], "{href}")
        page_element = {
            x.findAll('td')[1].get_text(): {
                "code": x.findAll('td')[0].get_text(),
                "next_level_url": next_level_url.format(
                    href=x.findAll('td')[1].find('a').attrs['href'] if x.findAll('td')[1].find('a') else None
                )
            } for x in html_parser.select('.citytr')
        }

        return page_element
