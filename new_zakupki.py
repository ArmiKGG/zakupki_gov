import datetime
import requests
from bs4 import BeautifulSoup
from fake_headers import Headers
from typing import AnyStr
from urllib.parse import urlparse
from urllib.parse import parse_qs
from elastic import *
import logging
import pandas as pd
from string import digits

NAMEING = "Наименование объекта закупки и его характеристики"
URL_TO_ITEMS = "https://zakupki.gov.ru/epz/contract/contractCard/payment-info-" \
               "and-target-of-order-list.html?reestrNumber={}&page=1&pageSize=200"

es = connect_elasticsearch()


def text_fixer(text):
    return text.replace("\xa0", " ")


def logger(message):
    print(message)
    #logging.info(message)


def get_last_value(org):
    if not org.get("statistics"):
        return 1
    return list(org.get("statistics").values())[0]


def parser(url: AnyStr):
    parsed_url = urlparse(url)
    captured_value = parse_qs(parsed_url.query)["reestrNumber"][0]
    r = requests.get(url, headers=Headers(headers=True).generate(), timeout=15)
    data_table = pd.read_html(r.content, encoding="utf-8")
    if data_table:
        data_table = data_table[0]
        data_table = data_table.to_dict("records")
        clean_list = []
        main_info = {}
        if data_table:
            for i, d in enumerate(data_table):
                first_col = list(d.keys())[0]
                if first_col != NAMEING:
                    del d[first_col]
                if type(d[NAMEING]) is str:
                    clean_list.append(dict((k, text_fixer(str(v))) for k, v in d.items()))

        main_info.update({"Объекты закупки подробнее": clean_list, "reestrNumber": int(captured_value), "url": url})
        logger(main_info)
        return main_info


def reformat_data(source):
    collect = []
    for num, j in enumerate(source["Объекты закупки подробнее"]):
        if j["Тип объекта закупки"]:
            nameing = j["Наименование объекта закупки и его характеристики"]
            if "страна происхождения" in nameing.lower():
                name2 = nameing.lower().split("страна происхождения")[0]
                desc = "Страна происхождения " + nameing.lower().split("страна происхождения")[1]
            else:
                name2 = nameing
                desc = ""

            units = j["Количество товара, объем работы, услуги,Единица измерения"]
            remove_digits = str.maketrans('', '', digits)
            res = units.translate(remove_digits)
            zed = {
                "sourceID": f'{source["reestrNumber"]}00{num + 1}',
                "providerID": f'{source["reestrNumber"]}',
                "providerName": "provider-zakupki",
                "price": float(j["Цена за единицу измерения, ₽"].replace(",", ".").replace(" ", "")),
                "name": name2.strip(),
                "description": desc.strip(),
                "unit": res.strip(),
                "url": source["url"],
                "currency": "RUB",
                "properties": {"contract": None,
                               "KTRU": j.get("Позиции по КТРУ, ОКПД2")},
                "time": 1,
                "imageURLs": [
                ]
            }
            collect.append(zed)
    return collect


def worker(org_num, number):
    org_id = org_num
    org_num = int(f"{org_num}000000")
    link = URL_TO_ITEMS.format(org_num + number)
    try:
        sources = reformat_data(parser(link))
        logger(sources)
        for source in sources:
            existence = is_exists(es, source["sourceID"])
            logger(f"existence - {existence} - {source['sourceID']}")
            if not existence:
                source["providerName"] = "provider-zakupki"
                # resp = requests.post("http://localhost:80/api/v1/products", headers={"Accept": "application/json",
                #                                                                        "X-API-Key": "9aH5Xnly55"
                #                                                                                     "PAFnMX3bVKNwHxkV7j60MSWPsDy"
                #                                                                                     "We7ZHsYx8bZhwu/oshdXibUFbbK",
                #                                                                        "Content-Type": "application/json"},
                #                      json=[source])
                logger(insert_product(es, source))

        logger(update_org(es, org_num, number, org_id))

        return True
    except Exception as e:
        logger(f"error: {e}, url: {link}")
        return False


data = match_all_orgs(es)

sid = data["_scroll_id"]
scroll_size = data["hits"]["total"]["value"]
all_properties = scroll_size
logger(f"All data {scroll_size}")
orgs_all = data["hits"]["hits"]
orgs_all_size = len(orgs_all)
for ind, org in enumerate(orgs_all):
    org = org["_source"]
    last_value = get_last_value(org)
    count = 0
    for i in range(last_value, 1000):
        status = worker(org["org_id"], i)
        if not status:
            count += 1
        if count > 10:
            break
