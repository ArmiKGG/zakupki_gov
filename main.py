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

es = connect_elasticsearch()
naming = "Наименование объекта закупки и его характеристики"


logging.basicConfig(handlers=[logging.FileHandler(filename="log_records.log",
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                    datefmt="%F %A %T",
                    level=logging.INFO)
columns = {
    "Заказчик",
    "Контракт",
    "Заключение контракта",
    "Срок исполнения",
    "Размещен контракт в реестре контрактов",
    "Обновлен контракт в реестре контрактов"
}


pre_url = "https://zakupki.gov.ru/epz/contract/contractCard/payment-info-and-target-of-order.html?reestrNumber={" \
          "}#contractSubjects "


def text_fixer(text):
    return text.replace("\xa0", " ")


def logger(message):
    print(message)
    logging.info(message)


def reformat_data(source):
    collect = []
    for num, j in enumerate(source["Объекты закупки подробнее"]):
        print(j["Тип объекта закупки"])
        if j["Тип объекта закупки"]:
            nameing = j["Наименование объекта закупки и его характеристики"][3:]
            if "страна происхождения" in nameing.lower():
                name2 = nameing.lower().split("страна происхождения")[0]
                desc = "Страна происхождения " + nameing.lower().split("страна происхождения")[1]
            else:
                name2 = nameing
                desc = ""


            try:
                time_data = int(datetime.datetime.strptime(str(source.get("Обновлен контракт в реестре контрактов")), '%d.%m.%Y').timestamp())
            except:
                time_data = 0

            units = j["Количество товара, объем работы, услуги,Единица измерения"]
            remove_digits = str.maketrans('', '', digits)
            res = units.translate(remove_digits)
            zed = {
                "sourceID": f'{source["reestrNumber"]}00{num+1}',
                "providerID": f'{source["reestrNumber"]}',
                "providerName": source.get("Заказчик"),
                "price": float(j["Цена за единицу измерения, ₽"].replace(",", ".").replace(" ", "")),
                "name": name2.strip(),
                "description": desc.strip(),
                "unit": res.strip(),
                "url": source["url"],
                "currency": "RUB",
                "properties":{"contract": source.get("Контракт"),
                              "KTRU": j.get("Позиции по КТРУ, ОКПД2")},
                "time": time_data,
                "imageURLs": [
                ]
            }
            collect.append(zed)

    return collect


def worker(org_num, number):
    org_id = org_num
    org_num = int(f"{org_num}000000")
    link = pre_url.format(org_num + number)
    try:
        sources = reformat_data(parser(link))
        logger(sources)
        for source in sources:
            existence = is_exists(es, source["sourceID"])
            logger(f"existence - {existence} - {source['sourceID']}")
            if not existence:
                source["providerName"] = "provider-zakupki"
                print("----------")
                print(source)
                print("----------------")
                resp = requests.post("http://localhost:8080/api/v1/products", headers={"Accept": "application/json", "X-API-Key": "9aH5Xnly55PAFnMX3bVKNwHxkV7j60MSWPsDyWe7ZHsYx8bZhwu/oshdXibUFbbK", "Content-Type": "application/json"},
                                     json=[source])
                logger(resp.json())

        logger(update_org(es, org_num, number, org_id))

        return True
    except Exception as e:
        logger(f"error: {e}, url: {link}")
        return False


def parser(url: AnyStr):
    parsed_url = urlparse(url)
    captured_value = parse_qs(parsed_url.query)["reestrNumber"][0]
    r = requests.get(url, headers=Headers(headers=True).generate(), timeout=15)
    soup = BeautifulSoup(r.content, "lxml")
    main_info = {}
    for match in columns:
        data = soup.find(lambda tag: tag.naming == "span" and match in tag.text)
        if data:
            data = data.find_next("span")
            data = data.text.strip()
            data = " ".join(data.split())
            main_info[match] = data
    try:
        main_info["Объекты закупки"] = " ".join(soup.find("span", class_="text-break").text.strip().split())
    except Exception as e:
        logger(f"Object Zakupki not found {e}")
    clean_list = []
    if data_table:
        for i, d in enumerate(data_table):
            first_col = list(d.keys())[0]
            if first_col != naming:
                del d[first_col]
            if type(d[naming]) is str:
                clean_list.append(dict((k, text_fixer(str(v))) for k, v in d.items()))

    main_info.update({"Объекты закупки подробнее": clean_list, "reestrNumber": int(captured_value), "url": url})
    logger(main_info)
    return main_info


def get_last_value(org):
    if not org.get("statistics"):
        return 1
    return list(org.get("statistics").values())[0]
