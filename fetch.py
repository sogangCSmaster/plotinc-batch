import requests
import json
from bs4 import BeautifulSoup
from config import host, user, password, db, charset, api_url, source, pushAPI, headers
import pymysql
import time

def save_without_pushNoti(datas):
    for data in datas:
        rcp_no = data['rcp_no']
        title_url = data['title']
        Type = data['type']
        datetime = data['datetime']

        soupTitle = BeautifulSoup(title_url, 'html.parser')
        soupType = BeautifulSoup(Type, 'html.parser')
        title = soupTitle.find('a').get_text()
        Type = soupType.find('span').get_text()
        companyName = title.split(',')[0]
        url = source + soupTitle.find('a', href=True)['href']
        print(title)
        print(companyName)
        try:
            sql = "INSERT INTO analyzed (rcp_no, type, title, datetime, companyName, url) VALUES (%s, %s, %s, %s, %s, %s)"
            curs.execute(sql, (rcp_no, Type, title, datetime, companyName, url))
        except Exception as e:
            print(e)

def save_with_pushNoti(datas):
    for data in datas:
        rcp_no = data['rcp_no']
        title_url = data['title']
        Type = data['type']
        datetime = data['datetime']

        soupTitle = BeautifulSoup(title_url, 'html.parser')
        soupType = BeautifulSoup(Type, 'html.parser')
        title = soupTitle.find('a').get_text()
        Type = soupType.find('span').get_text()
        companyName = title.split(',')[0]
        url = source + soupTitle.find('a', href=True)['href']
        try:
            sql = "INSERT INTO analyzed (rcp_no, type, title, datetime, companyName, url) VALUES (%s, %s, %s, %s, %s, %s)"
            curs.execute(sql, (rcp_no, Type, title, datetime, companyName, url))

            sql = "SELECT crpno FROM CrpNo WHERE Coname=%s"
            curs.execute(sql, (companyName))
            crpno = curs.fetchone()[0]

            sql = "SELECT token FROM push_service WHERE crpno=%s AND pushable=%s"

            curs.execute(sql, (crpno, 'Y'))
            tokens = curs.fetchall()

            pushArray = []
            for token in tokens:
                token = token[0]
                if token:
                    pushArray.append({
                        'to': token,
                        'title': '종목 실적 분석',
                        'sound': 'default',
                        'body': title,
                        'data': {
                            'title': '종목 실적 분석',
                            'body': title,
                            'link': url
                        }
                    })

            response = requests.post(pushAPI, data=json.dumps(pushArray), headers=headers)
            print(response.text)
            time.sleep(0.1)
        except Exception as e:
            print(e)
            break


def main():
    response = requests.get(api_url)
    datas = json.loads(response.text) #GET list
    save_with_pushNoti(datas)
    #save_without_pushNoti(datas)

if __name__ == "__main__":
    conn = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset, autocommit=True)
    curs = conn.cursor()
    main()
