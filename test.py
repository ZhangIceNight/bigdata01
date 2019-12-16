#
import requests
from bs4 import BeautifulSoup
import csv
def get_num(string):
    ret_list = []
    secret_font = {'38006':6,'38287':8,'39228':3,'39499':4,'40506':7,'40611':5,'40804':9,'40850':0,'40868':2,'40869':1}
    for char in string:
        decode_num = ord(char)
        num = secret_font[str(decode_num)]
        #num = int(num[-2:])-1
        ret_list.append(num)
    return ret_list

if __name__=='__main__':
    page_url = "http://hrb.58.com/nangang/chuzu/pn2"
    web_data = requests.get(page_url).content.decode('utf8')
    soup = BeautifulSoup(web_data, 'html.parser')
    for tag in soup.find_all(attrs="money"):
       # for a in tag:
       #     print(a.string)
       # price = []
       # price.append(tag.b.string)
       # print(tag.b.string[0])
       # print("\n")
        count = 0
        for b in tag:
            count += 1
            if count == 2:
                print("".join('%s' %id for id in get_num(b.string)))
