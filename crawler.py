import pandas as pd
from selenium import webdriver
import sqlite3

driver = webdriver.Chrome("chromedriver.exe")


class CrawlData:

    def get_quarter_data(self):
        pass

    def get_year_data(self):
        pass

    @staticmethod
    def open_browser():
        pass


class CrawlFinanceStatement(CrawlData):
    def __init__(self, code):
        url = "https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd=%s&amp;target=finsum_more" % code
        table_list = self.open_browser(url)

        self.finance_statement = table_list[12]
        self.price_info = table_list[1]
        self.fundamental = table_list[5]

    def get_quarter_data(self):
        # return type : Pandas Dataframe
        y_statement = self.finance_statement['분기분기컨센서스보기']
        y_statement.index = self.finance_statement[self.finance_statement.columns[0]]

        return y_statement

    def get_year_data(self):
        # return type : Pandas Dataframe
        q_statement = self.finance_statement['연간연간컨센서스보기']
        q_statement.index = self.finance_statement[self.finance_statement.columns[0]]

        return q_statement

    def get_price_info(self):
        return self.price_info

    def get_fundamental(self):
        return self.fundamental

    @staticmethod
    def open_browser(url):
        driver.get(url)
        source = driver.page_source
        table_list = pd.read_html(source)

        return table_list


class CrawlFinanceRatio(CrawlData):
    def __init__(self, code):
        url = "http://comp.fnguide.com/SVO2/asp/" \
              "SVD_FinanceRatio.asp?pGB=1&gicode=A%s&cID=30&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701" % code
        self.table_list = self.open_browser(url)

    def get_year_data(self):
        data = self.table_list[0]
        data.index = data[data.columns[0]]
        data.drop(data.columns[0], axis='columns', inplace=True)
        return data

    def get_quarter_data(self):
        data = self.table_list[1]
        data.index = data[data.columns[0]]
        data.drop(data.columns[0], axis='columns', inplace=True)
        return data

    @staticmethod
    def open_browser(url):
        driver.get(url)

        for index in range(1, 24):
            driver.find_element_by_id('grid1_%d' % index).click()
        for index in range(1, 8):
            driver.find_element_by_id('grid2_%d' % index).click()

        source = driver.page_source
        table_list = pd.read_html(source)

        return table_list


def get_code_list(market):
    f = open('data/%s_code_list.txt' % market)
    return f.readline().split()


def crawl(target, code_list):
    if target == 'statement':
        con_db = sqlite3.connect('data/shorted_finance_statement.db')
        con_Qdb = sqlite3.connect('data/quarter_finance_statement.db')
        con_Fdb = sqlite3.connect('data/fundamental.db')
        con_Pdb = sqlite3.connect('data/price_info.db')
    elif target == 'ratio':
        con_db = sqlite3.connect('data/finance_ratio.db')
        con_Qdb = sqlite3.connect('data/quarter_finance_ratio.db')
    else:
        return False

    for i, v in enumerate(code_list):
        # noinspection PyBroadException
        try:
            if target == 'statement':
                crawler = CrawlFinanceStatement(v)
            else:
                crawler = CrawlFinanceRatio(v)
            data = crawler.get_year_data()
            Qdata = crawler.get_quarter_data()

            if target == 'statement':
                Pdata = crawler.get_price_info()
                Fdata = crawler.get_fundamental()
        except:
            print(i, '/', code_list_length, ' failed')
            continue

        data.to_sql(v, con_db, if_exists='replace')
        Qdata.to_sql(v, con_Qdb, if_exists='replace')
        if target == 'statement':
            Pdata.to_sql(v, con_Pdb, if_exists='replace')
            Fdata.to_sql(v, con_Fdb, if_exists='replace')

        print(i, '/', code_list_length, ' completed')

    return True


if __name__ == "__main__":

    market = input('시장 구분을 영어로 입력해주세요 : ')
    code_list = get_code_list(market)
    code_list_length = len(code_list)

    # crawl statement
    crawl('statement', code_list)
    print('saving statement data done')

    # crawl ratio
    crawl('ratio', code_list)
    print('done')

    driver.close()
