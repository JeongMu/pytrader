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

    def get_quarter_data(self):
        y_statement = self.finance_statement['분기분기컨센서스보기']
        y_statement.index = self.finance_statement[self.finance_statement.columns[0]]

        return y_statement

    def get_year_data(self):
        q_statement = self.finance_statement['연간연간컨센서스보기']
        q_statement.index = self.finance_statement[self.finance_statement.columns[0]]

        return q_statement

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


def get_code_list():
    f = open('data/kospi_code_list.txt', 'r', encoding="UTF-8")
    return f.readline().split()


if __name__ == "__main__":
    kospi_code_list = get_code_list()
    code_list_length = len(kospi_code_list)

    # crawl statement
    con_statementDB = sqlite3.connect('data/shorted_finance_statement.db')
    con_QstatementDB = sqlite3.connect('data/quarter_finance_statement.db')
    for i, v in enumerate(kospi_code_list):
        # noinspection PyBroadException
        try:
            crawler = CrawlFinanceStatement(v)
            data = crawler.get_year_data()
            Qdata = crawler.get_quarter_data()
        except:
            print(i, '/', code_list_length, ' failed saving statement')
            continue

        data.to_sql(v, con_statementDB, if_exists='replace')
        Qdata.to_sql(v, con_QstatementDB, if_exists='replace')

        print(i, '/', code_list_length, ' completed saving statement')

    print('done')

    # crawl ratio
    con_ratioDB = sqlite3.connect('data/finance_ratio.db')
    con_QratioDB = sqlite3.connect('data/quarter_finance_ratio.db')
    for i, v in enumerate(kospi_code_list):
        # noinspection PyBroadException
        try:
            crawler = CrawlFinanceRatio(v)
            data = crawler.get_year_data()
            Qdata = crawler.get_quarter_data()
        except:
            print(i, '/', code_list_length, ' failed saving ratio')
            continue

        data.to_sql(v, con_ratioDB, if_exists='replace')
        Qdata.to_sql(v, con_QratioDB, if_exists='replace')

        print(i, '/', code_list_length, ' completed saving ratio')

    print('done')
