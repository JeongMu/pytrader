import sys
from PyQt5.QtWidgets import *
import Kiwoom
import time
import pandas as pd
import datetime
import sqlite3

MARKET_KOSPI = 0
MARKET_KOSDAQ = 10


class PyMon:
    def __init__(self):
        self.kospi_codes = None
        self.kosdaq_codes = None

        """
        self.kiwoom = Kiwoom.Kiwoom()
        self.kiwoom.comm_connect()
        self.get_code_list()
        """

        self.con_statementDB = sqlite3.connect('data/shorted_finance_statement.db')
        self.con_ratioDB = sqlite3.connect('data/finance_ratio.db')

        self.score_data = pd.read_excel('data/score.xlsx')
        self.buy_list_dataset = None
        self.buy_list = None


    def get_code_list(self):
        self.kospi_codes = self.kiwoom.get_code_list_by_market(MARKET_KOSPI)
        self.kosdaq_codes = self.kiwoom.get_code_list_by_market(MARKET_KOSDAQ)

        f = open('data/kospi_code_list.txt', 'w', encoding="UTF-8")
        for i in self.kospi_codes:
            f.write(i + ' ')
        f.close()

        f = open('data/kosdaq_code_list.txt', 'w', encoding="UTF-8")
        for i in self.kosdaq_codes:
            f.write(i + ' ')
        f.close()

    def check_f_score(self, score_data, target_score):
        for i in score_data.loc:
            return None

    # 매수 리스트 업데이트
    def update_buy_list(self, buy_list):
        f = open("data/buy_list.txt", "wt", encoding="UTF-8")
        for code in buy_list:
            f.writelines("매수;" + code + ";시장가;10;0;매수전\n")
        f.close()

    def get_buy_list_super_value_momentum(self):
        dataset = self.get_clean_data()

        get_total_score = lambda data_list: data_list[0] - data_list[1] - data_list[2] - data_list[3]

        col_total_score = list(
            map(get_total_score, zip(*[dataset['GP/A'], dataset['PBR'], dataset['PER'], dataset['PSR']])))

        dataset['total_score'] = col_total_score
        dataset = dataset.sort_values(by=['total_score'], axis=0, ascending=False)

        print(dataset)

        self.buy_list_dataset = dataset[:50]
        self.buy_list = self.buy_list_dataset['code']

    def get_sell_list_super_value_momentum(self):
        sell_list = self.buy_list_dataset[self.buy_list_dataset['1년 수익률'] < 0]

        if len(sell_list) / len(self.buy_list_dataset) >= 0.6:
            return self.buy_list_dataset
        else:
            return sell_list

    @staticmethod
    def normalization(column):
        col_max = column.max()
        col_min = column.min()
        column = [(x - col_min) / (col_max - col_min) for x in column]

        return column

    def get_clean_data(self):
        dataset = self.score_data.copy()

        # cleaning dataset
        dataset = dataset.sort_values(by=['시가총액'], axis=0)
        dataset = dataset[:round(len(dataset) * 0.2) + 1]  # 시가총액 하위 20%만 추출
        dataset.dropna(axis=0, inplace=True)  # NaN값이 있는 행 제거

        dataset = dataset[dataset['PER'] < 200]
        dataset = dataset[dataset['PBR'] < 5]
        dataset = dataset[dataset['PSR'] < 50]

        dataset['GP/A'] = self.normalization(dataset['GP/A'])
        dataset['PBR'] = self.normalization(dataset['PBR'])
        dataset['PER'] = self.normalization(dataset['PER'])
        dataset['PSR'] = self.normalization(dataset['PSR'])

        return dataset

    def run(self, f_target):
        buy_list = []
        # print(self.kospi_codes)
        self.get_buy_list_super_value_momentum()
        self.get_sell_list_super_value_momentum()

        """for i, code in enumerate(self.kospi_codes):

            f = open('data/result_data.txt', 'w', encoding="UTF-8")
            f.write(str(i) + '\n')

            for j in buy_list:
                f.write(j + ' ')
            f.close()

            # noinspection PyBroadException
            try:
                f_score = self.check_f_score(code)
            except:
                print("exception occured")
                continue

            print(i, '/', '1564 f_score = %d' % f_score)

            if f_score >= f_target:
                buy_list.append(code)"""

        self.update_buy_list(buy_list)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    pymon = PyMon()
    pymon.run(8)
