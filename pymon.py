import sys
from PyQt5.QtWidgets import *
import Kiwoom
import time
from pandas import DataFrame
import pandas as pd
import datetime
import sqlite3

MARKET_KOSPI = 0
MARKET_KOSDAQ = 10


class PyMon:
    def __init__(self):
        """
        self.kiwoom = Kiwoom.Kiwoom()
        self.kiwoom.comm_connect()
        self.get_code_list()
        """

        self.con_statementDB = sqlite3.connect('data/shorted_finance_statement.db')
        self.con_ratioDB = sqlite3.connect('data/finance_ratio.db')

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

    def super_value_momentum(self, dataset):
        # cleaning dataset
        dataset = dataset.sort_values(by=['시가총액'], axis=0)
        dataset = dataset[:round(len(dataset) * 0.2) + 1]
        dataset.dropna(axis=0, inplace=True)


        per_max = dataset['PER'].max()

        print(per_max)



    def normalization(self, data):
        pass

    def run(self, f_target):
        buy_list = []
        # print(self.kospi_codes)

        score_data = pd.read_excel('data/score.xlsx')

        self.super_value_momentum(score_data)

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
