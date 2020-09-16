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
        self.kiwoom = Kiwoom.Kiwoom()
        self.kiwoom.comm_connect()
        self.get_code_list()

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

    # 매수 리스트 업데이트
    def update_buy_list(self, buy_list):
        f = open("data/buy_list.txt", "wt", encoding="UTF-8")
        for code in buy_list:
            f.writelines("매수;" + code + ";시장가;10;0;매수전\n")
        f.close()

    def check_f_score(self, code):

        ys_data = pd.read_sql('SELECT * FROM "%s"' % code, self.con_statementDB)
        ys_data.set_index(ys_data.columns[0], drop=True, inplace=True)

        yr_data = pd.read_sql('SELECT * FROM "%s"' % code, self.con_ratioDB)
        yr_data.set_index(yr_data.columns[0], drop=True, inplace=True)

        f_score = 0

        if ys_data[ys_data.columns[2]]['당기순이익'] > 0:
            f_score += 1
        if ys_data[ys_data.columns[2]]['영업활동현금흐름'] > 0:
            f_score += 1
        if ys_data[ys_data.columns[2]]['ROA(%)'] > ys_data[ys_data.columns[1]]['ROA(%)']:
            f_score += 1
        if ys_data[ys_data.columns[2]]['영업활동현금흐름'] > ys_data[ys_data.columns[2]]['당기순이익']:
            f_score += 1
        if ys_data[ys_data.columns[2]]['부채비율'] < ys_data[ys_data.columns[1]]['부채비율']:
            f_score += 1
        if yr_data[yr_data.columns[3]]['유동비율계산에 참여한 계정 감추기'] > yr_data[yr_data.columns[2]]['유동비율계산에 참여한 계정 감추기']:
            f_score += 1
        if ys_data[ys_data.columns[2]]['발행주식수(보통주)'] <= ys_data[ys_data.columns[1]]['발행주식수(보통주)']:
            f_score += 1
        if yr_data[yr_data.columns[3]]['총자산회전율계산에 참여한 계정 감추기'] > yr_data[yr_data.columns[2]]['총자산회전율계산에 참여한 계정 감추기']:
            f_score += 1

        return f_score

    def run(self, f_target):
        buy_list = []
        print(self.kospi_codes)

        for i, code in enumerate(self.kospi_codes):

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
                buy_list.append(code)

        self.update_buy_list(buy_list)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    pymon = PyMon()
    pymon.run(8)
