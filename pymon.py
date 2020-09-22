import sys
from PyQt5.QtWidgets import *
import pandas as pd
import sqlite3
import score_checker


class PyMon:
    def __init__(self):
        self.kospi_codes = None
        self.kosdaq_codes = None

        self.con_statementDB = sqlite3.connect('data/shorted_finance_statement.db')
        self.con_ratioDB = sqlite3.connect('data/finance_ratio.db')

        self.score_data = pd.read_excel('data/score.xlsx')
        self.buy_list_dataset = None
        self.buy_list = []
        self.sell_list = []

        f = open('data/portfolio.txt', 'r', encoding="UTF-8")
        self.portfolio = f.readline().split()

        self.set_buy_list_by_svm()
        self.set_sell_list_by_svm()

    def check_f_score(self, score_data, target_score):
        for i in score_data.loc:
            return None

    def set_buy_list_by_svm(self):
        dataset = self.get_clean_data()

        get_total_score = lambda data_list: data_list[0] - data_list[1] - data_list[2] - data_list[3]

        col_total_score = list(
            map(get_total_score, zip(*[dataset['GP/A'], dataset['PBR'], dataset['PER'], dataset['PSR']])))

        dataset['total_score'] = col_total_score
        dataset = dataset.sort_values(by=['total_score'], axis=0, ascending=False)

        self.buy_list_dataset = dataset[:50]

        fixed_buy_list = self.buy_list_dataset[self.buy_list_dataset['1년 수익률'] > 0]

        if len(fixed_buy_list) / len(self.buy_list_dataset) <= 0.25:
            temp_dict = {'code': [], '시장구분': [], 'f-score': [], '시가총액': [], 'GP/A': [], 'PBR': [], 'PER': [],
                         '현재가': [], 'PSR': [], '1년 수익률': []}

            fixed_buy_list = pd.DataFrame(temp_dict)

        buy_list = list(fixed_buy_list['code'])

        for code in buy_list:
            if code in self.portfolio:
                continue
            else:
                self.portfolio.append(code)
                self.buy_list.append(code)

    def set_sell_list_by_svm(self):
        for code in self.portfolio:
            if not(code in self.buy_list):
                self.portfolio.remove(code)
                self.sell_list.append(code)

    # 매수 리스트 업데이트
    def update_buy_list(self):
        f = open("data/buy_list.txt", "wt", encoding="UTF-8")
        for code in self.buy_list:
            f.writelines("매수;" + code + ";시장가;10;0;매수전\n")
        f.close()

    def update_sell_list(self):
        f = open("data/sell_list.txt", "wt", encoding="UTF-8")
        for code in self.sell_list:
            f.writelines("매도;" + code + ";시장가;10;0;매도전\n")
        f.close()

    def update_portfolio(self):
        f = open("data/portfolio.txt", "wt", encoding="UTF-8")
        for code in self.portfolio:
            f.write(code + ' ')
        f.close()

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

    def run(self):
        checker = score_checker.Checker()
        checker.run()

        self.update_buy_list()
        self.update_sell_list()
        self.update_portfolio()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    pymon = PyMon()
    pymon.run()
