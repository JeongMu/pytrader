import pandas as pd
import sqlite3


class Checker:
    def __init__(self):
        self.kospi_codes = self.get_code_list('kospi')
        self.kosdaq_codes = self.get_code_list('kosdaq')

        self.con_statementDB = sqlite3.connect('data/shorted_finance_statement.db')
        self.con_ratioDB = sqlite3.connect('data/finance_ratio.db')
        self.con_priceDB = sqlite3.connect('data/price_info.db')
        self.con_fundamentalDB = sqlite3.connect('data/fundamental.db')

    @staticmethod
    def get_code_list(market):
        f = open('data/%s_code_list.txt' % market)
        return f.readline().split()

    def get_f_score(self, Sdata, Rdata):
        f_score = 0
        if Sdata[Sdata.columns[3]][4] > 0:
            f_score += 1
        if Sdata[Sdata.columns[3]][13] > 0:
            f_score += 1
        if Sdata[Sdata.columns[3]][22] > Sdata[Sdata.columns[2]][22]:
            f_score += 1
        if Sdata[Sdata.columns[3]][13] > Sdata[Sdata.columns[3]][4]:
            f_score += 1
        if Sdata[Sdata.columns[3]][23] < Sdata[Sdata.columns[2]][23]:
            f_score += 1
        if Rdata[Rdata.columns[2]][1] > Rdata[Rdata.columns[3]][1]:
            f_score += 1
        if Sdata[Sdata.columns[3]][32] <= Sdata[Sdata.columns[2]][32]:
            f_score += 1
        if Rdata[Rdata.columns[2]][61] > Rdata[Rdata.columns[3]][61]:
            f_score += 1

        return f_score

    def get_market_cap(self, Pdata):
        return Pdata['1'][4].replace('억원', '').replace(',', '')

    def get_gpa(self, Rdata):
        gross_margin = float(Rdata[Rdata.columns[4]][40])
        total_assets = float(Rdata[Rdata.columns[4]][21])

        return gross_margin / total_assets

    def get_pbr(self, Fdata):
        return float(Fdata[Fdata.columns[1]][1])

    def get_per(self, Fdata):
        per = Fdata[Fdata.columns[1]][0]

        if per is None:
            return None
        else:
            return float(per)

    def get_current_price(self, Pdata):
        return Pdata[Pdata.columns[1]][0].split()[0].replace('원', '').replace(',', '')

    def get_psr(self, market_cap, Sdata):
        return int(market_cap) / int(Sdata[Sdata.columns[3]][0])

    def get_data(self, target, *args):
        # noinspection PyBroadException
        try:
            if target == 'f-score':
                return self.get_f_score(args[0], args[1])  # Sdata, Rdata
            elif target == 'market_cap':
                return self.get_market_cap(args[0])  # Pdata
            elif target == 'gpa':
                return self.get_gpa(args[0])  # Rdata
            elif target == 'pbr':
                return self.get_pbr(args[0])  # Fdata
            elif target == 'per':
                return self.get_per(args[0])  # Fdata
            elif target == 'current_price':
                return self.get_current_price(args[0])  # Pdata
            elif target == 'psr':
                return self.get_psr(args[0], args[1])  # market_cap, Sdata
            else:
                return None
        except:
            return None

    def run(self):
        print(self.kospi_codes)

        temp_dict = {'code': [], '시장구분': [], 'f-score': [], '시가총액': [], 'GP/A': [], 'PBR': [], 'PER': [],
                     '현재가': [], 'PSR': []}
        df = pd.DataFrame(temp_dict)

        index = 0
        for i, code in enumerate(self.kospi_codes):
            # noinspection PyBroadException
            try:
                Pdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_priceDB)
                Pdata = Pdata.drop(Pdata.columns[0], axis=1)

                Fdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_fundamentalDB)
                Fdata = Fdata.drop(Fdata.columns[0], axis=1)

                Sdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_statementDB)
                Rdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_ratioDB)
            except:
                continue

            f_score = self.get_data('f-score', Sdata, Rdata)
            market_cap = self.get_data('market_cap', Pdata)
            gpa = self.get_data('gpa', Rdata)
            pbr = self.get_data('pbr', Fdata)
            per = self.get_data('per', Fdata)
            current_price = self.get_data('current_price', Pdata)
            psr = self.get_data('psr', market_cap, Sdata)

            temp = [code, 'kospi', f_score, market_cap, gpa, pbr, per, current_price, psr]
            df.loc[i] = temp
            index += 1

            print(i, '/', len(self.kospi_codes), '\tmarket : kospi', '\tf_score : ', f_score,
                  '\tmarket_cap : ', market_cap, '\tgpa : ', gpa, '\tpbr : ', pbr, '\tper : ', per,
                  '\tcurrent_price : ', current_price, '\tpsr: ', psr)

        print(len(df))

        for i, code in enumerate(self.kosdaq_codes):
            # noinspection PyBroadException
            try:
                Pdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_priceDB)
                Pdata = Pdata.drop(Pdata.columns[0], axis=1)

                Fdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_fundamentalDB)
                Fdata = Fdata.drop(Fdata.columns[0], axis=1)

                Sdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_statementDB)
                Rdata = pd.read_sql('SELECT * FROM "%s"' % code, self.con_ratioDB)
            except:
                continue

            f_score = self.get_data('f-score', Sdata, Rdata)
            market_cap = self.get_data('market_cap', Pdata)
            gpa = self.get_data('gpa', Rdata)
            pbr = self.get_data('pbr', Fdata)
            per = self.get_data('per', Fdata)
            current_price = self.get_data('current_price', Pdata)
            psr = self.get_data('psr', market_cap, Sdata)

            temp = [code, 'kosdaq', f_score, market_cap, gpa, pbr, per, current_price, psr]
            df.loc[i + index] = temp
            index += 1

            print(i, '/', len(self.kosdaq_codes), '\tmarket : kosdaq', '\tf_score : ', f_score,
                  '\tmarket_cap : ', market_cap, '\tgpa : ', gpa, '\tpbr : ', pbr, '\tper : ', per,
                  '\tcurrent_price : ', current_price, '\tpsr : ', psr)

        print(df)
        print(len(df))
        df.to_excel('data/score.xlsx', index=False)


if __name__ == "__main__":
    checker = Checker()
    checker.run()
