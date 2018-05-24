import datetime
import random

import luigi
import os
from luigi import LocalTarget
import pandas as pd
import numpy as np


class ExternalCheckOutput(luigi.ExternalTask):
    test_file = luigi.Parameter()

    def output(self):
        return LocalTarget(self.test_file)


class DatasetToLower(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_1.csv"

    def requires(self):
        return ExternalCheckOutput(test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'atm_address', 'atm_address_lat', 'atm_address_lon', 'city', 'country', 'currency',
             'customer_id', 'mcc', 'pos_address', 'pos_address_lat', 'pos_address_lon',
             'terminal_id', 'transaction_date'],
            [np.float32, np.str, np.float32, np.float32, np.str, np.str, np.str, np.str,
             np.str, np.str,
             np.float32, np.float32, np.str, np.str])))
        data = pd.read_csv(self.test_file, sep=',',
                           dtype=dt)
        len0 = data.shape[0]
        data['mcc'] = data['mcc'].fillna('0')
        data['mcc'] = data['mcc'].str.replace(',', '')
        data['amount'] = data['amount'].fillna(0.0)
        data['atm_address'] = data['atm_address'].fillna('unknown')
        data['atm_address_lat'] = data['atm_address_lat'].fillna(0.0)
        data['atm_address_lon'] = data['atm_address_lon'].fillna(0.0)
        data['city'] = data['city'].fillna('unknown')
        data['country'] = data['country'].fillna('unknown')
        data['currency'] = data['currency'].fillna(0.0)
        data['customer_id'] = data['customer_id'].fillna('unknown')
        data['pos_address'] = data['pos_address'].fillna('unknown')
        data['pos_address_lat'] = data['pos_address_lat'].fillna(0.0)
        data['pos_address_lon'] = data['pos_address_lon'].fillna(0.0)
        data['terminal_id'] = data['terminal_id'].fillna('unknown')
        data['transaction_date'] = data['transaction_date'].fillna('unknown')
        for key in data:
            if key != 'transaction_date':
                try:
                    data[key] = data[key].str.lower()
                    data[key] = data[key].str.strip()
                    data[key] = data[key].str.replace('.', '')
                    data[key] = data[key].str.replace('-', '')
                    if (key != 'pos_address') and (key != 'atm_address'):
                        data[key] = data[key].str.replace(' ', '')
                    data[key] = data[key].str.replace(',', '')
                    data[key] = data[key].str.replace('\'', '')
                    data[key] = data[key].str.replace('>', '')
                    data[key] = data[key].str.replace('<', '')
                    data[key] = data[key].str.replace('%', '')
                    data[key] = data[key].str.replace('*', '')
                    data[key] = data[key].str.replace('"', '')
                    data[key] = data[key].str.replace('&', '')
                    data[key] = data[key].str.replace('#', '')
                except AttributeError:
                    pass

        terminal_lat = []
        terminal_lon = []
        terminal_address = []
        terminal_type = []
        formatted_date = []
        for index, row in data.iterrows():
            if (row['transaction_date'] != 'unknown') and (row['transaction_date'] != 'nan'):
                if isinstance(row['transaction_date'], np.str):
                    formatted_date.append(row['transaction_date'][:10])
            else:
                formatted_date.append('1900-01-01')
            typeset = 0
            if (row['atm_address'] != 'unknown') and (row['atm_address'] != 'nan'):
                terminal_address.append(row['atm_address'])
                terminal_type.append('atm')
                typeset = 1
            else:
                if (row['pos_address'] != 'unknown') and (row['pos_address'] != 'nan'):
                    terminal_address.append(row['pos_address'])
                    terminal_type.append('pos')
                    typeset = 1
                else:
                    terminal_address.append('unknown')
                    terminal_type.append('unknown')
                    typeset = 1

            if (row['atm_address_lat'] != 0.0) and (row['atm_address_lon'] != 0.0):
                terminal_lat.append(row['atm_address_lat'])
                terminal_lon.append(row['atm_address_lon'])
                if not typeset:
                    terminal_type.append('atm')
                    typeset = 1
            else:
                if (row['pos_address_lat'] != 0.0) and (row['pos_address_lon'] != 0.0):
                    terminal_lat.append(row['pos_address_lat'])
                    terminal_lon.append(row['pos_address_lon'])
                    if not typeset:
                        terminal_type.append('pos')
                        typeset = 1
                else:
                    terminal_lat.append(0.0)
                    terminal_lon.append(0.0)
                    if not typeset:
                        terminal_type.append('unknown')
                        typeset = 1

        data['terminal_address'] = np.array(terminal_address, dtype=np.str)
        data['terminal_lat'] = np.array(terminal_lat, dtype=np.float32)
        data['terminal_lon'] = np.array(terminal_lon, dtype=np.float32)
        data['terminal_type'] = np.array(terminal_type, dtype=np.str)
        data['transaction_date'] = np.array(formatted_date, dtype=np.str)

        data = data.drop(columns=['atm_address', 'atm_address_lat', 'atm_address_lon', 'pos_address', 'pos_address_lat',
                                  'pos_address_lon'])

        len1 = data.shape[0]
        if len0 != len1:
            print("INITIAL LENGTH DOESN'T EQUAL TO LENGTH AFTER TRANSFORMATIONS")
            return
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetCityFilter(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_2.csv"

    def requires(self):
        return DatasetToLower(directory=self.directory, test_file=self.test_file)

    def output(self):
        print(os.path.join(self.directory, self.target_file))
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str])))
        data = pd.read_csv(os.path.join(self.directory, DatasetToLower.target_file), sep=',',
                           dtype=dt)
        len0 = data.shape[0]
        key = 'city'
        data[key] = data[key].fillna('')
        data[key] = data[key].replace("moskva", "moscow")
        data[key] = data[key].replace("moskv", "moscow")
        data[key] = data[key].replace("mosocw", "moscow")
        data[key] = data[key].replace("moskow", "moscow")
        data[key] = data[key].replace("mockva", "moscow")
        data[key] = data[key].replace("mlskva", "moscow")
        arr = np.unique(data[key])
        arrlen = len(arr)
        print("old number of unique cities {}".format(arrlen))
        mapping = dict()
        for idx in range(arrlen):
            cnt = 0
            if idx + 1 < arrlen:
                l = len(arr[idx])
                lnext = len(arr[idx + 1])
                if (lnext != 0) and (l != 0):
                    while 1:
                        if arr[idx][cnt] != arr[idx + 1][cnt]:
                            break
                        else:
                            cnt += 1
                            if (cnt == l) or (cnt == lnext):
                                break
                    if (cnt / float(l) > 0.7) and (cnt / float(lnext) > 0.4):
                        mapping[arr[idx + 1]] = arr[idx]
                        arr[idx + 1] = arr[idx]

        for idx in range(len(data[key])):
            if data[key][idx] in mapping:
                data[key] = data[key].replace(data[key][idx], mapping[data[key][idx]])

        print("new number of unique cities {}".format(len(np.unique(arr))))
        len1 = data.shape[0]
        if len0 != len1:
            print("INITIAL LENGTH DOESN'T EQUAL TO LENGTH AFTER TRANSFORMATIONS")
            return
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetCountryFilter(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_3.csv"

    def requires(self):
        return DatasetCityFilter(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str])))
        data = pd.read_csv(os.path.join(self.directory, DatasetCityFilter.target_file), sep=',',
                           dtype=dt)
        len0 = data.shape[0]
        key = 'country'
        print("old number of unique countries {}".format(len(np.unique(data[key]))))
        data[key] = data[key].replace('"', "")
        data[key] = data[key].replace("aus", "au")
        data[key] = data[key].replace("aze", "az")
        data[key] = data[key].replace("bgr", "bg")
        data[key] = data[key].replace("che", "ch")
        data[key] = data[key].replace("esp", "es")
        data[key] = data[key].replace("fin", "fi")
        data[key] = data[key].replace("fra", "fr")
        data[key] = data[key].replace("geo", "ge")
        data[key] = data[key].replace("grc", "gr")
        data[key] = data[key].replace("hkg", "hk")
        data[key] = data[key].replace("hrv", "hr")
        data[key] = data[key].replace("ita", "it")
        data[key] = data[key].replace("kgz", "kg")
        data[key] = data[key].replace("ltu", "lt")
        data[key] = data[key].replace("lva", "lv")
        data[key] = data[key].replace("mda", "md")
        data[key] = data[key].replace("mex", "me")
        data[key] = data[key].replace("mys", "my")
        data[key] = data[key].replace("nld", "nl")
        data[key] = data[key].replace("pol", "pl")
        data[key] = data[key].replace("rus", "ru")
        data[key] = data[key].replace("tha", "th")
        data[key] = data[key].replace("usa", "us")
        data[key] = data[key].replace("uzb", "uz")

        print("new number of unique countries {}".format(len(np.unique(data[key]))))
        len1 = data.shape[0]
        if len0 != len1:
            print("INITIAL LENGTH DOESN'T EQUAL TO LENGTH AFTER TRANSFORMATIONS")
            return
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetTerminalAddressFilter(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_4.csv"

    def requires(self):
        return DatasetCountryFilter(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str])))
        data = pd.read_csv(os.path.join(self.directory, DatasetCountryFilter.target_file), sep=',',
                           dtype=dt)
        len0 = data.shape[0]
        key = 'terminal_address'
        data[key] = data[key].fillna('')
        print("old number of unique atm addresses {}".format(len(np.unique(data[key]))))
        data[key] = data[key].replace('moskva', "moscow")
        print("new number of unique atm addresses {}".format(len(np.unique(data[key]))))
        len1 = data.shape[0]
        if len0 != len1:
            print("INITIAL LENGTH DOESN'T EQUAL TO LENGTH AFTER TRANSFORMATIONS")
            return
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetAddDayOfWeekAndIsWeekend(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_5.csv"

    def requires(self):
        return DatasetTerminalAddressFilter(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str])))
        data = pd.read_csv(os.path.join(self.directory, DatasetTerminalAddressFilter.target_file), sep=',',
                           dtype=dt)
        len0 = data.shape[0]
        key = 'transaction_date'
        l = len(data['amount'])
        dow = []
        iw = []
        for idx in range(l):
            dweek = 0
            try:
                dweek = datetime.datetime.strptime(data['transaction_date'][idx], '%Y-%m-%d').weekday()
            except ValueError:
                data['transaction_date'][idx] = '1900-01-01'
                dweek = datetime.datetime.strptime(data['transaction_date'][idx], '%Y-%m-%d').weekday()
            dow.append(dweek)
            if dweek >= 5:
                iw.append(1)
            else:
                iw.append(0)
        data['dayofweek'] = np.array(dow, dtype=np.int)
        data['isweekend'] = np.array(iw, dtype=np.int)
        len1 = data.shape[0]
        if len0 != len1:
            print("INITIAL LENGTH DOESN'T EQUAL TO LENGTH AFTER TRANSFORMATIONS")
            return
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetAddVarMean(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_6.csv"

    def requires(self):
        return DatasetAddDayOfWeekAndIsWeekend(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str, np.int8, np.int8])))
        data = pd.read_csv(os.path.join(self.directory, DatasetAddDayOfWeekAndIsWeekend.target_file), sep=',',
                           dtype=dt)

        len0 = data.shape[0]
        print(len0)

        var = data.groupby([
            'customer_id',
            'terminal_address',
            'terminal_lat',
            'terminal_lon',
            'terminal_type',
            'terminal_id',
            'currency'
        ], as_index=False).amount.var().rename(columns={'amount': 'amount_var'})
        mean = data.groupby([
            'customer_id',
            'terminal_address',
            'terminal_lat',
            'terminal_lon',
            'terminal_type',
            'terminal_id',
            'currency'
        ], as_index=False).amount.mean().rename(columns={'amount': 'amount_mean'})
        data = pd.merge(data, var, on=[
            'customer_id',
            'terminal_address',
            'terminal_lat',
            'terminal_lon',
            'terminal_type',
            'terminal_id',
            'currency'
        ])
        data = pd.merge(data, mean, on=[
            'customer_id',
            'terminal_address',
            'terminal_lat',
            'terminal_lon',
            'terminal_type',
            'terminal_id',
            'currency'
        ])
        data['amount_var'] = data['amount_var'].fillna(0.0)
        flags = []
        for index, row in data.iterrows():
            if row['amount'] <= row['amount_mean'] - 2 * np.sqrt(row['amount_var']):
                flags.append(0)
                continue
            if (row['amount'] > row['amount_mean'] - 2 * np.sqrt(row['amount_var'])) and (
                        row['amount'] <= row['amount_mean'] - np.sqrt(row['amount_var']) / 2):
                flags.append(1)
                continue
            if (row['amount'] > row['amount_mean'] - np.sqrt(row['amount_var']) / 2) and (
                        row['amount'] <= row['amount_mean'] + np.sqrt(row['amount_var']) / 2):
                flags.append(2)
                continue
            if (row['amount'] > row['amount_mean'] + np.sqrt(row['amount_var']) / 2) and (
                        row['amount'] <= row['amount_mean'] + 2 * np.sqrt(row['amount_var'])):
                flags.append(3)
                continue
            if row['amount'] > row['amount_mean'] + 2 * np.sqrt(row['amount_var']):
                flags.append(4)
                continue
            flags.append(-1)

        data['transaction_category'] = np.array(flags)
        len1 = data.shape[0]
        print(len1)
        if len0 != len1:
            print("INITIAL LENGTH DOESN'T EQUAL TO LENGTH AFTER TRANSFORMATIONS")
            # return
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetAddPeriodicityPartOne(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_7_slice_{}.csv"
    N = 200

    def requires(self):
        return DatasetAddVarMean(directory=self.directory, test_file=self.test_file)

    def output(self):
        for i in range(0, self.N):
            yield LocalTarget(os.path.join(self.directory + 'slices/', self.target_file.format(str(i))))

    def run(self):

        def f_slice(group):
            n = random.randint(0, self.N - 1)
            l = slices[n].shape[0]
            print(l)
            for key, row in group.iterrows():
                slices[n].loc[l] = row
                print('processing {}'.format(row))
                print('l equals {}'.format(l))
                l += 1

        parse_dates = ['transaction_date']
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8])))
        data = pd.read_csv(os.path.join(self.directory, DatasetAddVarMean.target_file), sep=',',
                           dtype=dt, parse_dates=parse_dates)

        len0 = data.shape[0]
        print(len0)

        slices = []
        for i in range(self.N):
            slices.append(pd.DataFrame(columns=['amount', 'city', 'country', 'currency',
                                                'customer_id', 'mcc',
                                                'terminal_id', 'transaction_date',
                                                'terminal_address', 'terminal_lat',
                                                'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend',
                                                'amount_var',
                                                'amount_mean', 'transaction_category']))

        data.sort_values([
            'transaction_date'
        ]).groupby([
            'transaction_category',
            'customer_id',
            'terminal_address',
            'terminal_lat',
            'terminal_lon',
            'terminal_type',
            'terminal_id',
            'currency'
        ], as_index=False).apply(f_slice)

        if not os.path.exists(self.directory + 'slices/'):
            os.mkdir(self.directory + 'slices/')
        for i in range(self.N):
            slices[i].to_csv(os.path.join(self.directory + 'slices/', self.target_file.format(str(i))), sep=',',
                             header=True, index=False)


class DatasetAddPeriodicityPartTwo(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_8_slice_{}.csv"
    N = DatasetAddPeriodicityPartOne.N

    def requires(self):
        return DatasetAddPeriodicityPartOne(directory=self.directory, test_file=self.test_file)

    def output(self):
        for i in range(0, self.N):
            yield LocalTarget(os.path.join(self.directory + 'slices/', self.target_file.format(str(i))))

    def run(self):

        def func(group):
            dists = []
            lgroup = len(group)
            print(lgroup)
            for idx in range(lgroup):
                if idx > 0:
                    dists.append(
                        (group.iloc[idx]['transaction_date'] - group.iloc[idx - 1]['transaction_date']).days)
                else:
                    dists.append(-1)

            m = []
            if len(dists) > 1:
                m = np.full(len(dists), np.mean(dists[1:]))
            else:
                m = np.full(len(dists), -1)
            cnt_times = np.full(len(dists), len(dists))
            group['dist_after_last'] = np.array(dists, dtype=np.int16)
            group['mean_dist'] = np.array(m, dtype=np.float32)
            group['place_visited'] = np.array(cnt_times, dtype=np.int16)
            return group

        parse_dates = ['transaction_date']
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8])))

        for i in range(self.N):
            data_slice = pd.read_csv(
                os.path.join(self.directory + 'slices/', DatasetAddPeriodicityPartOne.target_file.format(str(i))),
                sep=',',
                dtype=dt, parse_dates=parse_dates)

            data_slice = data_slice.sort_values([
                'transaction_date'
            ]).groupby([
                'transaction_category',
                'customer_id',
                'terminal_address',
                'terminal_lat',
                'terminal_lon',
                'terminal_type',
                'terminal_id',
                'currency'
            ], as_index=False).apply(func)

            data_slice['mean_dist'] = data_slice['mean_dist'].fillna(0.0)

            if not os.path.exists(self.directory):
                os.mkdir(self.directory)
            data_slice.to_csv(os.path.join(self.directory + 'slices/', self.target_file.format(str(i))), sep=',',
                              header=True, index=False)


# TODO: доделать словарь
class DatasetTransformAddressToPlace(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()

    cities_for_gis = {
        'moscow': (
            'moscow', 'moskva', 'gmoskow', 'sosenki', 'dzerzhinsk', 'khimki', 'gmoscow', 'krasnogorsk', 'voskresensk',
            'malyshevo', 'koteln', 'moskovskaya', 'rasskazovo', 'domodedov', 'mytish', 'yubileyniy', 'mitishchi',
            'korolev',
            'jubilejnyj', 'lotoshino', 'milkovo', 'novoe', 'klin', 'solnechnii', 'lyuberce', 'zhukov', 'sovkhozimeni',
            'koteln', 'belyaninovo', 'chelobitevo', 'mitishi', 'belyaninovo', 'odints', 'vniisok', 'litkarino',
            'yudino',
            'podolsk'),
        'spb': (
            'spb', 'stpete', 'sanktpetebur', 'saintpeterbu', 'peterburg', 'vyiborg', 'vyborg', 'pushkin',
            'sanktpeterbur',
            'kirishi', 'volkhov', 'volhov', 'staraya', 'pavlovsk', 'pavlovka', 'murino', 'gatchina', 'speter',
            'sestroreck',
            'taytsy', 'krasnoeselo', 'agalatovo', 'ivangorod', 'shushari', 'begunitsy'),
        'novosibirsk': ('novosibir', 'nobosibirsk', 'nvsibr', 'novosibirsk'),
        'krasnodar': ('krasnodar', 'defanovka'),
        'novorossiysk': ('novorossiisk', 'temryuk', 'cemdolina', 'yurovka', 'ejsk', 'simferopol'),
        'tula': ('tula', 'tularegion', 'shchekino', 'osinovayagor'),
        'barnaul': ('barnau'),
        'voronezh': ('voroneg'),
        'syktyvkar': ('syktyv'),
        'samara': ('samara'),
        'ufa': ('ufa', 'lenino'),
        'tyumen': ('tyumen', 'gtyumen', 'antipino'),
        'volgograd': ('volgograd'),
        'kazan': ('volzhsk', 'kazan', 'laishevo'),
        'sterlitamak': ('sterlitamak'),
        'saratov': ('sarai', 'saratov'),
        'tambov': ('kotor', 'tambov', 'michurinsk'),
        'petrozavodsk': ('petrozavods', 'petrozavodsk'),
        'tomsk': ('tomsk', 'zonalnayasta'),
        'miass': ('zlatoust', 'miass'),
        'vologda': ('cherepovec', 'vologda', 'gorka', 'kaduy'),
        'chelyabinsk': ('chelyabinsk'),
        'ekaterinburg': ('ekaterinb', 'ekt'),
        'penza': ('peno', 'penzo'),
        'tver': ('tver', 'tverprigorod'),
        'kaliningrad': ('svetogorsk', 'svetlogorsk', 'kaliningrad'),
        'n_novgorod': ('nnovgorod', 'nizhnovgorod'),
        'vladivostok': ('vladvs', 'vladivostok', 'melkovodnyy'),
        'yaroslavl': ('rybinsk', 'yaroslavl'),
        'makhachkala': ('makhachkala'),
        'perm': ('perm'),
        'sochi': ('sochi', 'tuapse'),
        'krasnoyarsk': ('krasnosk', 'krasnoyarsk', 'krasnojarsk'),
        'izhevsk': ('izhevsk'),
        'omsk': ('omsk'),
        'arkhangelsk': ('arkhangelsk', 'velsk')

    }

    def requires(self):
        return DatasetAddPeriodicityPartTwo(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget("test_set_stage_8.csv")

    def run(self):
        pass


class DatasetRemoveNoise(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_10.csv"
    N = DatasetAddPeriodicityPartTwo.N

    def requires(self):
        # return DatasetTransformAddressToPlace(directory=self.directory, test_file=self.test_file)
        return DatasetAddPeriodicityPartTwo(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16])))

        nf = pd.DataFrame(columns=['amount', 'city', 'country', 'currency',
                                   'customer_id', 'mcc',
                                   'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
                                   'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var',
                                   'amount_mean',
                                   'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'])

        slices = []
        for i in range(self.N):
            print("processing {}".format(DatasetAddPeriodicityPartTwo.target_file.format(str(i))))
            slices.append(pd.read_csv(
                os.path.join(self.directory + 'slices/', DatasetAddPeriodicityPartTwo.target_file.format(str(i))),
                sep=',',  # в обычной ситуации тут DatasetTransformAddressToPlace
                dtype=dt))

        nf = pd.concat(slices)
        print('done concat')

        def analyse(group):

            l = group.shape[0]
            print(group['customer_id'])
            print("initial length {}".format(l))
            x = group.groupby(['transaction_category',
                               'terminal_address',
                               'terminal_lat',
                               'terminal_lon',
                               'terminal_type',
                               'terminal_id',
                               'isweekend',
                               'currency']).filter(lambda x: x.shape[0] / float(l) > 0.01)
            if x.shape[0] == 0:
                return group
            else:
                return x

        new = nf.groupby([
            'customer_id',
        ], as_index=False).apply(analyse)
        print(new.head(5))
        print(new.columns)
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        new.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetRemoveCelebrations(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_11.csv"
    celebrations = (
        '2017-01-01',
        '2016-01-01',
        '2017-01-02',
        '2016-01-02',
        '2017-01-03',
        '2016-01-03',
        '2017-01-04',
        '2016-01-04',
        '2017-01-05',
        '2016-01-05',
        '2017-01-06',
        '2016-01-06',
        '2017-01-07',
        '2016-01-07',
        '2017-02-22',
        '2016-02-22',
        '2017-02-23',
        '2016-02-23',
        '2017-03-07',
        '2016-03-07'
        '2017-03-08',
        '2016-03-08',
        '2017-05-01',
        '2016-05-01',
        '2017-05-09',
        '2016-05-09',
        '2017-06-12',
        '2016-06-12',
        '2017-11-04',
        '2016-11-04',
    )

    def requires(self):
        # return DatasetTransformAddressToPlace(directory=self.directory, test_file=self.test_file)
        return DatasetRemoveNoise(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16])))

        data = pd.read_csv(
            os.path.join(self.directory, DatasetRemoveNoise.target_file),
            sep=',',  # в обычной ситуации тут DatasetTransformAddressToPlace
            dtype=dt)

        indexes = []
        for idx, row in data.iterrows():
            if row['transaction_date'] in self.celebrations:
                indexes.append(idx)

        data.drop(data.index[indexes], inplace=True)

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetRemoveUnusedColumns(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_12.csv"

    def requires(self):
        return DatasetRemoveCelebrations(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        parse_dates = ['transaction_date']
        dt = np.dtype(list(zip(
            ['amount', 'city', 'country', 'currency',
             'customer_id', 'mcc',
             'terminal_id', 'transaction_date', 'terminal_address', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'],
            [np.float32, np.str, np.str, np.str, np.str, np.int16, np.str,
             np.str, np.str, np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16])))
        data = pd.read_csv(os.path.join(self.directory, DatasetRemoveCelebrations.target_file), sep=',',
                           dtype=dt, parse_dates=parse_dates)

        len0 = data.shape[0]
        print(len0)

        data = data.drop(columns=['transaction_date',
                                  'terminal_address',
                                  'terminal_id',
                                  'city',
                                  'country'])

        len1 = data.shape[0]
        print(len1)
        if len0 != len1:
            print("INITIAL LENGTH DOESN'T EQUAL TO LENGTH AFTER TRANSFORMATIONS")
        # return
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class ExternalTrainStage12Check(luigi.ExternalTask):
    def output(self):
        return LocalTarget('process/train_set_stage_12.csv')


class DatasetLeaveMccIntersection(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_13.csv"

    def requires(self):
        yield DatasetRemoveUnusedColumns(directory=self.directory, test_file=self.test_file)
        yield ExternalTrainStage12Check()

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'currency',
             'customer_id', 'mcc', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'],
            [np.float32, np.str, np.str, np.int16,
             np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16])))
        test_data = pd.read_csv(os.path.join(self.directory, DatasetRemoveUnusedColumns.target_file), sep=',',
                                dtype=dt)

        dt = np.dtype(list(zip(
            ['amount', 'currency',
             'customer_id', 'home_add_lat', 'home_add_lon', 'mcc', 'work_add_lat', 'work_add_lon', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'],
            [np.float32, np.str, np.str, np.float32, np.float32, np.int16,
             np.float32, np.float32, np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16])))
        train_data = pd.read_csv('process/train_set_stage_12.csv', sep=',',
                                 dtype=dt)

        mccs1 = np.unique(train_data['mcc'])
        mccs2 = np.unique(test_data['mcc'])
        intersection = np.intersect1d(mccs1, mccs2)
        indexes = []
        for idx, row in test_data.iterrows():
            if row['mcc'] not in intersection:
                indexes.append(idx)

        test_data.drop(test_data.index[indexes], inplace=True)

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        test_data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetDropDups(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_14.csv"

    def requires(self):
        return DatasetLeaveMccIntersection(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'currency',
             'customer_id', 'mcc', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'],
            [np.float32, np.str, np.str, np.int16,
             np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16])))
        data = pd.read_csv(os.path.join(self.directory, DatasetLeaveMccIntersection.target_file), sep=',',
                           dtype=dt)

        print(data.shape[0])
        data = data.drop_duplicates()
        print(data.shape[0])

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetLeaveOnlyValuableTransactions(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_15.csv"

    def requires(self):
        return DatasetDropDups(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'currency',
             'customer_id', 'mcc', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited'],
            [np.float32, np.str, np.str, np.int16,
             np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16])))
        data = pd.read_csv(os.path.join(self.directory, DatasetDropDups.target_file), sep=',',
                           dtype=dt)

        print(data.shape[0])

        def compound(group):
            return group.groupby(['transaction_category',
                                  'mcc',
                                  'terminal_lat',
                                  'terminal_lon',
                                  'terminal_type',
                                  'isweekend',
                                  'currency']).apply(lambda x: x.iloc[int(x.shape[0] / 2)])

        data = data.sort_values([
            'place_visited',
            'dist_after_last',
        ], ascending=[False, False]).groupby([
            'customer_id'
        ], as_index=False).apply(compound)

        print(data.shape[0])

        lens = []

        def cnt(group):
            lens.append(len(group))

        data.groupby(['customer_id'], as_index=False).apply(cnt)
        print(lens)
        print(max(lens))

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetOneHotEncodeReconstruct(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_16.csv"
    Numsamples = 5

    def requires(self):
        return DatasetLeaveOnlyValuableTransactions(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'currency',
             'customer_id', 'mcc', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited', 'homelocality', 'worklocality',
             'worklocalitywide', 'homelocalitywide', 'home_cat', 'work_cat'],
            [np.float32, np.str, np.str, np.int16,
             np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16, np.int16, np.int16, np.int16, np.int16, np.int16,
             np.int16])))
        data = pd.read_csv(os.path.join(self.directory, DatasetLeaveOnlyValuableTransactions.target_file), sep=',',
                           dtype=dt)

        dummied = pd.get_dummies(data,
                                 columns=['mcc', 'currency', 'terminal_type', 'dayofweek', 'transaction_category'])
        cols = dummied.columns
        newdt = []
        columnsToDelete = []  # эти колонки будут удалены в конце, потому как они дублируют информацию
        for idx in range(self.Numsamples):
            for elem in cols:
                newcol = '{}_{}'.format(elem, idx)
                newdt.append(newcol)
                if (idx > 0) and (
                            elem in ['customer_id']):
                    columnsToDelete.append(newcol)

        new = pd.DataFrame(columns=newdt)

        def compound(group):
            rowstoinsert = []
            print(group['customer_id'])
            for i in range(self.Numsamples):
                l = group.shape[0]
                idx_to_add = random.randint(0, l - 1)
                elem = group.iloc[idx_to_add]
                elem.rename(lambda x: '{}_{}'.format(x, i), inplace=True)
                rowstoinsert.append(elem)
                if l > self.Numsamples:
                    group.drop(group.index[[idx_to_add]], inplace=True)
            l = new.shape[0]
            new.loc[l] = pd.concat(rowstoinsert, axis=0)

        dummied.groupby(['customer_id'], as_index=False).aggregate(compound)
        print(new.head(5))
        new.drop(columns=columnsToDelete, inplace=True)

        print(new.columns)
        print(len(new.columns))

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        new.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False, )


class DatasetOneHotEncodeOnly(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_16.csv"

    def requires(self):
        return DatasetLeaveOnlyValuableTransactions(directory=self.directory, test_file=self.test_file)

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        dt = np.dtype(list(zip(
            ['amount', 'currency',
             'customer_id', 'mcc', 'terminal_lat',
             'terminal_lon', 'terminal_type', 'dayofweek', 'isweekend', 'amount_var', 'amount_mean',
             'transaction_category', 'dist_after_last', 'mean_dist', 'place_visited', 'homelocality', 'worklocality',
             'worklocalitywide', 'homelocalitywide', 'home_cat', 'work_cat'],
            [np.float32, np.str, np.str, np.int16,
             np.float32, np.float32, np.str, np.int8, np.int8, np.float32,
             np.float32, np.int8, np.int16, np.float32, np.int16, np.int16, np.int16, np.int16, np.int16, np.int16,
             np.int16])))
        data = pd.read_csv(os.path.join(self.directory, DatasetLeaveOnlyValuableTransactions.target_file), sep=',',
                           dtype=dt)
        print(len(np.unique(data['customer_id'])))
        dummied = pd.get_dummies(data,
                                 columns=['mcc', 'currency', 'terminal_type', 'dayofweek', 'transaction_category'])

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        dummied.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class ExternalTrainStage16Check(luigi.ExternalTask):
    def output(self):
        return LocalTarget('process/train_set_stage_16.csv')


class ExternalTrainStage19Check(luigi.ExternalTask):
    def output(self):
        return LocalTarget('process/train_set_stage_19.csv')


class DatasetAddForeignColumns(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()
    target_file = "test_set_stage_17.csv"

    def requires(self):
        yield DatasetOneHotEncodeReconstruct(directory=self.directory, test_file=self.test_file)
        yield ExternalTrainStage19Check()

    def output(self):
        return LocalTarget(os.path.join(self.directory, self.target_file))

    def run(self):
        test_data = pd.read_csv(os.path.join(self.directory, DatasetOneHotEncodeReconstruct.target_file), sep=',')
        train_data = pd.read_csv('process/train_set_stage_19.csv', sep=',')
        train_cols = set(train_data.columns)
        test_cols = set(test_data.columns)

        print(len(train_cols))
        print(len(test_cols))

        for col in train_cols - test_cols:
            if ('home_' not in col) and ('work_' not in col):
                test_data[col] = np.zeros(test_data.shape[0])

        print(len(train_data.columns))
        print(len(test_data.columns))

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        test_data.to_csv(os.path.join(self.directory, self.target_file), sep=',', header=True, index=False)


class DatasetTrainModel(luigi.Task):
    directory = luigi.Parameter()
    test_file = luigi.Parameter()

    def requires(self):
        return DatasetRemoveNoise(directory=self.directory, test_file=self.test_file)

    def output(self):
        pass

    def run(self):
        pass


class GetTestAnswer(luigi.Task):
    directory = luigi.Parameter(default="process/")
    test_file = luigi.Parameter(default="train_set.csv")

    def requires(self):
        return DatasetTrainModel(directory=self.directory, test_file=self.test_file)

    def output(self):
        pass

    def run(self):
        pass
