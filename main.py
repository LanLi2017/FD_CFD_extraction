import csv
import re
import timeit
from pprint import pprint

from ctane_cp import main as ctane
from tane_cp import main as tane


def convert_tsv2csv(old_ds, new_ds):
    with open(old_ds, 'r')as tsvfile:
        with open(new_ds, 'w')as csvfile:
            for line in tsvfile:
                filecontent = re.sub("\t", ",", line)
                csvfile.write(filecontent)


def main():
    old_ds = 'data_preparation/data/restaurants.tsv'
    new_ds = 'testdata/restaurants.csv'
    # convert_tsv2csv(old_ds, new_ds)

    k = 20
    start = timeit.default_timer()
    list_of_cfds_old = ctane('testdata/restaurants_small.csv', k)  # TANE with messy testdata
    stop = timeit.default_timer()
    print('Before the data preparation:')
    print(f'CTANE running time: {stop - start}')
    print(f'The number of conditional functional dependency: {len(list_of_cfds_old)}')
    pprint(list_of_cfds_old)

    list_fd = tane('testdata/restaurants_small.csv')
    pprint(list_fd)

    # list_of_cfds_gt = ctane(clean_data_path, k)


if __name__ == '__main__':
    main()
