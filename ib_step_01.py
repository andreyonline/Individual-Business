from __future__ import division

# PHASE A #################################################################################################

__author__ = 'Collegium'

# PHASE A #################################################################################################

import pandas as pd
import sys
import argparse


def create_parser():
    parser = argparse.ArgumentParser(
        description='Parameters for IB script'
    )
    parser.add_argument(
        '--source',
        help='File to use. Must be CSV file - comma separated.',
    )

    parser.add_argument(
        '--breg',
        help='File to use. Must be CSV file - comma separated.',
    )

    parser.add_argument(
        '--ireg',
        help='File to use. Must be CSV file - comma separated.',
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Option is executing the main script.'
    )

    return parser


def main():
    parser = create_parser()
    options = parser.parse_args()

    if not options.source or not options.breg or not options.ireg:
        print "You must specify a data sources (--source and --breg and --ireg)."

        parser.print_help()
        sys.exit(1)

    print options

    parser = create_parser()
    options = parser.parse_args()
    dataset = pd.read_csv(options.source, header=1, sep=',')
    dataset.columns = ['zip', 'town', 'street', 'number', 'house', 'flat', 'name', 'id']

    # PHASE A #################################################################################################

    if options.all:
        # total_orig = dataset['id'].count()
        # print total_orig

        # drop duplicates in NAME
        dataset = dataset.drop_duplicates(subset=['name', 'street', 'id'], take_last=True)

        # drop null for address and company field
        # dataset = dataset[dataset.street.notnull()]
        dataset = dataset[dataset.name.notnull()]
        dataset = dataset.reset_index(drop=True)

        # resort
        dataset = dataset.sort(['name'], ascending=[1])
        dataset = dataset.reset_index(drop=True)

        # reseting to upper case + column selection
        dataset = dataset[['name', 'street', 'number', 'town']]
        name = dataset.name.str.upper()
        street = dataset.street.str.upper()
        town = dataset.town.str.upper()
        number = dataset.number
        result = pd.concat([name, street, number, town], axis=1)

        # extract name and address composite pattern
        result_cmp = pd.DataFrame()
        result_cmp["name"] = result['name']
        result_cmp["address"] = result['street'].map(str) + ' ' + result['number'] + ' ' + result['town']

        result_ds = result_cmp
        # print (result_ds)

    # PHASE 2 #################################################################################################

        # company register
        ts_data = pd.read_csv(options.breg, header=1, sep=',')
        ts_data.columns = ['zip', 'town', 'street', 'number', 'house', 'flat', 'name', 'id']

        ts_companies = ts_data[pd.notnull(ts_data['name'])]
        companies_sel = ts_companies['name'].str.upper()
        companies_sel = pd.DataFrame(companies_sel)

        # definite companies set (based on Ltd / plc) - classification also added
        companies_sel = companies_sel[companies_sel['name'].str.contains("LTD| PLC", na=False)]
        companies_sel['classification'] = 2
        companies_sel = companies_sel.reset_index(drop=True)

        # individuals register. Sample individuals set, randomly generated - classification also added
        individuals_set = pd.read_csv(options.ireg, header=0, sep=',')
        individuals_set_sel = individuals_set['name'].str.upper()
        individuals_set_sel = pd.DataFrame(individuals_set_sel)
        individuals_set_sel['classification'] = -2

        # concatenating commercial and individual sets
        frames = individuals_set_sel, companies_sel
        t_set = pd.concat(frames)
        t_set = t_set.reset_index(drop=True)

        # exact match. Populates 'classification' where match (currently only '2' as no individuals' names in dataset
        matched = result_ds.merge(t_set, how='left', on='name')
        matched = matched.reset_index(drop=True)
        # print matched.head

    # PHASE 3 #################################################################################################

        # distance methods

    # PHASE 4 #################################################################################################

        # phonetic methods

    # PHASE 5 #################################################################################################

        # split - only for name
        patt_nm_split = pd.DataFrame(matched.name.str.split().tolist()).ix[0:]
        df_nm_size = len(patt_nm_split.columns)

        # insert classification columns
        vals = []
        for x in range(0L, df_nm_size*2):
            if x % 2 == 1:
                vals.append(x)
        patt_nm_split.columns = vals

        for x in range(1L, df_nm_size*2):
            if x+2 <= df_nm_size*2:
                if x % 2 == 0 and x >= 1:
                    patt_nm_split[x] = None
                patt_nm_split[df_nm_size*2] = None
        patt_nm_split.sort_index(axis=1, inplace=True)

        result_ds = pd.DataFrame(patt_nm_split)
        result_ds["address"] = result_cmp["address"]
        result_ds["classification"] = matched["classification"]

        #print result_ds.head(10)

        for index, row in result_ds.iterrows():
            if row['classification'] != None:
                result_ds.set_value(index,2,row['classification'])

        print result_ds.head(10)

        # total with not null values
        # total_rows1 = dataset['name'].count()
        # total_rows2 = ts_companies['name'].count()
        # total_rows3 = companies_sel['name'].count()
        # total_rows4 = individuals_set['name'].count()
        # total_rows5 = result_ds[1].count()
        # print (total_rows1,total_rows2,total_rows3,total_rows4,total_rows5)

        # training set done

if __name__ == "__main__":
    main()