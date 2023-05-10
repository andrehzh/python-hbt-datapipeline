import pandas as pd

file_path = '/Users/andre/Plan3 Design & Build Dropbox/Andre Heng/Mac/Documents/HBT/Data/AccountStatements_10052023_1759.xls'


def extract_bank_data(file_path):
    bank_df = pd.read_excel(
        file_path, sheet_name='Account Activities_1022', header=10, nrows=280)
    return bank_df


def clean_bank_data(bank_df):
    clean_bank_df = bank_df[['Transaction Date', 'Timestamp',
                             'Description', 'Deposit', 'Withdrawal', 'Balance']]
    clean_bank_df.columns = ['date', 'time',
                             'description', 'deposit', 'withdrawal', 'balance']
    return clean_bank_df


def process_bank_data(file_path):
    bank_df = extract_bank_data(file_path)
    clean_bank_df = clean_bank_data(bank_df)
    return clean_bank_df


print(process_bank_data(file_path))
