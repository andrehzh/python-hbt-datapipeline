import pandas as pd
import re

file_path = "/Users/andre/Plan3 Design & Build Dropbox/Andre Heng/Mac/Documents/HBT/Data/MoeGo-Export-Client-HachibyTokyo-2023-06-01-10-01-58.csv"


def extract_clients_data(file_path):
    clients_df = pd.read_csv(file_path, delimiter='\t')
    return clients_df


def extract_pet_names(pet_info):
    pet_name = pet_info.split('(')[0].strip()
    return pet_name


def extract_pet_breeds(pet_info):
    # The pattern to extract data between the outer parentheses
    pattern = r'\((.*)\)'

    # Using regex to extract the pet breed
    match = re.search(pattern, pet_info)

    # Check if we found a match
    if match:
        # If we found a match, return the content inside the parentheses
        return match.group(1)
    else:
        # If no match, return None or an empty string or whatever you prefer
        return None


def clean_clients_data(clients_df):
    # Clean column names
    clients_df.columns = clients_df.columns.str.strip().str.replace('"',
                                                                    '').str.replace(',', '')

    # Remove commas and double quotes from the column values
    clients_df = clients_df.applymap(lambda x: x.replace(
        '"', '').replace(',', '') if isinstance(x, str) else x)

    # Extract required columns and rename them
    clean_clients_df = clients_df[[
        'First name', 'Last name', 'email', 'Primary contact', 'address']]
    clean_clients_df.columns = [
        'first_name', 'last_name', 'email', 'contact_no', 'address']

    return clean_clients_df


def create_pets_df(clients_df):
    clients_df.columns = clients_df.columns.str.strip().str.replace('"',
                                                                    '').str.replace(',', '')
    # clean the contacts column
    clients_df['Primary contact'] = clients_df['Primary contact'].apply(
        lambda x: x.replace('"', '').replace(',', '') if isinstance(x, str) else x)

    clients_df['pet(Breed)'] = clients_df['pet(Breed)'].apply(
        lambda x: x.lstrip(',').replace('"', '') if isinstance(x, str) else x)
    pets_data = []

    for _, row in clients_df.iterrows():
        pet_entries = row['pet(Breed)'].split(',')
        owner_contact_no = row['Primary contact']

        for pet_entry in pet_entries:
            pet_name = extract_pet_names(pet_entry)
            pet_breed = extract_pet_breeds(pet_entry)
            pets_data.append([owner_contact_no, pet_name, pet_breed])

    pets_df = pd.DataFrame(pets_data, columns=[
                           'owner_contact_no', 'pet_name', 'pet_breed'])
    return pets_df


def process_clients_data(file_path):
    clients_df = extract_clients_data(file_path)
    clean_clients_df = clean_clients_data(clients_df)
    pets_df = create_pets_df(clients_df)
    return clean_clients_df, pets_df


def output_df_to_excel(clients_df, pets_df):
    writer = pd.ExcelWriter("clients-cleaned.xlsx", engine='xlsxwriter')
    clients_df.to_excel(writer, sheet_name='clients', index=False)
    pets_df.to_excel(writer, sheet_name='pets', index=False)
    writer.save()


clients_df, pets_df = process_clients_data(file_path)
output_df_to_excel(clients_df, pets_df)
print(clients_df)
print(pets_df)
