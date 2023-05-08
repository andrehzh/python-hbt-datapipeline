import pandas as pd

file_path = "/Users/andre/Plan3 Design & Build Dropbox/Andre Heng/Mac/Documents/HBT/Data/Clients-HachibyTokyo-2023-05-07-06-38-58.csv"


def extract_clients_data(file_path):
    clients_df = pd.read_csv(file_path, delimiter='\t')
    return clients_df


def extract_pet_names(pet_info):
    pet_name = pet_info.split('(')[0].strip()
    return pet_name


def extract_pet_breeds(pet_info):
    pet_breed = pet_info.split('(')[-1].strip().replace(')', '')
    return pet_breed


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


clients_df, pets_df = process_clients_data(file_path)
print(clients_df)
print(pets_df)
