import pandas as pd

file_path = "/Users/andre/Plan3 Design & Build Dropbox/Andre Heng/Mac/Documents/HBT/Data/Appointment schedules(01_01_2023 - 30_04_2023).csv"


def extract_appointments_data(file_path):
    appointments_df = pd.read_csv(
        file_path)
    return appointments_df


def clean_appointments_data(appointments_df):
    appointments_df = appointments_df.astype(object)
    appointments_df = appointments_df.where(pd.notnull(appointments_df), None)

    clean_appointments_df = appointments_df[[
        'Booking ID', 'Date', 'Time', 'Primary number', 'Pet name', 'Service & add ons', 'Staffs', 'Create date']]
    clean_appointments_df.columns = ['appointment_id', 'date', 'time',
                                     'owner_contact_no', 'pet_name', 'service', 'staff', 'date_created']
    return clean_appointments_df


def process_appointments_data(file_path):
    appointments_df = extract_appointments_data(file_path)
    clean_appointments_df = clean_appointments_data(appointments_df)
    return clean_appointments_df


print(process_appointments_data(file_path))
