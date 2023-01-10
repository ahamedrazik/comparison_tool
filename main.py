# importing package
import sys

import psycopg2
import pandas as pd
import sqlalchemy
import warnings
import os
import json
import numpy as np
from dotenv import load_dotenv, find_dotenv
from pandas_profiling import ProfileReport
import matplotlib.pyplot as plt
from sqlalchemy import inspect

load_dotenv(find_dotenv())
user = os.getenv('USER_NAME')
password = os.getenv('PASS_WORD')
localhost = os.getenv('LOCALHOST')
port = os.getenv('PORT')
dbname = os.getenv('DBNAME')

# envirnoment variable for mysql
my_user = os.getenv('USER_NAME1')
my_password = os.getenv('PASS_WORD1')
my_localhost = os.getenv('LOCALHOST1')
my_port = os.getenv('PORT1')
my_dbname = os.getenv('DBNAME1')


# Define the MariaDB engine using MariaDB Connector/Python
def source_db_connect():
    print("********************SOURCE DATABASE************************")
    try:
        print('Connecting to the MYSQL database...')
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{my_user}:{my_password}@{my_localhost}:{my_port}/{my_dbname}")
        print("Connection Established..")
        # sql_query = pd.read_sql_query('SELECT * FROM samplesuperstorenew;', engine)
        # dynamic input
        tablename = input("Enter the table_name: ")
        insp = inspect(engine)
        table_exist = insp.has_table(tablename)
        table_checker = [tablename if table_exist == True else 'tablename not exits']
        table_found = ' '.join([str(tables) for tables in table_checker])
        if table_found == tablename:
            print("DATA PROFILING....")
            sql_query = pd.read_sql_query('SELECT * FROM {};'.format(tablename), engine)
            sql_db_df = pd.DataFrame(sql_query)
            profile_re_gen(sql_db_df)
            # return sql_db_df
            # profile_report_generator(sql_db_df, tablename)
        else:
            print("tablename not exits in database")


    except Exception as ex:
        print("No connection could be made because the target machine: \n", ex)


# read data from flat file with differents formats

# 1.csv formats
def csv_format_read():
    print("Data Profiling flat file with different format like csv,excel,txt")
    path = r"C:\Users\RAZIK KHAN\Downloads\SampleSuperstorenew.csv"
    csv_df = pd.read_csv(path)
    profile_re_gen(csv_df)


# 2.excel format explicitly ignore warning
def excel_format_read():
    warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
    path1 = r"C:\Users\RAZIK KHAN\Downloads\sample.xlsx"
    excel_df = pd.read_excel(path1)
    profile_re_gen(excel_df)


# 3.txt format
def txt_format_read():
    # path2 = r"C:\Users\RAZIK KHAN\Downloads\names.txt"
    path2 = r"D:\YearPredictionMSD.txt"
    df_txt_fm = pd.read_csv(path2, sep=',')
    profile_re_gen(df_txt_fm)
    # print(df_txt_fm)
    # return df_txt_fm


# Connection parameters
try:
    conn_string = f"host={localhost} dbname={dbname}\
        user={user} password={password}"
except Exception as err:
    print("connection_string credential is wrong: \n", err)


def target_db_connect():
    """ Connect to the PostgreSQL database server """
    print("********************DESTINATION DATABASE************************")
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(conn_string)
        print("Connection successful")
        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        tablename = input("Enter the table_name: ")
        cursor.execute(
            f"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '" + tablename + "');")

        if cursor.fetchone()[0]:
            cursor.execute("Select * FROM employee LIMIT 0")
            colnames = [desc[0] for desc in cursor.description]
            # # # Executing an MYSQL function using the execute() method

            cursor.execute("select * from employee")
            # # Fetch a single row using fetchall() method.
            postgres_data = cursor.fetchall()
            df = pd.DataFrame(postgres_data, columns=colnames)
            profile_re_gen(postgres_df)
        else:
            print("table not exits")

        # # creating dataframe using postgres data
        # postgres_df = pd.DataFrame(postgres_data, columns=['emp_ID', 'emp_NAME', 'DEPT_NAME', 'SALARY'])
        # profile_re_gen(postgres_df)
        # return postgres_df

        # Closing the connection
        conn.close()
    except (Exception, psycopg2.DatabaseError) as err:
        print(err)
        sys.exit(1)
    return conn


def profile_re_gen(df):
    # As a JSON string
    profile = ProfileReport(df, config_file="profiling_config.yml")
    json_data = profile.to_json()
    report_gen_from_json(json_data)


# read the json data

def report_gen_from_json(json_data):
    # convert string to  object
    json_dict = json.loads(json_data)
    print("****************************")
    print("TABLE DETAILS ")
    print("****************************")

    table_details = json_dict["table"]
    types_diff = table_details['types']
    print(f"TOTAL NUMBER OF ROWS:{table_details['n']}")
    print(f"TOTAL NUMBER OF COLUMNS:{table_details['n_var']}")
    print(f"TOTAL NUMBER OF VARIABLES MISSING:{table_details['n_vars_all_missing']}")
    print(f"NUMBER OF Numeric TYPE:{types_diff['Numeric']}")
    print(f"NUMBER OF Categorical TYPE:{types_diff['Categorical']}")
    print(f"TOTAL NUMBER OF DUPLICATES:{table_details['n_duplicates']}")

    print("****************************")
    print("columns DETAILS ")
    print("****************************")

    columns_details = json_dict["variables"]
    for each_col, each_value in iter(columns_details.items()):
        print("\ncolumns_name:", each_col)

        for key_value in iter(each_value):
            data = ["range", "n_distinct", "is_unique",
                    "n_missing", "n_unique", "count",
                    "type", "ordering", "n_negative", "mean", "std", "variance", "min", "max"]
            for cus_col in iter(data):
                if key_value == cus_col:
                    key_value = cus_col
                    print(key_value + ':', each_value[key_value])


def histogram_plot(json_data):
    json_dict = json.loads(json_data)
    columns_details = json_dict["variables"]
    for key, value in columns_details.items():
        print("\ncolumns_name:", key)
        for i, j in iter(value.items()):
            if i == 'histogram':
                i = 'histogram'
                keysList = [key for key in j]
                counts = keysList[0]
                bin_edges = keysList[1]

                plt.hist(j[counts], bins=j[bin_edges], histtype='bar', align='mid')

                # Set title
                plt.title("histogram")

                # adding labels
                plt.xlabel(key)
                plt.show()


def df_compare(source_df, target_df):
    try:
        data = source_df.compare(target_df, align_axis=1)
        print(data)
    except Exception as err:
        print(err)


def onemillion_rows():
    DATA_DIR = r'D:\partions_joins\Variant _p2.csv'
    df = pd.read_csv(DATA_DIR)
    altermethod_profiling(df)


def altermethod_profiling(df):
    row_count = df.shape[0]
    col_count = df.shape[1]
    ca_type = df.select_dtypes(include=['object']).columns.tolist()
    ca_count = len(ca_type)
    # dg = df.select_dtypes(include=np.number).columns.tolist()
    n_type = df.select_dtypes(include=['number']).columns.tolist()
    n_count = len(n_type)
    # Select duplicate rows of all columns
    duplicate = df.duplicated(keep=False)
    display_profiling(row_count, col_count, ca_count, n_count, duplicate)


def display_profiling(row_count, col_count, ca_count, n_count, duplicate):
    print("****************************")
    print("TABLE INFORMATION ")
    print("****************************")

    print("TOTAL NUMBER OF ROWS:", row_count)
    print("TOTAL NUMBER OF COLUMNS:", col_count)
    print("NUMBER OF Numeric TYPE:", n_count)
    print("NUMBER OF Categorical TYPE:", ca_count)
    print("TOTAL NUMBER OF DUPLICATES:", duplicate)


def each_col_analysis():
    # mean,std,min,max
    print("staticstic analysis")


if __name__ == '__main__':
    # source_db_connect()
    # csv_format_read()
    # excel_format_read()
    # txt_format_read()
    # target_db_connect()
    # df_compare(source_df, target_df)
    # profile_report_generator(source_df)
    # histogram_plot()
    # report_gen_from_json()
    onemillion_rows()
