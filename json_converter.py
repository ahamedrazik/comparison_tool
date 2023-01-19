import pandas as pd
import json
import numpy as np


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class NumpyEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)):
            return None

        return json.JSONEncoder.default(self, obj)


def statistics_analysis(df):
    n_type = df.select_dtypes(include=['number']).columns.tolist()
    for n_col in iter(n_type):
        s_mean = df[n_col].mean()
        s_std = df[n_col].std()
        s_median = df[n_col].median()
        s_max = df[n_col].max()
        s_min = df[n_col].min()
        s_count = df[n_col].count()

        statistics_info = {'statistics analysis': {
            n_col: ['Mean of Column Values:', s_mean, 'Standard Deviation of Values:', s_std, 'Median of Values:',
                    s_median,
                    'Maximum of Values:', s_max, 'Minimum of Values:', s_min, 'Count of Non-null Values:', s_count]}
        }

        json_formatter(statistics_info)


def columns_analysis(df):
    print("COLUMN LEVEL ANALYSIS ")

    for col_anysis in iter(df.columns):
        s_unique = df[col_anysis].nunique(dropna=True)
        is_unique = df[col_anysis].is_unique
        s_sort = df[col_anysis].is_monotonic_increasing
        s_missing = df[col_anysis].isnull().sum()
        s_nan = df[col_anysis].notnull().sum()
        # s_freq = df[col_anysis].mode()
        s_freq = df[col_anysis].value_counts().idxmax()

        columns_info = {'columns analysis': {
            col_anysis: ['number of unique values:', s_unique, 'Values are unique or not:', is_unique,
                         'Values are sorted or not:',
                         s_sort,
                         'Total NaN values:', s_missing, 'NOT NULL values:', s_nan, 'the most frequent value:', s_freq]}
        }

        json_formatter(columns_info)


def table_profiling(df):
    row_count = df.shape[0]
    col_count = df.shape[1]
    ca_type = df.select_dtypes(include=['object']).columns.tolist()
    ca_count = len(ca_type)
    # dg = df.select_dtypes(include=np.number).columns.tolist()
    n_type = df.select_dtypes(include=['number']).columns.tolist()
    n_count = len(n_type)
    # Select duplicate rows of all columns
    duplicate = df.duplicated(keep=False).sum()

    table_info = {'TABLE INFORMATION': ['TOTAL NUMBER OF ROWS:', row_count, 'TOTAL NUMBER OF COLUMNS:', col_count,
                                        'NUMBER OF Numeric TYPE:',
                                        n_count,
                                        'NUMBER OF Categorical TYPE:', ca_count, 'TOTAL NUMBER OF DUPLICATES:',
                                        duplicate]
                  }

    json_formatter(table_info)


def json_formatter(data: dict) -> json:
    json_obj = json.dumps(data, indent=4, cls=NumpyEncoder)
    print(json_obj)


# def to_json() -> json:
#     json_obj = json.dumps(statics_ana(df), indent=4, cls=NumpyEncoder)
#     return json_obj


if __name__ == '__main__':
    DATA_DIR = r'D:\partions_joins\Variant _p2.csv'
    df = pd.read_csv(DATA_DIR)
    table_profiling(df)
    statistics_analysis(df)
    columns_analysis(df)
