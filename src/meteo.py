import numpy as np
import pandas as pd


VAL_DIC = {
    'M_DATE': {
        'type': 'date',
        'name': 'date',
        'short_name': 'date',
        'iso': None
    },
    'S_INDEX': {
        'type': 'local',
        'value': None
    },
    'S_NAME': {
        'type': 'local',
        'value': None
    },
    'T_INDEX': {
        'type': 'default',
        'value': 1
    },
    'T_NAME': {
        'type': 'default',
        'value': 'P'
    },
    'M_LEVEL': {
        'type': 'default',
        'value': 1
    },
    '1_VAL': {
        'type': 'variable',
        'name': 'Speed',
        'short_name': 'VV',
        'iso': '51',
    },
    '2_VAL': {
        'type': 'variable',
        'name': 'Direction',
        'short_name': 'DV',
        'iso': '52',
    },
    '3_VAL': {
        'type': 'propiety',
        'name': 'Height',
        'short_name': 'H',
        'iso': 2,
    },
    '4_VAL': {
        'type': 'variable',
        'name': 'Temperature',
        'short_name': 'TC',
        'iso': '54',
    },
    '5_VAL': {
        'type': 'variable',
        'name': 'Rain',
        'short_name': 'Precipita',
        'iso': '60',
        'value': 0
    },
    '6_VAL': {
        'type': 'variable',
        'name': 'Relative Humidity',
        'short_name': 'HR',
        'iso': '58',
        'value': 50
    },
    '7_VAL': {
        'type': 'variable',
        'name': 'Pressure',
        'short_name': 'PA',
        'iso': '53',
        'value': 101325
    }
}


def list_of_strings(arg):
    return arg.split(',')


def create_obser_x_x(
    data_site: pd.DataFrame,
    site_info: pd.DataFrame,
    site_name: str,
    s_index: int,
    outdir: str,
    name: str,
    fill_col: bool,
):
    try:
        frame = pd.read_csv('./src/obser_templates/OBSER_X_X.csv', sep=';')
    except FileNotFoundError:
        print('OBSER_X_X.csv templqte not found in ./src/obser_templates')

    for col in frame.columns:

        col_type = VAL_DIC[col]['type']
        if col_type == 'date':
            frame[col] = data_site.reset_index()['date'].drop_duplicates()

        if col_type == 'variable':

            short_name = VAL_DIC[col]['short_name']

            values = data_site[
                data_site['id']
                .str.contains(f'{short_name}{site_name[:3].strip(' ')}')
            ]['value'].values

            if col == '7_VAL':
                values = values*1000

            print(VAL_DIC[col])
            if len(values) == 0:
                print(fill_col)
                if fill_col == "True":
                    print('ca passe')
                    frame[col] = [VAL_DIC[col]['value']]*len(frame.index)
                else:
                    frame[col] = [None]*len(frame.index)

            else:
                frame[col] = values

            # frame.dropna(how='all', axis=1, inplace=True)
        if col_type == 'default':
            frame[col] = [VAL_DIC[col]['value']]*len(frame.index)

    frame['S_INDEX'] = s_index+1
    frame['S_NAME'] = site_name
    frame['3_VAL'] = [site_info['altitude'].values[0]]*len(frame.index)
    frame['M_DATE'] = (
        pd.to_datetime(frame['M_DATE'])
        .apply(lambda time: time.strftime('%d-%m-%Y %H:%M:%S'))
        )
    frame.to_csv(f'{outdir}/{name}_1_{s_index+1}.csv', sep=';', index=False)
