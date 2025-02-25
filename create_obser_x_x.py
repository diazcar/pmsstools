###############################################################################
# This script genarates OBSER_X_X.csv and OBSER_stations for SPRADSG
# Author :  Carlos Diaz
###############################################################################
import argparse
from src.meteo import (
    create_obser_x_x,
    list_of_strings,
)
from src.xair import request_xr, wrap_xair_request

parser = argparse.ArgumentParser(
    description="""
        This script genarates OBSER files for SPRADSG
        """,
    formatter_class=argparse.RawTextHelpFormatter,
)

parser.add_argument(
    '-s', '--sites_names',
    help='Xair site name',
    type=list_of_strings,
    default='NCA',
    metavar='\b',
)

parser.add_argument(
    '-n', '--name',
    help='',
    default='OBSER',
    metavar='\b',
)

parser.add_argument(
    '-o', '--outdir',
    help='CSV output directory',
    default='./'
)

parser.add_argument(
    '-sd', '--start_date',
    help="""Start date for data in str format :
    YYYY-MM-DDT00:00:00""",
    default='2024-08-01T00:00:00'
)

parser.add_argument(
    '-ed', '--end_date',
    help="""End data for data in str format :
    YYYY-MM-DDT00:00:00""",
    default='2024-09-01T00:00:00',
)

parser.add_argument(
    '-fill', '--fill_columns',
    help='',
    metavar='\b'
)

# parser.add_argument(
#     '-met', '--meteo_dict',
#     help="""
#     Dictionary of meteorologic variables for station :
#     {'AEROPO': '60'}' for precipitation from the Nice
#     Aeroport station
#     """,
#     default={'AEROPO': '60'},
#     type={str:str},

# )

args = parser.parse_args()


if __name__ == "__main__":

    for i, site in enumerate(args.sites_names):

        site_info = request_xr(
            sites=site,
            folder='sites',
        )

        data = wrap_xair_request(
            fromtime=args.start_date,
            totime=args.end_date,
            sites=site,
            physicals='51,52,53,54,58,60',
            datatype='base',
        )
        print(data)
        site_info.to_csv(f'./notebook/{site}.csv')
        data.to_csv(f'./notebook/{site}_data.csv')

        create_obser_x_x(
            data_site=data,
            site_info=site_info,
            site_name=site,
            s_index=i,
            outdir=args.outdir,
            name=args.name,
            fill_col=args.fill_columns,
        )
