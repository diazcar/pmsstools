###############################################################################
#This script genarates OBSER_X_X.csv and OBSER_stations for SPRADSG
# Author :  Carlos Diaz
###############################################################################
import pandas as pd
import argparse
from src.utils import (
    list_of_strings
)
from xair import request_xr, wrap_xair_request

parser = argparse.ArgumentParser(
    description="""
        This script genarates OBSER files for SPRADSG
        """,
    formatter_class=argparse.RawTextHelpFormatter,
)

parser.add_argument(
    '-sites', '--sites_names',
    help='Xair site name',
    type=list_of_strings(),
    default='NCA',
    metavar='\b',
)
parser.add_argument(
    '-o', '--output',
    help='CSV output directory'
)

parser.add_argument(
    '-dr', '--date_range',
    help="""Date range for data in str format :
    YYYY-MM-DDT00:00:00Z,YYYY-MM-DDT00:00:00Z""",
    default='2024-08-01T00:00:00Z,2024-09-01T00:00:00Z'
)

args = parser.parse_args()


if __name__ == "__main__":

    site_info = request_xr(
        sites=args.sites_names,
        folder='sites',
    )

    data = wrap_xair_request(
        fromtime=args.date_range.split(',')[0],
        totime=args.date_range.split(',')[1],
        sites=args.sites,
        physicals='51,52,53,57,58,60',
        datatype='base',
    )

    site_info.to_csv('./notebook/sites.csv')

    data.to_csv('./notebook/data.csv')
