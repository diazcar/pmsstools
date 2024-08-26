import pandas as pd

VAL_DIC = dict(
    VAL_1='Speed',
    VAL_2='Direction',
    VAL_3='Height',
    VAL_4='TEMP',
    VAL_5='rain',
    VAL_6='RH',
    VAL_7='Pressure'
)

def list_of_strings(arg):
    return arg.split(',')
