import os
import pandas as pd

CALIBRATION_VAR_LIST = ["TEMPER", "SPEED", "HEIGHT", "DIAMET"]

# Default ship. Escale 27 port 5 2019
DEFAULT_GSP = {
        'IDSRCE': 1,
        'IDADM1': 99,
        'IDADM2': 99,
        'NAMADM': 'PASCAL LOTA',
        'NAMGP1': 'esc_27',
        'NAMGP2': -999,
        'NAMGP3': -999,
        'IDCLA1': -999,
        'IDCLA2': -999,
        'IDCLA3': -999,
        'IDCLA4': -999,
        'IDCLA5': -999,
        'IDPROJ': 93,
        'SHIFTX': 0.0,
        'ORIGX': 0.0,
        'ORIGY': 0.0,
        'POSX': 1045533.1409076374,
        'POSY': 6297678.170821678,
        'HEIGHT': 29,
        'DIAMET': 1.865,
        'TEMPER': 390,
        'SPEED': 1,
        'IDUNIT_NO2': 3,
        'Q_NO2': 0,
        'IDUNIT_NO': 3,
        'Q_NO': 0,
        'IDUNIT_SO2': 3,
        'Q_SO2': 0,
        'IDUNIT_PM25': 3,
        'Q_PM25': 0,
        'IDUNIT_PM1': 3,
        'Q_PM1': 0
    }

# Default ship. Escale 27 port 5 2019
DEFAULT_GSV = {
    'IDSRCE': 1,
    'DATEDEB': '02-08-2019 14:30:00',
    'DATEFIN': '02-08-2019 15:45:00',
    'Q_NO2': 5.413,
    'Q_NO': 21.652,
    'Q_SO2': 6.417,
    'Q_PM25': 1.904,
    'Q_PM1': 0.077,  # Q = FE*ConsomationQ+M = [0.6 g/KgFuel]*[(Conso_quai + Conso_mano) KgFuel]
}

EMISSION_FACTEURS = {
    "PM1": 0.6  # Le Barre et al. 2024
}


def compute_emisions(
        escale: pd.DataFrame,
        pol_list: list[str],
        emission_factor: dict = EMISSION_FACTEURS
):
    flux = dict()
    for pol in pol_list:
        flux[f"Q_{pol}"] = (
            (escale['conso_quai'] + escale['conso_mano']) * emission_factor
        )
    return flux


def build_file_name(
        df: pd.DataFrame,
):
    return None


def run_sensitivity_module(
        plan_simulation_file: str,
        outdir: str,
):
    if os.path.exists(outdir) is False:
        os.makedirs(outdir)
        
    plan_simulation = pd.read_csv(
        plan_simulation_file,
        delimiter=';',
    )

    for i in range(len(plan_simulation)):
        gsp = pd.DataFrame(
            data=DEFAULT_GSP,
            columns=DEFAULT_GSP.keys(),
            index=[0]
            )
        gsv = pd.DataFrame(
            data=DEFAULT_GSV,
            columns=DEFAULT_GSV.keys(),
            index=[0]
            )

        for col in CALIBRATION_VAR_LIST:
            gsp[col] = plan_simulation.iloc[i][col]

        gsp.to_csv(
            f"{outdir}/{plan_simulation.iloc[i]['NOM']}.gsp",
            sep=";",
            index=False
            )

        gsv.to_csv(
            f"{outdir}/{plan_simulation.iloc[i]['NOM']}.gsv",
            sep=";",
            index=False
            )
