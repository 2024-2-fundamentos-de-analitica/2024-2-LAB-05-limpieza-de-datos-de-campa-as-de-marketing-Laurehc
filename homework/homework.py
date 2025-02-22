"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
from zipfile import ZipFile
import pandas as pd


def clean_campaign_data():
    month_to_number = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    input_path = os.path.join("files", "input")
    zips = [nombre for nombre in os.listdir(input_path) if nombre.endswith(".zip")]
    dataframes = []

    for zip_file in zips:
        zip_path = os.path.join(input_path, zip_file)
        with ZipFile(zip_path) as zip_ref:
            csv_filename = zip_ref.namelist()[0]  
            with zip_ref.open(csv_filename) as file:
                df = pd.read_csv(file)
                if "Unnamed: 0" in df.columns:
                    df = df.drop(columns=["Unnamed: 0"])
                dataframes.append(df)

    final_df = pd.concat(dataframes, ignore_index=True)

    client = final_df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    client["job"] = client["job"].astype(str).str.strip().str.lower()
    client["job"] = client["job"].str.replace(r"[\.\-]", "_", regex=True)
    client["job"] = client["job"].str.rstrip("_")
    client["education"] = client["education"].replace("unknown", pd.NA)
    client["education"] = client["education"].apply(lambda x: x.replace("-", "_") if pd.notna(x) else x)
    client["education"] = client["education"].apply(lambda x: x.replace(".", "_") if pd.notna(x) else x)
    client["credit_default"] = client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    campaign = final_df[[
        "client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", 
        "previous_outcome", "campaign_outcome", "month", "day"
    ]].copy()

    campaign["month"] = campaign["month"].str.lower().map(month_to_number).fillna("00")
    campaign["previous_outcome"] = campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)

    campaign["month"] = campaign["month"].astype(str).str.zfill(2)
    campaign["day"] = campaign["day"].astype(str).str.zfill(2)

    campaign["last_contact_date"] = "2022-" + campaign["month"] + "-" + campaign["day"]
    campaign.drop(columns=["month", "day"], inplace=True)


    economic = final_df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()


    output_path = os.path.join("files", "output")
    os.makedirs(output_path, exist_ok=True)

    client.to_csv(os.path.join(output_path, "client.csv"), index=False)
    campaign.to_csv(os.path.join(output_path, "campaign.csv"), index=False)
    economic.to_csv(os.path.join(output_path, "economics.csv"), index=False)

    print(client["job"].value_counts())

""" En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """




if __name__ == "__main__":
    clean_campaign_data()

