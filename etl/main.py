from extract.extract       import (extract_velib_stations,
                                   extract_velib_disponibilite,
                                   extract_evenements,
                                   extract_amenagements)
from transform.transform   import (transform_velib_stations,
                                   transform_velib_disponibilite,
                                   transform_evenements,
                                   transform_amenagements)
from load.load             import load_dataframe

def run():
    print("=== ETL E4 - Mobilité Paris ===")

    print("\n[1/4] Vélib Stations...")
    df = extract_velib_stations()
    df = transform_velib_stations(df)
    load_dataframe(df, "velib_stations", if_exists="replace")

    print("\n[2/4] Vélib Disponibilité...")
    df = extract_velib_disponibilite()
    df = transform_velib_disponibilite(df)
    load_dataframe(df, "velib_disponibilite", if_exists="replace")

    print("\n[3/4] Événements Paris...")
    df = extract_evenements()
    df = transform_evenements(df)
    load_dataframe(df, "evenements_qfap", if_exists="replace")

    print("\n[4/4] Aménagements Cyclables...")
    df = extract_amenagements()
    df = transform_amenagements(df)
    load_dataframe(df, "amenagements_cyclables", if_exists="replace")

    print("\n=== ETL terminé avec succès ===")

if __name__ == "__main__":
    run()