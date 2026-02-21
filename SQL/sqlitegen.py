import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from collections import defaultdict
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Path to data folder and name for sqlite db
DATA_DIR = Path("data")
engine   = create_engine("sqlite:///WQ.sqlite", isolation_level="SERIALIZABLE")

# Note that map keys are all lower case since they are cast as such in the func
MASTER_MAP = {# identifiers / cruise metadata
              "unique id": "unique_id",
              "cruise id": "cruise_id",
              "year":      "year",
              
              # time
              "date":         "date",
              "time (local)": "time_local",
              "time (utc)":   "time_utc",
              
              # station / location
              "station":              "station_id",
              "station id":           "station_id",
              "station type":         "station_type",
              "latitude":             "latitude",
              "longitude":            "longitude",
              "latitude (intended)":  "latitude_intended",
              "longitude (intended)": "longitude_intended",
              "node (schism)":        "node_schism",
              
              # vertical structure
              "layer":                 "layer",
              "measurement depth (m)": "measurement_depth_m",
              "secchi depth (m)":      "secchi_depth_m",
              "sonar depth (m)":       "sonar_depth_m",
              "ave depth (model , m)": "ave_depth_model_m",
              
              # instruments
              "ctd #": "CTD_number",
              "ctd":   "CTD_number",
              
              # physical
              "temp (c)":                "Temp_C",
              "temperature (c)":         "Temp_C",
              "do (mg/l)":               "DO_mg_L",
              "dissolved oxygen (mg/l)": "DO_mg_L",
              "do (%)":                  "DO_percent",
              "conductivity (spc)":      "Conductivity_SPC_uS_cm",
              "salinity (psu)":          "Salinity_PSU",
              "ph":                      "pH",
              
              # carbon system
              "dic (ppm)":  "DIC_ppm",
              "doc (ppm)":  "NPOC_ppm",
              "npoc (ppm)": "NPOC_ppm",
              
              # nutrients
              "no3 no2 (µm)":   "NO3_NO2_uM",
              "no3+no2 (µm)":   "NO3_NO2_uM",
              "no3 (µm)":       "NO3_uM",
              "no2 (µm)":       "NO2_uM",
              "nh4 (µm)":       "NH4_uM",
              "po4 (µm)":       "PO4_uM",
              "d si (µm)":      "DSi_uM",
              "dsi (µm)":       "DSi_uM",
              "nitrogen concentration (ug/l)": "Nitrogen_ug_L",
              "carbon concentration (ug/l)":   "Carbon_ug_L",
              "tn (ppm)":       "TN_ppm",
              "pp (µm)":        "PP_uM",
              "tdp (µm)":       "TDP_uM",
              
              # other
              "chla (ug/l)":              "Chla_ug_l",
              "chlorophyll a":            "Chla_ug_L",
              "tss concentration (mg/l)": "TSS_mg_L",

              # misc
              "notes": "Notes"
              }

STATION_MAP = {"station id":                "station_id",
               "off shore sites":           "station_id",
               "latitude":                  "latitude",
               "lat":                       "latitude",
               "longitude":                 "longitude",
               "lon":                       "longitude",
               "station type":              "station_type",
               "node (schism)":             "node_schism",
               "ave depth (model , m)":     "ave_depth_model_m",
               "ave depth (model , meter)": "ave_depth_model_m"
               }

DTYPES = {# identifiers
          "unique_id": str,
          "cruise_id": str,
          "year":      int,
          
          # time
          "date":       str,
          "time_local": str,
          "time_utc":   str,
          "datetime":   str,
          
          # station / location
          "station_id":         str,
          "station_type":       str,
          "latitude":           float,
          "longitude":          float,
          "latitude_intended":  float,
          "longitude_intended": float,
          "node_schism":        str,
          
          # vertical structure
          "layer":               str,
          "measurement_depth_m": float,
          "secchi_depth_m":      float,
          "sonar_depth_m":       float,
          "ave_depth_model_m":   float,
          
          # instruments
          "CTD_number": str,
          
          # physical
          "Temp_C":                 float,
          "DO_%":                   float,
          "DO_mg_L":                float,
          "Salinity_PSU":           float,
          "Conductivity_SPC_µS_cm": float,
          "pH":                     float,
          
          # carbon
          "DIC_ppm":  float,
          "NPOC_ppm": float,
          
          # nutrients
          "NO3_NO2_uM": float,
          "NO3_uM":     float,
          "NO2_uM":     float,
          "NH4_uM":     float,
          "PO4_uM":     float,
          "DSi_uM":     float,
          "Nitrogen_ug_L": float,
          "Carbon_ug_L":   float,
          "TN_ppm":  float,
          "PP_uM":  float,
          "TDP_uM": float,
          
          # other
          "Chla_ug_L": float,
          "TSS_mg_L":  float,
          
          # misc
          "Notes":       str,
          "source_file": str
          }   

##-----------------------------------------------------------------------------
# Check if there are some inconsistencies in the columns
def check_columns_consistency(data_dir, sheet_filter=lambda s: True, rename_map=None,name=None):
    """
    Parameters:
    - data_dir: Path or str to directory containing XLSX files
    - sheet_filter: function(sheet_name) -> bool to select which sheets to check
    - rename_map: dict to normalize column names
    """
    cols     = defaultdict(set)
    unmapped = defaultdict(set)
    rename_keys = set(rename_map.keys()) if rename_map is not None else set()

    for xlsx_path in data_dir.glob("**/*.xlsx"):
        xls = pd.ExcelFile(xlsx_path)
        for sheet in filter(sheet_filter, xls.sheet_names):
            df         = pd.read_excel(xlsx_path, sheet_name=sheet, nrows=0)
            df.columns = df.columns.str.strip().str.lower()
            loc = f"{xlsx_path.name}::{sheet}"
            for c in df.columns:
                cols[c].add(loc)
                # Track unmapped columns
                if rename_map is not None and c not in rename_keys:
                    unmapped[c].add(loc)
    if rename_map is not None:
        if unmapped:
            print(f"\n=== Columns NOT mapped in {name} ===")
            for c, locs in sorted(unmapped.items()):
                print(f"{c}:")
                for loc in sorted(locs):
                    print(f"  - {loc}")

# Call check
check_columns_consistency(DATA_DIR,
                          sheet_filter=lambda s: "station" in s.strip().lower(),
                          rename_map=STATION_MAP, name="Stations")
check_columns_consistency(DATA_DIR,
                          sheet_filter=lambda s: "master" in s.strip().lower(),
                          rename_map=MASTER_MAP, name="Masters")

##-----------------------------------------------------------------------------
## Load & QA data
# Pull in xlsx sheet, rename, drop -999999s
def loader(xlsx,sheet,column_map):
    df                = pd.read_excel(xlsx, sheet_name=sheet)
    df.columns        = (df.columns.str.strip().str.lower())
    df                = df.rename(columns=column_map)
    df.columns        = df.columns.str.replace(r"[^a-zA-Z0-9_]", "_", regex=True)\
                       .str.replace(r"_+", "_", regex=True)\
                       .str.strip("_")
    df["source_file"] = xlsx.name
    for col in df.columns:
        if col == "layer":
            df[col] = df[col].fillna("S")
        else:
            df[col] = df[col].replace([np.nan, pd.NA, None, ""], -999999)
    return df

# Make dtypes consistent, needs to be periodically called
def enforce_dtypes(df, dtypes_map):
    for col, dtype in dtypes_map.items():
        if col in df.columns:
            if dtype in (int, float):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(-999999)
            elif dtype is str:
                df[col] = df[col].astype("string")
            else:
                df[col] = df[col].astype(dtype)
    return df

# Initialize empty lists
all_master_rows  = []
all_station_rows = []

for xlsx in DATA_DIR.glob("**/*.xlsx"):
    # Load xlsx
    xls = pd.ExcelFile(xlsx)
    
    # Handle naming
    station_sheets = [s for s in xls.sheet_names if "station" in s.strip().lower()]
    master_sheets  = [s for s in xls.sheet_names if "master" in s.strip().lower()]
    
    # Load station data
    for sheet in station_sheets:
        df = loader(xlsx,sheet,STATION_MAP)
        all_station_rows.append(df)

    # Load WQ data
    for sheet in master_sheets:
        df = loader(xlsx,sheet,MASTER_MAP)
        # Fix weird time artifacting
        df["time_local"] = df["time_local"].astype(str).str[:5]        
        # Combine datetime and move new column after time column
        df.insert(df.columns.get_loc("time_local") + 1,
                  "datetime",
                  pd.to_datetime(df["date"].astype(str).str.strip() + " " + df["time_local"].astype(str).str.strip(),
                                 errors="coerce"
                                 ).dt.strftime("%Y-%m-%d %H:%M:%S") 
                  )
        df["datetime"] = df["datetime"].fillna("")
          
        all_master_rows.append(df)
      
# Concat the lists of tables
master_df  = pd.concat(all_master_rows, ignore_index=True)
station_df = (pd.concat(all_station_rows, ignore_index=True)
              .drop_duplicates(subset=["station_id"]))

# Enforce dtypes
master_df = enforce_dtypes(master_df, DTYPES)
##-----------------------------------------------------------------------------
## Key functions
def normalize(df):
    df = df.copy()
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    if "station_id" in df.columns:
        df["station_id"] = df["station_id"].astype(str).str.strip()
    df = df.fillna(-999999)
    return df

def upsert_dataframe(df, conn, table_name, key_cols, interactive_dupes=True):
    df = normalize(df)
    # Handle duplicates within the input DataFrame
    if interactive_dupes:
        dup_check = df[df.duplicated(subset=key_cols, keep=False)]
        if not dup_check.empty:
            print(f"\nWARNING: Found {len(dup_check)} duplicate rows based on {', '.join(key_cols)}!")
            print(dup_check.sort_values(key_cols))
            
            while True:
                choice = input("\nKeep only the first of each duplicate and continue? (y/n): ").strip().lower()
                if choice in ["y", "n"]:
                    break
                print("Please enter 'y' or 'n'.")
            
            if choice == "y":
                df = df.drop_duplicates(subset=key_cols, keep="first")
                print(f"Duplicates removed. Proceeding with {len(df)} rows.")
            else:
                raise ValueError("Aborted by user due to duplicate rows. Resolve dupes and rerun.")
    
    # Create table if it doesn't exist
    inspector = inspect(conn)
    if not inspector.has_table(table_name):
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        # Create unique index on key columns
        idx_cols = ", ".join(key_cols)
        conn.execute(text(f"""CREATE UNIQUE INDEX IF NOT EXISTS
                              ux_{table_name}_{'_'.join(key_cols)}
                              ON {table_name} ({idx_cols})"""))
        print(f"Inserted {len(df)} rows into new {table_name} table.")
        return
    
    # Compare with existing DB to find new or changed rows
    existing_df  = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    existing_df  = enforce_dtypes(existing_df, DTYPES)
    
    # Align cols before merge
    common_cols  = [c for c in df.columns if c in existing_df.columns]
    df           = df[common_cols]
    existing_df  = existing_df[common_cols]
    
    # Merge to detect new or changed rows
    merged       = df.merge(existing_df, on=key_cols, how="left", 
                            suffixes=('', '_db'), indicator=True)
    non_key_cols = [c for c in df.columns if c not in key_cols]
    if non_key_cols:
        diffs = merged[non_key_cols].ne(merged[[f"{c}_db" for c in non_key_cols]].values).any(axis=1)
    else:
        diffs = pd.Series(False, index=merged.index)
    
    changed_mask = (merged["_merge"] == "left_only") | diffs

    # Get new_or_changed rows from merged, not df
    new_or_changed = merged.loc[changed_mask, df.columns]
    
    if new_or_changed.empty:
        print(f"No new or changed rows detected in {table_name}. Database is up-to-date.")
        return
    
    # Upsert only new or changed rows
    insert_cols   = ", ".join(new_or_changed.columns)
    placeholders  = ", ".join([f":{c}" for c in new_or_changed.columns])
    update_clause = ", ".join([f"{c} = excluded.{c}" for c in non_key_cols]) if non_key_cols else f"{key_cols[0]} = excluded.{key_cols[0]}"
    
    upsert_sql = text(f"""INSERT INTO {table_name} ({insert_cols})
                          VALUES ({placeholders})
                          ON CONFLICT({', '.join(key_cols)})
                          DO UPDATE SET {update_clause}""")
    
    conn.execute(upsert_sql, new_or_changed.to_dict(orient="records"))
    print(f"\nUpserted {len(new_or_changed)} new or changed rows into {table_name} table.")

##-----------------------------------------------------------------------------
# Call funcs for upsert
with engine.begin() as conn:
    # Upsert stations
    upsert_dataframe(station_df, conn, table_name="stations", key_cols=["station_id"])
    
    # Upsert master data
    upsert_dataframe(master_df, conn, table_name="data", key_cols=["station_id", "datetime", "layer"])
