import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import numpy as np
import datetime as dt
from collections import defaultdict

# Path to data folder and name for sqlite db
DATA_DIR = Path("data")
engine   = create_engine("sqlite:///WQ.sqlite")

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
              "do (%)":                  "DO_%",
              "conductivity (spc)":      "Conductivity_SPC_µS_cm",
              "salinity (psu)":          "Salinity_PSU",
              "ph":                      "pH",
              
              # carbon system
              "dic (ppm)":  "DIC_ppm",
              "doc (ppm)":  "NPOC_ppm",
              "npoc (ppm)": "NPOC_ppm",
              
              # nutrients
              "no3 no2 (µm)":   "NO3_NO2_µM",
              "no3+no2 (µm)":   "NO3_NO2_µM",
              "no3 (µm)":       "NO3_µM",
              "no2 (µm)":       "NO2_µM",
              "nh4 (µm)":       "NH4_µM",
              "po4 (µm)":       "PO4_µM",
              "d si (µm)":      "DSi_µM",
              "dsi (µm)":       "DSi_µM",
              "nitrogen concentration (ug/l)": "Nitrogen_µg_L",
              "carbon concentration (ug/l)":   "Carbon_µg_L",
              "tn (ppm)":       "TN_ppm",
              "pp (µm)":        "PP_µM",
              "tdp (µm)":       "TDP_µM",
              
              # other
              "chla (ug/l)":              "Chla_µg_l",
              "chlorophyll a":            "Chla_µg_L",
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
          "date":           str,
          "time_local":     str,
          "time_utc":       str,
          "datetime": "datetime64[ns]",
          
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
          "NO3_NO2_µM": float,
          "NO3_µM":     float,
          "NO2_µM":     float,
          "NH4_µM":     float,
          "PO4_µM":     float,
          "DSi_µM":     float,
          "Nitrogen_ug_L": float,
          "Carbon_ug_L":   float,
          "TN_ppm":  float,
          "PP_µM":  float,
          "TDP_µM": float,
          
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
    - rename_map: optional dict to normalize column names
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

# Clean columns for rename/flatten
def normalize_columns(df,column_map):
    df.columns = (df.columns
                  .str.strip()
                  .str.lower())
    return df.rename(columns=column_map)

##-----------------------------------------------------------------------------
## Sanity checks
check_columns_consistency(DATA_DIR,
                          sheet_filter=lambda s: "station" in s.strip().lower(),
                          rename_map=STATION_MAP, name="Stations")
check_columns_consistency(DATA_DIR,
                          sheet_filter=lambda s: "master" in s.strip().lower(),
                          rename_map=MASTER_MAP, name="Masters")
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
        station_df = pd.read_excel(xlsx, sheet_name=sheet)
        station_df = normalize_columns(station_df,STATION_MAP)

        station_df["source_file"] = xlsx.name
        all_station_rows.append(station_df)

    # Load WQ data
    for sheet in master_sheets:
        df = pd.read_excel(xlsx, sheet_name=sheet)
        
        # Basic QC
        df.replace(-999999, np.nan, inplace=True)
        df = normalize_columns(df,MASTER_MAP)
        
        # Fix weird time artifacting
        try:
            df["time_local"] = df["time_local"].astype(str).str[:5]        
            # Combine datetime and move new column after time column
            df.insert(df.columns.get_loc("time_local") + 1,
                      "datetime",
                      pd.to_datetime(df["date"].astype(str) + " " + df["time_local"].astype(str), 
                                     errors="coerce"
                                     )
                      )
        except:
            print(f"\nError in time column of {xlsx}::{sheet}\nImporting as is\n")
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, dt.time)).any():
                    df[col] = df[col].astype(str)
        
        df["source_file"] = xlsx.name
        all_master_rows.append(df)
        
# Concat the lists of tables
master_df  = pd.concat(all_master_rows, ignore_index=True)
station_df = (pd.concat(all_station_rows, ignore_index=True)
              .drop_duplicates(subset=["station_id"]))

# Enforce dtypes
for col, dtype in DTYPES.items():
    if col in master_df.columns:
        if dtype == "datetime64[ns]":
            master_df[col] = pd.to_datetime(master_df[col], errors="coerce")
        else:
            master_df[col] = master_df[col].astype(dtype, errors="ignore")

# Create the sqlite tables
station_df.to_sql("stations",
                  engine,
                  if_exists="replace",
                  index=False)
master_df.to_sql("data",
                 engine,
                 if_exists="replace",
                 index=False)
