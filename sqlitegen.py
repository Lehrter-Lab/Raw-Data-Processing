import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
from collections import defaultdict
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

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
    df = df.replace(-999999, pd.NA)
    df = df.where(pd.notnull(df), None)
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
for col, dtype in DTYPES.items():
    if col in master_df.columns:
        if dtype in (int, float):
            master_df[col] = pd.to_numeric(master_df[col], errors="coerce")
        elif dtype is str:
            master_df[col] = master_df[col].astype("string")
        else:
            master_df[col] = master_df[col].astype(dtype)

##-----------------------------------------------------------------------------
## Upserting data
def normalize(df):
    df = df.copy()
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    if "station_id" in df.columns:
        df["station_id"] = df["station_id"].astype(str).str.strip()
    df = df.where(pd.notnull(df), None)
    return df

## Create/Append the SQLite tables
inspector = inspect(engine)

# Read station table and append only new station_id rows
if inspector.has_table("stations"):
    # Read existing station IDs
    existing_stations = pd.read_sql("SELECT station_id FROM stations",
                                    engine)["station_id"]

    new_stations = station_df[~station_df["station_id"].isin(existing_stations)]

    if not new_stations.empty:
        new_stations.to_sql("stations",
                             engine,
                             if_exists="append",
                             index=False)
    else:
        print("No new station rows to append.")
else:
    station_df.to_sql("stations",
                      engine,
                      if_exists="replace",
                      index=False)
    
# Read master table and upsert new data
with engine.begin() as conn:
    if not inspector.has_table("data"):
            master_df.to_sql("data",
                             conn,
                             if_exists="replace",
                             index=False)

    else:
        # Load existing keys
        existing = pd.read_sql("SELECT * FROM data", conn)
        # Normalize inputs/remove artifacting
        existing  = normalize(existing)
        master_df = normalize(master_df)
        
        # Do a join and use suffixes to figure out upsert locations
        merged = master_df.merge(existing,
                                 on=["station_id", "datetime"],
                                 how="left",
                                 suffixes=("_new", "_old"),
                                 indicator=True)
        
        # Sort rows into useful categories
        both_rows    = merged[merged["_merge"] == "both"]
        key_cols     = ["station_id", "datetime"]
        compare_cols = [c for c in master_df.columns if c not in key_cols]
        
        # Check what rows do not exist yet
        to_insert = merged.loc[merged["_merge"] == "left_only", 
                               key_cols + [c+"_new" for c in compare_cols]].copy()
        to_insert.columns = key_cols + compare_cols
        
        # Checker func
        def row_differs(row):
            for c in compare_cols:
                new_val = row[f"{c}_new"]
                old_val = row[f"{c}_old"]
                if pd.isna(new_val) and pd.isna(old_val):
                    continue
                if new_val != old_val:
                    return True
            return False
        
        # Call func and mask rows that need updating
        diff_mask = both_rows.apply(row_differs, axis=1)
        to_update = both_rows.loc[diff_mask, 
                                  key_cols + [f"{c}_new" for c in compare_cols]].copy()
        
        # Update
        if not to_update.empty:
            cols = compare_cols
            set_clause = ", ".join([f"{c} = :{c}" for c in cols])
    
            update_sql = text(f"""
                            UPDATE data
                            SET {set_clause}
                            WHERE station_id = :station_id
                              AND datetime   = :datetime
                              """)
    
            conn.execute(update_sql,to_update.to_dict(orient="records"))
            print(f"Updated {len(to_update)} rows in data table.")
            
        # Insert
        if not to_insert.empty:
            to_insert.to_sql("data",
                             conn,
                             if_exists="append",
                             index=False)
            print(f"Inserted {len(to_insert)} new rows into data table.")
            
        # Clarify what's going on
        if to_insert.empty and to_update.empty:
            print("No new or updated rows to upsert in data table.")
