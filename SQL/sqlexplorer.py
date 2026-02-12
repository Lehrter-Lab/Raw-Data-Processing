import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx

# Paths
DB_PATH = "WQ.sqlite"
Station = "stations"
data    = "data"

# Load data
con = sqlite3.connect(DB_PATH)
dfs = pd.read_sql(f"SELECT * FROM {Station}", con)
dfd = pd.read_sql(f"SELECT * FROM {data}", con)
con.close()

##-----------------------------------------------------------------------------
# Plot Stations
gdf = gpd.GeoDataFrame(dfs, geometry=gpd.points_from_xy(dfs.longitude,dfs.latitude),
                       crs="EPSG:4326")
gdf = gdf.to_crs(epsg=3857)             # Reproject for basemap
fig, ax = plt.subplots(figsize=(9, 7))
gdf.plot(ax=ax,markersize=15,alpha=0.8)
ctx.add_basemap(ax,source=ctx.providers.OpenStreetMap.Mapnik)
ax.set_axis_off()
ax.set_title("Station Locations")
for txt in ax.texts:
    txt.set_fontsize(2)
plt.show()
# fig.savefig("stations.png",dpi=300,bbox_inches="tight")

##-----------------------------------------------------------------------------
# Plot variable grouped by stations, **note: not actual sample location**
def plot_by_var(variable="NPOC_ppm", agg="mean",
                cmap="turbo", markersize=30):
    
    # Cmap suggestions:
    # "viridis", "plasma", "inferno", "cividis", "turbo", "seismic"

    # Sanity check
    if variable not in dfd.columns:
        raise ValueError(f"{variable} not found in DataFrame.")
        
    agg_funcs = {"mean": "mean",
                 "median": "median",
                 "min": "min",
                 "max": "max",
                 "std": "std",
                 "count": "count"}
    if agg not in agg_funcs:
        raise ValueError(f"Unsupported aggregation: {agg}")
        
    # Make some useful strings
    col     = f"{variable}_{agg}"
    aggname = agg.capitalize()
    varname = variable.split("_")[0]
    units   = variable.split("_", 1)[1] if "_" in variable else ""
    
    # Means by station
    agg_vals = (dfd.groupby("station_id", as_index=False)[variable]
                 .agg(agg_funcs[agg])
                 .rename(columns={variable: col}))
    df = dfs.merge(agg_vals, on="station_id", how="left")
    vmin = agg_vals[col].min()
    vmax = agg_vals[col].max()
    vmin_str = f"{vmin:.3f}"
    vmax_str = f"{vmax:.3f}"
    
    # Build gpd for plotting
    gdf = gpd.GeoDataFrame(df,geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
                           crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3857) # Reproject for basemap
    
    # Plot
    fig, ax = plt.subplots(figsize=(9, 7))
    plot    = gdf.plot(ax=ax,column=f"{variable}_mean",
                       cmap=cmap,markersize=markersize,alpha=0.8,legend=True)
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
    ax.set_axis_off()
    cbar = plot.get_figure().axes[-1]
    cbar.set_title(units.upper())

    ax.set_title(f"{aggname} {varname} (min: {vmin_str}, max: {vmax_str})")
    
    plt.show()
    # outname = f"{aggname}_{variable}.png"
    # fig.savefig(outname, dpi=300, bbox_inches="tight")
    
# Call
plot_by_var(variable="DIC_ppm",agg="mean")
