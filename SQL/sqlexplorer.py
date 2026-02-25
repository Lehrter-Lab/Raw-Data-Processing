import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import contextily as ctx
import pymannkendall as mk
import matplotlib.patheffects as pe
from mpl_toolkits.axes_grid1 import make_axes_locatable

# rcParams
plt.rcParams["figure.dpi"] = 300

# Paths
DB_PATH = "WQ.sqlite"
Station = "stations"
data    = "data"

# Load data
con = sqlite3.connect(DB_PATH)
dfs = pd.read_sql(f"SELECT * FROM {Station}", con).replace(-999999,np.nan)
dfd = pd.read_sql(f"SELECT * FROM {data}", con).replace(-999999,np.nan)
con.close()

##-----------------------------------------------------------------------------
# Plot Stations
gdf = gpd.GeoDataFrame(dfs, geometry=gpd.points_from_xy(dfs.longitude,dfs.latitude),
                       crs="EPSG:4326")
gdf = gdf.to_crs(epsg=3857)             # Reproject for basemap
fig, ax = plt.subplots(figsize=(9, 7))
gdf.plot(ax=ax,markersize=12,alpha=0.8)
ctx.add_basemap(ax,source=ctx.providers.OpenStreetMap.Mapnik)
# Map frame
ax.set_xticks([])
ax.set_yticks([])
# Title
ax.set_title("Station Locations")
# Make citation small
for txt in ax.texts:
    txt.set_fontsize(1)
plt.show()
# fig.savefig("stations.png",dpi=300,bbox_inches="tight")

##-----------------------------------------------------------------------------
# Plot variable grouped by stations, **note: not actual sample location**
def plot_by_var(variable="NPOC_ppm", agg="mean",
                cmap="turbo", markersize=12):
    
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
    
    # Do some stats on aggregated values
    median_val          = agg_vals[col].median()
    agg_vals["abs_dev"] = (agg_vals[col] - median_val).abs()
    extreme_ids         = (agg_vals.sort_values("abs_dev", ascending=False)
                           .head(5)["station_id"])
    vmin     = agg_vals[col].min()
    vmax     = agg_vals[col].max()
    vmin_str = f"{vmin:.3f}"
    vmax_str = f"{vmax:.3f}"
    
    # Build gpd for plotting
    gdf = gpd.GeoDataFrame(df,geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
                           crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3857) # Reproject for basemap
    
    # Get bounds
    xmin, ymin, xmax, ymax = gdf.total_bounds
    xrange = xmax - xmin
    yrange = ymax - ymin
    
    pad_frac = 0.03
    xmin -= xrange * pad_frac
    xmax += xrange * pad_frac
    ymin -= yrange * pad_frac
    ymax += yrange * pad_frac
    
    # Start Plot
    fig, ax = plt.subplots(figsize=(9, 7))
    plot    = gdf.plot(ax=ax,column=col,
                       cmap=cmap,markersize=markersize,alpha=0.8,legend=False)
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
    
    # Map frame
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Make citation small
    for txt in ax.texts:
        txt.set_fontsize(1)
        
    # Colorbar setting
    divider = make_axes_locatable(ax)
    cax     = divider.append_axes("right", size="8%", pad=0.05)
    cbar    = fig.colorbar(plot.collections[0], cax=cax)
    cbar.ax.tick_params(labelsize=9)
    cbar.ax.set_title(units.upper(), fontsize=10)
    
    # Set map bounds based off universal range not non-NaN range
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    
    # Title settings
    title    = (f"{aggname} {varname}")
    subtitle = f"(Min: {vmin_str}, Max: {vmax_str})"
    ax.set_title(title, fontsize=14, pad=16)
    ax.text(0.5, 1.00, subtitle,
            transform=ax.transAxes,
            ha="center",
            va="bottom",
            fontsize=12)
    
    # Add labels for top 5 highest values
    for _, row in gdf[gdf["station_id"].isin(extreme_ids)].iterrows():
        x = row.geometry.x
        y = row.geometry.y
        
        txt = ax.text(x, y,
                      str(row["station_id"]),
                      fontsize=5,
                      ha="left",
                      va="bottom",
                      color="black")
    
        # Add white halo for clarity
        txt.set_path_effects([pe.Stroke(linewidth=1.0, foreground="white"),
                              pe.Normal()])
    
    plt.show()
    # outname = f"{aggname}_{variable}.png"
    # fig.savefig(outname, dpi=300, bbox_inches="tight")
##-----------------------------------------------------------------------------
# Station explorer
def plot_station(station=None, variable="NPOC_ppm",
                  cmap="viridis", markersize=40):
    # Sanity check
    if variable not in dfd.columns:
        raise ValueError(f"{variable} not found in DataFrame.")
    
    # Load data
    if station:
        df = dfd[dfd["station_id"] == station].copy()
        if df.empty:
            raise ValueError(f"No data found for station {station}")
    else:
        df = dfd.copy()
    
        
    # Parse year
    df["datetime"]  = pd.to_datetime(df["datetime"], errors="coerce")
    df              = df.dropna(subset=["datetime"])
    df["year"]      = df["datetime"].dt.year
    df["month"]     = df["datetime"].dt.month
    df["dayofyear"] = df["datetime"].dt.dayofyear
    df              = df.sort_values("datetime")
    
    # Initialize plot variables
    years    = sorted(df["year"].unique())
    cmap_obj = plt.get_cmap(cmap)
    colors   = cmap_obj(np.linspace(0, 1, len(years)))
    plt.rcParams.update({"axes.titlesize": 18,
                         "axes.titleweight": "bold",
                         "axes.labelsize": 16,
                         "xtick.labelsize": 14,
                         "ytick.labelsize": 14,
                         "legend.fontsize": 13})
    varname = variable.split("_")[0]
    units   = variable.split("_", 1)[1] if "_" in variable else ""
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    for i, year in enumerate(years):
        yearly_data = df[df["year"] == year]
        ax.scatter(yearly_data["dayofyear"], yearly_data[variable],
                   label=str(year),
                   color=colors[i],
                   s=markersize)
    ax.set_xlim(1, 366)
    
    # Put approximate month labels **note: leap year messes things slightly**
    month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    ax.set_xticks(month_starts)
    ax.set_xticklabels(month_labels)
    
    # Title
    if station:
        ax.set_title(f"{varname} ({units}) at {station}")
    else:
        ax.set_title(f"{varname} ({units})")
        
    # Legend
    ax.legend(loc="upper right", bbox_to_anchor=(1.005, 1.01), ncols=2, framealpha=0.8,
              labelspacing=0.1, columnspacing=0.1, handletextpad=0.01)
    
    # Seasonal Mann-Kendall taking mean of each months data per year
    month_groups = df.groupby(['year', 'month'])[variable].mean()
    month_array = month_groups.unstack('month')
    
    # Mann Kendall package expects columns of seasons and rows as cycles
    seasonalmk   = mk.seasonal_test(month_array.values, period=12)
    slope        = seasonalmk.slope
    mk_text      = f"Trend: {seasonalmk.trend}\nSlope: {slope:.3f}\np-value: {seasonalmk.p:.3f}"
    ax.text(0.010, 0.982, mk_text, transform=ax.transAxes, 
            fontsize=12, verticalalignment='top', 
            bbox=dict(facecolor='white', alpha=0.7, edgecolor='gray'))
    
    plt.tight_layout()
    plt.show()
    return month_array
##-----------------------------------------------------------------------------
# Call
plot_by_var(variable="DIC_ppm",agg="median")
ma=plot_station(variable="DIC_ppm")
