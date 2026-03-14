"""
plot_coverage.py
Generate 5 coverage visualizations from station_coverage_geo.
Requires: matplotlib, cartopy (pip install cartopy)
"""

import os
import warnings
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors
import duckdb

warnings.filterwarnings('ignore')

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RECON  = os.path.expanduser("~/Documents/WeatherData/recon")
INV_DB = os.path.join(RECON, "file_inventory.duckdb")
OUT    = RECON   # output directory

# ---------------------------------------------------------------------------
# Color palette (consistent across all charts)
# ---------------------------------------------------------------------------
RES_COLORS = {
    '1min':          '#1f77b4',   # blue
    '15min':         '#2ca02c',   # green
    'hourly':        '#d62728',   # red
    'hourly_precip': '#ff7f0e',   # orange
    'daily':         '#aec7e8',   # light blue
    'multi':         '#ffd700',   # gold
    'none':          '#c7c7c7',   # gray
}

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
def load_data():
    con = duckdb.connect(INV_DB)
    df = con.execute("""
        SELECT station_id_raw, id_format, source, temporal_resolution,
               file_count, total_rows, total_gb,
               earliest_date, latest_date, years_span,
               empty_files, error_files,
               station_name, state, lat, lon
        FROM station_coverage_geo
        WHERE lat IS NOT NULL AND lon IS NOT NULL
          AND lat BETWEEN -90 AND 90
          AND lon BETWEEN -180 AND 180
    """).fetchdf()
    con.close()
    print(f"Loaded {len(df):,} geo-located station-records")
    return df


# ---------------------------------------------------------------------------
# Cartopy helpers
# ---------------------------------------------------------------------------
def get_us_extent():
    return [-126, -65, 23, 51]   # CONUS lon/lat bounds

def make_us_axes(fig, rect, projection=None):
    """Create a cartopy axes with US state/country features."""
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    if projection is None:
        projection = ccrs.LambertConformal(central_longitude=-96, central_latitude=38)
    ax = fig.add_axes(rect, projection=projection)
    ax.set_extent(get_us_extent(), crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.STATES.with_scale('50m'), linewidth=0.4,
                   edgecolor='#666666', facecolor='none')
    ax.add_feature(cfeature.COASTLINE.with_scale('50m'), linewidth=0.5,
                   edgecolor='#333333')
    ax.add_feature(cfeature.BORDERS.with_scale('50m'), linewidth=0.5,
                   edgecolor='#333333')
    return ax

def make_ak_axes(fig, rect):
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    proj = ccrs.LambertConformal(central_longitude=-152, central_latitude=63)
    ax = fig.add_axes(rect, projection=proj)
    ax.set_extent([-170, -130, 53, 72], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.STATES.with_scale('50m'), linewidth=0.3,
                   edgecolor='#666666', facecolor='none')
    ax.add_feature(cfeature.COASTLINE.with_scale('50m'), linewidth=0.4,
                   edgecolor='#333333')
    return ax

def make_hi_axes(fig, rect):
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    proj = ccrs.LambertConformal(central_longitude=-157, central_latitude=20)
    ax = fig.add_axes(rect, projection=proj)
    ax.set_extent([-161, -154, 18, 23], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.STATES.with_scale('50m'), linewidth=0.3,
                   edgecolor='#666666', facecolor='none')
    ax.add_feature(cfeature.COASTLINE.with_scale('50m'), linewidth=0.4,
                   edgecolor='#333333')
    return ax


def scatter_on_ax(ax, lons, lats, **kwargs):
    import cartopy.crs as ccrs
    if len(lons) == 0:
        return
    ax.scatter(lons, lats, transform=ccrs.PlateCarree(), **kwargs)


# ---------------------------------------------------------------------------
# Map 1: Station locations by data resolution
# ---------------------------------------------------------------------------
def map1_station_types(df, total_master_stations):
    import cartopy.crs as ccrs
    print("  Drawing Map 1: station types...")

    # Determine per-lat/lon how many resolutions
    station_res = (
        df.groupby(['lat', 'lon'])['temporal_resolution']
        .apply(lambda x: set(x)).reset_index()
    )
    station_res.columns = ['lat', 'lon', 'resolutions']
    station_res['n_res'] = station_res['resolutions'].apply(len)
    station_res['dominant'] = station_res['resolutions'].apply(
        lambda s: (
            'multi'         if len(s) > 1 else
            '1min'          if '1min' in s else
            '15min'         if '15min' in s else
            'hourly'        if 'hourly' in s else
            'hourly_precip' if 'hourly_precip' in s else
            'daily'         if 'daily' in s else
            'none'
        )
    )

    fig = plt.figure(figsize=(16, 11), dpi=300)
    ax  = make_us_axes(fig, [0.01, 0.08, 0.82, 0.86])

    # Layer order: daily (background) → hourly_precip → hourly → 15min → 1min → multi
    layer_order = [
        ('daily',         '#aec7e8', 0.35, 2,  'Daily (GHCN)'),
        ('hourly',        '#d62728', 0.55, 4,  'Hourly ISD (ASOS)'),
        ('hourly_precip', '#ff7f0e', 0.65, 4,  'Hourly Precip (HPD)'),
        ('15min',         '#2ca02c', 0.75, 5,  '15-min (NCEI)'),
        ('1min',          '#1f77b4', 0.85, 6,  '1-min (ASOS)'),
        ('multi',         '#ffd700', 0.95, 7,  'Multi-resolution'),
    ]

    counts = {}
    for res, color, alpha, size, label in layer_order:
        mask = station_res['dominant'] == res
        sub  = station_res[mask]
        counts[label] = len(sub)
        scatter_on_ax(ax, sub['lon'].values, sub['lat'].values,
                      c=color, alpha=alpha, s=size, linewidths=0, zorder=2)

    # AK inset
    ax_ak = make_ak_axes(fig, [0.01, 0.06, 0.22, 0.22])
    ax_hi = make_hi_axes(fig, [0.24, 0.06, 0.12, 0.12])
    for inset_ax in [ax_ak, ax_hi]:
        for res, color, alpha, size, label in layer_order:
            mask = station_res['dominant'] == res
            sub  = station_res[mask]
            scatter_on_ax(inset_ax, sub['lon'].values, sub['lat'].values,
                          c=color, alpha=alpha, s=max(1, size - 2), linewidths=0, zorder=2)
    ax_ak.set_title('Alaska', fontsize=7, pad=2)
    ax_hi.set_title('Hawaii', fontsize=7, pad=2)

    # Legend
    handles = [
        mpatches.Patch(color=color, alpha=alpha,
                       label=f"{label} ({counts.get(label, 0):,})")
        for _, color, alpha, _, label in layer_order
    ]
    fig.legend(handles=handles, loc='lower right',
               bbox_to_anchor=(0.99, 0.12),
               fontsize=9, framealpha=0.9, title='Resolution', title_fontsize=9)

    fig.suptitle(
        f"US Weather Station Coverage — {total_master_stations:,} stations across 5 sources",
        fontsize=14, fontweight='bold', y=0.97
    )
    out = os.path.join(OUT, 'map_stations_by_type.png')
    fig.savefig(out, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"  Saved: {out}")


# ---------------------------------------------------------------------------
# Map 2: Temporal depth per resolution
# ---------------------------------------------------------------------------
def map2_temporal_depth(df):
    import cartopy.crs as ccrs
    print("  Drawing Map 2: temporal depth...")

    resolutions = ['1min', '15min', 'hourly', 'hourly_precip', 'daily']
    res_labels  = ['1-min', '15-min', 'Hourly ISD', 'Hourly Precip', 'Daily']

    fig = plt.figure(figsize=(20, 10), dpi=300)
    cmap = plt.cm.viridis

    # Shared colorbar range
    all_spans = df['years_span'].dropna()
    vmin, vmax = 0, min(int(all_spans.quantile(0.98)) + 1, 130)

    axes = []
    for i, (res, label) in enumerate(zip(resolutions, res_labels)):
        sub = df[df['temporal_resolution'] == res].copy()
        sub = sub.dropna(subset=['years_span', 'lat', 'lon'])

        rect = [0.02 + i * 0.196, 0.06, 0.18, 0.82]
        ax   = make_us_axes(fig, rect)
        axes.append(ax)
        ax.set_title(f"{label}\n({len(sub):,} stations)", fontsize=9)

        if len(sub) > 0:
            sc = ax.scatter(sub['lon'].values, sub['lat'].values,
                            c=sub['years_span'].values,
                            cmap=cmap, vmin=vmin, vmax=vmax,
                            s=3, alpha=0.7, linewidths=0,
                            transform=ccrs.PlateCarree(), zorder=2)

    # Shared colorbar
    cax = fig.add_axes([0.92, 0.15, 0.015, 0.65])
    sm  = plt.cm.ScalarMappable(cmap=cmap, norm=mcolors.Normalize(vmin=vmin, vmax=vmax))
    sm.set_array([])
    cb = fig.colorbar(sm, cax=cax)
    cb.set_label('Years of data (span)', fontsize=10)

    fig.suptitle('Temporal Depth by Network — Years of Record per Station',
                 fontsize=13, fontweight='bold', y=0.98)
    out = os.path.join(OUT, 'map_temporal_depth.png')
    fig.savefig(out, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"  Saved: {out}")


# ---------------------------------------------------------------------------
# Map 3: Data density heatmap (log total_rows per 0.5° grid cell)
# ---------------------------------------------------------------------------
def map3_data_density(df):
    import cartopy.crs as ccrs
    from matplotlib.colors import LogNorm
    print("  Drawing Map 3: data density...")

    resolutions = ['1min', '15min', 'hourly', 'hourly_precip', 'daily']
    res_labels  = ['1-min', '15-min', 'Hourly ISD', 'Hourly Precip', 'Daily']

    fig = plt.figure(figsize=(20, 10), dpi=300)

    lon_bins = np.arange(-126, -64, 0.5)
    lat_bins = np.arange(23,   52,  0.5)

    for i, (res, label) in enumerate(zip(resolutions, res_labels)):
        sub = df[df['temporal_resolution'] == res].copy()
        sub = sub.dropna(subset=['total_rows', 'lat', 'lon'])
        sub = sub[(sub['lon'] > -126) & (sub['lon'] < -65) &
                  (sub['lat'] > 23)  & (sub['lat'] < 51)]

        rect = [0.02 + i * 0.196, 0.06, 0.18, 0.82]
        ax   = make_us_axes(fig, rect)
        ax.set_title(f"{label}\n({len(sub):,} stations)", fontsize=9)

        if len(sub) > 0:
            # Aggregate total_rows per grid cell
            i_lon = np.digitize(sub['lon'].values, lon_bins) - 1
            i_lat = np.digitize(sub['lat'].values, lat_bins) - 1
            grid  = np.zeros((len(lat_bins), len(lon_bins)))
            for il, ila, rows in zip(i_lon, i_lat, sub['total_rows'].fillna(0).values):
                if 0 <= il < len(lon_bins) and 0 <= ila < len(lat_bins):
                    grid[ila, il] += max(rows, 0)

            # Mask zeros
            grid_masked = np.ma.masked_where(grid == 0, grid)
            lon_c = (lon_bins[:-1] + lon_bins[1:]) / 2
            lat_c = (lat_bins[:-1] + lat_bins[1:]) / 2

            vmax = grid_masked.max() if grid_masked.count() > 0 else 1
            ax.pcolormesh(lon_bins, lat_bins, grid_masked[:-1, :-1],
                          transform=ccrs.PlateCarree(),
                          cmap='YlOrRd',
                          norm=LogNorm(vmin=1, vmax=max(vmax, 2)),
                          zorder=2, alpha=0.85)

    fig.suptitle('Data Density — log(Total Rows) per 0.5° Grid Cell by Network',
                 fontsize=13, fontweight='bold', y=0.98)
    out = os.path.join(OUT, 'map_data_density.png')
    fig.savefig(out, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f"  Saved: {out}")


# ---------------------------------------------------------------------------
# Chart 4: Network timeline (active stations per year)
# ---------------------------------------------------------------------------
def chart4_timeline(df):
    print("  Drawing Chart 4: network timeline...")

    resolutions = ['1min', '15min', 'hourly', 'hourly_precip', 'daily']
    res_labels  = ['1-min ASOS', '15-min NCEI', 'Hourly ISD', 'Hourly Precip', 'Daily GHCN']
    colors      = ['#1f77b4', '#2ca02c', '#d62728', '#ff7f0e', '#aec7e8']

    years = np.arange(1900, 2027)

    def get_year(s):
        if s and len(str(s)) >= 4:
            try:
                return int(str(s)[:4])
            except ValueError:
                pass
        return None

    df2 = df.copy()
    df2['yr_min'] = df2['earliest_date'].apply(get_year)
    df2['yr_max'] = df2['latest_date'].apply(get_year)

    fig, ax = plt.subplots(figsize=(14, 7), dpi=300)

    for res, label, color in zip(resolutions, res_labels, colors):
        sub = df2[df2['temporal_resolution'] == res].dropna(subset=['yr_min', 'yr_max'])
        counts = []
        for yr in years:
            n = ((sub['yr_min'] <= yr) & (sub['yr_max'] >= yr)).sum()
            counts.append(n)
        if max(counts) > 0:
            ax.plot(years, counts, label=f"{label} (peak {max(counts):,})",
                    color=color, linewidth=2)
            ax.fill_between(years, counts, alpha=0.12, color=color)

    ax.set_xlabel('Year', fontsize=11)
    ax.set_ylabel('Active Stations', fontsize=11)
    ax.set_title('Network Growth — Active Stations per Year by Resolution',
                 fontsize=13, fontweight='bold')
    ax.legend(fontsize=9, loc='upper left')
    ax.set_xlim(1900, 2027)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
    ax.grid(True, alpha=0.3)
    ax.axvline(2026, color='#999999', linestyle='--', linewidth=0.8, alpha=0.6)
    ax.text(2026.3, ax.get_ylim()[1] * 0.95, '2026', fontsize=8, color='#666666')

    fig.tight_layout()
    out = os.path.join(OUT, 'chart_timeline.png')
    fig.savefig(out, dpi=300, facecolor='white')
    plt.close(fig)
    print(f"  Saved: {out}")


# ---------------------------------------------------------------------------
# Chart 5: State bar chart (stacked by resolution)
# ---------------------------------------------------------------------------
def chart5_states(df):
    print("  Drawing Chart 5: state bars...")

    resolutions = ['1min', '15min', 'hourly', 'hourly_precip', 'daily']
    res_labels  = ['1-min', '15-min', 'Hourly ISD', 'Hourly Precip', 'Daily']
    colors      = ['#1f77b4', '#2ca02c', '#d62728', '#ff7f0e', '#aec7e8']

    # Aggregate per state × resolution
    sub = df.dropna(subset=['state'])
    sub = sub[sub['state'].str.len() == 2]

    by_state = {}
    for state, grp in sub.groupby('state'):
        by_state[state] = {}
        for res in resolutions:
            by_state[state][res] = grp[grp['temporal_resolution'] == res]['station_id_raw'].nunique()

    # Sort by total
    states_sorted = sorted(by_state.keys(), key=lambda s: sum(by_state[s].values()), reverse=True)

    # US states + DC only
    VALID_STATES = {
        'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN',
        'IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV',
        'NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN',
        'TX','UT','VT','VA','WA','WV','WI','WY','DC',
    }
    states_sorted = [s for s in states_sorted if s in VALID_STATES]

    fig, ax = plt.subplots(figsize=(10, 14), dpi=300)
    y = np.arange(len(states_sorted))

    lefts = np.zeros(len(states_sorted))
    for res, label, color in zip(resolutions, res_labels, colors):
        vals = np.array([by_state[s].get(res, 0) for s in states_sorted], dtype=float)
        bars = ax.barh(y, vals, left=lefts, color=color, alpha=0.85, label=label, height=0.7)
        lefts += vals

    ax.set_yticks(y)
    ax.set_yticklabels(states_sorted, fontsize=8)
    ax.set_xlabel('Number of Stations', fontsize=10)
    ax.set_title('Station Coverage by State and Resolution',
                 fontsize=12, fontweight='bold')
    ax.legend(loc='lower right', fontsize=9)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{int(x):,}'))
    ax.grid(True, axis='x', alpha=0.3)
    ax.set_xlim(left=0)

    fig.tight_layout()
    out = os.path.join(OUT, 'chart_states.png')
    fig.savefig(out, dpi=300, facecolor='white')
    plt.close(fig)
    print(f"  Saved: {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("STEP 5: Generating coverage visualizations")
    print("=" * 60)

    # Total master station count
    con = duckdb.connect(INV_DB)
    total_master = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{os.path.join(RECON, 'master_station_list.parquet')}')"
    ).fetchone()[0]
    con.close()

    df = load_data()

    import time
    t0 = time.time()

    map1_station_types(df, total_master)
    map2_temporal_depth(df)
    map3_data_density(df)
    chart4_timeline(df)
    chart5_states(df)

    elapsed = time.time() - t0
    print(f"\nAll visualizations complete in {elapsed:.1f}s")
    print(f"Output directory: {OUT}")
    for fn in ['map_stations_by_type.png', 'map_temporal_depth.png',
               'map_data_density.png', 'chart_timeline.png', 'chart_states.png']:
        fp = os.path.join(OUT, fn)
        sz = os.path.getsize(fp) / 1e6 if os.path.exists(fp) else 0
        print(f"  {fn:<35s}  {sz:.1f} MB")


if __name__ == '__main__':
    main()
