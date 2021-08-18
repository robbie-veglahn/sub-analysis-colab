import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import us
import contextily as ctx
import requests

# global font
font = {'fontname':'Helvetica'}

# lookup for mggg-states shapefile raw links
mggg_states = {
    'Ohio': 'https://github.com/mggg-states/OH-shapefiles/blob/master/OH_precincts.zip?raw=true',
    'Alaska': 'https://github.com/mggg-states/AK-shapefiles/blob/master/AK_precincts.zip?raw=true',
    'Michigan': 'https://github.com/mggg-states/MI-shapefiles/blob/main/MI.zip?raw=true',
    'Wisconsin': 'https://github.com/mggg-states/WI-shapefiles/blob/master/WI_2020_wards.zip?raw=true',
    'Wisconsin10': 'https://github.com/mggg-states/WI-shapefiles/blob/master/WI_2011_wards.zip?raw=true',
    'Maryland': 'https://github.com/mggg-states/MD-shapefiles/blob/master/MD_precincts.zip?raw=true',
    'North Carolina': 'https://github.com/mggg-states/NC-shapefiles/blob/master/NC_VTD.zip?raw=true',
    'New Hampshire': 'https://github.com/mggg-states/NH-shapefiles/blob/main/NH.zip?raw=true',
    'Virginia': 'https://github.com/mggg-states/VA-shapefiles/blob/master/VA_precincts.zip?raw=true',
    'Massachusetts': 'https://github.com/mggg-states/MA-shapefiles/blob/master/MA_precincts_12_16.zip?raw=true',
    'Indiana': 'https://github.com/mggg-states/IN-shapefiles/blob/main/Indiana.zip?raw=true',
    'Puerto Rico': 'https://github.com/mggg-states/PR-shapefiles/blob/main/PR.zip?raw=true',
    'Nebraska': 'https://github.com/mggg-states/NE-shapefiles/blob/main/NE.zip?raw=true',
    'Maine': 'https://github.com/mggg-states/ME-shapefiles/blob/master/Maine.zip?raw=true',
    'Pennsylvania': 'https://github.com/mggg-states/PA-shapefiles/blob/master/PA.zip?raw=true',
    'Louisiana': 'https://github.com/mggg-states/LA-shapefiles/blob/main/LA_1519.zip?raw=true',
    'Minnesota': 'https://github.com/mggg-states/MN-shapefiles/blob/master/MN12_18.zip?raw=true',
    'Delaware': 'https://github.com/mggg-states/DE-shapefiles/blob/master/DE_precincts.zip?raw=true',
    'Arizona': 'https://github.com/mggg-states/AZ-shapefiles/blob/master/az_precincts.zip',
    'Connecticut': 'https://github.com/mggg-states/CT-shapefiles/blob/master/CT_precincts.zip?raw=true',
    'Georgia': 'https://github.com/mggg-states/GA-shapefiles/blob/master/GA_precincts.zip?raw=true',
    'Hawaii': 'https://github.com/mggg-states/HI-shapefiles/blob/master/HI_precincts.zip?raw=true',
    'Colorado': 'https://github.com/mggg-states/CO-shapefiles/blob/master/CO_precincts.zip?raw=true',
    'Oklahoma': 'https://github.com/mggg-states/OK-shapefiles/blob/master/OK_precincts.zip?raw=true',
    'Utah': 'https://github.com/mggg-states/UT-shapefiles/blob/master/UT_precincts.zip?raw=true',
    'Oregon': 'https://github.com/mggg-states/OR-shapefiles/blob/master/OR_precincts.zip?raw=true',
    'New Mexico': 'https://github.com/mggg-states/NM-shapefiles/blob/master/new_mexico_precincts.zip?raw=true',
    'Missouri': 'https://github.com/mggg-states/MO-shapefiles/blob/master/MO_vtds.zip?raw=true',
    'Vermont': 'https://github.com/mggg-states/VT-shapefiles/blob/master/VT_towns.zip?raw=true',
    'Texas': 'https://people.csail.mit.edu/ddeford/TX_vtds.zip',
    'Rhode Island': 'https://github.com/mggg-states/RI-shapefiles/blob/master/RI_precincts.zip?raw=true',
    'Iowa': 'https://github.com/mggg-states/IA-shapefiles/blob/master/IA_counties.zip?raw=true'
}

# takes in an assignment df (coi_df from fetch) and spits it out with geometries
# takes in an assignment df and spits it out with geometries
def assignment_to_shape(df):
    # add a units row to the df
    crs = None
    df['units'] = df['districtr_data'].apply(lambda x: x['plan']['units']['id'])
    try:
        state = df.iloc[0]['districtr_data']['plan']['place']['state']
    except:
        print(f"ERROR: {len(df)} COI SUBMISSIONS")
        return None
    
    fips = us.states.lookup(state).fips
    
    acc = pd.DataFrame(columns = ['id', 'plan_id', 'coi_id', 'tile_id', 'geometry'])
    # iterate over units
    for unit in set(df['units']):
        print(f'Downloading shapefile for {unit.upper()}')
        # download appropriate shape
        if unit == "blockgroups":
            link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state.replace(" ", "_")}/{fips}/tl_2010_{fips}_bg10.zip'
        elif unit == "blocks":
            link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state.replace(" ", "_")}/{fips}/tl_2010_{fips}_tabblock10.zip'
        else:
            link = mggg_states[state]
        shp = gpd.read_file(link)

        # get everything into the same crs
        if not crs:
            crs = shp.crs
            print(f'Projecting into crs {crs.to_epsg()}')
        else:
            shp = shp.to_crs(crs)

        subset = df[df['units'] == unit]
        print(f'{len(subset)} submissions using {unit}')
        
        # each COI is a row
        for idx, row in subset.iterrows():
            # get all info
            plan_id = row['plan_id']
            key = row['districtr_data']['plan']['idColumn']['key']
            if state == "Wisconsin" and key == "GEOID10" and unit == "wards":
                print("Skipping a plan because it is on old WI wards")
                continue
                
            # cast everything to int (and do some error checking)
            casting = True
            try:
                shp[key] = shp[key].apply(int)
            except KeyError:
                if key == "GEOID":
                    try:
                        key = "GEOID10"
                        shp[key] = shp[key].apply(int)
                    except KeyError:
                        print("ERROR: GEOID and GEOID10 not in shapefile.")
                        continue
                else:
                    print(f"ERROR: {key} not in shapefile.")
                    continue
            except ValueError:
                shp[key] = shp[key] # can't be turned to an int (not a GEOID)
                casting = False
                
            try:
                asn = row['districtr_data']['plan']['assignment']
            except KeyError: # empty plan
                print("Empty plan...")
                continue

            # make lists
            ids = []
            plan_ids = []
            coi_ids = []
            tile_ids = []
            geoms = []
            for k, v in asn.items():
                # cast everything to int if we successfully cast the key column
                if casting:
                    try:
                        k = int(k)
                    except ValueError:
                        k = k # see above
                 
                if isinstance(v, list):
                    for v_prime in v:
                        ids.append(f'{plan_id}-{v_prime}')
                        plan_ids.append(plan_id)
                        coi_ids.append(v_prime)
                        tile_ids.append(k)
                        geoms.append(shp[shp[key] == k]['geometry'].iloc[0])
                else:
                    ids.append(f'{plan_id}-{v}')
                    plan_ids.append(plan_id)
                    coi_ids.append(v)
                    tile_ids.append(k)
                    geoms.append(shp[shp[key] == k]['geometry'].iloc[0])
            tmp = pd.DataFrame(zip(ids, plan_ids, coi_ids, tile_ids, geoms), 
                               columns = ['id', 'plan_id', 'coi_id', 'tile_id', 'geometry'])
            acc = acc.append(tmp, ignore_index = True)
    return gpd.GeoDataFrame(acc, crs = crs)
               
# in these, clip_bounds can either be a capitalized state name or a geometry to clip to
def plot_coi_boundaries(coi_df, clip_bounds, 
                        osm = False, outfile = None, 
                        show = True, title = None,
                        writer = None, weekly = False, monday = None):
    statewide = isinstance(clip_bounds, str)
    if statewide:
        state_gdf = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_5m.zip')
        clip_bounds = state_gdf[state_gdf['NAME'] == clip_bounds]
    clip_bounds = clip_bounds.to_crs(coi_df.crs)
    if osm:
        # 3857 from geopandas docs
        clip_bounds = clip_bounds.to_crs(3857)
        coi_df = coi_df.to_crs(3857)
    clipped = gpd.clip(coi_df, clip_bounds)
    # if we have to clip to the expanded bounding box
    if not statewide:
        clipped_ids = clipped['id']
        # get our bounding box
        bbox = clip_bounds.to_crs(coi_df.crs).buffer(20000).envelope
        # clip the cois to the bounding box
        clipped = gpd.clip(coi_df, bbox)
        # limit to only in the original bounds
        clipped = clipped[clipped['id'].isin(clipped_ids)]

    
    if (len(clipped) == 0):
        print(f"No COIs in {title}")
        if writer:
            if weekly:
                writer.write(f" Date Range: {monday - np.timedelta64(7)} - {monday}\n")
            else:
                writer.write(f" Date Range: cumulative through {monday}\n")
            writer.write(" 0 areas of interest from 0 submissions\n")
        return


    fig, ax = plt.subplots(figsize = (20,10))
    dissolved = gpd.clip(coi_df.dissolve(by = 'id'), clip_bounds).buffer(0)
    # to avoid some errors
    dissolved = dissolved[~dissolved.is_empty]
    ax.set_axis_off()
    dissolved.boundary.plot(ax = ax, cmap = 'tab20')
    clipped.plot(ax = ax, column = 'id', cmap = 'tab20', alpha = 0.5)
    clip_bounds.boundary.plot(ax = ax, color = 'black', linewidth = 2)
    ncois = len(list(set(clipped["id"])))
    nsubs = len(list(set(clipped["plan_id"])))
    if writer:
        if weekly:
            writer.write(f" Date Range: {monday - np.timedelta64(7)} - {monday}\n")
        else:
            writer.write(f" Date Range: cumulative through {monday}\n")
        writer.write(f" {ncois} areas of interest from {nsubs} submissions\n")

    if osm:
        try:
            ctx.add_basemap(ax, alpha = 0.5)
        except requests.HTTPError:
            ctx.add_basemap(ax, alpha = 0.5, source = 'http://a.tile.openstreetmap.org/{z}/{x}/{y}.png')
    if outfile:
        plt.savefig(f"{outfile}.png", bbox_inches = "tight")
    if show:
        plt.show()
    plt.close()
    
def plot_coi_heatmap(coi_df, clip_bounds, color = 'purple', osm = False, outfile = None, show = True, title = None):
    statewide = isinstance(clip_bounds, str)
    if statewide:
        state_gdf = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_5m.zip')
        clip_bounds = state_gdf[state_gdf['NAME'] == clip_bounds]
    clip_bounds = clip_bounds.to_crs(coi_df.crs)
    if osm:
        # 3857 from geopandas docs
        clip_bounds = clip_bounds.to_crs(3857)
        coi_df = coi_df.to_crs(3857)
    clipped = gpd.clip(coi_df, clip_bounds)
    # if we have to clip to the expanded bounding box
    if not statewide:
        clipped_ids = clipped['id']
        # get our bounding box
        bbox = clip_bounds.to_crs(coi_df.crs).buffer(20000).envelope
        # clip the cois to the bounding box
        clipped = gpd.clip(coi_df, bbox)
        # limit to only in the original bounds
        clipped = clipped[clipped['id'].isin(clipped_ids)]

    if (len(clipped) == 0):
        print(f"No COIs in {title}")
        return

    # no file writing in this one, all in boundaries
    fig, ax = plt.subplots(figsize = (20,10))
    ax.set_axis_off()
    clip_bounds.boundary.plot(ax = ax, color = 'black', linewidth = 2)
    clipped.plot(ax = ax, color = color, alpha = 0.05)
    if osm:
        try:
            ctx.add_basemap(ax, alpha = 0.5)
        except requests.HTTPError:
            ctx.add_basemap(ax, alpha = 0.5, source = 'http://a.tile.openstreetmap.org/{z}/{x}/{y}.png')
    if outfile:
        plt.savefig(f"{outfile}.png", bbox_inches = "tight")
    if show:
        plt.show()
    plt.close()