import fetch
import coi_maps
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import us
import fetch
import contextily as ctx

pref_units = {
    "Michigan": 'blockgroups',
    "Missouri": 'blockgroups',
    "New Mexico": 'precincts',
    "Ohio": 'blockgroups',
    "Texas": 'precincts',
    "Utah": 'blocks',
    "Virginia": 'precincts',
    "Wisconsin": 'wards'
}

def assignment_to_pivot(df, outfile = None):
    # add a units col to the df
    df['units'] = df['districtr_data'].apply(lambda x: x['plan']['units']['id'])
    try:
        state = df.iloc[0]['districtr_data']['plan']['place']['state']
    except:
        print(f"ERROR: {len(df)} COI SUBMISSIONS")
        return None
    unit = pref_units[state]
    
    fips = us.states.lookup(state).fips
    
    acc = pd.DataFrame(columns = ['id', 'plan_id', 'coi_id', 'tile_id', 'geometry'])
        
    # download appropriate shape
    if unit == "blockgroups":
        link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state.replace(" ", "_")}/{fips}/tl_2010_{fips}_bg10.zip'
    elif unit == "blocks":
        link = f'https://www2.census.gov/geo/pvs/tiger2010st/{fips}_{state.replace(" ", "_")}/{fips}/tl_2010_{fips}_tabblock10.zip'
    else:
        link = coi_maps.mggg_states[state]
    shp = gpd.read_file(link)
    

    subset = df[df['units'] == unit]
    if len(subset) == 0:
        print(f"No COIs submitted on {unit} yet in {state}")
        return

    key = subset.iloc[0]['districtr_data']['plan']['idColumn']['key']
    if state == 'Wisconsin':
        key = "Code-2"


    # cast everything to int (and do some error checking)
    try:
        shp[key] = shp[key].apply(int)
    except KeyError:
        if key == "GEOID":
            try:
                key = "GEOID10"
                shp[key] = shp[key].apply(int)
            except KeyError:
                print("ERROR: GEOID and GEOID10 not in shapefile.")
                return None
        else:
            print(f"ERROR: {key} not in shapefile.")
            return None
    except ValueError:
        shp[key] = shp[key] # can't be turned to an int (not a GEOID)
            
    tiles = list(shp[key].apply(str))
    cols = ['submission_text', 'area_text', 'area_name'] + tiles
    pivot = pd.DataFrame(columns = cols)

    # each COI is a row
    for _idx, row in subset.iterrows():
        # get all info
        plan_id = row['plan_id']
        row_key = row['districtr_data']['plan']['idColumn']['key']
        if state == "Wisconsin" and row_key == "GEOID10" and unit == "wards":
            continue

        try:
            asn = row['districtr_data']['plan']['assignment']
        except KeyError: # empty plan
            continue

        sub_text = row['text']
        parts = row['districtr_data']['plan']['parts']
        texts = {p['id']: p['name'] for p in parts}
        titles = {p['id']: (p['description'] if 'description' in p else "") for p in parts}

        # make lists
        assigned = asn.keys()
        distinct_cois = {}
        for tile in assigned:
            tmp = asn[tile]
            if not isinstance(tmp, list):
                tmp = [tmp]
            for coi in tmp:
                if coi not in distinct_cois.keys():
                    distinct_cois[coi] = {
                        'sub_text': sub_text,
                        'title': titles[coi] if coi in titles else "",
                        'area_text': texts[coi] if coi in texts else "",
                        'tiles': []
                    }
                distinct_cois[coi]['tiles'].append(tile)

        
        plan_ids = [f'{plan_id}-{d+1}' for d in distinct_cois.keys()]
        acc = pd.DataFrame(index = plan_ids, columns = cols)
        for (d, p) in zip(distinct_cois.keys(), plan_ids):
            acc.at[p, 'submission_text'] = distinct_cois[d]['sub_text']
            acc.at[p, 'area_text'] = distinct_cois[d]['area_text']
            acc.at[p, 'area_name'] = distinct_cois[d]['title']
            for t in distinct_cois[d]['tiles']:
                acc.at[p, t] = 1
        pivot = pivot.append(acc)
        
    pivot = pivot.fillna(0)
    if outfile:
        pivot.to_csv(outfile)
    return pivot