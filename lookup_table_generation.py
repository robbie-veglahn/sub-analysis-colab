import fetch
import coi_maps
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import us
import fetch
import contextily as ctx
import coi_dataset
import coi_final_report as coi_report
from typing import Tuple
import utils as utils

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
    # This code was written for final coi submission analysis for these 4 states
    "michigan": 'blockgroups',
    "missouri": 'blockgroups',
    "ohio": 'blockgroups',
    "wisconsin": 'wards'
}

# lookup for mggg-states shapefile raw links
mggg_states = {
    'ohio': 'https://github.com/mggg-states/OH-shapefiles/blob/master/OH_precincts.zip?raw=true',
    'michigan': 'https://github.com/mggg-states/MI-shapefiles/blob/main/MI.zip?raw=true',
    'wisconsin': 'https://github.com/mggg-states/WI-shapefiles/blob/master/WI_2020_wards.zip?raw=true',
    'wisconsin10': 'https://github.com/mggg-states/WI-shapefiles/blob/master/WI_2011_wards.zip?raw=true',
    'missouri': 'https://github.com/mggg-states/MO-shapefiles/blob/master/MO_vtds.zip?raw=true',
}

def generate_full_lookup_table(state: str, outfile = None) -> None:
    """
    Takes in a state as a string and an optional outfile to export to as a csv,
    and returns a full lookup table in the same format produced by Jack's ...
    assignment_to_pivot function in coi_dataset. Will contain plan id, area text,
    area name, submission text, and all assignments on whatever unit is ...
    "preferred" to be drawn in by our portal states.

    NOTE: this function produces lookup tables that Ari and Other Parker (tm)...
    use to geographically cluster. Computationally expensive, takes awhile!
    """
    print("fetching endpnts")
    ids_url, plans_url, cois_url, written_url, subs = utils.submission_endpts(state)
    print(ids_url, plans_url, cois_url, written_url, subs)
    print("fetching submissions")
    plans_df, cois_df, _ = fetch.submissions(
                                     ids_url, plans_url, cois_url, written_url)
    print("fetched submissions! len plans: {} len cois: {}".format(
                                      len(plans_df), len(cois_df)))
    print("fetching singletons...")
    singleton_dists  = coi_report.find_pseudo_cois(plans_df)
    print("found singletons! len singletons: {}".format)

    # Filter out invalid plans
    idxs = []
    for _idx, row in cois_df.iterrows():
        if row['districtr_data']['msg'] != 'Plan successfully found':
            idxs.append(_idx)
    cois_df = cois_df.drop(idxs)
    idxs = []
    for _idx, row in singleton_dists.iterrows():
        if row['districtr_data']['msg'] != 'Plan successfully found':
            idxs.append(_idx)
    singleton_dists = singleton_dists.drop(idxs)

    coi_lookup_table = generate_lookup_tables(state, cois_df)
    print("coi lookup table generated! now generating plan")
    # If no singleton districts exist, stop and return only cois
    if singleton_dists is None:
        print("returning just cois")
        return coi_lookup_table
    elif len(singleton_dists) == 0: #initial none-check to avoid invalid len()
        print("returning just cois")
        return coi_lookup_table

    plans_lookup_table = generate_lookup_tables(state, singleton_dists)
    # If plans lookup table is empty, return only coi lookup
    if plans_lookup_table is None:
        print("returning just cois")
        return coi_lookup_table
    elif len(plans_lookup_table) == 0:
        print("returning just cois")
        return coi_lookup_table
    print("length of coi lookup table: {} length of plan lookup: {}".format(
                            len(coi_lookup_table), len(plans_lookup_table)))
    coi_lookup_table.columns = coi_lookup_table.columns.astype(str)
    plans_lookup_table.columns = plans_lookup_table.columns.astype(str)
    complete_lookup = coi_lookup_table.append(plans_lookup_table)
    complete_lookup = complete_lookup.fillna(0)
    if outfile != None:
        complete_lookup.to_csv(outfile)
    return complete_lookup

def generate_lookup_tables(state: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes in a state as a string and a dataframe to convert to a lookup table,
    and generates a lookup table in the same format produced by Jack's ...
    assignment_to_pivot function in coi_dataset. Will 

    NOTE: this is a helper function for generate_full_lookup_tables, and...
    is used to walk over submissions drawn on precincts to the preferred...
    block group units
    """
    if 'districtr_data' not in df:
        print("ERROR: df contains no 'districtr_data' field, returning None object")
        return
    # determine pref units, and find subsets drawn in pref units and...
    # ...subsets drawn in precincst
    unit = pref_units[state]
    temp = df
    temp['units'] = temp['districtr_data'].apply(lambda x: x['plan']['units']['id'])
    subset = temp[temp['units'] == unit]
    precinct_subset = temp[temp['units'] == 'precincts']
    print("this is the len of pref unit subset: {} this is len of precinct unit subset: {}".format(
                                                                len(subset), len(precinct_subset)))
    pref_pivot = None # initializing sentinel
    non_pref_pivot = None # initializing sentinel
    # If no precincts to move over, just return Jack's pref unit code
    if len(precinct_subset) == 0:
        pref_pivot = coi_dataset.assignment_to_pivot(df)
        print("No precinct submissions received, so using coi_dataset to generate lookup table")
        return pref_pivot
    # If there are preferred submissions, run Jack's code to generate lookup table on...
    # preferred units
    elif len(subset) != 0:
        pref_pivot = coi_dataset.assignment_to_pivot(df)
    # generates a precinct level lookup table that treats precincts as the...
    # prefered unit
    non_pref_pivot = precinct_to_pivot(df)
    if non_pref_pivot is not None:
        # take the precinct level lookup table and walk it over to a block...
        # group level lookup table in the same format as coi_dataset.py
        non_pref_lookup_bg = crosswalk_precinct_to_bg(subset, precinct_subset, state, non_pref_pivot)

    if pref_pivot is not None:
        pref_lookup = pref_pivot 
        pref_lookup.columns = pref_lookup.columns.astype(str)
        non_pref_lookup_bg.columns = non_pref_lookup_bg.columns.astype(str)
        # append preferred unit (bg) lookup and precinct (now bgs) lookup tables
        complete_pivot = pref_lookup.append(non_pref_lookup_bg)
        complete_pivot = complete_pivot.fillna(0)
        return complete_pivot
    else:
        # if no preferred units submitted, return crosswalked preferred lookup
        return non_pref_lookup_bg




def crosswalk_precinct_to_bg(subset: pd.DataFrame, precinct_subset: pd.DataFrame, state: str, precinct_pivot: pd.DataFrame) -> pd.DataFrame:
    """

    """
    print("inside crosswalk")
    path = state + "/" + state + "_bg10.shp"
    # download blockgroup shapefile
    bg_shp = gpd.read_file(path)
    link = mggg_states[state]
    # download precinct shapefile
    pcn_shp = gpd.read_file(link).to_crs(bg_shp.crs)
    if len(precinct_subset != 0):
        prcn_key = precinct_subset.iloc[0]['districtr_data']['plan']['idColumn']['key']
    else:
        prcn_key = "VTD"
    print("prcn_key is", prcn_key)
    if len(subset != 0):
        key = subset.iloc[0]['districtr_data']['plan']['idColumn']['key']
    else:
        key = "GEOID10"
    print("key is: ", key)
    if state == 'Wisconsin':
        key = "Code-2"
    try:
        bg_shp[key] = bg_shp[key].apply(int)
    except KeyError:
        if key == "GEOID":
            try:
                key = "GEOID10"
                bg_shp[key] = bg_shp[key].apply(int)
            except KeyError:
                print("ERROR: GEOID and GEOID10 not in shapefile.")
        else:
            print(f"ERROR: {key} not in shapefile.")
    except ValueError:
        bg_shp[key] = bg_shp[key]

    assignment = []
    tiles = list(bg_shp[key].apply(str))
    print(len(tiles))
    individ_cols = ['submission_text', 'area_text', 'area_name']
    cols = individ_cols + tiles
    print(len(cols))
    temp_pivot = pd.DataFrame(columns = cols)
    column = temp_pivot.columns
    print(len(column))
    for _idx, row in precinct_pivot.iterrows():
        print("hmm1 ", len(temp_pivot.columns))
        plan_id = [row.name.split("-")[0] + "-1"]
        print(plan_id)
        temp_row = row
        pct_names = []
        for key, value in row.items():
            # if key.startswith("P") and value == 1:
            if key not in individ_cols and value == 1:
                pct_names.append(key)
        coi_geoms = pcn_shp[pcn_shp[prcn_key].apply(lambda x: x in pct_names)]
        union = coi_geoms.unary_union
        possible_matches_index = bg_shp.sindex.intersection(union.bounds)
        
        possible_matches = bg_shp.iloc[possible_matches_index]
        precise_matches_vtd = list(possible_matches[possible_matches.intersects(union)]["GEOID10"])
        print(plan_id)
        acc = pd.DataFrame(index = plan_id, columns = cols)
        print("ope", len(acc.columns))
        acc.at[plan_id, 'submission_text'] = row['submission_text']
        acc.at[plan_id, 'area_name'] = ""
        acc.at[plan_id, 'area_text'] = ""
        temp_pivot.columns = temp_pivot.columns.astype(str)
        acc.columns = acc.columns.astype(str)
        for bg in precise_matches_vtd:
            bg = str(bg)
            acc.at[plan_id, bg] = int(1)
        print(len(acc.columns))
        print("hmm", len(temp_pivot.columns))
        temp_pivot.columns = temp_pivot.columns.astype(str)
        acc.columns = acc.columns.astype(str)
        temp_pivot = temp_pivot.append(acc, sort=True)
        print("hmm2", len(temp_pivot.columns))
        print(len(set(precise_matches_vtd)))
    temp_pivot = temp_pivot.fillna(0)
    return temp_pivot



def crosswalk_bg_to_block(state: str, bg_pivot: pd.DataFrame) -> pd.DataFrame:
    """
    Takes in a state and a block group level lookup table, and walks the assigments...
    from being on block group ids (usually GEOID10s) to block level ids (usually GEOID10s).
    Returns a lookup table where each submission has a one-hot encoding of submission to
    block id assignment.

    NOTE: Can run very slow for a state like MI with a large number of blocks, so...
    recomend running on the cluster if access.
    """
    # pull down block group and block level shapefiles
    bg_path = state + "/" + state + "_bg10.shp"
    block_path = state + "/blocks/" + state + "_tabblock10.shp"
    block_shp = gpd.read_file(block_path)
    bg_shp = gpd.read_file(bg_path).to_crs(block_shp.crs)

    bg_key = "GEOID10"
    key = "GEOID10" # block group level key
    # if state == 'Wisconsin':
    #     key = "Code-2"
    try:
        block_shp[key] = block_shp[key].apply(int)
    except KeyError:
        if key == "GEOID":
            try:
                key = "GEOID10"
                block_shp[key] = block_shp[key].apply(int)
            except KeyError:
                print("ERROR: GEOID and GEOID10 not in shapefile.")
        else:
            print(f"ERROR: {key} not in shapefile.")
    except ValueError:
        block_shp[key] = block_shp[key]

    assignment = []
    # initialize all the block tile ids
    tiles = list(block_shp[key].apply(str))
    individ_cols = ['submission_text', 'area_text', 'area_name']
    cols = individ_cols + tiles
    # initialize pivot table with relevant cols
    temp_pivot = pd.DataFrame(columns = cols)
    column = temp_pivot.columns
    i = 0
    for _idx, row in bg_pivot.iterrows():
        if i % 10 == 0:
            print("In loop, on row: {}".format(i))
        if i == 1 or i == 3 or i == 5:
            print("why is this so slowwwww")
        plan_id = [row.name.split("-")[0] + "-1"]
        temp_row = row
        bg_names = []
        # loop through row's assignments and grab all block group...
        # ids that the submission is assigned to
        for key, value in row.items():
            if key not in individ_cols and value == 1:
                bg_names.append(key)
        # retrieve the geometry of the coi submission using bg id assignment
        coi_geoms = bg_shp[bg_shp[bg_key].apply(lambda x: x in bg_names)]
        union = coi_geoms.unary_union
        # find the block level interesection of the coi geometries
        possible_matches_index = block_shp.sindex.intersection(union.bounds)
        possible_matches = block_shp.iloc[possible_matches_index]
        precise_matches_blocks = list(possible_matches[possible_matches.intersects(union)]["GEOID10"])
        acc = pd.DataFrame(index = plan_id, columns = cols)
        # fill in row submission
        acc.at[plan_id, 'submission_text'] = row['submission_text']
        acc.at[plan_id, 'area_name'] = row['area_name'] if 'area_name' in row.columns else "" #@ROB BROKEN
        acc.at[plan_id, 'area_text'] = row['area_text'] if 'area_text' in row.columns else ""
        temp_pivot.columns = temp_pivot.columns.astype(str)
        acc.columns = acc.columns.astype(str)
        # fill in the new block level assignments for the row
        for bg in precise_matches_blocks:
            bg = str(bg)
            acc.at[plan_id, bg] = int(1)

        temp_pivot.columns = temp_pivot.columns.astype(str)
        acc.columns = acc.columns.astype(str)
        temp_pivot = temp_pivot.append(acc, sort=True)
        i += 1
    temp_pivot = temp_pivot.fillna(0)
    return temp_pivot


def crosswalk_2010b_to_2020b(state: str, block10_pivot: pd.DataFrame) -> pd.DataFrame:
    """
    Takes in a state and a 2010 census block level lookup table, and walks the assigments...
    from being on 2010 block ids (GEOID10s) to 2020 block level ids (GEOID20s).
    Returns a lookup table where each submission has a one-hot encoding of submission to
    2020 block id assignment.

    NOTE: Uses the NHGIS preliminary crosswalk to avoid expesive computation, should be...
    updated when NHGIS updates.
    """
    # read in the NHGIS 2010 to 2020 crosswalk reference dataframe 
    crosswalk_path = "nhgis_blk2010_blk2020_ge_v0_26/" + state + "nhgis_blk2010_blk2020_ge_v0_26.csv"
    crosswalk_ref = pd.read_csv(crosswalk_path)
    assignment = []
    # generate list of tiles
    tiles = crosswalk_ref['GEOID20'].to_list()

    individ_cols = ['submission_text', 'area_text', 'area_name']

    cols = individ_cols + tiles
    temp_pivot = pd.DataFrame(columns = cols)
    column = temp_pivot.columns
    i = 0
    block20_names = []
    for _idx, row in block10_pivot.iterrows():
        if i % 10 == 0:
            print("In loop, on row: {}".format(i))
        if i == 1 or i == 3 or i == 5:
            print("why is this so slowwwww")

        plan_id = [row.name.split("-")[0] + "-1"]
        # print(plan_id)
        temp_row = row
        block10_names = []
        # block20_names = []
        acc = pd.DataFrame(index = plan_id, columns = cols)
        # fill in row info
        acc.at[plan_id, 'submission_text'] = row['submission_text']
        acc.at[plan_id, 'area_name'] = row['area_name'] if 'area_name' in row.columns else ""
        acc.at[plan_id, 'area_text'] = row['area_text'] if 'area_name' in row.columns else ""

        for key, value in row.items():
            if key not in individ_cols and value == 1:
                block10_names.append(key)
                # store the corresponding 2020 block id
                block20 = crosswalk_ref[crosswalk_ref['GEOID10'] == key]['GEOID20']
                block20_names.append(block)
                acc.at[plan_id, block20] = int(1)

        temp_pivot.columns = temp_pivot.columns.astype(str)
        acc.columns = acc.columns.astype(str)

        # for block in block20_names:
        #     block = str(block)
        #     acc.at[plan_id, block] = int(1)

        temp_pivot.columns = temp_pivot.columns.astype(str)
        acc.columns = acc.columns.astype(str)
        temp_pivot = temp_pivot.append(acc, sort=True)
        print("end of one row!")
        i += 1
    temp_pivot = temp_pivot.fillna(0)
    return temp_pivot, block20_names

def shp_crosswalk_2010b_to_2020b(state: str, block10_pivot: pd.DataFrame, b20_shp):
    """

    """
    block20_shp = b20_shp
    block20_shp['NUMAREAS'] = 0
    crosswalk_path = "nhgis_blk2010_blk2020_ge_v0_26/" + state + "/nhgis_blk2010_blk2020_ge_v0_26.csv"
    crosswalk_ref = pd.read_csv(crosswalk_path)
    assignment = []
    # generate list of tiles
    tiles = crosswalk_ref['GEOID20'].to_list()
    individ_cols = ['submission_text', 'area_text', 'area_name']
    cols = individ_cols + tiles
    i = 0
    for _idx, row in block10_pivot.iterrows():
        plan_id = [row.name.split("-")[0] + "-1"]
        temp_row = row
        block10_names = []
        block20_names = []
        for key, value in row.items():
            if key not in individ_cols and value == 1:
                block10_names.append(key)
                # store the corresponding 2020 block id
                subset = crosswalk_ref[crosswalk_ref['GEOID10'] == int(key)]
                block20 = int(subset[subset.PAREA == subset.PAREA.max()].GEOID20)
                block20_names.append(block20)
                index = block20_shp[block20_shp['GEOID20'] == str(block20)].index
                block20_shp.at[index, 'NUMAREAS'] = block20_shp.loc[index]['NUMAREAS'] + 1

        print("end of one row!")
        i += 1
    return block20_shp

# def crosswalk_2010b_to_2020b_and_store_shp(state: str, block10_pivot: pd.DataFrame, block20_shp) -> pd.DataFrame:
#     """
#     Takes in a state and a 2010 census block level lookup table, and walks the assigments...
#     from being on 2010 block ids (GEOID10s) to 2020 block level ids (GEOID20s).
#     Returns a lookup table where each submission has a one-hot encoding of submission to
#     2020 block id assignment.

#     NOTE: Uses the NHGIS preliminary crosswalk to avoid expesive computation, should be...
#     updated when NHGIS updates.
#     """
#     # read in the NHGIS 2010 to 2020 crosswalk reference dataframe 
#     crosswalk_path = "nhgis_blk2010_blk2020_ge_v0_26/" + state + "nhgis_blk2010_blk2020_ge_v0_26.csv"
#     crosswalk_ref = pd.read_csv(crosswalk_path)
#     assignment = []
#     # generate list of tiles
#     tiles = crosswalk_ref['GEOID20'].to_list()

#     individ_cols = ['submission_text', 'area_text', 'area_name']

#     cols = individ_cols + tiles
#     temp_pivot = pd.DataFrame(columns = cols)
#     column = temp_pivot.columns
#     i = 0
#     for _idx, row in block10_pivot.iterrows():
#         if i % 10 == 0:
#             print("In loop, on row: {}".format(i))
#         if i == 1 or i == 3 or i == 5:
#             print("why is this so slowwwww")

#         plan_id = [row.name.split("-")[0] + "-1"]
#         # print(plan_id)
#         temp_row = row
#         block10_names = []
#         block20_names = []

#         acc = pd.DataFrame(index = plan_id, columns = cols)
#         # fill in row info
#         acc.at[plan_id, 'submission_text'] = row['submission_text']
#         acc.at[plan_id, 'area_name'] = row['area_name'] if 'area_name' in row.columns else ""
#         acc.at[plan_id, 'area_text'] = row['area_text'] if 'area_name' in row.columns else ""

#         for key, value in row.items():
#             if key not in individ_cols and value == 1:
#                 block10_names.append(key)
#                 # store the corresponding 2020 block id
#                 block20 = crosswalk_ref[crosswalk_ref['GEOID10'] == key]['GEOID20']
#                 block20_names.append(block)
#                 acc.at[plan_id, block20] = int(1)

#                 #fill shp file in one swoop

#                 index = block20_shp[block20_shp['GEOID20'] == block20].index
#                 block20_shp.at[index, 'NUMAREAS'] += 1


#         temp_pivot.columns = temp_pivot.columns.astype(str)
#         acc.columns = acc.columns.astype(str)

#         # for block in block20_names:
#         #     block = str(block)
#         #     acc.at[plan_id, block] = int(1)

#         temp_pivot.columns = temp_pivot.columns.astype(str)
#         acc.columns = acc.columns.astype(str)
#         temp_pivot = temp_pivot.append(acc, sort=True)
#         print("end of one row!")
#         i += 1
#     temp_pivot = temp_pivot.fillna(0)
#     return temp_pivot










def precinct_to_pivot(df, outfile = None):
    # add a units col to the df
    df['units'] = df['districtr_data'].apply(lambda x: x['plan']['units']['id'])
    try:
        state = df.iloc[0]['districtr_data']['plan']['place']['state']
    except:
        print(f"ERROR: {len(df)} COI SUBMISSIONS")
        return None

    unit = "precincts"
    
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
        titles = {p['id']: p['name'] for p in parts}
        texts = {p['id']: (p['description'] if 'description' in p else "") for p in parts}

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