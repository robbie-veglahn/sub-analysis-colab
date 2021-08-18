# more imports than we need
# import geopandas as gpd
import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
import coi_maps
import fetch as fetch
import utils as utils
import csv
import pydantic
from pydantic import BaseModel
from datetime import datetime as dt
import numpy as np
from typing import Tuple


states = ["wisconsin", 
          "ohio",
          "missouri",
          "michigan",
          "massachusetts",
          "texas",
          "newmexico"]

state_abv = ["WI",
             "OH",
             "MO",
             "MI",
             "MA",
             "TX",
             "NM"]

def join_area_text(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes in a dataframe of all submissions from a given state (using fetch.py),
    and returns a dataframe with the area texts joined in.
    """
    df = input_df 
    cols = ['area_text', 'area_name', 'num_areas', 'id']
    pivot = pd.DataFrame(columns = cols)
    count = 0
    for _idx, row in df.iterrows():
        if row['type'] == "plan":
            if 'assignment' in row['districtr_data']['plan'].keys():         
                asn_list = list(row['districtr_data']['plan']['assignment'].items())
                asn = []
                for assignment in asn_list:
                    _, assign = assignment
                    if isinstance(assign, list):
                        assign = assign[0]
                    asn.append(assign)
                num_dists = len(set(asn))
                if num_dists == 1:
                    count += 1
                    acc = pd.DataFrame({'area_name': [None], 'area_text': [None], 'num_areas': [1], 'id': [row['id']], 'pseudo_coi': True})
                    pivot = pivot.append(acc, ignore_index = True)
            continue
        elif row['type'] != "coi":
            continue

        # get all info
        parts = row['districtr_data']['plan']['parts']
        row_id = row['id']
        new_row_id = row_id
        num_areas = len(parts)
        names = ""
        texts = ""
        i = 0
        for p in parts:
            if num_areas > 1:
                temp_row = row
                new_row_id = f'{row_id}-{i+1}'
                temp_row['id'] = new_row_id
                df = df.append(temp_row)
            names = p['name'] if 'name' in p else ""
            texts = (p['description'] if 'description' in p else "")
            acc = pd.DataFrame({'area_name': [names], 'area_text': [texts], 'num_areas': [num_areas], 'id': [new_row_id]})
            pivot = pivot.append(acc, ignore_index = True)
            i += 1
        if num_areas > 1:
            # drop the original duplicate row
            a = [row_id]
            df = df[~df['id'].isin(a)]    
    pivot = df.join(pivot.set_index('id'), on='id')
    return pivot

def all_submissions_file_other(state: str="michigan") -> pd.DataFrame:
    """ 
    Takes in the desired state portal as a string and retrieves filled pd ...
    dataframe of all portal submissions with metadata and districtr assignment
    Note: wrapper function of fetch.py function for user-facing utils.py
    To use:
    >>> submissions_df = all_submissions_df("ohio")
    >>> submissions_df = all_submissions_df("michigan")
    """
    ids_url, plans_url, cois_url, written_url, subs = utils.submission_endpts(state)
    state = state.lower()
    if state == "michigan":
        file_url = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?type=file&length=100000"
        other_url = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?type=other&length=100000"
    else:
        csv_url = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/%s" % state
        file_url = csv_url + "?type=other&length=100000"
        other_url = csv_url + "?type=file&length=100000"
    plans_df, cois_df, written_df, file_df, other_df = submissions_file_other(
                                     ids_url, plans_url, cois_url, file_url, other_url, written_url)
    dfs = [plans_df, cois_df, written_df, file_df, other_df]
    all_submissions = pd.concat(dfs, ignore_index=True)
    return all_submissions

def submissions_file_other(ids_url: str, plans_url: str, cois_url: str, file_url: str,
    other_url: str, wr_url: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Takes in endpoint for all districtr ids in a portal along with csv api  ...
    calls for plans, cois, and written submissions, and retrieves filled pd ...
    dataframes for each submission type with metadata and districtr assignments
    """
    plans_df = fetch.csv_read(plans_url) # gathers plan metadata in df
    cois_df = fetch.csv_read(cois_url) # gathers coi metadata in df
    written_df = fetch.csv_read(wr_url) # gathers written metadata in df
    file_df = fetch.csv_read(file_url) # gathers file metadata in df
    other_df = fetch.csv_read(other_url) # gathers other metadata in df

    submissions = fetch.retrieve_submission_ids_json(ids_url)
    submissions.sort(key=lambda x: str(x.id)) # sorts submission jsons by id
    plan_submissions = [sub.districtr_plan for sub in submissions #filters plan
                                                    if sub.plan_type == "plan"]
    coi_submissions = [sub.districtr_plan for sub in submissions #filters cois
                                                    if sub.plan_type == "coi"]
    assert len(plan_submissions) == len(plans_df)
    assert len(coi_submissions) == len(cois_df)
    # parse for plan id and add in submission dfs
    plans_df['plan_id'] = plans_df["link"].map(
                                lambda link: link.split("/")[-1].split("?")[0])
    cois_df['plan_id'] = cois_df["link"].map(
                                lambda link: link.split("/")[-1].split("?")[0])
    # sort dfs by plan id to correctly join w/ json information
    plans_df = plans_df.sort_values(by=['plan_id'], ascending=True)
    cois_df = cois_df.sort_values(by=['plan_id'], ascending=True)
    # join in districtr json assignments into 'districtr_data column'
    plans_df['districtr_data'] = plan_submissions
    cois_df['districtr_data'] = coi_submissions
    # make datetime fields parseable:
    plans_df['datetime'] = plans_df['datetime'].map( lambda datetime: (
        datetime.split("+")[0] + " +" + datetime.split("+")[1].split(" ")[0]))
    cois_df['datetime'] = cois_df['datetime'].map( lambda datetime: (
        datetime.split("+")[0] + " +" + datetime.split("+")[1].split(" ")[0]))
    written_df['datetime'] = written_df['datetime'].map( lambda datetime: (
        datetime.split("+")[0] + " +" + datetime.split("+")[1].split(" ")[0]))
    # # convert datetime fields from str's to datetime objects in all dataframe
    plans_df['datetime'] = plans_df['datetime'].map(lambda datetime: (
                                dt.strptime(datetime, '%a %b %d %Y %X %Z %z')))
    cois_df['datetime'] = cois_df['datetime'].map(lambda datetime: (
                                dt.strptime(datetime, '%a %b %d %Y %X %Z %z')))
    written_df['datetime'] = written_df['datetime'].map(lambda datetime: (
                                dt.strptime(datetime, '%a %b %d %Y %X %Z %z')))
    if len(file_df) > 0:
        file_df['datetime'] = file_df['datetime'].map( lambda datetime: (
            datetime.split("+")[0] + " +" + datetime.split("+")[1].split(" ")[0]))
    if len(other_df) > 0:
        other_df['datetime'] = other_df['datetime'].map( lambda datetime: (
            datetime.split("+")[0] + " +" + datetime.split("+")[1].split(" ")[0]))
    # return relevant dataframes
    return plans_df, cois_df, written_df, file_df, other_df

def find_coi_subset(coi_df: pd.DataFrame, plan_ids: list) -> pd.DataFrame:
    """
    Takes in a submission dataframe and a list of plan_ids and...
    returns a dataframe of filled with submissions of those plan ids
    """
    count = 0
    single_area = []
    multi_area = []
    multi_area_num = []
    subset = pd.DataFrame()
    for p_id in plan_ids:
        tempid = p_id
        if len(tempid.split("-")) == 1:
            single_area.append(p_id)
        else:
            multi_area.append(p_id.split("-")[0])
            multi_area_num.append(p_id.split("-")[1])
    for _idx, row in coi_df.iterrows():
        if row['plan_id'] in single_area:
            subset = subset.append(row)
        elif row['plan_id'] in multi_area:
            sub_id_num = row['id'].split("-")[1]
            if sub_id_num in multi_area_num:
                subset = subset.append(row)
    return subset

    # count = 0
    # subset = pd.DataFrame()
    # for _idx, row in coi_df.iterrows():
    #     if row['plan_id'] in plan_ids:
    #         subset = subset.append(row)
    # return subset

def visualize_coi(plan_ids, df, lookup_table, state="Michigan", title=None, read_csv=True, simple_plot=False):
    """ 
    Takes in a list of plan_ids that make up a coi and a state to draw them on,
    plots the heatmap using Jack's plot_coi_heatmap code, and returns the
    dataframe of the newly formed coi

    Returns: both the subset dataframe as well as the subset map dissolved by...
    id for the purposes of passing into plotly chloropleth_mapbox funct
    """
    subset = find_coi_subset(df, plan_ids)
    if 'area_text' not in subset.columns:
        subset = join_area_text(subset)
    # If true, just plot simple heatmap
    if simple_plot == True:
        # since pandas reads in csvs in a wonky format, converts datatypes so...
        # Jack's function doesn't break
        if read_csv == True:
            # to ensure subset doesn't have incorrect plan types, apply will break
            # otherwise
            df = subset[subset['type'] == 'coi']
            df = df.append(subset[subset['pseudo_coi'] == True])
            subset = df
            subset['districtr_data'] = subset['districtr_data'].apply(lambda x: eval(x))
        subset_map = coi_maps.assignment_to_shape(subset)
        dissolved_map = subset_map.dissolve('id')
        coi_maps.plot_coi_heatmap(dissolved_map, state, title=title)

    lookup_subset = find_lookup_subset(lookup_table, plan_ids)
    coi_geoms = bg_lookup_table_to_geometry(lookup_subset, state)
    fig = px.choropleth_mapbox(lookup_subset, geojson=coi_geoms.geometry, 
                                locations=lookup_subset.index, color=lookup_subset.index,
                                color_continuous_scale=[[0, 'rgba(0,0,255,0)'], [1, 'rgb(255,0,0,1)']],
                                # range_color=(0, 12),
                                mapbox_style="carto-positron",
                                zoom=7, center = {"lat": 42.96, "lon": -86.66},
                                hover_data={"Plan ID":lookup_subset.index},
                                # opacity=0.25,
                            #    facet_col="cluster",
                            #    scope="usa"
                            #    labels={'unemp':'unemployment rate'}
                            )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return subset, fig

def find_lookup_subset(lookup_df, plan_ids: list):
    """
    Takes in a submission dataframe and a list of plan_ids and...
    returns a dataframe of filled with submissions of those plan ids
    """
    count = 0
    subset = pd.DataFrame()
    temp = lookup_df
    single_area = []
    multi_area_num = []
    multi_area_full = []
    ids = []
    for p_id in plan_ids:
        tempid = p_id
        if len(tempid.split("-")) == 1:
            single_area.append(p_id)
        else:
            multi_area_full.append(p_id)
            multi_area_num.append(p_id.split("-")[1])
        ids.append(p_id)
    # print("single areas: {}, multi areas: {}".format(single_area, multi_area))
    temp['stripped_id'] = temp.apply(lambda x: str(x.name.split("-")[0]), axis=1)
    subset = temp[temp['stripped_id'].isin(ids)]
    return subset


def bg_lookup_table_to_geometry(lookup_df, state, colab=False):
    """
    Takes a block group (pref unit) lookup table, and returns a lookup table with...
    geopandas geometries for plotting with plotly chloropleth_mapbox
    """
    bg_key = "GEOID10"
    if colab == True:
          bg_path = "/content/sub-analysis-colab/michigan/michigan_bg10.shp"
    else:
          bg_path = state + "/" + state + "_bg10.shp"
    bg_shp = gpd.read_file(bg_path)
    individ_cols = ['submission_text', 'area_text', 'area_name']
    geometry_lookup = pd.DataFrame()
    for _idx, row in lookup_df.iterrows():
        bg_names = []
        plan_id = row.name
        for key, value in row.items():
            if key not in individ_cols and value == 1:
                bg_names.append(key)
        if len(bg_names) != 0:
            coi_geoms = bg_shp[bg_shp[bg_key].apply(lambda x: x in bg_names)]
            coi_geoms['plan_id'] = plan_id
            coi_geoms = coi_geoms.dissolve('plan_id')
            geometry_lookup = geometry_lookup.append(coi_geoms)
    return geometry_lookup


def find_pseudo_cois(submissions_df):
    """
    Searches through a DataFrame of submissions and returns a dataframe full...
    of all the "plan" submissions that are only one district to treat as pseduo
    cois
    """
    count = 0
    one_dist = pd.DataFrame()
    for _idx, row in submissions_df.iterrows():
        if row['type'] == "plan":
            if 'assignment' in row['districtr_data']['plan'].keys():         
                asn_list = list(row['districtr_data']['plan']['assignment'].items())
                asn = []
                for assignment in asn_list:
                    _, assign = assignment
                    if isinstance(assign, list):
                        assign = assign[0]
                    asn.append(assign)
                num_dists = len(set(asn))
                if num_dists == 1:
                    count += 1
                    one_dist = one_dist.append(row)
    return one_dist

def all_states_csvs_cumulative(folder_path: str, end_date: str) -> None:
    """
    Goes through all the states and exports full csvs for each one
    """
    i = 0
    for state in states:
        filename = folder_path + state_abv[i] + "Cumulative" + end_date + ".csv"
        print(filename)
        export_full_csv(state, filename)
        i += 1

def export_full_csv(state: str, outfile: str) -> None:
    """
    Takes in a state, fetches all of that states relevant submissions, joins...
    in area text and pre-processes data for NLP, and exports in csv format to..
    the specified outfile
    """
    all_submissions = all_submissions_file_other(state)
    all_submissions = all_submissions.replace('\n',' ', regex=True)
    joined = join_area_text(all_submissions)
    joined['id'] = joined['id'].astype(str)
    if 'pseudo_coi' not in joined.columns:
        joined["pseudo_coi"] = ""
    joined.to_csv(outfile)

def all_subs_joined(state: str) -> pd.DataFrame:
    """
    Takes in a state, fetches all of that states relevant submissions, joins...
    in area text and pre-processes data for NLP, and returns the dataframe

    Wrapper function
    """
    all_submissions = all_submissions_file_other(state)
    all_submissions = all_submissions.replace('\n',' ', regex=True)
    joined = join_area_text(all_submissions)
    joined['id'] = joined['id'].astype(str)
    if 'pseudo_coi' not in joined.columns:
        joined["pseudo_coi"] = ""
    return joined

def all_states_pseduo_csvs_cumulative(folder_path: str, end_date: str) -> None:
    """
    Goes through all the states and exports full csvs for each one
    """
    i = 0
    for state in states:
        filename = folder_path + state_abv[i] + "Cumulative" + end_date + ".csv"
        print(filename)
        export_coi_and_pseudo_csv(state, filename)
        i += 1

def export_coi_and_pseudo_csv(state: str, outfile: str) -> None:
    """
    Takes in a state, fetches all of that states relevant submissions, joins...
    in area text and pre-processes data for NLP, and exports in csv format to..
    the specified outfile
    """
    ids_url, plans_url, cois_url, written_url, subs = utils.submission_endpts(state)
    plans_df, cois_df, _ = fetch.submissions(
                                     ids_url, plans_url, cois_url, written_url)
    dfs = [plans_df, cois_df]
    all_submissions = pd.concat(dfs, ignore_index=True)

    joined = join_area_text(all_submissions)
    joined.to_csv(outfile)
