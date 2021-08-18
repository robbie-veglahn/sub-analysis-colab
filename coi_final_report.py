# more imports than we need
# import geopandas as gpd
import pandas as pd
import requests
import json

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
            names = p['name']
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
    subset = pd.DataFrame()
    for _idx, row in coi_df.iterrows():
        if row['plan_id'] in plan_ids:
            subset = subset.append(row)
    return subset

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
    joined = join_area_text(all_submissions, state)
    joined.to_csv(outfile)