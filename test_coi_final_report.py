import coi_final_report as coi_report
import utils as utils
import pandas as pd

def test_join_area_text():
    # df = pd.read_csv("test_MI_data.csv")
    df = coi_report.all_submissions_file_other("michigan")
    plans_df = df[df['type'] == "plan"]
    cois_df = df[df['type'] == "coi"]
    written_df = df[df['type'] == "written"]
    other_df = df[df['type'] == "other"]
    file_df = df[df['type'] == "file"]
    joined_plan = coi_report.join_area_text(plans_df)
    joined_written = coi_report.join_area_text(written_df)
    joined_other = coi_report.join_area_text(other_df)
    joined_file = coi_report.join_area_text(file_df)
    assert(len(plans_df) == len(joined_plan))
    assert(len(written_df) == len(joined_written))
    assert(len(other_df) == len(joined_other))
    assert(len(file_df) == len(joined_file))
    cois_joined = coi_report.join_area_text(cois_df)
    multi = cois_joined[cois_joined['num_areas'] > 1]
    single = cois_joined[cois_joined['num_areas'] == 1]
    assert((len(multi) + len(single)) == len(cois_joined))

def test_submissions_file_other():
    state = "michigan"
    ids_url, plans_url, cois_url, written_url, subs = utils.submission_endpts(state)
    state = state.lower()
    if state == "michigan":
        file_url = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?type=file&length=100000"
        other_url = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?type=other&length=100000"
    else:
        csv_url = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/%s" % state
        file_url = csv_url + "?type=other&length=100000"
        other_url = csv_url + "?type=file&length=100000"
    plans_df, cois_df, written_df, file_df, other_df = coi_report.submissions_file_other(
                                     ids_url, plans_url, cois_url, file_url, other_url, written_url)
    all_submissions = coi_report.all_submissions_file_other(state)
    plans_len = len(plans_df)
    cois_len = len(cois_df)
    written_len = len(written_df)
    other_len = len(other_df)
    file_len = len(file_df)
    assert(abs(len(all_submissions) - (plans_len + cois_len + written_len + other_len + file_len)) < 2)

def test_find_coi_subset():
    # df = pd.read_csv("test_MI_data.csv")
    df = coi_report.all_submissions_file_other("michigan")
    cois_df = df[df['type'] == "coi"]
    UP_plan_ids = ["20348", "31207", "32661", "29545", "31223", "29749", "30771", "32594"]
    subset = coi_report.find_coi_subset(cois_df, UP_plan_ids)
    assert(len(subset) == len(UP_plan_ids))

def test_find_pseudo_cois():
    # df = pd.read_csv("test_MI_data.csv")
    df = coi_report.all_submissions_file_other("michigan")
    joined = coi_report.join_area_text(df)
    pseudo_cois = joined[joined['pseudo_coi'] == True]
    pseudo_df = coi_report.find_pseudo_cois(df)
    assert(len(pseudo_df) == len(pseudo_cois))
