import utils
import fetch

def test_submission_endpts():
    ids_url, plans_url, cois_url, written_url, all_subs = utils.submission_endpts("michigan")
    mi_ids = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/districtr-ids/michigan"
    mi_plan = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?type=plan&length=10000"
    mi_cois = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?type=coi&length=10000"
    mi_written = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?type=written&length=10000"
    mi_all = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?length=10000"
    assert(ids_url == mi_ids and plans_url == mi_plan and cois_url == mi_cois and written_url == mi_written and all_subs == mi_all)
    ids_url, plans_url, cois_url, written_url, all_subs = utils.submission_endpts("ohio")
    oh_ids = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/districtr-ids/ohio"
    oh_plan = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/ohio?type=plan&length=10000"
    oh_cois = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/ohio?type=coi&length=10000"
    oh_written = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/ohio?type=written&length=10000"
    oh_all = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/ohio?length=10000"
    assert(ids_url == oh_ids and plans_url == oh_plan and cois_url == oh_cois and written_url == oh_written and all_subs == oh_all)

def test_summary_table():
    plans_df, cois_df, written_df = utils.submission_dfs("michigan")
    weeks_1_7_dates = [('2021-5-01', '2021-5-07'), ('2021-5-08', '2021-5-14'), ('2021-5-15', '2021-5-21'), ('2021-5-22', '2021-5-28'), ('2021-5-29', '2021-6-4'), ('2021-6-05', '2021-6-11')]
    weeks_1_7_summary = utils.summary_table(weeks_1_7_dates, plans_df, cois_df, written_df)
    assert(len(weeks_1_7_dates) == len(weeks_1_7_summary))

def test_all_submissions_df():
    all_submissions = utils.all_submissions_df("Michigan")
    plans_df, cois_df, written_df = utils.submission_dfs("miChIgAn")
    len_writ, len_coi, len_plan = len(written_df), len(cois_df), len(plans_df)
    len_tot = len_writ + len_coi + len_plan
    assert(len(all_submissions) == len_tot)

def test_all_submissions_endpts():
    ids_url, all_subs = utils.all_submissions_endpts("michigan")
    mi_ids = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/districtr-ids/michigan"
    mi_all_subs = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan?length=10000"
    assert(ids_url == mi_ids and all_subs == mi_all_subs)
    ids_url, all_subs = utils.all_submissions_endpts("ohio")
    oh_ids = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/districtr-ids/ohio"
    oh_all_subs = "https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/ohio?length=10000"
    assert(ids_url == oh_ids and all_subs == oh_all_subs)

def test_submissions_in_range():
    date_range = ('2021-5-01', '2021-5-07')
    range_df = utils.submissions_in_range(date_range, "michigan")
    assert(len(range_df) == 41)

def test_summary_table_wrapper():
    weeks_1_6_dates = [('2021-5-01', '2021-5-07'), ('2021-5-08', '2021-5-14'), ('2021-5-15', '2021-5-21'), ('2021-5-22', '2021-5-28'), ('2021-5-29', '2021-6-4'), ('2021-6-05', '2021-6-11')]
    weeks_1_6_summary = utils.summary_table_wrapper(weeks_1_6_dates, "michigan")
    plans_df, cois_df, written_df = utils.submission_dfs("miChIgAn")
    weeks_1_6_summary2 = utils.summary_table(weeks_1_6_dates, plans_df, cois_df, written_df)
    assert(len(weeks_1_6_summary) == len(weeks_1_6_summary2))