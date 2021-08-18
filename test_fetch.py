import fetch

def test_retrieve_submission_json():
    url = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/districtr-ids/michigan"
    submissions = fetch.retrieve_submission_ids_json(url)
    assert submissions != 0

def test_fetch_submissions():
    ids_url = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/districtr-ids/michigan"
    csv_url = "https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/michigan"
    plans_url   = csv_url + "?type=plan&length=10000"
    cois_url    = csv_url + "?type=coi&length=10000"
    written_url = csv_url + "?type=written&length=10000"
    plans_df, cois_df, written_df = fetch.submissions(ids_url, plans_url, cois_url, written_url)
    assert len(plans_df) != 0
    assert len(cois_df) != 0
    assert len(written_df) != 0