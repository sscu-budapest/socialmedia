import os

import datazimmer as dz
import pandas as pd
from dotenv import load_dotenv

from .facebook_util import parse_raw_extract
from .meta import FacebookPage, FacebookPost, PageRecord, PostRecord

page_table = dz.ScruTable(FacebookPage)
post_table = dz.ScruTable(FacebookPost)
page_record_table = dz.ScruTable(PageRecord)
post_record_table = dz.ScruTable(PostRecord)


@dz.register_data_loader
def template_proc():
    # v4
    load_dotenv()
    broot = os.environ["BEER_ROOT"]
    raw_page_df = pd.read_parquet(broot + "pages.parquet")
    raw_post_df = pd.read_parquet(broot + "posts.parquet")

    parse_raw_extract(
        raw_page_df,
        raw_post_df,
        page_table,
        post_table,
        page_record_table,
        post_record_table,
    )
