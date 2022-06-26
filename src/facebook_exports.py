import datazimmer as dz
from src import meta
from src.facebook_beer_pages import page_record_table, post_record_table, post_table


class PostExport(meta.PostRecord):
    link = str
    page_followers = int
    page_likes = int
    page_name = str


post_export_table = dz.ScruTable(PostExport)


@dz.register(
    dependencies=[page_record_table, post_record_table, post_table],
    outputs=[post_export_table],
)
def step():
    post_df = post_table.get_full_df()
    pager_df = page_record_table.get_full_df()
    postr_df = post_record_table.get_full_df()

    page_cols = ["followers", "likes", "name"]
    post_export_table.replace_all(
        post_df.reset_index()
        .merge(
            pager_df,
            left_on=meta.FacebookPost.source.fid,
            right_on=meta.PageRecord.page.fid,
        )
        .merge(
            postr_df.drop(meta.PostRecord.recorded, axis=1),
            left_on=post_table.index_cols,
            right_on=[meta.PostRecord.post.pid, meta.PostRecord.post.posted],
        )
        .rename(columns={k: f"page_{k}" for k in page_cols})
    )
