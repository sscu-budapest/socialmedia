import datetime as dt
import re

import datazimmer as dz
import pandas as pd

from .meta import FacebookPage, FacebookPost, PageRecord, PostRecord


def parse_raw_extract(
    pages_df,
    posts_df,
    page_table: dz.ScruTable,
    post_table: dz.ScruTable,
    page_record_table: dz.ScruTable,
    post_record_table: dz.ScruTable,
):

    parsed_pages = (
        pages_df.dropna(subset=["title"])
        .drop_duplicates(subset=["page_id"])
        .assign(
            **{
                PageRecord.page.fid: lambda df: df["page_id"].str.replace("/", ""),
                PageRecord.name: lambda df: df["title"]
                .str.replace("| Facebook", "", regex=False)
                .str.strip(),
                PageRecord.likes: _p_number("like this"),
                PageRecord.followers: _p_number("follow this"),
            }
        )
    )

    page_table.replace_records(
        parsed_pages.rename(columns={PageRecord.page.fid: FacebookPage.fid})
    )
    page_record_table.extend(parsed_pages)

    parsed_post_df = (
        posts_df.drop_duplicates()
        .loc[
            lambda df: ~df["post_link"].str.startswith("/ufi/reaction/")
            & ~df["post_link"].str.endswith("m_entstream_source=timeline&__tn__=H-R")
        ]
        .assign(
            **{
                FacebookPost.posted: lambda df: parse_post_date(df["post_date"]),
                PostRecord.recorded: pages_df["recorded"].iloc[0],
                PostRecord.reactions: lambda df: df["reac_txt"]
                .where(lambda s: s != "", "0")
                .where(lambda s: getothers(s).isna(), lambda s: getothers(s))
                .where(lambda s: s.str[-1] != "K", lambda s: s.str[:-1] + " * 1000")
                .apply(eval)
                .astype(int),
                FacebookPost.pid: lambda df: df["post_link"]
                .pipe(_getpid)
                .astype(int)
                .astype(str),
            }
        )
        .dropna(subset=[FacebookPost.posted])
        .reset_index(drop=True)
        .rename(
            columns={"post_link": FacebookPost.link, "page_id": FacebookPost.source.fid}
        )
        .pipe(concat_interacts)
        .drop_duplicates(subset=[FacebookPost.pid, FacebookPost.posted])
    )
    post_table.replace_records(parsed_post_df)
    post_record_table.extend(
        parsed_post_df.rename(
            columns={
                FacebookPost.pid: PostRecord.post.pid,
                FacebookPost.posted: PostRecord.post.posted,
            }
        )
    )


def _p_number(col):
    def f(df):
        return (
            df[col]
            .str.extract("(.*) people")
            .iloc[:, 0]
            .str.replace(",", "")
            .astype(int)
        )

    return f


def parse_post_date(s):
    npw = s.where(
        s.str.contains(","), s.str.replace(" at ", f", {dt.date.today().year} at")
    )
    return pd.to_datetime(npw, errors="coerce")


def _ext(s, pat):
    return s.str.extract(pat).iloc[:, 0]


def _getpid(s):
    pats = {
        "photo": r"^/.+/photos/[a|p|o]\.\d+/(\d+)/",
        "story": r"^/story.php\?story_fbid=(\d+)\&",
        "photo-raw": r"^/photo.php\?fbid=(\d+)\&",
        "event": r"^/events/(\d+)\?",
        "post": r"^/\d+/posts/(\d+)/",
        "vid": r"^/.+/videos/(\d+)/",
        "note": r"^/notes/.+/.+/(\d+)/",
        "question": r"^/questions.php\?question_id=(\d+)\&",
        "ufi": r"^/ufi/reaction/profile/browser/\?ft_ent_identifier=(\d+)\&",
        "group-post": r"facebook.com/groups/.+/permalink/(\d+)/",
    }
    return (
        pd.DataFrame({k: _ext(s, v) for k, v in pats.items()})
        .fillna("")
        .sum(axis=1)
        .values
    )


def getothers(s):
    return s.str.extract("and (.*) others").iloc[:, 0]


def extract_num(s, suffix):
    found = re.findall(r"([\d\,\.K]+) " + suffix, s)
    if not found:
        return 0
    to_parse = found[0]
    if to_parse.endswith("K"):
        to_int = float(to_parse[:-1]) * 1000
    else:
        to_int = to_parse.replace(",", "")
    return int(to_int)


def parse_ls(ls_txt):
    return {
        col: extract_num(ls_txt, k)
        for k, col in [("Comment", PostRecord.comments), ("Share", PostRecord.shares)]
    }


def concat_interacts(df):
    recs = [
        parse_ls((ls or "")[len(reac or "") :])
        for reac, ls in df[["reac_txt", "ls_txt"]].values
    ]
    return pd.concat([df, pd.DataFrame(recs, index=df.index)], axis=1)
