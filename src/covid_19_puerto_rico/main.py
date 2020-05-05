#/usr/bin/env/python3

import altair as alt
import argparse
import datetime
import io
import logging
import numpy as np
import pandas as pd

def main():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)
    bulletin_date = datetime.date(2020, 5, 3)
    output_dir = "output"
    logging.info("bulletin-date is %s; output-dir is %s",
                 bulletin_date, output_dir)

    df = daily_deltas_data()
    logging.info("deltas frame: %s", describe_frame(df))

    # This one runs the chart code as I think it ought to be
    # and gets a bad chart:
    basename = f"{output_dir}/daily_deltas_BUGGY_{bulletin_date}"
    save_graph(daily_deltas_graph(df), basename)

    # This one applies the workaround I've had to do to make
    # it look right:
    basename = f"{output_dir}/daily_deltas_WORKAROUND_{bulletin_date}"
    save_graph(workaround_daily_deltas_graph(df), basename)

    # This one illustrates that if I don't sort then no bug.
    basename = f"{output_dir}/daily_deltas_UNSORTED_{bulletin_date}"
    save_graph(unsorted_daily_deltas_graph(df), basename)

def daily_deltas_data():
    return pd.read_csv("data/altair_issue_frame.csv")

def daily_deltas_graph(df):
    """The daily deltas graph, as it should be written, but
    produces bad output."""
    return alt.Chart(df).transform_filter(
        alt.datum.value != 0
    ).mark_bar().encode(
        x=alt.X('value', title="Cases added/subtracted"),
        y=alt.Y('datum_date:T', title="Event date"),
        color=alt.Color('variable', legend=None),
        tooltip = ['variable', 'datum_date:T', 'value']
    ).properties(
        width=250,
        height=250
    ).facet(
        row=alt.Y('bulletin_date:T', sort="descending",
                  title="Bulletin date"),
        column=alt.X('variable', title=None,
                     sort=['Confirmed and probable',
                           'Confirmed',
                           'Probable',
                           'Deaths'])
    )

def workaround_daily_deltas_graph(df):
    """The workaround I've had to apply to get around this issue:

    1. Manually find the earliest and latest datum_date with
       non-zero and non-NaN values
    2. Apply that as a domain to the scale manually
    3. Clip points that fall outside that domain"""
    def bug_workaround(df):
        filtered = df\
            .replace(0, np.nan)\
            .dropna()
        return (min(filtered['datum_date']), max(filtered['datum_date']))

    return alt.Chart(df).mark_bar(clip=True).encode(
        x=alt.X('value', title="Cases added/subtracted"),
        y=alt.Y('datum_date:T', title="Event date",
                scale=alt.Scale(domain=bug_workaround(df))),
        color=alt.Color('variable', legend=None),
        tooltip = ['variable', 'datum_date:T', 'value']
    ).properties(
        width=250,
        height=250
    ).facet(
        row=alt.Y('bulletin_date:T', sort="descending",
                  title="Bulletin date"),
        column=alt.X('variable', title=None,
                     sort=['Confirmed and probable',
                           'Confirmed',
                           'Probable',
                           'Deaths'])
    )

def unsorted_daily_deltas_graph(df):
    """This is the same as daily_deltas_graph above, but
    without the sorting, and it doesn't have the problem."""
    return alt.Chart(df).transform_filter(
        alt.datum.value != 0
    ).mark_bar().encode(
        x=alt.X('value', title="Cases added/subtracted"),
        y=alt.Y('datum_date:T', title="Event date"),
        color=alt.Color('variable', legend=None),
        tooltip = ['variable', 'datum_date:T', 'value']
    ).properties(
        width=250,
        height=250
    ).facet(
        row=alt.Y('bulletin_date:T', sort="descending",
                  title="Bulletin date"),
        column=alt.X('variable', title=None)
    )

def save_graph(graph, basename):
    filename = f"{basename}.html"
    logging.info("Writing graph to %s", filename)
    graph.save(filename)

def describe_frame(df):
    """Because df.info() prints instead of returning a string."""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()


if __name__ == '__main__':
    main()