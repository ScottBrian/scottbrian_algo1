"""scottbrian_algo1 sec_data.

========
sec_data
========

sec_data creates fundamental data sets from the sec download.

"""

# import pandas as pd  # type: ignore
# from threading import Event, get_ident, get_native_id, Thread, Lock
from pathlib import Path

from typing import Any

# from plotly.subplots import make_subplots
# import plotly.express as px
# import plotly.graph_objects as go
import pandas as pd  # type: ignore
import numpy as np

# import os

########################################################################
# pandas options
########################################################################
pd.set_option("mode.chained_assignment", "raise")
pd.set_option("display.max_columns", 15)

sec_data_path = Path("/home/Tiger/sec_data/2020q1")

sub_path = sec_data_path / "sub.txt"
num_path = sec_data_path / "num.txt"

sub = pd.read_csv(sub_path, sep="\t", dtype={"cik": str})

print(sub.shape)

# load the num.txt file containing detailed data
num = pd.read_csv(num_path, sep="\t")
print("num.shape:", num.shape)


def data_frame_stats(df: Any) -> Any:
    """Dataframe stats.

    Args:
        df: DataFrame to get stats from

    Returns:
        DataFrame of stats
    """
    cnt = df.count()
    max_cnt = cnt.max()
    unique = df.nunique()
    return pd.DataFrame(
        {
            "notnulls": cnt,
            "notnulls%": (cnt / max_cnt) * 100,
            "unique": unique,
            "unique%": (unique / max_cnt) * 100,
        }
    )


def explore_sub() -> None:
    """Explore some data sets."""
    stats_df = data_frame_stats(sub)

    print(stats_df)  # .head().style.format({"notnulls%": "{:.2%}",
    # "unique%": "{:.2%}"})

    sub_stats = sub["form"].value_counts().reset_index()

    sub_stats["label"] = np.where(
        sub_stats["form"].rank(ascending=False) < 8, sub_stats["index"], "Other"
    )

    sub_stats = (
        sub_stats.groupby("label")
        .sum()
        .sort_values(by="form", ascending=False)
        .reset_index()
    )

    print(sub_stats)

    # plot using Plotly
    # because the awesome Plotly.Express strugle with the subplots, I'll use
    # lower level API to create two charts.
    # Bar chart on the left and pie chart on the right\n",
    # fig = make_subplots(rows=1, cols=2, specs=[[{'type':'bar'},
    # {'type':'pie'}]], subplot_titles=["Forms - Bar Chart","Forms -
    # Pie Chart"])
    #
    # # Creating the Bar chart\n",
    # fig.add_trace(
    #     go.Bar(y=sub_stats["form"],
    #            x=sub_stats["label"],
    #            text=sub_stats["form"], # values displayed in the bars
    #            textposition='auto',
    #            name="Forms - Bar Chart"),
    #            row=1, col=1)
    #
    # # Creating the Pie chart
    # fig.add_trace(
    #     go.Pie(values=sub_stats["form"],
    #            labels=sub_stats["label"],
    #            textinfo='label+percent'),
    #            row=1, col=2)
    # # adding the title
    # fig.update_layout(title="Most common forms in 2020Q1")

    # display the chart in the jupyter notebook
    # fig.show()

    # let's filter adsh, unique report identifier of the `8-K`s
    eight_k_filter = sub[sub["form"] == "8-K"][["name", "adsh"]]
    print("eight_k_filter.shape:", eight_k_filter.shape)

    print("\neight_k_filter.head()\n", eight_k_filter.head())

    print("\nnum.info()\n", num.info())

    print("\nnum.head()\n", num.head())
    # merge the file headers with the detailed data
    eight_k_nums = num.merge(eight_k_filter)
    print("eight_k_nums.shape:", eight_k_nums.shape)

    print("\neight_k_nums.head()\n", eight_k_nums.head())

    print('len(eight_k_nums["adsh"].unique())', len(eight_k_nums["adsh"].unique()))

    # num contain many lines for each `adsh` so we only .merge on unique
    # values using .drop_duplicates()
    # we left join to see all the records from sub.txt and possibly related
    # record in num.txt
    # we apply an indicator which highlights whether the adsh appear in
    # `both` files or `left_only`
    contain_data_df = sub[["adsh", "form"]].merge(
        num["adsh"].drop_duplicates(), how="left", indicator=True
    )
    print("\ncontain_data_df.sample(3)\n", contain_data_df.sample(3))

    # we pivot the data to have 2 columns "both" and "left_only"
    contain_data_stats = contain_data_df.pivot_table(
        index="form", columns="_merge", values="adsh", aggfunc="count"
    )

    print("\ncontain_data_stats 1\n", contain_data_stats.head())

    # if there's no data in left_only (100% are in both) we fill in zero
    # .fillna(0)
    # since the columns are categorical we need to turn them to string with
    # .rename(columns=str) in
    # order to .reset_index()

    contain_data_stats = contain_data_stats.fillna(0).rename(columns=str).reset_index()
    print("\ncontain_data_stats 2\n", contain_data_stats.head())

    # finally we sort the data and rename the columns to be more user-friendly
    # "both" --> "numeric data"
    contain_data_stats = contain_data_stats.sort_values(
        by="both", ascending=False
    ).rename(columns={"both": "numeric data", "left_only": "no data"})

    print("\ncontain_data_stats 3\n", contain_data_stats.head())

    # we calculate the percentage of adsh which have numeric data\n",
    contain_data_stats["perc_filled"] = contain_data_stats["numeric data"] / (
        contain_data_stats["numeric data"] + contain_data_stats["no data"]
    )
    print("\ncontain_data_stats 4\n", contain_data_stats.head())

    # fig = px.bar(contain_data_stats,
    # x="form", y=["numeric data","no data"], text="perc_filled")
    # fig.update_traces(texttemplate='%{text:.2%}', textposition='auto')
    # fig.update_layout(yaxis_title="Count")
    # fig.show()

    # filter only 10-K and 10-Q forms
    tens = sub[sub["form"].isin(["10-Q", "10-K"])]
    print("\ntens\n", tens.head())

    # count how many forms of each type are in the dataset
    tens_counts = (
        tens["form"]
        .value_counts()
        .reset_index()
        .rename(columns={"index": "form type", "form": "count"})
    )
    print("\ntens_counts\n", tens_counts)

    # using Plotly.Express create a bar chart
    # fig = px.bar(tens_counts,
    #              x="form type",
    #              y="count",
    #              barmode='group',
    #              text="count",
    #              title="Number of Annual and Quarterly forms in 2020Q1"
    #              )
    # fig.show()


def explore_num() -> None:
    """Explore data sets."""
    print("num.shape:", num.shape)
    print("\nnum.head()\n", num.head())
    print("\nnum.info()\n", num.info())
    num_1 = num.loc[
        (num["adsh"] == "0000028823-20-000056") & (num["ddate"] == 20191231)
    ]

    print("num_1.shape:", num_1.shape)
    print("\nnum_1.head()\n", num_1.head())
    print("\nnum_1.info()\n", num_1.info())

    num_2 = num_1[["tag", "qtrs", "value"]]

    pd.set_option("display.max_colwidth", 120)

    for i in range(0, 376, 10):
        print("\nnum_2[i:i+10]\n", num_2[i : i + 10])


def main() -> None:
    """Main routine."""
    # explore_num()
    data = {"a": [1, 2, 3, 3], "b": ["x", "x", "y", "y"]}
    frame = pd.DataFrame(data)
    print(frame.value_counts())

    arr = sorted(np.random.randn(20))
    print("\narr\n", arr)
    factor = pd.cut(arr, 4)
    print("\nfactor\n", factor)
    # myapp = AlgoApp()


if __name__ == "__main__":
    main()
