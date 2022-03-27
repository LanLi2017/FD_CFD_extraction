import os

import pandas as pd
import sqlalchemy
from pylab import *

from prepare import CONF

pretify = {
    "dataset": "Dataset",
    "preparator": "Preparator",
    "remove special characters": "remove special    \ncharacters",
    "remove_special_characters": "remove_special    \n_characters",
    "cd": "cddb",
    "baselinefull": "Baseline",
    # "minimized": "Minimized"
    "minimized": "Prepared"
}

charts_to_generate = [
    # "preparations_by_dataset",
    "two_stage_f1_barchart",
    # "similarity_mobility_barchart",
    # "similarity_mobility_scatterplot",
]

db_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'
                                         .format(CONF["database_username"],
                                                 CONF["database_password"],
                                                 CONF["database_ip"] + ":" + CONF["database_connection_port"],
                                                 CONF["database_schemaname"]))


def import_differences():
    # sql_query = """
    #                 select * from classification
    #                 where `dataset` = '{dataset}' and `xstandard` = '{xstandard}' and `classifier` = '{classifier}'
    #                 and `preparations` = '{attribute_preparations}'
    #                 """ \
    #     .format(dataset=dataset, xstandard="goldstandard", classifier="THRESHOLD",
    #             attribute_preparations=CONF["preparations"][dataset]))
    sql_query = """
                        select * from preparation_similarities_differences_cube
                        where `xstandard` = '{xstandard}'
                        """ \
        .format(xstandard="goldstandard")
    df = pd.read_sql_query(sql_query, db_connection)
    # df = df.sample(n= 1000)
    return df


def main():
    datasets = [
        # "cddb",
        # "census",
        # "restaurants",
        # "cora",
        # "movies",
        # "hotels"
    ]

    figdir_path = CONF["workspace_dir_base"] + "imgs/"
    if not os.path.exists(figdir_path):
        os.makedirs(figdir_path)

    matplotlib.rcParams.update({'font.size': 15})

    # Generate bar charts: baseline, good preps, minimized
    if "preparations_by_dataset" in charts_to_generate:
        preparations_by_dataset("dataset")
    if "two_stage_f1_barchart" in charts_to_generate:
        figpath_barchart_aucpr = figdir_path + "classification_auc_pr.pdf"
        classification_barcharts(figpath_barchart_aucpr, "auc_pr")

        figpath_barchart_f1 = figdir_path + "classification_f1.pdf"
        classification_barcharts(figpath_barchart_f1, "auc_pr_best_f1")

    if "similarity_mobility_barchart" in charts_to_generate:
        figpath_barchart_similarity_mobility_dataset = figdir_path + "similarity_mobility_pairclasses_by_dataset_"
        barchart_similarity_mobility(figpath_barchart_similarity_mobility_dataset, "dataset")

        # figpath_barchart_similarity_mobility_preparator = figdir_path + "similarity_mobility_pairclasses_by_preparator_"
        # barchart_similarity_mobility(figpath_barchart_similarity_mobility_preparator, "preparator")

    if "similarity_mobility_scatterplot" in charts_to_generate:
        figpath_scatterplot_changed_class_dataset = figdir_path + "scatterplot_changed_class_dataset_"
        scatterplot_before_after_similarity(figpath_scatterplot_changed_class_dataset, "dataset")

        # figpath_scatterplot_changed_class_preparator = figdir_path + "scatterplot_changed_class_preparator_"
        # scatterplot_before_after_similarity(figpath_scatterplot_changed_class_preparator, "preparator")


def preparations_by_dataset(detail_level):
    sql_query = """select *
                   from classification
                   where `xstandard` = 'goldstandard'"""
    df_classification = pd.read_sql_query(sql_query, db_connection)
    data_values = set()
    datasets_in_results = set()
    dataset = None

    for i, r in df_classification.iterrows():
        if dataset != r["dataset"]:
            dataset = r["dataset"]
            counter = 0
        else:
            counter = 1
        if counter % 2 == 1:
            dataset_preparations_str = r["preparations"]
            dataset_preparations = dataset_preparations_str.split("-")
            for attribute_preparation in dataset_preparations:
                attribute, preparators_str = attribute_preparation.split("@")
                preparators = preparators_str.split("~")
                for preparator in preparators:
                    # datasets_in_results.append((dataset, attribute, preparator))
                    if preparator == "_":
                        continue
                    data_values.add((r["dataset"], attribute, preparator))
            datasets_in_results.add(r["dataset"])
    for dataset in set(CONF["datasets"]).difference(datasets_in_results):
        attributes = CONF["datasets_info"][dataset]["attributes"]
    data_values_list = sorted(list(data_values))
    df = pd.DataFrame.from_records(data_values_list, columns=["dataset", "attribute", "preparator"])

    # for (dataset, attribute), preparators in df.groupby(["dataset", "attribute"]).agg(tuple).applymap(list):
    all_preparators = CONF["all_preparators"]
    data_for_latex = []

    preparators_participating = list(df["preparator"].unique())
    preparators_participating.sort(key=lambda x: int(CONF["preparators_info"][x]["order"]))

    for x in df.groupby(["dataset", "attribute"])['preparator']:
        dataset, attribute = x[0]
        preparators = set(x[1])
        values = [dataset, attribute]
        for preparator in preparators_participating:
            values.append("X" if preparator in preparators else "")
        data_for_latex.append(tuple(values))
    df_for_latex = pd.DataFrame.from_records(data_for_latex,
                                             columns=["dataset", "attribute"] + preparators_participating)
    pd.set_option('display.expand_frame_repr', False)
    print(df_for_latex.to_latex(index=False))


def classification_barcharts(figpath, metric_to_use):
    sql_query = """select *
                   from classification
                   where `xstandard` = 'goldstandard'"""
    df_classification = pd.read_sql_query(sql_query, db_connection)

    # df = df_classification[["dataset", "preparations", metric_to_use]]
    cases = ["baseline", "prepared"]
    data_values = []
    datasets_in_results = set()
    dataset = None
    counter = 0
    for i, r in df_classification.iterrows():
        if dataset != r["dataset"]:
            dataset = r["dataset"]
            counter = 0
        else:
            counter = 1
        data_values.append((r["dataset"], "baseline" if counter % 2 == 0 else "prepared", r[metric_to_use]))
        datasets_in_results.add(r["dataset"])
    for dataset in set(CONF["datasets"]).difference(datasets_in_results):
        data_values.append((dataset, "baseline", 0.0))
        data_values.append((dataset, "prepared", 0.0))

    df = pd.DataFrame.from_records(data_values, columns=["dataset", "state", "metric"])

    ind = np.arange(len(df["dataset"].unique()))  # the x locations for the groups
    # width = 0.35  # the width of the bars
    width = 0.25  # the width of the bars

    fig, ax = plt.subplots()

    values_cases = {case: [] for case in cases}

    data_values = sorted(df["dataset"].unique())

    for dataset in data_values:
        try:
            v_baseline = float(df[(df["dataset"] == dataset) & (df["state"] == "baseline")]["metric"].iat[0])
            v_prepared = float(df[(df["dataset"] == dataset) & (df["state"] == "prepared")]["metric"].iat[0])
        except:
            continue
        values_cases["baseline"].append(v_baseline)
        values_cases["prepared"].append(v_prepared)

    colors = ['#feb24c', '#e31a1c']
    hatches = ["/", "\\", "-"]
    rects = {}
    for i, case in enumerate(cases):
        # rects[case] = ax.bar(ind + i*width, np.array(values_cases[case]), width, color=colors[i], hatch=hatches[i], alpha=1.0)
        rects[case] = ax.bar(ind + i * width, tuple(values_cases[case]), width=width, color=colors[i], alpha=1.0)

    # add some text for labels, title and axes ticks
    ax.set_ylabel('F-measure' if metric_to_use == "auc_pr_best_f1" else "Area under precision-cecall curve")
    ax.set_xlabel("Dataset")
    # ax.set_title('Scores by group and gender')
    # ax.set_xticks(ind + width / 2)
    ax.set_xticks(ind + 0.5 * width)

    group_labels = []
    for item in data_values:
        # records = nmetadata[]
        # group_labels.append((item if item != "overall_enrichment" else "overall\nenrichment") + "\n" + "(" + (
        # "{:,}".format(num_pairs)) + ")")

        v = item

        if v in pretify:
            v = pretify[v]

        group_labels.append(v)

    ax.set_xticklabels(tuple(group_labels), rotation="45")

    plt.ylim([0.0, 1.0])

    ax.legend(
        tuple(rects[case] for case in cases),
        tuple([pretify[case] if case in pretify else case for case in cases]),
        loc='lower right'
    )

    # plt.show()
    plt.savefig(figpath, bbox_inches='tight', format="pdf")
    plt.savefig(figpath + ".png", bbox_inches='tight')


def barchart_similarity_mobility(base_filepath, data_to_plot):
    data_chart = []
    result_types = ["-", "+", "="]
    if data_to_plot == "dataset":
        sql_relation = "(select * from preparation_similarities_differences_cube)"
        sql_query = """select R.dataset, count(*) as cnt
                       from {relation} as R
                       where (R.pair_class = 'dpl' and R.difference > 0.0) or 
                       (R.pair_class = 'ndpl' and R.difference < 0.0)
                       group by R.dataset""".format(relation=sql_relation)
        df_improved = pd.read_sql_query(sql_query, db_connection)

        sql_query = """select R.dataset, count(*) as cnt
                       from {relation} as R
                       where (R.pair_class = 'dpl' and R.difference < 0.0) or 
                       (R.pair_class = 'ndpl' and R.difference > 0.0)
                       group by dataset""".format(relation=sql_relation)
        df_worsened = pd.read_sql_query(sql_query, db_connection)

        sql_query = """select R.dataset, count(*) as cnt
                       from {relation} as R
                       group by R.dataset""".format(relation=sql_relation)
        df_total = pd.read_sql_query(sql_query, db_connection)

        if len(df_total) == 0:
            return
        for i, r in df_total.iterrows():
            dataset = r["dataset"]

            total_count = r["cnt"]
            preparator_improved_count = 0.0
            if len(df_improved[df_improved["dataset"] == dataset]) > 0:
                preparator_improved_count = df_improved[df_improved["dataset"] == dataset]["cnt"].iloc[0]
            preparator_worsened_count = 0.0
            if len(df_worsened[df_worsened["dataset"] == dataset]) > 0:
                preparator_worsened_count = df_worsened[df_worsened["dataset"] == dataset]["cnt"].iloc[0]

            data_chart.append(("+", dataset, float(preparator_improved_count * 100) / float(total_count)))
            data_chart.append(("-", dataset, float(preparator_worsened_count * 100) / float(total_count)))

        df_results = pd.DataFrame.from_records(data_chart, columns=["result_type", "dataset", "ratio"])
        print(df_results)
        _plot_barchart_aggregated(df_results, base_filepath + ".pdf", data_to_plot)
    elif data_to_plot == "preparator":
        preparators = CONF["all_preparators"]

        # sql_relation = "(select * from preparation_similarities_differences_cube order by rand(1) limit 100)"
        # sql_relation = "(select * from preparation_similarities_differences_cube limit 500000)"
        # sql_relation = "(select * from preparation_similarities_differences_cube limit 100000)"
        sql_relation = "(select * from preparation_similarities_differences_cube)"
        # sql_query = "select * from preparation_similarities_differences_cube where "
        preparators_in_results = set()
        for preparator in preparators:
            sql_query = """select R.dataset, count(*) as cnt
                           from {relation} as R
                           where (
                           (R.`pair_class` = 'dpl' and R.`difference` > 0.0) or 
                           (R.`pair_class` = 'ndpl' and R.`difference` < 0.0)
                           ) 
                           and R.`{preparator}` = True
                           group by R.dataset""".format(relation=sql_relation, preparator=preparator)
            df_improved = pd.read_sql_query(sql_query, db_connection)

            sql_query = """select R.dataset, count(*) as cnt
                           from {relation} as R
                           where (
                           (R.`pair_class` = 'dpl' and R.`difference` < 0.0) or 
                           (R.`pair_class` = 'ndpl' and R.`difference` > 0.0)
                           )
                           and R.`{preparator}` = True
                           group by R.dataset""".format(relation=sql_relation, preparator=preparator)
            df_worsened = pd.read_sql_query(sql_query, db_connection)

            sql_query = """select R.dataset, count(*) as cnt
                           from {relation} as R
                           where R.`{preparator}` = True
                           group by R.dataset""".format(relation=sql_relation, preparator=preparator)
            df_total = pd.read_sql_query(sql_query, db_connection)

            if len(df_total) == 0:
                continue
            preparators_in_results.add(preparator)
            for i, r in df_total.iterrows():
                dataset = r["dataset"]

                total_count = r["cnt"]
                preparator_improved_count = 0.0
                if len(df_improved[df_improved["dataset"] == dataset]) > 0:
                    preparator_improved_count = df_improved[df_improved["dataset"] == dataset]["cnt"].iloc[0]
                preparator_worsened_count = 0.0
                if len(df_worsened[df_worsened["dataset"] == dataset]) > 0:
                    preparator_worsened_count = df_worsened[df_worsened["dataset"] == dataset]["cnt"].iloc[0]

                data_chart.append(
                    ("+", dataset, preparator, float(preparator_improved_count * 100) / float(total_count)))
                data_chart.append(
                    ("-", dataset, preparator, float(preparator_worsened_count * 100) / float(total_count)))

        df_results = pd.DataFrame.from_records(data_chart, columns=["result_type", "dataset", "preparator", "ratio"])
        df_results.to_csv(base_filepath + "_results.tsv", sep="\t")
        _plot_barchart_stacked_by_preparator(df_results, base_filepath + ".pdf")


def _plot_barchart_aggregated(df_results, fpath, data_to_plot):
    # dpl_or_ndpl_types = ["DPL", "NDPL"]
    # result_types = ["+", "-", "="]

    # colors = ["r", "g", "b"]
    colors = ['#e31a1c', '#feb24c']
    hatches = ["/", "\\", "-"]
    width = 0.35  # the width of the bars

    plt.clf()
    # figure = plt.figure()

    if data_to_plot == "preparator":
        font = {'family': 'normal',
                # 'weight': 'bold',
                'size': 22
                }

        plt.rc('font', **font)

        # fig, ax = plt.subplots()
        fig, ax = plt.subplots(figsize=(8, 8), squeeze=True)
    else:
        fig, ax = plt.subplots()

    values_to_plot = sorted(df_results[data_to_plot].unique())
    ind = np.arange(len(values_to_plot))

    rects = {}
    for i, type in list(enumerate(["-", "+"])):
        # rects[type] = ax.bar(ind + i * width, data_for_barchart[type], width, color=colors[i], hatch=hatches[i], alpha=1.0)
        values = []
        for v_to_plot in reversed(values_to_plot):
            values.append(
                df_results[(df_results[data_to_plot] == v_to_plot) & (df_results["result_type"] == type)]["ratio"].iat[
                    0])
        # rects[type] = ax.barh(ind + i * width, values, width, color=colors[i], hatch=hatches[i], alpha=1.0)
        rects[type] = ax.barh(ind + i * width, values, width, color=colors[i], alpha=1.0)

    ax.set_ylabel(pretify[data_to_plot])

    ax.set_xlabel('Percentage (%) of record pairs (DPL and NDPL)')

    ax.set_yticks(ind + 0.5 * width)

    group_labels = []
    for item in sorted(values_to_plot, reverse=True):
        # records = nmetadata[]
        # group_labels.append((item if item != "overall_enrichment" else "overall\nenrichment") + "\n" + "(" + (
        # "{:,}".format(num_pairs)) + ")")

        v = item

        # v = v.replace("_", " ")
        v = v.replace(" ", "_")

        if v in pretify:
            v = pretify[v]

        group_labels.append(v)

    ax.set_yticklabels(tuple(group_labels))

    # plt.ylim([0.0, 1.0])
    # plt.yticks(rotation=90)

    # ax.invert_yaxis()  # labels read top-to-bottom
    # plt.gca().invert_yaxis()

    # ax.legend((rects["+"], rects["-"], rects["="]), ("+", "-", "="), loc='lower right')
    # ax.legend((rects["+"], rects["-"], rects["="]), ("+", "-", "="), loc='upper right')
    # ax.legend((rects["-"], rects["+"]), ("-", "+"), loc='upper right')

    # ax.legend((rects["+"], rects["-"]), ("+", '-'), loc='lower right', handlelength=10)
    # ax.legend((rects["+"], rects["-"]), ("+", '-'), loc='lower right')
    ax.legend((rects["+"], rects["-"]), ("+", '−'), loc='upper right')

    # plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    # plt.tight_layout(h_pad=1.0)

    plt.margins(tight=False)

    plt.savefig(fpath, bbox_inches='tight', format="pdf")
    plt.close()


def _plot_barchart_stacked_by_preparator(df, fpath):
    data_to_plot = "preparator"
    colors = ["#b35806", "#f1a340", "#fee0b6", "#d8daeb", "#998ec3", "#542788"]
    hatches = ["/", "\\", "+", "x", "*", "."]

    width = 0.35  # the width of the bars
    plt.clf()

    fontsize = 32
    font = {
        # 'family': 'normal',
        'size': fontsize
    }
    matplotlib.rcParams.update({'font.size': fontsize})

    plt.rc('font', **font)

    # width, height
    fig, ax = plt.subplots(figsize=(11, 10.5), squeeze=True)
    # fig, ax = plt.subplots(figsize=(8, 8))

    rects_by_type_dataset = {}
    datasets = sorted(list(df["dataset"].unique()))
    # preparators = sorted(list(df["preparator"].unique()), key=lambda x: CONF["all_preparators"].index(x))
    preparators = sorted(list(df["preparator"].unique()), key=lambda x: CONF["preparators_info"][x]["order"],
                         reverse=True)
    ind = np.arange(len(preparators))
    print(df)
    # datasets = datasets[:2]
    print(preparators)
    max_width = -1
    for i, type in list(enumerate(["-", "+"])):
        rects_by_type_dataset[type] = {}
        df_rt = df[df["result_type"] == type]
        # previous_dataset_values = [0.0] * len(preparators)
        # previous_dataset_values = np.zeros((1, len(preparators)))
        previous_dataset_values = np.zeros((len(preparators),), dtype=float)
        # previous_dataset_values = np.zeros([1, len(datasets)])
        for j, dataset in enumerate(datasets):
            df_rt_dt = df_rt[df_rt["dataset"] == dataset]
            # values = []
            values = []
            for prp_index, prp in enumerate(preparators):
                df_rt_dt_prp = df_rt_dt[df_rt_dt["preparator"] == prp]
                # v = previous_dataset_values[prp_index]
                v = 0.0
                if len(df_rt_dt_prp) > 0:
                    v += float(df_rt_dt_prp["ratio"].iat[0])
                values.append(v)

            values_nparray = np.array(values)
            mask = values_nparray.nonzero()  # Necessary to hide thin bars even in preparators where only one dataset has an effect.

            rects_by_type_dataset[type][dataset] = ax.barh((ind[mask]) + i * (width + 0.05), width=values_nparray[mask],
                                                           height=width, left=previous_dataset_values[mask],
                                                           color=colors[j], alpha=1.0, align="center")

            # previous_dataset_values += np.array(values)
            for li in range(len(previous_dataset_values)):
                previous_dataset_values[li] += values[li]
            max_width = max(max_width, max(values))
            print("Type: " + type + " , dataset: " + dataset + " , preparators: " + str(values))
    ax.set_ylabel(pretify[data_to_plot])
    ax.set_xlabel('Percentage (%) of record pairs (DPL and NDPL) per dataset', horizontalalignment='right', x=1.0)
    # ax.set_xticks(np.arange(0, max_width + 1, 100))

    group_labels = []
    for item in reversed(preparators):
        v = item
        v = v.replace(" ", "_")
        if v in pretify:
            v = pretify[v]
        group_labels.append(v)

    y = list()
    for i in ind:
        y.extend([i + 0.05, i + width / 2. + 0.05, i + width + 0.05])
    # ax.set_yticks(ind + 0.5 * width)
    ax.set_yticks(y)
    # ax.set_xticks(y)
    multi_group_labels = []
    for gl in group_labels:
        multi_group_labels.extend(["+", gl + "    ", "−"])

    ax.tick_params(axis='y', which='both', length=0)
    ax.set_xticks(np.arange(0, 600, step=100))
    # ax.set_yticklabels(tuple(group_labels))
    ax.set_yticklabels(reversed(tuple(multi_group_labels)), fontsize=fontsize)
    ax.legend(tuple([rects_by_type_dataset["+"][dataset][0] for dataset in datasets]), tuple(datasets),
              loc='upper right', prop={'size': 30})
    # ax.legend(loc='upper right')

    plt.ylim([-0.3, len(preparators) - 0.3])
    # plt.axis('tight')
    # plt.margins(0.02, 0)
    # plt.margins(tight=True)

    plt.savefig(fpath, bbox_inches='tight', format="pdf")
    # plt.savefig(fpath, format="pdf")
    # plt.close()


def scatterplot_before_after_similarity(base_filepath, group_by):
    pair_types = ["DPL", "NDPL"]
    # data_values = sorted(set(data[data_to_plot]), reverse=True)
    if group_by == "dataset":
        datasets = CONF["datasets"]
        for dataset in datasets:
            sql_query = """
                                            select * from preparation_similarities_differences_cube
                                            where `dataset` = '{dataset}'
                                            order by rand(1)
                                            limit 50000 
                                            """ \
                .format(dataset=dataset)
            df = pd.read_sql_query(sql_query, db_connection)
            data_chart = []
            for i, r in df.iterrows():
                before = r["similarity_before"]
                after = r["similarity_after"]

                # if after == before:
                #     continue

                data_chart.append((r["pair_class"], before, after))
            _plot_scatterplot(data_chart, base_filepath + dataset + ".pdf")
    elif group_by == "preparator":
        preparators = CONF["all_preparators"]
        # sql_query = "select * from preparation_similarities_differences_cube "
        for preparator in preparators:
            # preparator_sql_query = sql_query + " and ".join(["`" + p  + "`=" + str(p==preparator) for p in preparators])
            # preparator_sql_query = sql_query + " where `" + preparator + "` = True"
            # preparator_sql_query += " order by rand(1) limit 500000 "
            data_chart = []
            datasets = CONF["datasets"]
            for dataset in datasets:
                sql_query = "select * from preparation_similarities_differences_cube  where " \
                            "`dataset`='" + dataset + "' and `" + preparator + "` = True order by rand(0) limit 5000"
                df = pd.read_sql_query(sql_query, db_connection)
                for i, r in df.iterrows():
                    before = r["similarity_before"]
                    after = r["similarity_after"]
                    # if after == before:
                    #     continue
                    data_chart.append((r["pair_class"], before, after))
            _plot_scatterplot(data_chart, base_filepath + preparator + ".pdf")
    print("hi")


def _plot_scatterplot(data_chart, fpath):
    plt.clf()
    # figure = plt.figure()
    fig, ax = plt.subplots()
    # fig.set_size_inches(18.5, 10.5)

    data_chart_df = pd.DataFrame.from_records(data_chart, columns=["class", "before", "after"], index=None)

    sample_size = 5000
    if len(data_chart_df) > sample_size:
        data_chart_df = data_chart_df.sample(sample_size)

    condition_DPL = data_chart_df["class"] == "dpl"
    condition_NDPL = data_chart_df["class"] == "ndpl"

    sc2 = plt.scatter(data_chart_df[condition_NDPL]["after"], data_chart_df[condition_NDPL]["before"], marker='^',
                      # facecolors='none', edgecolors="r")
                      facecolors='none', edgecolors="#feb24c")
    sc1 = plt.scatter(data_chart_df[condition_DPL]["after"], data_chart_df[condition_DPL]["before"], marker="o",
                      # facecolors='none', edgecolors="g")
                      facecolors='none', edgecolors="#e31a1c")

    # diag_line, = ax.plot(ax.get_xlim(), ax.get_ylim(), ls="--", c=".3")
    diag_line, = ax.plot(ax.get_xlim(), ax.get_ylim(), ls="--", c="#0000FF")

    # ax.set_aspect()
    # ax.set_title("Similarity of pairs before and after " + data_to_plot + ": " + (pretify[group_by_value] if group_by_value in pretify else group_by_value))
    ax.set_ylabel("Similarity before")
    ax.set_xlabel("Similarity after")

    # plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    # plt.tight_layout(h_pad=1.0)
    ax.legend((sc1, sc2), ("DPL pairs", "NDPL pairs"), loc="upper left")
    plt.margins(tight=False)

    # plt.show()
    # plt.savefig(, bbox_inches='tight', format="pdf")
    plt.savefig(fpath, bbox_inches='tight', format="pdf")
    plt.close()


def set_box_color(bp, color):
    plt.setp(bp['boxes'], color=color)
    plt.setp(bp['whiskers'], color=color)
    plt.setp(bp['caps'], color=color)
    plt.setp(bp['medians'], color=color)


if __name__ == '__main__':
    main()
