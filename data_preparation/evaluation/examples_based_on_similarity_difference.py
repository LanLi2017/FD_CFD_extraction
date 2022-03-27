import os

import pandas as pd
import sqlalchemy

from prepare import CONF

db_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'
                                             .format(CONF["database_username"],
                                              CONF["database_password"],
                                              CONF["database_ip"] + ":" + CONF["database_connection_port"],
                                              CONF["database_schemaname"]))


def produce_value_pairs_based_on_differences():
    xstandard = "silverstandard_single_preparations"
    datasets = CONF["datasets"]
    preparators = CONF["all_preparators"]

    # datasets = ["restaurants"]
    # preparators = ["acronymize"]

    # no_preparation_str = " and ".join(prp + " = False" for prp in preparators)

    statistics_dir = CONF["workspace_dir_base"] + "pair_stats/"
    if not os.path.exists(statistics_dir):
        os.makedirs(statistics_dir)

    for dataset in datasets:
        sql_query_data = """select * from {dataset}_of_{xstandard}""" \
            .format(dataset= dataset, xstandard=xstandard)
        df_data_before = pd.read_sql_query(sql_query_data, db_connection, index_col=["id"])
        data_before = df_data_before.to_dict(orient="index")
        for preparator in preparators:
            sql_query_data = """select * from preparation_data_cube 
                               where dataset='{dataset}' and `xstandard` = '{xstandard}' and `{preparator}`={preparator_state}""" \
                .format(dataset=dataset, xstandard="silverstandard_single_preparations", preparator=preparator, preparator_state=True)
            print(sql_query_data)
            df_data_after = pd.read_sql_query(sql_query_data, db_connection)
            # data_after = df_data_after.to_dict(orient="index")

            sql_query_dpl = """select * from preparation_similarities_differences_cube
                               where dataset='{dataset}' and `xstandard` = '{xstandard}' and `{preparator}`=True and `pair_class`='dpl'
                               order by difference
                               limit 1000
                               """ \
                .format(dataset=dataset, xstandard=xstandard, preparator=preparator)
            df_dpl = pd.read_sql_query(sql_query_dpl, db_connection)

            sql_query_ndpl = """select * from preparation_similarities_differences_cube
                                where dataset='{dataset}' and `xstandard` = '{xstandard}' and `{preparator}`=True and `pair_class`='ndpl'
                                order by difference desc
                                limit 1000
                                """ \
                .format(dataset=dataset, xstandard=xstandard, preparator=preparator)
            df_ndpl = pd.read_sql_query(sql_query_ndpl, db_connection)

            results = []
            for i, r in df_dpl.iterrows():
                id1 = r["record_id1"]
                id2 = r["record_id2"]
                attribute = r["attribute"]
                similarity_before = r["similarity_before"]
                similarity_after = r["similarity_after"]
                difference = r["difference"]

                v1_before = data_before[id1][attribute]
                v2_before = data_before[id2][attribute]
                # v1_after = data_after[id1][attribute]
                # v2_after = data_after[id2][attribute]
                v1_after = df_data_after[(df_data_after["record_id"] == str(id1)) & (df_data_after["attribute"] == attribute)]["value"].iat[0]
                v2_after = df_data_after[(df_data_after["record_id"] == str(id2)) & (df_data_after["attribute"] == attribute)]["value"].iat[0]

                results.append(("dpl", id1, id2, attribute, similarity_before, similarity_after, difference, v1_before, v1_after, v2_before, v2_after))
            for i, r in df_ndpl.iterrows():
                id1 = r["record_id1"]
                id2 = r["record_id2"]
                attribute = r["attribute"]
                similarity_before = r["similarity_before"]
                similarity_after = r["similarity_after"]
                difference = r["difference"]

                if id1 not in data_before:
                    print("id: " + str(id1) + " not found, continuing")
                    continue
                if id2 not in data_before:
                    print("id: " + str(id2) + " not found, continuing")
                    continue

                v1_before = data_before[id1][attribute]
                v2_before = data_before[id2][attribute]
                v1_after = \
                df_data_after[(df_data_after["record_id"] == str(id1)) & (df_data_after["attribute"] == attribute)][
                    "value"].iat[0]
                v2_after = \
                df_data_after[(df_data_after["record_id"] == str(id2)) & (df_data_after["attribute"] == attribute)][
                    "value"].iat[0]

                results.append(("ndpl", id1, id2, attribute, similarity_before, similarity_after, difference, v1_before, v1_after, v2_before, v2_after))

            df_results = pd.DataFrame(results, columns=["pair_class", "id1", "id2", "attribute", "similarity_before",
                                                        "similarity_after","difference", "v1_before", "v1_after",
                                                        "v2_before", "v2_after"])
            df_results.to_csv(statistics_dir + dataset + "_" + preparator + ".tsv", sep="\t", index=False)


    pass

if __name__ == '__main__':
    produce_value_pairs_based_on_differences()