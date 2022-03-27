import os
import sqlite3

import pandas as pd
import pymysql as pymysql
import sqlalchemy as sqlalchemy


def organize_pairs(pairs):
    c1, c2 = "id1", "id2"
    invalid_ids = []
    for row_index, row in pairs.iterrows():
        try:
            if int(row[c1]) > int(row[c2]):
                tmp = row[c1]
                # pairs.set_value(row_index, c1, row[c2])
                # pairs.set_value(row_index, c2, tmp)
                pairs.at[row_index, c1] = row[c2]
                pairs.at[row_index, c2] = tmp
        except:
            pass
    return pairs


def prepare_data_and_pairs_for_xstandard(dataset, datasets_info, xstandard_label,
                                         xstandard_ratio, CONF):
    # if dataset in ["cora", "movies_metablocking", "imdb_tmdb" "hotels"]:
    #     xstandard_ratio *= 0.01

    dataset_table = dataset + "_of_" + xstandard_label
    pairs_table = dataset + "_pairs_of_" + xstandard_label
    if not (_check_if_table_exists(dataset_table, CONF)) or not (_check_if_table_exists(pairs_table, CONF)):
        # Limit dataset records to those existing in the gold standard.
        attributes = set(CONF["datasets_info"][dataset]["attributes"])
        attributes.add("id")
        data = pd.read_csv(datasets_info[dataset]["dataset"], sep="\t", header=0, na_filter=False, index_col=None,
                           dtype=str, usecols=attributes, encoding='utf-8')
        data.set_index('id', inplace=True)

        if xstandard_label == "silverstandard_single_preparations":
            dpl = pd.read_csv(datasets_info[dataset]["dpl"], sep="\t", dtype=str, index_col=None,
                              usecols=["id1", "id2", "sim_total"])
            ndpl = pd.read_csv(datasets_info[dataset]["ndpl"], sep="\t", dtype=str, index_col=None,
                               usecols=["id1", "id2", "sim_total"])

            if dataset == "hotels":
                xstandard_ratio = 0.03
            elif dataset == "cora":
                xstandard_ratio = 0.03
                # xstandard_ratio = 0.1

            dpl["sim_total"] = pd.to_numeric(dpl["sim_total"])
            ndpl["sim_total"] = pd.to_numeric(ndpl["sim_total"])
            dpl.sort_values(by=["sim_total"], inplace=True, ascending=True)
            ndpl.sort_values(by=["sim_total"], inplace=True, ascending=False)
            dpl = dpl.head(n=int(len(dpl) * xstandard_ratio)).reset_index(drop=True)
            ndpl = ndpl.head(n=int(len(ndpl) * xstandard_ratio)).reset_index(drop=True)

            # dpl = dpl.sample(n=int(len(dpl) * xstandard_ratio), random_state=0).reset_index(drop=True)
            # ndpl = ndpl.sample(n=int(len(ndpl) * xstandard_ratio), random_state=0).reset_index(drop=True)

            dpl = organize_pairs(dpl)
            ndpl = organize_pairs(ndpl)
        elif xstandard_label == "silverstandard":
            dpl = pd.read_csv(datasets_info[dataset]["dpl"], sep="\t", dtype=str, index_col=None,
                              usecols=["id1", "id2", "sim_total"])
            ndpl = pd.read_csv(datasets_info[dataset]["ndpl"], sep="\t", dtype=str, index_col=None,
                               usecols=["id1", "id2", "sim_total"])

            # dpl = dpl.sample(n=int(len(dpl)), random_state=1).reset_index(drop=True)
            # ndpl = ndpl.sample(n=int(len(ndpl)), random_state=1).reset_index(drop=True)

            if dataset == "hotels":
                xstandard_ratio = 0.01
            elif dataset == "cora":
                xstandard_ratio = 0.01
                # xstandard_ratio = 0.1

            dpl["sim_total"] = pd.to_numeric(dpl["sim_total"])
            ndpl["sim_total"] = pd.to_numeric(ndpl["sim_total"])
            dpl.sort_values(by=["sim_total"], inplace=True, ascending=True)
            ndpl.sort_values(by=["sim_total"], inplace=True, ascending=False)
            dpl = dpl.head(n=int(len(dpl) * xstandard_ratio)).reset_index(drop=True)
            ndpl = ndpl.head(n=int(len(ndpl) * xstandard_ratio)).reset_index(drop=True)

            dpl = organize_pairs(dpl)
            ndpl = organize_pairs(ndpl)
        elif xstandard_label == "goldstandard":
            # dpl = pd.read_csv(datasets_info[dataset]["dpl"], sep="\t", dtype=str, index_col=None, usecols=["id1", "id2"])
            # ndpl = pd.read_csv(datasets_info[dataset]["ndpl"], sep="\t", dtype=str, index_col=None, usecols=["id1", "id2"])
            dpl = pd.read_csv(datasets_info[dataset]["dpl"], sep="\t", dtype=str, index_col=None,
                              usecols=["id1", "id2", "sim_total"])
            ndpl = pd.read_csv(datasets_info[dataset]["ndpl"], sep="\t", dtype=str, index_col=None,
                               usecols=["id1", "id2", "sim_total"])
            # dpl = dpl.sample(n=xstandard_ratio, random_state=0).reset_index(drop=True)
            # ndpl = ndpl.sample(n=len(dpl), random_state=0).reset_index(drop=True)

            dpl["sim_total"] = pd.to_numeric(dpl["sim_total"])
            ndpl["sim_total"] = pd.to_numeric(ndpl["sim_total"])
            dpl.sort_values(by=["sim_total"], inplace=True, ascending=True)
            ndpl.sort_values(by=["sim_total"], inplace=True, ascending=False)

            # dpl.sort_values(by=["sim_total"], inplace=True, ascending=False)
            # ndpl.sort_values(by=["sim_total"], inplace=True, ascending=True)

            if dataset in ["cora"]:
                xstandard_ratio = 0.1
                # xstandard_ratio = 1.0
                dpl = dpl.head(n=int(len(dpl) * xstandard_ratio)).reset_index(drop=True)
                ndpl = ndpl.head(n=int(len(ndpl) * xstandard_ratio)).reset_index(drop=True)
            elif dataset in ["hotels"]:
                xstandard_ratio = 0.4
                dpl = dpl.head(n=int(len(dpl) * xstandard_ratio)).reset_index(drop=True)
                ndpl = ndpl.head(n=int(len(ndpl) * xstandard_ratio)).reset_index(drop=True)

                # dpl = dpl.sample(n=int(len(dpl) * xstandard_ratio), random_state=0).reset_index(drop=True)
                # ndpl = ndpl.sample(n=int(len(ndpl) * xstandard_ratio), random_state=0).reset_index(drop=True)

            # dpl = dpl.sample(n=int(len(dpl) * xstandard_ratio), random_state=0).reset_index(drop=True)
            # ndpl = ndpl.sample(n=int(len(ndpl) * xstandard_ratio), random_state=0).reset_index(drop=True)

            dpl = organize_pairs(dpl)
            ndpl = organize_pairs(ndpl)

            # ndpl.sort_values(by=["id1", "id2"], inplace=True, ascending=True)
            # ndpl = ndpl.head(n=len(dpl)).reset_index(drop=True)
        elif xstandard_label == "goldstandard_nolimit":
            dpl = pd.read_csv(datasets_info[dataset]["dpl"], sep="\t", dtype=str, index_col=None,
                              usecols=["id1", "id2"])
            ndpl = pd.read_csv(datasets_info[dataset]["ndpl"], sep="\t", dtype=str, index_col=None,
                               usecols=["id1", "id2"])

            dpl = organize_pairs(dpl)
            ndpl = organize_pairs(ndpl)

        dpl["class"] = "dpl"
        ndpl["class"] = "ndpl"

        # Shuffle
        # dpl = dpl.head(n=int(len(dpl) * xstandard_ratio)).reset_index(drop=True)
        # ndpl = ndpl.head(n=int(len(ndpl) * xstandard_ratio)).reset_index(drop=True)
        # if float(len(ndpl)) > len(dpl) * ndpl_to_dpl_ratio:
        #     ndpl = ndpl.sample(n=int(len(dpl) * ndpl_to_dpl_ratio), random_state=0).reset_index(drop=True)

        pairs = pd.DataFrame(columns=["id1", "id2", "class"])
        pairs = pairs.append(dpl, sort=False, ignore_index=True)
        pairs = pairs.append(ndpl, sort=False, ignore_index=True)

        # def swap_ids(row):
        #     c1, c2 = "id1", "id2"
        #     if int(row[c1]) > int(row[c2]):
        #         return row[c2], row[c1]
        #     else:
        #         return row[c1], row[c2]
        #
        # pairs = pairs.apply(swap_ids, axis=1).to_frame()
        # pairs.columns = ["id1", "id2", "class"]

        # Keep only the relevant records.
        data, pairs = _keep_relevant_data(dataset, data, pairs, datasets_info)

        data["id"] = data.index

        db_con = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'
                                          .format(CONF["database_username"], CONF["database_password"],
                                                  CONF["database_ip"] + ":" + CONF["database_connection_port"],
                                                  CONF["database_schemaname"]))
        # db_con = pymysql.connect(host=CONF["database_ip"], port=int(CONF["database_connection_port"]),
        #                                 user=CONF["database_username"],
        #                                 password=CONF["database_password"], db=CONF["database_schemaname"],
        #                                 charset='utf8',  # charset='utf8mb4',
        #                                 cursorclass=pymysql.cursors.DictCursor)

        # cursor = db_connection.cursor()
        data.to_sql(con=db_con, name=dataset_table, if_exists='replace', index=False, chunksize=10000)
        pairs.to_sql(con=db_con, name=pairs_table, if_exists='replace', index=False, chunksize=10000)


def _keep_relevant_data(dataset, data, pairs, datasets_info):
    if dataset == "hotels":
        con = sqlite3.connect(datasets_info["hotels"]["dataset_geocoded"])
        # conn_in_main.row_factory = dict_factory

        # c_in_main = conn_in_main.cursor()
        data_normalized = pd.read_sql(sql="select * from hotels", con=con, index_col=None, columns=["id"], )
        # address_normalized_ids = set(data_normalized["id"].sample(n=2000, random_state=0).astype(str))
        address_normalized_ids = set(data_normalized["id"].astype(str))

        # Remove any pairs that are not in the normalized record ids.
        pairs_both_address_normalized_list = []
        # pairs = pairs.sample(n=2000, random_state=0)
        # pairs = pairs
        for i, p in pairs.iterrows():
            if p["id1"] in address_normalized_ids and p["id2"] in address_normalized_ids:
                pairs_both_address_normalized_list.append((p["id1"], p["id2"], p["class"]))

        pairs_both_address_normalized = pd.DataFrame.from_records(pairs_both_address_normalized_list,
                                                                  columns=["id1", "id2", "class"])

        pairs = pairs_both_address_normalized

    pair_ids = set()
    pair_ids.update(pairs["id1"])
    pair_ids.update(pairs["id2"])

    data_ids = set(data.index.values)
    intersection = data_ids & pair_ids

    valid_data = data[data.index.isin(intersection)]

    return valid_data, pairs


def _check_if_table_exists(table_name, CONF):
    db_connection = pymysql.connect(host=CONF["database_ip"], port=int(CONF["database_connection_port"]),
                                    user=CONF["database_username"],
                                    password=CONF["database_password"], db=CONF["database_schemaname"],
                                    charset='utf8',  # charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

    cursor = db_connection.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = '{0}' and table_name = '{1}'
            """.format(CONF["database_schemaname"].replace('\'', '\'\''), table_name.replace('\'', '\'\'')))
    except:
        pass
    if cursor.fetchone()["COUNT(*)"] == 1:
        cursor.close()
        return True

    cursor.close()
    db_connection.close()
    return False


def create_schema(schemaname, CONF):
    # db_connection = pymysql.connect(host=CONF["database_ip"], user=CONF["database_username"],
    #                                 password=CONF["database_password"], db=CONF["database_name"],
    #                                 charset='utf8',  # charset='utf8mb4',
    #                                 cursorclass=pymysql.cursors.DictCursor)
    # cursor = db_connection.cursor()
    try:

        # mydb = mysql.connector.connect(
        #     host=CONF["database_ip"],
        #     user=CONF["database_username"],
        #     passwd=CONF["database_password"]
        # )
        #
        # mycursor = mydb.cursor()

        mydb = pymysql.connect(host=CONF["database_ip"], user=CONF["database_username"],
                               password=CONF["database_password"],
                               charset='utf8',  # charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
        mycursor = mydb.cursor()

        mycursor.execute("CREATE DATABASE IF NOT EXISTS " + schemaname + ";")
        mycursor.close()
        mydb.close()
    except:
        pass


def _get_execution_default_parameters(dataset, preparations, attributes, xstandard, is_chain_preparators, CONF):
    parameters = []
    parameters.append(("dataset", dataset))
    parameters.append(("attributes", "-".join(attributes)))
    parameters.append(("db_username", CONF["database_username"]))
    parameters.append(("db_password", CONF["database_password"]))
    parameters.append(("db_schemaname", CONF["database_schemaname"]))
    parameters.append(("db_driver", CONF["database_driver"]))
    parameters.append(("db_connection_url", "'" + CONF["database_connection_url"] + "'"))

    parameters.append(("db_tablename", dataset + "_of_" + xstandard))
    parameters.append(("xstandard", xstandard))
    parameters.append(("is_chain_preparators", str(is_chain_preparators)))

    if preparations is not None:
        parameters.append(("total_preparations", str(len(preparations))))
        for i, prp in enumerate(preparations):
            parameters.append(("attribute_" + str(i), prp[0]))
            parameters.append(("preparator_" + str(i), prp[1]))
            parameters.append(("order_" + str(i), str(CONF["preparators_info"][prp[1]]["order"])))
            parameters.append(
                ("deduplication_specific_" + str(i), str(CONF["preparators_info"][prp[1]]["deduplication_specific"])))
    else:
        parameters.append(("total_preparations", str(len(attributes))))
        for i, attr in enumerate(attributes):
            parameters.append(("attribute_" + str(i), attr))
            parameters.append(("preparator_" + str(i), "_"))
            parameters.append(("order_" + str(i), "-1"))
            parameters.append(("deduplication_specific_" + str(i), "false"))
    return parameters


def calculate_similarities_preparations(dataset, preparations, attributes, xstandard, is_chain_preparators, CONF):
    jar_path = CONF["dataprepdedup_jar"]
    main_class = "deduplication.similarities.ProcessSimilarityCalculationController"
    java_jar_exec_base = "java " + CONF["java_vm_parameters"] + " -cp " + jar_path + " " + main_class

    parameters = _get_execution_default_parameters(dataset, preparations, attributes, xstandard, is_chain_preparators,
                                                   CONF)

    cmd = java_jar_exec_base + " " + " ".join(v[0] + "==" + v[1] for v in parameters)
    print(cmd)
    os.system(cmd)


def calculate_candidate_preparations(dataset, preparations, attributes, xstandard, is_chain_preparators, CONF):
    jar_path = CONF["dataprepdedup_jar"]
    main_class = "deduplication.preparation.DecideCandidatePreparationsController"
    java_jar_exec_base = "java " + CONF["java_vm_parameters"] + " -cp " + jar_path + " " + main_class

    parameters = _get_execution_default_parameters(dataset, preparations, attributes, xstandard, is_chain_preparators,
                                                   CONF)

    cmd = java_jar_exec_base + " " + " ".join(v[0] + "==" + v[1] for v in parameters)
    print(cmd)
    os.system(cmd)


def retrieve_candidate_preparations(dataset, xstandard, db_connection, CONF):
    sql_query = """
                select * from candidate_preparations
                where `dataset` = '{dataset}' and `xstandard` = '{xstandard}' """ \
        .format(dataset=dataset, xstandard=xstandard)
    df = pd.read_sql_query(sql_query, db_connection)

    candidate_preparations = df.at[0, "candidate_preparations"]

    # convert into a list
    candidate_preparations_list = []
    for part in candidate_preparations.split("-"):
        attribute_preparator = part.split("@")
        candidate_preparations_list.append((attribute_preparator[0], attribute_preparator[1]))

    return candidate_preparations_list


def calculate_similarities_differences_preparations(dataset, preparations, attributes, xstandard, is_chain_preparators,
                                                    CONF):
    jar_path = CONF["dataprepdedup_jar"]
    main_class = "deduplication.similarities.ProcessSimilarityDifferencesController"
    java_jar_exec_base = "java " + CONF["java_vm_parameters"] + " -cp " + jar_path + " " + main_class

    parameters = _get_execution_default_parameters(dataset, preparations, attributes, xstandard, is_chain_preparators,
                                                   CONF)

    cmd = java_jar_exec_base + " " + " ".join(v[0] + "==" + v[1] for v in parameters)
    print(cmd)
    os.system(cmd)
