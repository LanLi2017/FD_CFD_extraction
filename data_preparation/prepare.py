import logging.config
import logging.config
import os

import pandas as pd
import sqlalchemy
import yaml
from pyhocon import ConfigFactory
from tqdm import tqdm

# from dpd_utils.datasets_info import get_domains_dataset_goldstandard
from dpd_utils.utils import calculate_similarities_preparations, prepare_data_and_pairs_for_xstandard, create_schema, \
    calculate_candidate_preparations, retrieve_candidate_preparations, calculate_similarities_differences_preparations
from dpd_utils.utils_classification import classification_calculate_pr_curve_auc
from dpd_utils.utils_preparation import apply_preparations, \
    get_valid_preparations

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONF = ConfigFactory.parse_file(ROOT_DIR + '/resources/reference.conf')

execute_similarities = True
exit_after_similarities = False
# execute_preparations = False
execute_classification = True

db_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'
                                         .format(CONF["database_username"],
                                                 CONF["database_password"],
                                                 CONF["database_ip"] + ":" + CONF["database_connection_port"],
                                                 CONF["database_schemaname"]))

classifier = "THRESHOLD"

# score_metric = "classifier_f1"
# classification_style = "kfold"

score_metric = "auc_pr"
classification_style = "test"


def prepare_datasets_for_ndpl_ratio_experiment(dataset, datasets_info):
    # Prepare data and pairs for goldstandard.
    # prepare_data_and_pairs_for_xstandard(dataset, datasets_info, "goldstandard_nolimit", 1.0, CONF)
    prepare_data_and_pairs_for_xstandard(dataset, datasets_info, "goldstandard_nolimit", 1.0, CONF)


def calculate_differences_for_preparation(dataset):
    # datasets = CONF["datasets"]
    all_preparators = CONF["all_preparators"]
    # all_preparators = ["geocode"]

    attributes = CONF["datasets_info"][dataset]["attributes"]
    valid_preparations = get_valid_preparations(dataset, attributes, all_preparators)
    attributes = CONF["datasets_info"][dataset]["attributes"]  # Dataset's attributes.

    datasets_info = CONF["datasets_info"]

    prepare_data_and_pairs_for_xstandard(dataset, datasets_info, "silverstandard_single_preparations", 0.1, CONF)

    apply_preparations(dataset, None, attributes, "silverstandard_single_preparations", False, CONF)
    calculate_similarities_preparations(dataset, None, attributes, "silverstandard_single_preparations", False, CONF)

    apply_preparations(dataset, valid_preparations, attributes, "silverstandard_single_preparations", False, CONF)
    calculate_similarities_preparations(dataset, valid_preparations, attributes, "silverstandard_single_preparations",
                                        False, CONF)

    calculate_similarities_differences_preparations(dataset, valid_preparations, attributes,
                                                    "silverstandard_single_preparations", False, CONF)


def main():
    workspace_dir_base = CONF["workspace_dir_base"]
    all_preparators = CONF["all_preparators"]
    # sampling_percentage = CONF["sampling_percentage"]
    ndpl_to_dpl_ratio = CONF["ndpl_to_dpl_ratio"]
    classifiers = CONF["classifiers"]

    setup_logging()
    datasets_info = CONF["datasets_info"]

    create_schema(CONF["database_schemaname"], CONF)

    for dataset in CONF["datasets"]:
        logging.debug("dataset: " + dataset)

        # calculate_differences_for_preparation(dataset)
        # prepare_datasets_for_ndpl_ratio_experiment(dataset, datasets_info)
        # continue

        # Prepare data and pairs for silverstandard.
        prepare_data_and_pairs_for_xstandard(dataset, datasets_info, "silverstandard", 0.1, CONF)
        print("Prepared data and pairs for silverstandard")

        # Prepare data and pairs for goldstandard.
        prepare_data_and_pairs_for_xstandard(dataset, datasets_info, "goldstandard", 1.0, CONF)
        print("Prepared data and pairs for goldstandard")

        # Dataset directory for the experiments.
        workspace_dir = workspace_dir_base + dataset + "_" + str(CONF["sampling_percentage"]) + "/"
        if not os.path.isdir(workspace_dir):
            os.makedirs(workspace_dir)

        attributes = datasets_info[dataset]["attributes"]  # Dataset's attributes.

        # Execute
        discover_useful_preparations(dataset, attributes, classifiers, all_preparators, workspace_dir)


def discover_useful_preparations(dataset, attributes, classifiers, preparators, dataset_workspace_dir):
    """
    Calculate similarities, apply preparations and re-calculate them.
    """
    valid_preparations = get_valid_preparations(dataset, attributes, preparators)

    # Step 1. Prepare baseline: default values and similarities.
    apply_preparations(dataset, None, attributes, "silverstandard", False, CONF)
    calculate_similarities_preparations(dataset, None, attributes, "silverstandard", False, CONF)

    # Step 2. Apply simple preparations and calculate similarities
    apply_preparations(dataset, valid_preparations, attributes, "silverstandard", False, CONF)
    calculate_similarities_preparations(dataset, valid_preparations, attributes, "silverstandard", False, CONF)

    # Step 3. Classification baseline
    classification_calculate_pr_curve_auc(classifier, dataset, None, attributes, "silverstandard", False,
                                          classification_style, CONF)
    baseline_score = retrieve_classification_for_preparations(dataset, attributes, "silverstandard", "THRESHOLD",
                                                              score_metric, [], CONF)

    # Step 4. Decide candidate preparations (based on similarity)
    candidate_preparations = decide_candidate_preparations(dataset, attributes, "silverstandard", valid_preparations,
                                                           CONF)
    # candidate_preparations = valid_preparations[0:6]
    print("Candidate preparations: " + str(candidate_preparations))

    # Step 5. Classify the candidate preparations altogether.
    # First apply some new "complex" preparations that do not exist.
    apply_preparations(dataset, candidate_preparations, attributes, "silverstandard", True, CONF)
    calculate_similarities_preparations(dataset, candidate_preparations, attributes, "silverstandard", True, CONF)
    classification_calculate_pr_curve_auc(classifier, dataset, candidate_preparations, attributes, "silverstandard",
                                          True, classification_style, CONF)

    # Retrieve classification metrics
    candidate_preparation_score = retrieve_classification_for_preparations(dataset, attributes, "silverstandard",
                                                                           "THRESHOLD", score_metric,
                                                                           candidate_preparations, CONF)

    print("Candidate preparation score: " + str(candidate_preparation_score))

    # Step 6. Leave-one-out minimization
    minimized_preparations = set(candidate_preparations)
    for preparation in tqdm(candidate_preparations, desc="Leave-one-out minimization"):
        leave_one_out_preparations = list(candidate_preparations)
        leave_one_out_preparations.remove(preparation)

        apply_preparations(dataset, leave_one_out_preparations, attributes, "silverstandard", True, CONF)
        calculate_similarities_preparations(dataset, leave_one_out_preparations, attributes, "silverstandard", True,
                                            CONF)
        classification_calculate_pr_curve_auc(classifier, dataset, leave_one_out_preparations, attributes,
                                              "silverstandard", True, classification_style, CONF)
        leave_one_out_score = retrieve_classification_for_preparations(dataset, attributes, "silverstandard",
                                                                       "THRESHOLD", score_metric,
                                                                       leave_one_out_preparations, CONF)
        if leave_one_out_score >= candidate_preparation_score:
            # leave_one_out_preparators_successful.append(preparation)
            minimized_preparations.remove(preparation)

            print("Removing " + str(
                preparation) + " improved (or kept equal) the classification. Therefore we permanently remove it. " + str(
                candidate_preparation_score) + " -> " + str(leave_one_out_score))

    # The final scores will be calculated on the gold standards
    apply_preparations(dataset, None, attributes, "goldstandard", True, CONF)
    calculate_similarities_preparations(dataset, None, attributes, "goldstandard", True, CONF)
    classification_calculate_pr_curve_auc(classifier, dataset, None, attributes, "goldstandard", True,
                                          classification_style, CONF)
    baseline_goldstandard_score = retrieve_classification_for_preparations(dataset, attributes, "goldstandard",
                                                                           "THRESHOLD", score_metric, [], CONF)

    apply_preparations(dataset, minimized_preparations, attributes, "goldstandard", True, CONF)
    calculate_similarities_preparations(dataset, minimized_preparations, attributes, "goldstandard", True, CONF)
    classification_calculate_pr_curve_auc(classifier, dataset, minimized_preparations, attributes, "goldstandard", True,
                                          classification_style, CONF)
    minimized_goldstandard_score = retrieve_classification_for_preparations(dataset, attributes, "goldstandard",
                                                                            "THRESHOLD", score_metric,
                                                                            minimized_preparations, CONF)

    print("Minimized preparations: " + str(minimized_preparations))

    print("Final classification from " + str(baseline_goldstandard_score) + " -> " + str(minimized_goldstandard_score))


def decide_candidate_preparations(dataset, attributes, xstandard, valid_preparations, CONF):
    calculate_candidate_preparations(dataset, valid_preparations, attributes, "silverstandard", False, CONF)
    candidate_preparations = retrieve_candidate_preparations(dataset, xstandard, db_connection, CONF)
    return candidate_preparations


def retrieve_classification_for_preparations(dataset, attributes: set, xstandard, classifier, classification_metric,
                                             preparations, CONF):
    chain_preparations = get_chain_preparations(preparations, True)
    attributes_in_chain_preparations = set()
    for signature in chain_preparations.keys():
        attributes_in_chain_preparations.add(signature.split("@")[0])
    for attribute in set(attributes).difference(attributes_in_chain_preparations):
        chain_preparations[attribute + "@_"] = {"_"}

    attribute_preparations = []
    for signature, chain_preparation in chain_preparations.items():
        attr = signature.split("@")[0]
        preparators = chain_preparation
        attribute_preparations.append(attr + "@" + "~".join(sorted(preparators, key=lambda x: int(
            CONF["preparators_info"][x]["order"]) if x in CONF["preparators_info"] else 1)))

    sql_query = """
                select * from classification
                where `dataset` = '{dataset}' and `xstandard` = '{xstandard}' and `classifier` = '{classifier}'
                and `preparations` = '{attribute_preparations}'
                """ \
        .format(dataset=dataset, xstandard=xstandard, classifier=classifier,
                attribute_preparations="-".join(sorted(attribute_preparations)))
    df = pd.read_sql_query(sql_query, db_connection)
    row = df.head(n=1)
    return row[classification_metric][0]


def get_chain_preparations(preparations, is_chain_preparators):
    chained_preparations = {}
    if is_chain_preparators:
        attr_to_nondeduplication_preps = {}
        attr_to_deduplication_preps = {}

        attributes = set()
        for prp in preparations:
            attr = prp[0]
            preparator = prp[1]
            attributes.add(attr)

            if CONF["preparators_info"][preparator]["deduplication_specific"]:
                set_of_preps = attr_to_deduplication_preps[attr] if attr in attr_to_deduplication_preps else set()
                set_of_preps.add(preparator)
                attr_to_deduplication_preps[attr] = set_of_preps
            else:
                set_of_preps = attr_to_nondeduplication_preps[attr] if attr in attr_to_nondeduplication_preps else set()
                set_of_preps.add(preparator)
                attr_to_nondeduplication_preps[attr] = set_of_preps

        for attr in attributes:
            chained_preparation = set()
            if attr in attr_to_nondeduplication_preps:
                chained_preparation.update(attr_to_nondeduplication_preps[attr])
                chained_preparations[attr + "@" + "~".join(sorted(list(chained_preparation)))] = set(
                    chained_preparation)
            if attr in attr_to_deduplication_preps:
                for preparator in attr_to_deduplication_preps[attr]:
                    chained_preparation_deduplication = set(chained_preparation)
                    chained_preparation_deduplication.add(preparator)
                    chained_preparations[attr + "@" + "~".join(
                        sorted(list(chained_preparation_deduplication)))] = chained_preparation_deduplication

    else:
        chained_preparations = {}
        for prp in preparations:
            attribute = prp[0]
            preparator = prp[1]
            chained_preparations[attribute + "@" + preparator] = {preparator}
    return chained_preparations


def setup_logging(
        default_path='logging.yaml',
        default_level=logging.DEBUG,
        env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


if __name__ == '__main__':
    main()
