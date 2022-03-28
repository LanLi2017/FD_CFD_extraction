import copy
import os

from dpd_utils.utils import _get_execution_default_parameters


def _group_by_attribute(preparations_info):
    group_by_attribute = {}
    for prtn in preparations_info.values():
        attr = prtn["attribute"]
        if attr not in group_by_attribute:
            group_by_attribute[attr] = set()
        group_by_attribute[attr].update(prtn["preparators"].split("~"))
    return group_by_attribute


def get_valid_preparations(dataset, attributes, preparators):
    dataset_preparators = copy.deepcopy(preparators)

    if len(dataset_preparators) == 0:
        dataset_preparators.append("_")

    if dataset != "hotels":
        if "normalize_address" in dataset_preparators:
            dataset_preparators.remove("normalize_address")
        if "geocode" in dataset_preparators:
            dataset_preparators.remove("geocode")
    if dataset != "census":
        if "split_attribute" in dataset_preparators:
            # There is nothing clear to split in the other datasets. More sophisticated methods need to be applied
            # prior to splitting an attribute.
            dataset_preparators.remove("split_attribute")
    elif dataset == "census":
        if "merge_attributes" in dataset_preparators:
            dataset_preparators.remove("merge_attributes")  # They are already merged initially

    valid_preparations = []
    for attr in sorted(attributes):
        for pr in sorted(dataset_preparators):
            # if pr == "split_attribute" and attr != "text":
            #     continue
            if pr == "merge_attributes" and attr != "merged_values":
                continue
            if pr == "geocode" and attr != "latitude_longitude":
                continue
            if dataset == "hotels" and pr == "normalize_address":
                if attr not in ["street_address1", "city", "zip", "state_code", "country_code"]:
                    continue

            valid_preparations.append((attr, pr))

    return valid_preparations


def apply_preparations(dataset, preparations, attributes, xstandard, is_chain_preparators, CONF):
    jar_path = CONF["dataprepdedup_jar"]
    main_class = "deduplication.preparation.DataPreparationController"
    java_jar_exec_base = "java " + CONF["java_vm_parameters"] + " -cp " + jar_path + " " + main_class

    parameters = _get_execution_default_parameters(dataset, preparations, attributes, xstandard, is_chain_preparators,
                                                   CONF)

    cmd = java_jar_exec_base + " " + " ".join(v[0] + "==" + v[1] for v in parameters)
    print(cmd)
    os.system(cmd)
