if "f_compact" in types:
    types = list(map(lambda t: t.replace("f_compact", "find=f_compact"), types))

    for mld_idx, mld_resource_request in enumerate(resource_request):
        resource_desc, walltime = mld_resource_request
        for prop_res in resource_desc:
            for resource_value in prop_res["resources"]:
                if not resource_value["resource"] == "core":
                    raise Exception("# ADMISSION RULE> Error: If type f_compact is given, only core can be used as resource type.")
