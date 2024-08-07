if "no_pref" in types:
    types = list(map(lambda t: t.replace("no_pref", "find=no_pref"), types))

    for mld_idx, mld_resource_request in enumerate(resource_request):
        resource_desc, walltime = mld_resource_request
        for prop_res in resource_desc:
            for resource_value in prop_res["resources"]:
                if not resource_value["resource"] == "core":
                    raise Exception(
                        "# ADMISSION RULE> Error: If type no_pref is given, only core can be used as resource type."
                    )
