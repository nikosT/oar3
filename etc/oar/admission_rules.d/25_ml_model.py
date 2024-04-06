def model(session, config, resource_request, properties, job_name):
    """
    Return: String
    "find=compact" or "find=spread" or "find=no_pref"
    """
    import numpy as np
    import pandas as pd
    import sqlalchemy.orm
    from sqlalchemy import exc

    import oar.lib.globals
    from oar.lib.resource_handling import get_ml_model, get_performance_counters

    def estimate_job_nb_resources_in_spread(
        session, config, resource_request, j_properties
    ):
        """
        returns an array with an estimation of the number of resources that can be used by a job:
        (resources_available, [(nbresources => int, walltime => int)])
        """

        # estimate_job_nb_resources
        estimated_nb_resources = []
        is_resource_available = False
        resource_set = ResourceSet(session, config)
        resources_itvs = resource_set.roid_itvs

        for mld_idx, mld_resource_request in enumerate(resource_request):
            resource_desc, walltime = mld_resource_request

            if not walltime:
                walltime = str(config["DEFAULT_JOB_WALLTIME"])

            estimated_nb_res = 0

            for prop_res in resource_desc:
                jrg_grp_property = prop_res["property"]
                resource_value_lst = prop_res["resources"]

                #
                # determine resource constraints
                #
                if (not j_properties) and (
                    not jrg_grp_property or (jrg_grp_property == "type='default'")
                ):  # TODO change to re.match
                    # copy itvs
                    constraints = copy.copy(resource_set.roid_itvs)
                else:
                    and_sql = ""
                    if j_properties and jrg_grp_property:
                        and_sql = " AND "
                    if j_properties is None:
                        j_properties = ""
                    if jrg_grp_property is None:
                        jrg_grp_property = ""

                    sql_constraints = j_properties + and_sql + jrg_grp_property

                    try:
                        request_constraints = (
                            session.query(Resource.id)
                            .filter(text(sql_constraints))
                            .all()
                        )
                    except exc.SQLAlchemyError:
                        error_code = -5
                        error_msg = (
                            "Bad resource SQL constraints request:"
                            + sql_constraints
                            + "\n"
                            + "SQLAlchemyError: "
                            + str(exc)
                        )
                        error = (error_code, error_msg)
                        return (error, None, None)

                    roids = [
                        resource_set.rid_i2o[int(y[0])] for y in request_constraints
                    ]
                    constraints = ProcSet(*roids)

                hy_levels = []
                hy_nbs = []
                for resource_value in resource_value_lst:
                    res_name = resource_value["resource"]
                    if res_name not in resource_set.hierarchy:
                        possible_options = ", ".join(resource_set.hierarchy.keys())
                        error_code = -3
                        error_msg = (
                            f"Bad resources name: {res_name} is not a valid resources name."
                            f"Valid resource names are: {possible_options}"
                        )
                        error = (error_code, error_msg)
                        return (error, None, None)

                    value = resource_value["value"]
                    hy_levels.append(resource_set.hierarchy[res_name])
                    hy_nbs.append(int(value))

                cts_resources_itvs = constraints & resources_itvs

                for soc in resource_set.hierarchy["cpu"]:
                    avail_cores = soc & cts_resources_itvs
                    cts_resources_itvs -= ProcSet(
                        *avail_cores[int(len(soc) / 2) : len(soc)]
                    )

                res_itvs = find_resource_hierarchies_scattered(
                    cts_resources_itvs, hy_levels, hy_nbs
                )
                if res_itvs:
                    estimated_nb_res += len(res_itvs)
                    # break

            if estimated_nb_res > 0:
                is_resource_available = True

            estimated_nb_resources.append((estimated_nb_res, walltime))

        if not is_resource_available:
            error = (-5, "There are not enough resources for your request")
            return (error, None, None)

        return ((0, ""), is_resource_available, estimated_nb_resources)

    type_from_ml = "find=compact"

    try:
        (name, procs) = job_name.split("-")

        perf_counters = get_performance_counters(session, name=name, procs=procs)

        if perf_counters is None:
            print(f"No performance counters found for the {name}")
            vector = np.zeros((1, 12))
        else:
            print(f"Performance counters found for the {name}")
            data = perf_counters[
                [
                    "avg_total_time",
                    "compute_time",
                    "mpi_time",
                    "ipc",
                    "dp_flops_per_node",
                    "bw_per_node",
                ]
            ]
            vector = np.append(data.values[0], np.zeros(6)).reshape((1, 12))

        # Building a feature vector
        feature_vector = pd.DataFrame(
            vector,
            columns=[
                "avg_total_time_A",
                "compute_time_A",
                "mpi_time_A",
                "ipc_A",
                "dp_FLOPS_per_node_A",
                "bw_per_node_A",
                "avg_total_time_B",
                "compute_time_B",
                "mpi_time_B",
                "ipc_B",
                "dp_FLOPS_per_node_B",
                "bw_per_node_B",
            ],
        )

        # Loading the ML model
        model = get_ml_model(session, "iccs_v1")

        # Making a prediction
        prediction = model.predict(feature_vector)[0]

        if prediction < 0.9:
            type_from_ml = "find=compact"
        elif prediction > 1.1:
            type_from_ml = "find=spread"
        else:
            type_from_ml = "find=no_pref"

        print("Prediction made by ML model: {}".format(type_from_ml))

        if type_from_ml == "find=spread":
            if (
                estimate_job_nb_resources_in_spread(
                    session, config, resource_request, properties
                )[0][0]
                < 0
            ):
                type_from_ml = "find=compact"
                print(
                    "Fallback to compact since cluster does not support so large allocation"
                )

        return type_from_ml

    except:
        return type_from_ml


# make sure that user specifies "ml" without
# "compact", "spread", or "no_pref" tag
if "ml" in types:
    if (
        ("find=compact" not in types)
        and ("find=spread" not in types)
        and ("find=no_pref" not in types)
        and ("compact" not in types)
        and ("spread" not in types)
        and ("no_pref" not in types)
    ):

        # type_from_ml can be: "find=compact" or "find=spread" or "find=no_pref"
        type_from_ml = model(session, config, resource_request, properties, name)
        types.append(type_from_ml)
