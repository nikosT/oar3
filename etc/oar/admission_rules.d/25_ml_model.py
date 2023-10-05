
# check variables
print(resource_request, properties, types) # debug


def model(resource_request, properties):
    """
    Return: String
    "find=compact" or "find=spread" or "find=no_pref"
    """
    type_from_ml = 'find=no_pref'

    # placeholder functionality of ML
    from joblib import load
    import pandas as pd
    import numpy as np

    model = load('/etc/oar/admission_rules.d/trainedGradientBoostingRegressor.model')
    dummy_feature_vector = pd.DataFrame(np.zeros((1, 12)), columns=['avg_total_time_A',
                                                                    'compute_time_A',
                                                                    'mpi_time_A',
                                                                    'ipc_A',
                                                                    'dp_FLOPS_per_node_A',
                                                                    'bw_per_node_A',
                                                                    'avg_total_time_B',
                                                                    'compute_time_B',
                                                                    'mpi_time_B',
                                                                    'ipc_B',
                                                                    'dp_FLOPS_per_node_B',
                                                                    'bw_per_node_B',])

    prediction = model.predict(dummy_feature_vector)[0]

    if prediction < 0.9:
        type_from_ml = 'find=compact'
    elif prediction > 1.1:
        type_from_ml = 'find=spread'
    else:
        type_from_ml = 'find=no_pred'

    return type_from_ml


# check if user wants specifically a particular tag
# if yes, let it be
# if no, use ml model to define it
# this is: if no
if not (('find=compact' in types) or ('find=spread' in types) or ('find=no_pref' in types)) or\
   not (('compact' in types) or ('spread' in types) or ('no_pref' in types)):

    # type_from_ml can be: "find=compact" or "find=spread" or "find=no_pref"
    type_from_ml = model(resource_request, properties)

    # append type in submission
    types.append(type_from_ml)
