
# check variables
print(resource_request, properties, types) # debug


def model(resource_request, properties, command):
    """
    Return: String
    "find=compact" or "find=spread" or "find=no_pref"
    """
    type_from_ml = 'find=no_pref'

    # placeholder functionality of ML
    from joblib import load
    import pandas as pd
    import numpy as np

    # Import the NAS 'database'
    db = pd.read_csv('/etc/oar/admission_rules.d/nas-oar-db.csv')

    # Check if it is a NAS benchmark
    # bench = command.split('.')
    # if len(bench) < 3:
    #     print("We have to gather perf counters")
    #     return type_from_ml
    # else:
    #     app, cls, procs = bench

    # Check if the executable's name exists inside the db
    indices = db[db['name'].str.contains(command)].index.values

    if len(indices) == 0:
        print("The executable is not inside the database")
        vector = np.zeros((1, 12))
    else:
        print(f"{command} exists inside the db!")
        vector = np.append(db.iloc[indices, 1:].values[0], np.zeros(6)).reshape((1, 12))

    # Building a feature vector
    feature_vector = pd.DataFrame(vector, columns=['avg_total_time_A',
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

    # Loading the ML model
    model = load('/etc/oar/admission_rules.d/trainedGradientBoostingRegressor.model')

    # Making a prediction
    prediction = model.predict(feature_vector)[0]

    if prediction < 0.9:
        type_from_ml = 'find=compact'
    elif prediction > 1.1:
        type_from_ml = 'find=spread'
    else:
        type_from_ml = 'find=no_pref'

    return type_from_ml


# check if user wants specifically a particular tag
# if yes, let it be
# if no, use ml model to define it
# this is: if no
if not (('find=compact' in types) or ('find=spread' in types) or ('find=no_pref' in types)) or\
   not (('compact' in types) or ('spread' in types) or ('no_pref' in types)):

    # type_from_ml can be: "find=compact" or "find=spread" or "find=no_pref"
    type_from_ml = model(resource_request, properties, command)

    # append type in submission
    types.append(type_from_ml)
    print(types)
