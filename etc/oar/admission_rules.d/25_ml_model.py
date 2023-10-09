
# check variables
#print(resource_request, properties) # debug
#print(f"Types from previous admission rules = {types}")


def model(config, resource_request, properties, job_name):
    """
    Return: String
    "find=compact" or "find=spread" or "find=no_pref"
    """
    import pandas as pd
    import numpy as np
    from oar.lib.resource_handling import get_ml_model, get_performance_counters
    import oar.lib.globals
    import sqlalchemy.orm

    type_from_ml = 'find=compact'

    try:
        # initialize a session (a connector) to DB
        engine = oar.lib.globals.init_db(config)
        session_factory = sqlalchemy.orm.sessionmaker(bind=engine)
        scoped = sqlalchemy.orm.scoped_session(session_factory)
        session = scoped()

        (name, procs) = job_name.split('-')

        perf_counters = get_performance_counters(session, name=name, procs=procs)

        if perf_counters is None:
            print(f"No performance counters found for the {name}")
            vector = np.zeros((1,12))
        else:
            print(f"Performance counters found for the {name}")
            data = perf_counters[['avg_total_time',
                                  'compute_time',
                                  'mpi_time',
                                  'ipc',
                                  'dp_flops_per_node',
                                  'bw_per_node']]
            vector = np.append(data.values[0], np.zeros(6)).reshape((1, 12))

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
        model = get_ml_model(session, 'iccs_v1')

        # Making a prediction
        prediction = model.predict(feature_vector)[0]

        if prediction < 0.9:
            type_from_ml = 'find=compact'
        elif prediction > 1.1:
            type_from_ml = 'find=spread'
        else:
            type_from_ml = 'find=no_pref'

        print('Prediction made by ML model')
        return type_from_ml

    except:
        return type_from_ml

# check if user wants specifically a particular tag
# if yes, let it be
# if no, use ml model to define it
# this is: if no
if ('find=compact' not in types) and ('find=spread' not in types) and ('find=no_pref' not in types) and\
   ('compact' not in types) and ('spread' not in types) and ('no_pref' not in types):

    # type_from_ml can be: "find=compact" or "find=spread" or "find=no_pref"
    type_from_ml = model(config, resource_request, properties, name)


    print(types)


