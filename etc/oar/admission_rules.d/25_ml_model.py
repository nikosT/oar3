
# check variables
print(resource_request, properties) # debug
print(f"Types from previous admission rules = {types}")

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
    import re
    from os import path

    # Import the NAS/SPEC 'database'
    db = pd.read_csv('/etc/oar/admission_rules.d/nas-spec-oar-db.csv')

    print(f"BEFORE>{command}")

    # Remove the mpirun call
    command = command.replace("mpirun", "")
    command = command.replace("perf stat", "")

    # Find all the arguments passed to mpirun
    # and the executable
    m = re.findall(r"-+\w+\s*\w*", command)
    for string in m:
        command = command.replace(string, "")

    # Remove all the whitespaces
    command = command.strip()

    # Return the basename of the executable
    name = path.basename(command)
    name = name.lstrip().rstrip()

    print()
    print(f"NOW>{name}")
    print()

    # Check if the executable's name exists inside the db
    indices = db[db['name'].str.contains(name)].index.values

    if len(indices) == 0:
        print("The executable is not inside the database")
        print("We will collect the necessary performance counters")
        return "profiling"
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
if ('find=compact' not in types) and ('find=spread' not in types) and ('find=no_pref' not in types) and\
   ('compact' not in types) and ('spread' not in types) and ('no_pref' not in types):

    # type_from_ml can be: "find=compact" or "find=spread" or "find=no_pref"
    type_from_ml = model(resource_request, properties, command)

    if type_from_ml == "profiling":

        perf_str = "perf stat "

        fd = open("/etc/perf_codes", "r")

        # Skip csv header line
        for line in fd.readlines()[1:]:
            perf_expr = ','.join(line.split(',')[1:]).replace('\n', '')
            perf_str += "-e " + perf_expr + " "

        command = perf_str + command
        print(command)
        # command = "perf stat -e instructions " +\
        #         "-e r531010 -e r538010 "  +\
        #         "-e cpu/config=0x5301b7,config1=0x3f80408fff,name=OFFCORE_RESPONSE_0:LLC_MISS_LOCAL/ " +\
        #         "-e cycles " +\
        #         "-e fp_arith_inst_retired.128b_packed_double " +\
        #         "-e fp_arith_inst_retired.256b_packed_double " +\
        #         "-e fp_arith_inst_retired.scalar_double " +\
        #         "-e offcore_response.demand_code_rd.l3_miss_local_dram.any_snoop " + command
        type_from_ml = "find=compact"

    # append type in submission
    types.append(type_from_ml)
    print()
    print(types)
    print()
