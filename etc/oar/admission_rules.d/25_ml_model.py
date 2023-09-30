
# check variables
print(resource_request, properties, types) # debug


def model(resource_request, properties):
    """
    Return: String
    "find=compact" or "find=spread" or "find=no_pref"
    """
    type_from_ml = 'find=no_pref'

    # placeholder functionality of ML

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
