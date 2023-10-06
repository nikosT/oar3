
if 'no_pref' in types:
    types = list(map(lambda t: t.replace('no_pref','find=no_pref'),types))

# if not (('find=compact' in types) or ('find=spread' in types) or ('find=no_pref' in types)):
#     types.append('find=no_pref')

