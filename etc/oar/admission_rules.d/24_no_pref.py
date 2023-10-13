if "no_pref" in types:
    types = list(map(lambda t: t.replace("no_pref", "find=no_pref"), types))
