exists = 0

for t in types:
    if re.match("^(?:exclusive|spread|dont-care)$", t):
        exists = 1
        break

if not exists:
    types.append("dont-care")
