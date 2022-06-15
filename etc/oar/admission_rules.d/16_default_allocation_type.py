exists = 0

for t in types:
    if re.match("^allocation_type=(?:compact|spread|dont-care)$", t):
        exists = 1
        break

if not exists:
    types.append("allocation_type=dont-care")
