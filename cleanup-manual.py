# grep -oE '\/share\/[^"]+' log.txt  >> file.txt
# drc logs harvest_cleanup | grep 'could not delete.*share://[^'\'']*'  | grep -oE '/share/[^'\'']+' >> /tmp/file.txt
import os
f = open("file.txt", "r")
for p in f:
    p = p.strip().replace("/share/","/data/app-lblod-harvester-qa-files/")
    if os.path.exists(p):
        os.remove(p)
    else:
        print(p, "doesnt exist")
