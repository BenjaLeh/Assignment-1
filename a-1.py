import re
import json
import linecache
from util import get_block,merge as gB,merge
from functools import reduce
from mpi4py import MPI

#_____generating MPI Environment
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

#_____Master-proc pre-processing data_____
if rank==0:
    data_raw=linecache.getlines("smallTwitter.json")
    data=[]
    for line in data_raw:
        if re.search("Melbourne",line)is not None:
            line_json=json.loads(line[:-2])
            data.append(line_json["json"]["coordinates"]["coordinates"])
else:
    data=None
#_____Scattering data_____
data = comm.scatter(data, root=0)

#_____Counting_____
d_blk,d_row,d_col={},{},{}
for coodinate in data:
   if gB(coodinate)is not None:
       d_blk[gB(coodinate)] = d_blk.setdefault(gB(coodinate), 0) + 1
       d_row[gB(coodinate)[0]]=d_row.setdefault((gB(coodinate)[0]), 0) + 1
       d_col[gB(coodinate)[1]] = d_col.setdefault(gB(coodinate)[1], 0) + 1
data_send=(d_blk,d_row,d_col)

#____Gathering Data_____
newData = comm.gather(data_send,root=0)
#____Merge Data_____
if rank==0:
    outputs=reduce(lambda a, b: list(map(lambda x, y: merge(x, y), a, b)), newData)
#____Output____
for output in outputs:
    s=sorted(output.items(), key=lambda x: x[1], reverse=True)
    for key,val in s:
        if len(key)==1:
            if key.isdigit():
                print("Column "+key,val)
            else:
                print("Row "+key,val)
        else:
            print("Block "+key,val)





