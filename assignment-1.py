import sys
from mpi4py import MPI
import json
import linecache
import re
import numpy as np
from util import get_block as gB
from util import merge
from functools import reduce

#_____generating MPI Environment
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
filename=sys.argv[1]
print("Generating rank",rank,"of",size)
# comm.Barrier()

#_____Master-proc pre-processing data_____
if rank==0:
	data_raw=linecache.getlines(filename)
	data=[]
	for line in data_raw:
		if bool(re.search(r'"full_name":"(Melbourne|Victoria)', line)):
			line_json=json.loads(line[:-2])
			data.append(line_json["json"]["coordinates"]["coordinates"])
	data=np.array_split(data,size)
	print("Master 0 is preparing for scattering")
else:
	data=None
#_____Scattering data_____
data=comm.scatter(data,root=0)
print("after scattering",rank,"has:",len(data),"data")

#_____subtasking_____
d_blk,d_row,d_col={},{},{}
for coodinate in data:
   if gB(coodinate)is not None:
       d_blk[gB(coodinate)] = d_blk.setdefault(gB(coodinate), 0) + 1
       d_row[gB(coodinate)[0]]=d_row.setdefault((gB(coodinate)[0]), 0) + 1
       d_col[gB(coodinate)[1]] = d_col.setdefault(gB(coodinate)[1], 0) + 1
data_to_send=(d_blk,d_row,d_col)

#____Gathering Data_____
data = comm.gather(data_to_send,root=0)

#____Merge Data_____
if rank==0:
	outputs=reduce(lambda a, b: list(map(lambda x, y: merge(x, y), a, b)), data)

#____Output Result____
	print("\noutput is:\n"+"*"*20)
	for output in outputs:
		s=sorted(output.items(), key=lambda x: x[1], reverse=True)
		for key,val in s:
			if len(key)==1:
				if key.isdigit():
					print("Column",key,"has:",val)
				else:
					print("Row",key,"has:",val)
			else:
				print("Block",key,"has:",val)
		print("*"*20)

