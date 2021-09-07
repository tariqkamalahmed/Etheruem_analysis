f=open("partD1_gas2.txt","r")# file used to take the file emitted by the reducer, file name inside f=open was changed for partD1_gas and partD_scam.txt 
f2=open("partD1_gas2RA.txt","w")# file  created that removes the "" around the emitted values finally ouputted files end in R.txt 
for x in f:
	new_x=x.replace('"',"")
	new_x1=new_x.replace('[',"")
	new_x2=new_x1.replace(']',"")
	f2.writelines(new_x2)

f.close()
f2.close()
