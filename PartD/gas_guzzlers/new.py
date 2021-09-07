f=open("partD1_gas.txt","r")# file used to take the file emitted by the reducer, file name inside f=open was changed for partD1_gas and partD_scam.txt 
f2=open("partD1_gas1R.txt","w")# file  created that removes the "" around the emitted values finally ouputted files end in R.txt 
for x in f:
	new_x=x.replace('"',"")
	f2.writelines(new_x)

f.close()
f2.close()
