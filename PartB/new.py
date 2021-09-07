f=open("partB1.txt","r")# file used to take the file emitted by the reducer, file name inside f=open was changed for partb1,partb2,part3.txt 
f2=open("PartB1R.txt","w")# file  created that removes the "" around the emitted values finally ouputted files end in R.txt 
for x in f:
	new_x=x.replace('"',"")
	f2.writelines(new_x)

f.close()
f2.close()
