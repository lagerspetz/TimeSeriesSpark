#!/usr/bin/awk -f

BEGIN{
	FS=",";
	sum=0;
	last=0;
	runBegan = 0;
	temp = 0
}

$1 ~ "^\(" {
	$1 = sub("\(", "", $1)
}

{
	if (last == 0){
		last = $1;
		runBegan = $1
	}
	sum += ($1 - last) * temp;
	temp=$2*$3;
	last=$1;
}

$0 ~ ";$" {
	print (last-runBegan)", "sum/1000/3600, "Wh"
	sum = 0
	temp = 0
	runBegan = last
}


#END{ print (last-runBegan)", "sum/1000/3600, "Wh" }
