#!/usr/bin/awk -f

BEGIN {

for (i = 2; i < ARGC; i++)
    run[i-1] = ARGV[i]

	FS=","
	# 1 = sample #, 2 = time, 3 = mA, 4 = V
	sum = 0
	temp = 0
	last=0
    if (idle == 0){
	    idle = 120
    }
	checkPoint = 0
	interval = 0.5
	avg = 0
    runEnded = 0
    runBegan = 0
    count = 1
    atleast=run[count]
}

NR == 1 { next }

$2 >= checkPoint + interval {
	if (avg/(5000/(1/interval)) < idle) {
        if (debug){
		    print "checkpoint: "$2, avg/5000/(1/interval), avg
        }
        runEnded = $2
	}
	checkPoint=$2
	avg = 0
}

runEnded > runBegan {
    if (runEnded > runBegan +2){
        if (runEnded > runBegan+atleast){
	        print runBegan", "runEnded", "(runEnded-runBegan)", "sum / 1000 / 3600, "Wh"
            sum = 0
            # reset after print
            runBegan = runEnded
            count++
            atleast=run[count]
        }
        # else
        # silent addition to sum
    }else{
        # Reset only for short oscillations (<3sec)
        runBegan = runEnded
        sum = 0
    }
}

{
	# s * mA * V = mWs
	sum += ($2 - last) * temp
	# mA * V
	temp=$3*$4
	last=$2
	avg+=temp
}

END{
	print "tail:", sum", "(last - runBegan)", "sum / 1000 / 3600 , "Wh"
}
