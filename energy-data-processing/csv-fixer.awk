#!/usr/bin/awk -f
BEGIN{
    FS=","
    firstTime=-1
}

NR == 1 {print "#,"$0}

NR > 1 {
    if (firstTime == -1){
        firstTime = $1"."$2 
    }
    time = $1"."$2
    time = time - firstTime
    print NR-2","(time)","$3"."$4","$5"."$6
}
