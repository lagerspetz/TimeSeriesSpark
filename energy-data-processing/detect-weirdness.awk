#!/usr/bin/awk -f

BEGIN {
    FS=","
    last=0
    step=0
} 

NR == 1 { next }

{
    
    if (last == 0){
        last=$2; next
    }
    
    step = $2 - last
    if (step != "0.0002"){
        print "weird step from", last, "to", $2, ":", ($2-last)
    }
    
    last=$2
}
