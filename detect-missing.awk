#!/usr/bin/awk -f

BEGIN {
    FS=","
    last=0
    step=0
} 

{
    if($1 ~ "^\("){
        sub("\(", "", $1)
    }
    
    if (last == 0){
        last=$1; next
    }
    
    # because of floating point inaccuracy, do arithmetic with ints
#    split($1, divided, ".")
#    split(last, lastparts, ".")
#    while (length(lastparts[2]) > length(divided[2]) && divided[2] != 0){
#	divided[2] = divided[2]*10
#    }
#	while (length(lastparts[2]) < length(divided[2]) && lastparts[2] != 0){
#        lastparts[2] = lastparts[2]*10
#    }
 #   wholediff = divided[1] - lastparts[1]
 #   decimaldiff = divided[2] - lastparts[2]
#    print(divided[1], lastparts[1], divided[2], lastparts[2])

 #   if (wholediff > 0 && decimaldiff != -9998){
 #       print "weird step from", last, "to", $1, ":", ($1-last)        
 #   }else if (wholediff == 0 && decimaldiff != 2){
#        print "weird step from", last, "to", $1, ":", ($1-last)        
#    }
  step = $1 - last
    if (step != "0.0002"){
        print "weird step from", last, "to", $1, ":", ($1-last)        
    }
  
    last=$1
}
