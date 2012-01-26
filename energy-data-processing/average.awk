#!/usr/bin/awk -f
# Calculates average instant energy use (mW) assuming data format:
# measurement number, timestamp, milliamperes, volts

BEGIN{
    FS=","
    # With 5kHz, this gives 2 samples per second.
    if ( span < 1 || span > 5000 ){
        span=2500
    }
	print span
    count = 0.0
    sec = 0.0
    sum = 0.0
}

# Header:
NR == 1 { next }

# Data lines:
{
    sum+=$3 * $4
    sec+=$2
    count+=1
}

count == span {
    print sec/count,sum/count
    count = 0.0
    sec = 0.0
    sum = 0.0
}


END{
if (count > 0){
    print sec/count,sum/count
}
}
