#!/usr/bin/awk -f
# Calculates average instant energy use (mW) assuming data format:
# measurement number, timestamp, milliamperes, volts

BEGIN{
#  FS=","
  # With 5kHz, this gives 2 samples per second.
  if ( span < 1){
      span=2500
  }
  if ( battery < 1){
      battery=1320 # N900
  }
  #print span
  count = 0.0
  sec = 0.0
  sumA = 0.0
  sumV = 0.0
}

# Header or non-numeric line:
$0 ~ "[A-Za-z]" { next }

function floor(x)
{
  return (x == int(x)) ? x : int(x)
}


# Data lines:
{
    sumA+=$2
#    sumV+=$4
    sec+=$1
    count+=1
}

count == span {
  temp = starth*3600 + startm*60 + starts + sec/count
  hrs = floor(temp/3600)
  temp -= hrs*3600
  mins = floor(temp/60)
  temp -= mins*60
  secs = temp
  print hrs":"mins":"secs,(battery-prev-sumA/count/3600)/battery*100,sumV/count
  prev=prev+sumA/count/3600
  count = 0.0
  sec = 0.0
  sumA = 0.0
  sumV = 0.0
}


END{
  if (count > 0){
    temp = starth*3600 + startm*60 + starts + sec/count
    hrs = floor(temp/3600)
    temp -= hrs*3600
    mins = floor(temp/60)
    temp -= mins*60
    secs = temp
    print hrs":"mins":"secs,(battery-prev-sumA/count/3600)/battery*100,sumV/count
  }
}

