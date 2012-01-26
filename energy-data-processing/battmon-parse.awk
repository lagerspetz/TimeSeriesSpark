#!/usr/bin/awk -f

BEGIN{
  if (day == 0) day = "Dec 22"
  rightday=0
  zeroh=0
  zerom=0
  zeros=0
}

$0 ~ day {
  #if (zeros==0){
    #zeroh=substr($4,1,2)
    #zerom=substr($4,4,2)
    #zeros=substr($4,7,2)
  #}
  #sh=substr($4,1,2)
  #sm=substr($4,4,2)
  #ss=substr($4,7,2)

  rightday=1
  date=$1" "$2" "$3
  #time=(sh - zeroh)*3600 + (sm - zerom)*60 + (ss - zeros)
  time=$4
}

rightday==1 && $0 ~ "level" {
  print time,$2
}
