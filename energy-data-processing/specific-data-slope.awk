#!/usr/bin/awk -f

BEGIN {
  FS="[ :]"
  if (end==0) end = "0:01"
  if (start == 0) start = "0:00"
  y0 = 0
  y1 = 0
  split(start,sa,":")
  split(end,ea,":")
}

function floor(x)
{
  return (x == int(x)) ? x : int(x)
}


($1 > sa[1] || (sa[1] == $1 && $2 >= sa[2])) && y0 == 0{
#print $1,$2, "matches", start
  x0=$1*3600+$2*60+$3
  y0 = $4
}

($1 > ea[1] || (ea[1] == $1 && $2 >= ea[2])) && y1 == 0{
  #print $1,$2, "matches", end
  x1=$1*3600+$2*60+$3
  y1 = $4
}

END{
    if (y1 == 0) y1 = $4
    if (x1 == 0) x1=$1*3600+$2*60+$3
#    print y1,y0,x1,x0
    if (x1 == x0)
      print "same"
    else
       print (y0-y1)/(x1-x0)
}
