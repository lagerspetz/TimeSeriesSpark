#!/usr/bin/awk -f

BEGIN {
  FS="[ :]"
  if (end==0) end = "9:44"
  if (start == 0) start = "9:43"
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
  y0 = $4
}

($1 > ea[1] || (ea[1] == $1 && $2 >= ea[2])) && y1 == 0{
  #print $1,$2, "matches", end
  y1 = $4
}

END{
    if (y1 == 0) y1 = $4
    print (y1-y0)
}
