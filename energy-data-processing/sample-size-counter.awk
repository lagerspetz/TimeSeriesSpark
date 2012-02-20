#!/usr/bin/awk -f
# Uses a simple formula to calculate sample length in kB.
# Please run gz-size.sh on the original database dump file to also get the gzipped size of records.
BEGIN{
FS=" -> {"
}

$1 ~ "memoryInactive" {
  count+=1
  split($6, a, ", ")
  c=0
  for (k in a){ c+=1}

  split($3,b," ")
  gsub(",", "", b[2])

  s[b[2]]=(20+36+c*(7+16)+5*8+4+10+50)/1000

  split($NF, gza, "")
  gz[b[2]]=gza[4]
}

END {
  for (k in s) {
    print k, s[k],gz[k]
  }
}

