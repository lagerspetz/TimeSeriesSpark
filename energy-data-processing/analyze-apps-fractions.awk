#!/usr/bin/awk -f

$0 ~ "app fractions:" {
  print $0
  c=1
}

c == 1 {
  w=$(NF-1)
  if (w+0 != w)
    next
  v=$(NF)
  if (v+0 != v)
    next
  if (w == 0 && v == 0)
    next
  print $0
}

