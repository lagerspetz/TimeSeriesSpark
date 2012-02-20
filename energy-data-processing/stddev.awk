#!/usr/bin/awk -f
BEGIN{
    avg = 0.0
    avgtime = 0.0
    num = 0.0
    diff = 0.0
    difftime = 0.0
    if (field==0)
     field = 2
}

{
    avg += $field
    avgtime += $1
    num++
}

END {
    avg = avg / num
    avgtime = avgtime / num
    #print "average:",avg
    #print "#calculating standard deviation:"
    close(FILENAME)
    diff=0;
    while (getline<FILENAME > 0){
        diff += ( avg - $field ) ^ 2;
        difftime += ( avgtime - $1 ) ^ 2;
    }

    if (num > 1){
        diff = sqrt( diff / (num-1) );
        difftime = sqrt( difftime / (num-1) );
    }
    #print "|file|\tavg\tstddev";
    #printf("%8s %8s %8s %8s\n", "AVG", "STDDEV", "AVGTIME", "STDTIME" );
    printf("%8f %8f %8f %8f\n", avg, diff, avgtime, difftime);
}
