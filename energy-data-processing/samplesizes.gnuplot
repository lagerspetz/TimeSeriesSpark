set term postscript eps enhanced color 'Arial' 24
set xtics out
set size 1.93,1.1
set xdata time
set timefmt "%s"
set xrange ["1327600000":"1329800000"]
set xlabel "Date"
set ylabel "Length, kB"
set output "samplesizes.eps"
plot "samplesizes.txt" using 1:2 with linespoints lt rgb "#007777" lw 2 title "Uncompressed Sample Length", \
"samplesizes.txt" using 1:($3/1000) with linespoints lt rgb "#f3b14d" lw 2 title "Compressed Sample Length"
