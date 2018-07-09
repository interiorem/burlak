tail -f $1 | grep keep-alive --line-buffered | awk '{print $3,$9, $12; fflush()}' | sed -u -e 's/\..*://' -e 's/\/overseer//'
