#!/usr/bin/ksh

 while getopts s:t: par
        do      case "$par" in
                s)      srcFileName="$OPTARG";;
                t)      tgtFileName="$OPTARG";;
                [?])    echo "Correct Usage -->  ksh translate_cor_check_detail.sh -s <srcFileName> -t <tgtFileName>"
                        exit 999;;
		esac
	done

tr '\\' ' ' < $srcFileName > $tgtFileName
