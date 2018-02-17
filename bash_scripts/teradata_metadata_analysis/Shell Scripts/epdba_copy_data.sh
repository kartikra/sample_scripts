#!/usr/bin/ksh


#------------------------------------------------------------------------------#
function PipeInit
#------------------------------------------------------------------------------#
{
	echo "\n***START    :  PipeInit - `date` ***"

	# generate named pipe name
	PIPE=namedpipe.$(date "+%Y%m%d%H%M%S").$$  

	# Create a FIFO (named pipe).       
	rm -f 		${PIPE} > /dev/null 2>&1
	mknod 		${PIPE} p
	chmod 666 	${PIPE}     

	echo "*** END:  PipeInit - `date` ***"
}



#------------------------------------------------------------------------------#
function GetTDPwd
#------------------------------------------------------------------------------#
{
	TDHOST=$1
	
	USR_PROF=$HOME/user.profile
        . $USR_PROF >> $logFile 2>&1
        rt_cd=$?
        if [ $rt_cd -ne 0 ]
        then
                echo "Error while running runSQLFile.sh - user.profile cannot be found, Exiting" >> $logFile
                exit 902
        fi
	
	case "$TDHOST" in
   		"tdp1") REPO=$RECP1
   		;;
   		"tdp2") REPO=$RECP2
   		;;
   		"tdp3") REPO=$RECP3
   		;;
		"tdp5") REPO=$RECP5
   		;;
   		"tdd1") REPO=$RECD1
   		;;
   		"tdd3") REPO=$RECD3
   		;;
		"tdd4") REPO=$RECD4
   		;;
	esac

}



#------------------------------------------------------------------------------#
function DoFastExport
#------------------------------------------------------------------------------#
{
	echo "\n***START    :  DoFastExport - `date` ***"

	# generate fast export script for extract

fexp << !

.LOGTABLE ${SrcDB}.${LogTBL} ;

.LOGON ${SrcTDPID}/${SrcUSERID},${SrcPASSWORD};

.BEGIN EXPORT SESSIONS 4;

.EXPORT OUTFILE ${PIPE}
	MODE INDICATOR 
		FORMAT FASTLOAD;

LOCKING 			${SrcDB}.${SrcTBL} FOR ACCESS
SELECT * FROM ${SrcDB}.${SrcTBL};

.END EXPORT ;

.LOGOFF 0;  
!

	Res=$?
	if [[ ${Res} -ne 0 ]] ; then
		echo "Failed in DoFastExport."
		exit 1
	fi

	fexp_pid=$(ps -f | grep $$ | cut -c10-14 )
	sleep 2

	echo "*** END:  DoFastExport - `date` ***"
}

#------------------------------------------------------------------------------#
function DoFastLoad

# RDBMS error 3621: Cannot load table lu_upc_formula 
# unless secondary indexes and join indexes are removed.
# axsmod np_axsmod.so "fallback_directory=/db2dev/db2clnt/PMCP/CAP";
#------------------------------------------------------------------------------#
{
	echo "\n***START    :  DoFastLoad - `date` ***"

	# generate fast load script for extract

fastload << !

SESSIONS 4;

.LOGON ${DesTDPID}/${DesUSERID},${DesPASSWORD}

DELETE FROM 	${DesDB}.${DesTBL} ALL;
DROP TABLE  	${DesDB}.${DesErr1};
DROP TABLE  	${DesDB}.${DesErr2};

CLEAR;

SET RECORD FORMATTED;
axsmod np_axsmod.so "fallback_directory=.";
DEFINE FILE=${PIPE};                                
										   
BEGIN LOADING                                         
	 ${DesDB}.${DesTBL}
   ERRORFILES                                         
	 ${DesDB}.${DesErr1},               
	 ${DesDB}.${DesErr2}
	 INDICATORS;                
	 
ERRLIMIT 100;                                           
RECORD 1;   

INSERT ${DesDB}.${DesTBL}.*;

END LOADING;
.LOGOFF;  
!

	Res=$?
	if [[ ${Res} -ne 0 ]] ; then
		echo "Failed to do DoFastLoad."
		exit 1
	fi

	echo "*** END:  DoFastLoad - `date` ***"
}





#------------------------------------------------------------------------------#
function DoCheckDDL
# check if table DDL exist in the DesTD
#------------------------------------------------------------------------------#
{
	echo "\n*** START: DoCheckDDL  - `date` ***"

	if [[ ${#SrcDB} -gt 30 ]]; then
		echo "length of ${SrcDB} = ${#SrcDB} > 30"
		exit 1
	fi	

	if [[ ${#DesDB} -gt 30 ]]; then
		echo "length of ${DesDB} = ${#DesDB} > 30"
		exit 1
	fi	

	set +x
bteq << !
.LOGON ${SrcTDPID}/${SrcUSERID},${SrcPASSWORD};
.IF ERRORCODE     <> 0 THEN .QUIT 1

select 'check if empty table' from ${SrcDB}.${SrcTBL} sample 1;
.IF ERRORCODE     <> 0 THEN .QUIT 1
.IF ACTIVITYCOUNT  = 0 THEN .QUIT 4

SELECT	distinct
			Dbase.DataBaseName, 
			TVM.TVMName( Title 'TableName' )
FROM  	DBC.dbase, DBC.TVM
WHERE  	Dbase.DatabaseID  = TVM.DatabaseID
AND	TVM.TableKind 	   		= 'T'  
AND dbase.DatabaseNameI 	= '${SrcDB}'
AND	TVM.TVMName						= '${SrcTBL}'; 
.IF ERRORCODE     <> 0 THEN .QUIT 1
.IF ACTIVITYCOUNT <> 1 THEN .QUIT 2

.LOGON ${DesTDPID}/${DesUSERID},${DesPASSWORD}
.IF ERRORCODE     <> 0 THEN .QUIT 1

SELECT	distinct
			Dbase.DataBaseName, 
			TVM.TVMName( Title 'TableName' )
FROM  	DBC.dbase, DBC.TVM
WHERE  	Dbase.DatabaseID  = TVM.DatabaseID
AND	TVM.TableKind 	   		= 'T'  
AND dbase.DatabaseNameI 	= '${DesDB}'
AND	TVM.TVMName						= '${DesTBL}'; 
.IF ERRORCODE     <> 0 THEN .QUIT 1
.IF ACTIVITYCOUNT <> 1 THEN .QUIT 3

.QUIT 0;
!

	Res=$?
	if [[ ${Res} -ne 0 ]];then

		case ${Res} in
		2) echo "Error in DoCheckDDL SrcDB [${SrcTD}] ${SrcDBnTBL}";;
		3) echo "Error in DoCheckDDL DesDB [${DesTD}] ${DesDBnTBL}";;
		4) echo "Empty in DoCheckDDL SrcDB [${SrcTD}] ${SrcDBnTBL}";;
		*) echo "Error in DoCheckDDL funtcion";;
		esac
		
		[ -f ${FnDat} ] && rm -f ${FnDat} 
		[[ ${Res} -ne 4 ]] && exit ${Res}
	fi

	echo "*** END: DoCheckDDL  - `date` ***\n"
	return ${Res}
}





#------------------------------------------------------------------------------#
function DoFE2FL
#
# DoFE2FL SrcTD DesTD [SrcDB.]SrcTBL [[DesDB.]DesTBL]
#
#------------------------------------------------------------------------------#
{
	echo "\n***START    :  DoFE2FL - `date` ***"

	TDSOURCE=$1
	TDTARGET=$2
	SrcDBnTBL=$3
	DesDBnTBL=$4

	SrcDB=$( echo ${SrcDBnTBL} | cut  -d. -f1)
	SrcTBL=$(echo ${SrcDBnTBL} | cut  -d. -f2)

	if [ ${#SrcTBL} > 24 ]
	then
		inSrcTBL=`echo $SrcTBL | awk '{print substr($0,1,24)}'`
	else
		inSrcTBL=`echo $SrcTBL`
	fi
	
	
	if [[ -z ${DesDBnTBL} ]]; then
		DesDBnTBL=${SrcDBnTBL}
	fi


	set +x

	GetTDPwd ${TDSOURCE}
	SrcTDPID=${TDSOURCE}
	SrcUSERID=${USER}
	SrcPASSWORD=${REPO}

	GetTDPwd ${TDTARGET}
	DesTDPID=${TDTARGET}
	DesUSERID=${USER}
	DesPASSWORD=${REPO}
	set -x

	DesDB=$( echo ${DesDBnTBL} | cut  -d. -f1)
	DesTBL=$(echo ${DesDBnTBL} | cut  -d. -f2)

	if [ ${#DesTBL} > 24 ]
	then
		outDesTBL=`echo $DesTBL | awk '{print substr($0,1,24)}'`
	else
		outDesTBL=`echo $DesTBL`
	fi
	
	DesErr1="${outDesTBL}_Err1"
	DesErr2="${outDesTBL}_Err2"
	LogTBL="${inSrcTBL}_fexp"   
	

	# echo DesDBnTBL=${DesDBnTBL} DesDB=${DesDB} DesTBL=${DesTBL}

	#- STEP-1
	DoCheckDDL
	if [[ ${?} -eq 0 ]]; then
	
		#- STEP-2
		PipeInit

		#- STEP-3
		DoFastExport & DoFastLoad
		if [[ ${?} != 0 ]] ; then
				echo "Error:  Pipe!"
				exit 1
		fi

		rm -f ${PIPE} > /dev/null 2>&1
	fi

	echo "*** END:  DoFE2FL - `date` ***"
}


	# Invoke the Functions here -
	
	DoFE2FL tdp2 tdd1 KPBIPSC_T.PMS_NCOA_CC_ROLUP KPBIDSC_T.PMS_NCOA_CC_ROLUP

	