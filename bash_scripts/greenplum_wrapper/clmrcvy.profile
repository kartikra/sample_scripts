#
# Project Path
#
HOMEDIR=$CODE/clmrcvy_gp;   export HOMEDIR
SCRIPTSDIR=$CODE/clmrcvy_gp/scripts;  export SCRIPTSDIR
SQLDIR=$CODE/clmrcvy_gp/sql_files; export SQLDIR
LOGDIR=$CODE/clmrcvy_gp/logs;  export LOGDIR
TEMPDIR=$CODE/clmrcvy_gp/temp; export TEMPDIR
CTLDIR=$CODE/clmrcvy_gp/ctlfiles; export CTLDIR
PARMDIR=$PMDIR/InfaParm; export PARMDIR
SRCFILES=$PMDIR/SrcFiles; export SRCFILES
TGTFILES=$PMDIR/TgtFiles; export TGTFILES

#
# GREENPLUM VARIABLES
#
case "$RUN_ENV" in
        "dev")  GP_ENV=DEV; export GP_ENV;
                GPDB=gp_db; export GPDB;
                GPUSER=clm_rcvy; export GPUSER;
                GPHOST=dwtestgp.mycompany.com; export GPHOST;
                GPSTGSCHEMA=clm_rcvy_stg_prj; export GPSTGSCHEMA;
                GPRPTSCHEMA=clm_rcvy_sit; export GPRPTSCHEMA;
                GPLZSCHEMA=clm_rcvy_lz_sit; export GPLZSCHEMA;
                GPBUSSCHEMA=clm_rcvy_buss; export GPBUSSCHEMA;
        ;;
        "sit")  GP_ENV=DEV; export GP_ENV;
                GPDB=gp_db; export GPDB;
                GPUSER=clm_rcvy; export GPUSER;
                GPHOST=dwtestgp.mycompany.com; export GPHOST;
                GPSTGSCHEMA=clm_rcvy_stg_sit; export GPSTGSCHEMA;
                GPRPTSCHEMA=clm_rcvy_sit; export GPRPTSCHEMA;
                GPLZSCHEMA=clm_rcvy_lz_sit; export GPLZSCHEMA;
                GPBUSSCHEMA=clm_rcvy_buss; export GPBUSSCHEMA;
        ;;
        "uat")  GP_ENV=TEST; export GP_ENV;
                GPDB=gp_db; export GPDB;
                GPUSER=t1_clm_rcvy_id; export GPUSER;
                GPHOST=dwtestgp.mycompany.com; export GPHOST;
                GPSTGSCHEMA=t1_clm_rcvy_stg; export GPSTGSCHEMA;
                GPRPTSCHEMA=t1_clm_rcvy; export GPRPTSCHEMA;
                GPLZSCHEMA=t1_clm_rcvy_lz; export GPLZSCHEMA;
                GPBUSSCHEMA=t1_clm_rcvy_buss; export GPBUSSCHEMA;
        ;;
        "rt")   GP_ENV=TEST; export GP_ENV;
                GPDB=gp_db; export GPDB;
                GPUSER=clm_rcvy; export GPUSER;
                GPHOST=dwtestgp.mycompany.com; export GPHOST;
                GPSTGSCHEMA=clm_rcvy_stg; export GPSTGSCHEMA;
                GPRPTSCHEMA=clm_rcvy; export GPRPTSCHEMA;
                GPLZSCHEMA=clm_rcvy_lz; export GPLZSCHEMA;
                GPBUSSCHEMA=clm_rcvy_buss; export GPBUSSCHEMA;
        ;;
        "prod") GP_ENV=PROD; export GP_ENV;
                GPDB=gp_db; export GPDB;
                GPUSER=clm_rcvy; export GPUSER;
                GPHOST=dwprodgp.mycompany.com; export GPHOST;
                GPSTGSCHEMA=clm_rcvy_stg; export GPSTGSCHEMA;
                GPRPTSCHEMA=clm_rcvy; export GPRPTSCHEMA;
                GPLZSCHEMA=clm_rcvy_lz; export GPLZSCHEMA;
                GPBUSSCHEMA=clm_rcvy_buss; export GPBUSSCHEMA;
        ;;
esac

#
#           Greenplum Common Information
#
GPDATADIR=/data/clm_rcvy/data; export GPDATADIR
GPHOMEDIR=/export/home/clm_rcvy; export GPHOMEDIR
PROJ=clmrcvy_gp; export PROJ
WGSHOST=main.mycompany.com; export WGSHOST

#
#           ACRE Connection Information
#
case "$GP_ENV" in
        "DEV")
        ACRENV=ACRE_TEST; export ACRENV;
        ACRHOST=mycompany.prod;  export ACRHOST
        ACRDIR=/acre/tst/ftp; export ACRDIR
        ACRLOG=/acre/tst/data/loaded/facets export ACRLOG
        ACRNOTIFY=ab88758@mycompany.com,ab88757@mycompany.com; export ACRNOTIFY
        ;;
        "TEST")
        ACRENV=ACRE_TEST; export ACRENV;
        ACRHOST=mycompany.prod;  export ACRHOST
        ACRDIR=/acre/tst/ftp; export ACRDIR
        ACRLOG=/acre/tst/data/loaded/facets export ACRLOG
        ACRNOTIFY=ab88758@mycompany.com,ab88757@mycompany.com; export ACRNOTIFY
        ;;
        "PROD")
        ACRENV=ACRE_PROD; export ACRENV;
        ACRHOST=mycompany.test;  export ACRHOST
        ACRDIR=/opt/acre/ftp; export ACRDIR
        ACRLOG=/opt/acre/data/loaded/facets; export ACRLOG
        ACRNOTIFY=dl-BABW-Claims-Accuracy-Buss@mycompany.com; export ACRNOTIFY
        ;;
esac





