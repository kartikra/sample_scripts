--
-- ER/Studio Data Architect XE2 SQL Code Generation
-- Project :      KEDW_PHASE2_PDM.DM1
--
-- Date Created : Thursday, December 20, 2012 09:29:37
-- Target DBMS : Teradata 12.0
--
--
---
--


-- 
-- TABLE: AssociatedBusinessUnitGrp 
--

CREATE MULTISET TABLE ATOMICDATA.AssociatedBusinessUnitGrp, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
    BusinessUnitGroupFunctionID       INTEGER         NOT NULL,
    ParentBusinessUnitGroupLevelID    INTEGER         NOT NULL,
    ParentBusinessUnitGroupID         INTEGER         NOT NULL,
    ChildBusinessUnitGroupID          INTEGER         NOT NULL,
    ValidPeriod    					  PERIOD(DATE) , --                       DATE,
    EffectiveDate                     DATE,
    ExpirationDate                    DATE,
    CurrentInd                         BYTEINT,
    RecordPeriod   					  PERIOD(TIMESTAMP(0)), --                      INTERVAL,
    ExtDttm                           TIMESTAMP(0),
    TblID                             INTEGER
)
PRIMARY INDEX PI_AssocBusnsUntGrp (ChildBusinessUnitGroupID)
PARTITION BY CASE_N(CurrentInd =  1 ,CurrentInd =  0, NO CASE OR UNKNOWN)
;







-- 
-- TABLE: BricConsumerCategorization 
--

CREATE MULTISET TABLE ATOMICDATA.BricConsumerCategorization, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    MapSquareId                       DECIMAL(10, 0)    NOT NULL,
    ValidPeriod    PERIOD(DATE) , --                       DATE,
    CityId                            VARCHAR(10)       CHARACTER SET LATIN  FORMAT 'X(10)',
    TerritoryId                       VARCHAR(30)       CHARACTER SET LATIN  FORMAT 'X(30)',
    ISOCountryCode                    CHAR(3)           CHARACTER SET LATIN  FORMAT 'X(3)',
    ResidentialAreaCategory           CHAR(1),
    EducationLevelCategory            CHAR(1),
    HomeOwnershipCategory             CHAR(1),
    HousingCategory                   CHAR(1),
    PaymentDefaultProbabilityCtgy     CHAR(1),
    PurchasingPowerAmount             DECIMAL(6, 5),
    LifestyleYoungCouplesNoKidsQty    DECIMAL(6, 5),
    LifestyleFamiliesQty              DECIMAL(6, 5),
    LifestyleAdultHouseholdsQty       DECIMAL(6, 5),
    LifestyleSeniorHouseholdsQty      DECIMAL(6, 5),
    ResidentialAreaRuralQuantity      DECIMAL(6, 5),
    ResidentialAreaSuburbsQuantity    DECIMAL(6, 5),
    ResidentialAreaCitiesQuantity     DECIMAL(6, 5),
    ResidentialAreaMajorCitiesQty     DECIMAL(6, 5),
    ResidentialAreaCapitalAreaQty     DECIMAL(6, 5),
    EducationBasicQuantity            DECIMAL(6, 5),
    EducationMiddleQuantity           DECIMAL(6, 5),
    EducationHighQuantity             DECIMAL(6, 5),
    HousingRentalQuantity             DECIMAL(6, 5),
    HousingOwnedQuantity              DECIMAL(6, 5),
    HousingDetachedHouseQuantity      DECIMAL(6, 5),
    HousingApartmentQuantity          DECIMAL(6, 5),
    PaymentDefaultQuantity            DECIMAL(6, 5),
    PopulationDensity                 DECIMAL(6, 5),
    PurchasingPowerProportionedAmt    DECIMAL(6, 5),
    PurchasingPowerCategory           CHAR(1),
    PurchasingPowerSubcategory        VARCHAR(2),
    LifestyleCategory                 CHAR(1),
    LifestyleSubcategory              VARCHAR(2),
    HouseholdQuantity                 INTEGER,
    PopulationQuantity                INTEGER,
    CurrentInd                         BYTEINT,
    RecordPeriod                      PERIOD(TIMESTAMP(0)),
    ExtDttm                           TIMESTAMP(0),
    TblID                             INTEGER
)
PRIMARY INDEX PI_BRICCustCat (MapSquareId)
PARTITION BY CASE_N(CurrentInd =  1 ,CurrentInd =  0, NO CASE OR UNKNOWN) 
;



-- 
-- TABLE: BusinessUnitGroup 
--

CREATE MULTISET TABLE ATOMICDATA.BusinessUnitGroup, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    BusinessUnitGroupID       INTEGER         NOT NULL,
	SrcBusinessUnitGroupID    VARCHAR(20),
    ValidPeriod               PERIOD(DATE),
    Logo                      CHAR(10),
    TypeCode                  BYTEINT         NOT NULL,
    CurrentInd                BYTEINT,
    RecordPeriod              PERIOD(TIMESTAMP(0)),
    ExtDttm                   TIMESTAMP(0),
    TblID                     INTEGER
)
PRIMARY INDEX PI_BusnsUntGrp (BusinessUnitGroupID)
PARTITION BY CASE_N(CurrentInd =  1 ,CurrentInd =  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: BusinessUnitGroupName 
--

CREATE MULTISET TABLE ATOMICDATA.BusinessUnitGroupName, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    BusinessUnitGroupID      INTEGER         NOT NULL,
    LanguageID               BYTEINT         NOT NULL,
    BusinessUnitGroupName    VARCHAR(50),
    CurrentInd               BYTEINT,
    RecordPeriod             PERIOD(TIMESTAMP(0)),
    TblID                    INTEGER,
    ExtDttm                  TIMESTAMP(0)
)
PRIMARY INDEX PI_BusnsUnitGroupName (BusinessUnitGroupID)
;


-- 
-- TABLE: BusinessUnitLoyaltyProgram 
--

CREATE MULTISET TABLE ATOMICDATA.BusinessUnitLoyaltyProgram, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    BusinessUnitID      INTEGER         NOT NULL,
    ValidPeriod    PERIOD(DATE) , --         DATE,
    InformationText     CHAR(10),
    IsIncluded          CHAR(10),
    PlussaMbrAppElig    CHAR(10),
    SortCategory        CHAR(10),
    IsSiteShowed        CHAR(10),
    CurrentInd          BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --        INTERVAL,
    ExtDttm             TIMESTAMP(0),
    TblID               INTEGER
)
PRIMARY INDEX PI_BusnsUntLyltyPgm (BusinessUnitID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



COMMENT ON TABLE BusinessUnitLoyaltyProgram IS 'Describes how the business unit group behaves in the loyalt reward program'
;
-- 
-- TABLE: BusinessUnitSrcIdentifcn 
--

CREATE MULTISET TABLE ATOMICDATA.BusinessUnitSrcIdentifcn, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    BusinessUnitID        INTEGER         NOT NULL,
    IdentificationType    INTEGER         NOT NULL,
    SrcBusinessId         VARCHAR(17),
    ValidPeriod    PERIOD(DATE) , --           DATE,
    CurrentInd            BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --          INTERVAL,
    ExtDttm               TIMESTAMP(0),
    TblID                 INTEGER
)
PRIMARY INDEX PI_BUSrcId (BusinessUnitID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: Campaign 
--

CREATE MULTISET TABLE ATOMICDATA.Campaign, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID                INTEGER         NOT NULL,
    OrderId                   INTEGER,
    Name                      VARCHAR(255),
    StartDate                 DATE,
    EndDate                   DATE,
    SrcCampaignId             INTEGER,
    CampaignTypeCode          BYTEINT,
    CampaignSourceSystemCd    VARCHAR(20),
    StatusCode                BYTEINT,
    IsTargeted                CHAR(1),
    Description               VARCHAR(200),
    ValidPeriod    PERIOD(DATE) , --               DATE,
    CurrentInd                BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --              INTERVAL,
    ExtDttm                   TIMESTAMP(0),
    TblId                     INTEGER
)
PRIMARY INDEX PI_Cmpgn (CampaignID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CampaignBusinessUnit 
--

CREATE MULTISET TABLE ATOMICDATA.CampaignBusinessUnit, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID        INTEGER         NOT NULL,
    CurrentInd        BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --      INTERVAL,
    ExtDttm           TIMESTAMP(0),
    TblID             INTEGER,
    BusinessUnitID    INTEGER         NOT NULL
)
PRIMARY INDEX PI_CampgnBusnsUnit (CampaignID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CampaignMemberStatistics 
--

CREATE MULTISET TABLE ATOMICDATA.CampaignMemberStatistics, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID                       INTEGER           NOT NULL,
    CustAcctID                       INTEGER           NOT NULL,
    SiteID                           INTEGER           NOT NULL,
    ValidPeriod                      PERIOD(DATE) , --                      DATE,
    YearMonthNum                     INTEGER,
    SalesAmount                      DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardSalesAmountUnltd    DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardSalesAmount         DECIMAL(13, 2)    DEFAULT 0,
    VisitQuantity                    INTEGER,
    CampaignVisitQuantity            INTEGER,
    CampaignResponseQuantity         INTEGER,
    AdditionalRewardQuantity         INTEGER,
    CampaignMatrix                   SMALLINT,
    RecordPeriod                     PERIOD(TIMESTAMP(0)), --                     INTERVAL,
    ExtDttm                          TIMESTAMP(0),
    TblID                            INTEGER,
    CurrentInd                       BYTEINT
)
PRIMARY INDEX PI_CmpgnMmbrStats (CustAcctID)
PARTITION BY (RANGE_N(YearMonthNum BETWEEN 200101 AND 201412 EACH 1 )
, CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN))
;



COMMENT ON TABLE CampaignMemberStatistics IS 'Statistics is gathered (manually and automatically) in Plussa operational system and brought to EDW for reporting purposes.'
;

CREATE MULTISET TABLE ATOMICDATA.CampaignOffering, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID            INTEGER         NOT NULL,
    CampaignOfferingID    INTEGER         NOT NULL,
    ValidPeriod    PERIOD(DATE) , --           DATE,
    ResponseEAN           VARCHAR(255),
    CurrentInd            BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --          INTERVAL,
    ExtDttm               TIMESTAMP(0),
    TblID                 INTEGER
)
PRIMARY INDEX PI_CampaignOffering (CampaignID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CampaignOrder 
--

CREATE MULTISET TABLE ATOMICDATA.CampaignOrder, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    OrderId                 INTEGER         NOT NULL,
    OrderHeader             VARCHAR(255),
    InvoicingStatusCode     INTEGER,
    CampaignOrderStartDt    DATE,
    CampaignOrderEndDt      DATE,
    ValidPeriod    PERIOD(DATE) , --             DATE,
    CurrentInd              BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --            INTERVAL,
    ExtDttm                 TIMESTAMP(0),
    TblID                   INTEGER
)
PRIMARY INDEX PI_CampaignOrder (OrderId)
;



-- 
-- TABLE: CampaignResponse 
--

CREATE MULTISET TABLE ATOMICDATA.CampaignResponse, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID                    INTEGER         NOT NULL,
    TargetGroupID                 INTEGER         NOT NULL,
    CustomerId                    BIGINT          NOT NULL,
    CustomerTypeCode              BYTEINT         NOT NULL,
    CampaignResponseTypeCode      INTEGER,
    ResponseEAN                   VARCHAR(255),
    ResponseDt                    DATE,
    ResponseTm                    TIME(0),
    RetailTrnID                   BIGINT,
    CampaignOfferingID            INTEGER,
    CurrentInd                    BYTEINT,
    RecordPeriod                  PERIOD(TIMESTAMP(0)), --          INTERVAL,
    ExtDttm                       TIMESTAMP(0),
    TblID                         INTEGER
)
PRIMARY INDEX PI_CampaignResponse (CampaignID)
PARTITION BY (RANGE_N(ResponseDt BETWEEN DATE '2008-01-01' AND DATE '2014-12-31' EACH INTERVAL '1' MONTH )
, CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN))
;



-- 
-- TABLE: CampaignTargetGroup 
--

CREATE MULTISET TABLE ATOMICDATA.CampaignTargetGroup, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID          INTEGER         NOT NULL,
    TargetGroupID       INTEGER         NOT NULL,
    TargetGroupClass    VARCHAR(255),
    ValidPeriod    PERIOD(DATE) , --         DATE,
    SrcTargetGroupId    INTEGER         NOT NULL,
    Name                VARCHAR(255),
    LevelId             INTEGER,
    SelectionLevelId    INTEGER,
    TypeCode            CHAR(1),
    FollowUpInd         CHAR(1),
    CurrentInd          BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --        INTERVAL,
    ExtDttm             TIMESTAMP(0),
    TblID               INTEGER
)
PRIMARY INDEX PI_CampaignTargetGroup (CampaignID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CampaignTargetGroupMember 
--

CREATE MULTISET TABLE ATOMICDATA.CampaignTargetGroupMember, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID          INTEGER         NOT NULL,
    TargetGroupID       INTEGER         NOT NULL,
    CustAcctID          INTEGER         NOT NULL,
	CustomerId          BIGINT          NOT NULL,
    CustomerTypeCode    BYTEINT         NOT NULL,
    BusinessUnitID      INTEGER,
    StatusCode          CHAR(10),
    ValidPeriod    PERIOD(DATE) , --         DATE,
    CurrentInd          BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --        TIMESTAMP(0),
    ExtDttm             TIMESTAMP(0),
    TblID               INTEGER
)
PRIMARY INDEX PI_CampgnTgtGrpMbr (CustAcctID)
;



-- 
-- TABLE: CampaignTargetGroupOffrng 
--

CREATE MULTISET TABLE ATOMICDATA.CampaignTargetGroupOffrng, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignOfferingID    INTEGER         NOT NULL,
    CampaignID            INTEGER         NOT NULL,
    TargetGroupID         INTEGER         NOT NULL,
    CustomerId            BIGINT          NOT NULL,
    CustomerTypeCode      BYTEINT         NOT NULL,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --          INTERVAL,
    ExtDttm               TIMESTAMP(0),
    TblID                 INTEGER
)
PRIMARY INDEX PI_CampgnTgtGrpOffrng (CampaignID)
;



-- 
-- TABLE: CustAcctAffiliation 
--
DROP TABLE ATOMICDATA.CustAcctAffiliation;
CREATE MULTISET TABLE ATOMICDATA.CustAcctAffiliation, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustGroupID                   INTEGER           NOT NULL,
    CustAcctID                    INTEGER           NOT NULL,
    OrgLevelId                    CHAR(10)          NOT NULL,
    OrgLevelTypeCode              CHAR(10)          NOT NULL,
    YearMonthNum                  INTEGER           NOT NULL,
	CustGroupTypeCode             VARCHAR(20),
    Amount                        DECIMAL(13, 2)    DEFAULT 0,
    Indx                          CHAR(10),
    Probability                   DECIMAL(9, 6),
    ValidPeriod                   PERIOD(DATE) , --                   DATE,
    CurrentInd                    BYTEINT,
    RecordPeriod                  PERIOD(TIMESTAMP(0)), --                  INTERVAL,
    ExtDttm                       TIMESTAMP(0),
    TblID                         INTEGER
)
PRIMARY INDEX PI_CustAffln (CustAcctID)
PARTITION BY (
RANGE_N(YearMonthNum BETWEEN 200101 AND 201412 EACH 1 )
, CASE_N( CustGroupTypeCode = '1000'
		, CustGroupTypeCode = '2000'
		, CustGroupTypeCode = '3000'
		, CustGroupTypeCode = '3100'
		, CustGroupTypeCode = '3200'
		, CustGroupTypeCode = '4100'
		, CustGroupTypeCode = '4200'
		, CustGroupTypeCode = '4300'
		, CustGroupTypeCode = '6100'
		, CustGroupTypeCode = '6110'
		, CustGroupTypeCode = '6120'
		, CustGroupTypeCode = '6130'
		, CustGroupTypeCode = '6140'
		, CustGroupTypeCode = '6150'
		, CustGroupTypeCode = '6160'
		, CustGroupTypeCode = '6170'
		, CustGroupTypeCode = '6200'
		, CustGroupTypeCode = '6210'
		, CustGroupTypeCode = '6220'
		, CustGroupTypeCode = '6230'
		, CustGroupTypeCode = '6240'
		, CustGroupTypeCode = '6250'
		, CustGroupTypeCode = '6260'
		, CustGroupTypeCode = '6270'
		, CustGroupTypeCode = '6280'
		, CustGroupTypeCode = '6290'
		, CustGroupTypeCode = '6410'
		, CustGroupTypeCode = '6420'
		, CustGroupTypeCode = '6430'
		, CustGroupTypeCode = '6510'
		, CustGroupTypeCode = '6520'
		, CustGroupTypeCode = '6530'
		, CustGroupTypeCode = '6540'
		, CustGroupTypeCode = '6550'
		, NOCASE OR UNKNOWN
)
)
;



COMMENT ON TABLE CustAcctAffiliation IS 'Associates a customer with a customer group.'
;
-- 
-- TABLE: CustAcctAssoc 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctAssoc, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    ParentCustAcctID    INTEGER         NOT NULL,
    ChildCustAcctID     INTEGER         NOT NULL,
    TransactionDate     DATE,
    ValidPeriod    PERIOD(DATE) , --         DATE,
    CurrentInd          BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --        INTERVAL,
    ExtDttm             TIMESTAMP(0),
    TblID               INTEGER
)
PRIMARY INDEX PI_CustAcctAssoc (ParentCustAcctID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CustAcctChild 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctChild, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustAcctID      INTEGER         NOT NULL,
    ChildId         CHAR(10)        NOT NULL,
    BirthYear       SMALLINT,
    ValidPeriod    PERIOD(DATE) , --     DATE,
    CurrentInd      BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --    INTERVAL,
    ExtDttm         TIMESTAMP(0),
    TblID           INTEGER
)
PRIMARY INDEX PI_CustAcctChild (CustAcctID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CustAcctConsumerCtgrzn 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctConsumerCtgrzn, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    MapSquareId     DECIMAL(10, 0)    NOT NULL,
    CustAcctID      INTEGER           NOT NULL,
    CurrentInd      BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --    INTERVAL,
    ExtDttm         TIMESTAMP(0),
    TblID           INTEGER
)
PRIMARY INDEX PI_CustAcctCnsmrCtgrzn (CustAcctID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CustAcctLoyaltyRewardEntry 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctLoyaltyRewardEntry, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustAcctID                INTEGER         NOT NULL,
    RewardPeriodStartDt       DATE            NOT NULL,
    RewardPeriodEndDt         DATE            NOT NULL,
    TypeCode                  CHAR(2)         NOT NULL,
    RewardModel               INTEGER         NOT NULL,
    StatementStartDt          DATE,
    StatementEndDt            DATE,
    TransactionDt             DATE,
    RewardQty                 INTEGER,
    RewardFactor              INTEGER         NOT NULL,
    AdditionalRewardFactor    INTEGER,
    PreviousRewardBalance     INTEGER         NOT NULL,
    CurrentInd                BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --              INTERVAL,
    ExtDttm                   TIMESTAMP(0),
    TblID                     INTEGER
)
PRIMARY INDEX PI_CustAccLyltyRwdEnt (CustAcctID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CustAcctLoyaltyStatement 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctLoyaltyStatement, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustAcctID                INTEGER         NOT NULL,
    StatementStartDt          DATE            NOT NULL,
    StatementEndDt            DATE            NOT NULL,
    StatementType             CHAR(10)        NOT NULL,
    BenefitID                 VARCHAR(50)     NOT NULL,
    PersonalizationSqNr       VARCHAR(100)    NOT NULL,
    PersonalizationContent    VARCHAR(100),
    TransactionDt             DATE,
    FormatID                  SMALLINT,
    TransactionUserID         VARCHAR(8),
    TransaferDt               DATE,
    CurrentInd                BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --              INTERVAL,
    ExtDttm                   TIMESTAMP(0),
    TblID                     INTEGER
)
PRIMARY INDEX PI_CustLyltyStmt (CustAcctID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CustAcctOrgAffiliation 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctOrgAffiliation, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustAcctID                     INTEGER         NOT NULL,
    CustAcctAffiliationTypeCode    CHAR(10)        NOT NULL,
    OrgLevelId                     CHAR(10)        NOT NULL,
    OrgLevelTypeCode               CHAR(10)        NOT NULL,
    YearMonthNum                   INTEGER         NOT NULL,
    CustAcctAfflnOrgLevelId        CHAR(10),
    CustAcctAfflnOrgTypeCode       CHAR(10),
    StatusCode                     SMALLINT,
    ValidPeriod                    PERIOD(DATE) , 
    CurrentInd                     BYTEINT,
    RecordPeriod                   PERIOD(TIMESTAMP(0)), 
    ExtDttm                        TIMESTAMP(0),
    TblID                          INTEGER
)
PRIMARY INDEX PI_CustAcctOrgAffiliation (CustAcctID)
PARTITION BY 
(RANGE_N(YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
, CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
)
;


COMMENT ON TABLE CustAcctOrgAffiliation IS 'This is a preliminary version of an entity listing the customers of a certain account. There is currently (during CI2) no source for the actual account data so only a relationship is stored with a type code denoting what kind of account this would be. In the future, if there is master data for the account, the type code should be stored in CustomerAccount.'
;
-- 
-- TABLE: CustAcctShareOfWallet 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctShareOfWallet, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustAcctID                 INTEGER          NOT NULL,
    SowTypeCode                VARCHAR(20)      NOT NULL,
    YearMonthNum               INTEGER          NOT NULL,
    ValidPeriod                PERIOD(DATE) , --                DATE,
	TotalPurchasingPowerAmount DECIMAL(8, 2),
    PurchaseAmount             DECIMAL(8, 2),
    ShareOfWalletPercentage    INTEGER,
    ShareOfWalletCategory      VARCHAR(2),
    PotentialAmount            INTEGER,
    CurrentInd                 BYTEINT,
    RecordPeriod               PERIOD(TIMESTAMP(0)), --               INTERVAL,
    ExtDttm                    TIMESTAMP(0),
    TblID                      INTEGER
)
PRIMARY INDEX PI_CSOW (CustAcctID)
PARTITION BY (
RANGE_N(YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
, CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
)

;



-- 
-- TABLE: CustomerGroup 
--

CREATE MULTISET TABLE ATOMICDATA.CustomerGroup, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustGroupID           INTEGER         NOT NULL,
    CustGroupTypeCode     VARCHAR(20),
    SrcCustGroupId        CHAR(10),
    Name                  VARCHAR(40),
    Description           VARCHAR(255),
    GroupOwner            CHAR(10),
    GroupOwnerTypeCode    CHAR(10),
    ValidPeriod    PERIOD(DATE) , --           DATE,
    CurrentInd            BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --          INTERVAL,
    ExtDttm               TIMESTAMP(0),
    TblID                 INTEGER
)
PRIMARY INDEX PI_CustGrp (CustGroupID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



COMMENT ON TABLE CustomerGroup IS 'A group of customers based on specific demographic and marketing attributes and properties.  Examples include over 65 year old customers, students, unions, and other associations.

Also used to classify customer''s known taxability; eg: Hospital, Charity, etc.'
;
-- 
-- TABLE: CustomerGroupAssociation 
--

CREATE MULTISET TABLE ATOMICDATA.CustomerGroupAssociation, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustGroupID       INTEGER         NOT NULL,
    SubCustGroupId    INTEGER         NOT NULL,
    ValidPeriod    PERIOD(DATE) , --       DATE,
    CurrentInd        BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --      INTERVAL,
    ExtDttm           TIMESTAMP(0),
    TblID             INTEGER
)
PRIMARY INDEX PI_CustGrpAssoc (CustGroupID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: CustomerGroupType 
--

CREATE MULTISET TABLE ATOMICDATA.CustomerGroupType, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustGroupTypeCode    VARCHAR(20)     NOT NULL,
    Name                 VARCHAR(255),
    ValidPeriod    PERIOD(DATE) , --          DATE,
    CurrentInd           BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --         INTERVAL,
    ExtDttm              TIMESTAMP(0),
    TblID                INTEGER
)
PRIMARY INDEX PI_CustGrpTypCd (CustGroupTypeCode)
;


COMMENT ON TABLE CustomerGroupType IS 'A categorization of customer groups based on their uses.  For instance, customers may be grouped for tax, promotional, or other reasons.'
;



-- 
-- TABLE: LoyaltyProgramCampaign 
--

CREATE MULTISET TABLE ATOMICDATA.LoyaltyProgramCampaign, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CampaignID                      INTEGER           NOT NULL,
    CustomerAccountTermCode         INTEGER,
    ResponseCouponDiscountAmount    DECIMAL(10, 2),
    AdditionalRewardPercentage      DECIMAL(10, 4),
    CampaignContactMethodGroup      BYTEINT,
    OneTimePurchaseLimit            DECIMAL(10, 2),
    TotalPurchaseLimit              DECIMAL(10, 2),
    PurchaseFactor                  BYTEINT,
    AdditionalRewardBalance         INTEGER,
    LoyaltyRewardQuantity           INTEGER,
    PostalCodeID                    VARCHAR(6),
    CurrentInd                      BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --                    INTERVAL,
    ExtDttm                         TIMESTAMP(0),
    TblID                           INTEGER
)
PRIMARY INDEX PI_LyltyPgmCmpgn (CampaignID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: OperPtyLyltyPrgmAgmt 
--

CREATE MULTISET TABLE ATOMICDATA.OperPtyLyltyPrgmAgmt, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    BusinessUnitID             INTEGER         NOT NULL,
    OperationalPartyID         INTEGER         NOT NULL,
    ValidPeriod    PERIOD(DATE) , --                DATE,
    TypeCode                   CHAR(1),
    AccountNumber              INTEGER,
    BankAccountNumber          VARCHAR(20),
    BankAccountTypeCode        CHAR(1),
    RetailerName               VARCHAR(50),
    StatementCode              CHAR(1),
    ReferenceNumber            VARCHAR(20),
    InvoicingNumber            VARCHAR(10),
    ExternalInvoicingNumber    INTEGER,
    LedgerNumber               VARCHAR(10),
    StartDt                    DATE,
    EndDt                      DATE,
    CurrentInd                 BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --               INTERVAL,
    ExtDttm                    TIMESTAMP(0),
    TblID                      INTEGER
)
PRIMARY INDEX PI_OperPtyLyltyPrgmAgmt (BusinessUnitID)
PARTITION BY CASE_N (CurrentInd= 1, CurrentInd= 0, NO CASE OR UNKNOWN)
;



COMMENT ON TABLE OperPtyLyltyPrgmAgmt IS 'Stores data about the contract the K-retailer has made with Kesko'
;
-- 
-- TABLE: OperPtyLyltyPrgmAgmtApndx 
--

CREATE MULTISET TABLE ATOMICDATA.OperPtyLyltyPrgmAgmtApndx, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    BusinessUnitID        INTEGER         NOT NULL,
    OperationalPartyID    INTEGER         NOT NULL,
    TypeCode              CHAR(10)        NOT NULL,
    Agreementid           CHAR(10),
    StartDt               DATE,
    ValidPeriod    PERIOD(DATE) , --           DATE,
    EndDt                 DATE,
    CurrentInd            BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --          INTERVAL,
    ExtDttm               TIMESTAMP(0),
    TblID                 INTEGER
)
PRIMARY INDEX PI_OperPtyLyltyPrgmAgmtApndx (BusinessUnitID)
PARTITION BY CASE_N (CurrentInd= 1, CurrentInd= 0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: Partner 
--

CREATE MULTISET TABLE ATOMICDATA.Partner, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    PartnerId       INTEGER         NOT NULL,
    ValidPeriod    PERIOD(DATE) , --     DATE,
    CurrentInd      BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --    INTERVAL,
    ExtDttm         TIMESTAMP(0),
    TblID           INTEGER
)
PRIMARY INDEX PI_Prtnr (PartnerId)
;



-- 
-- TABLE: PartnershipAgreement 
--

CREATE MULTISET TABLE ATOMICDATA.PartnershipAgreement, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    PartnershipAgreementId    VARCHAR(30)      NOT NULL,
    Description               VARCHAR(2000),
    AgreementStartDate        DATE,
    AgreementEndDate          DATE,
    StatusCode                CHAR(1),
    ValidPeriod    PERIOD(DATE) , --               DATE,
    CurrentInd                BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --              INTERVAL,
    ExtDttm                   TIMESTAMP(0),
    TblID                     INTEGER
)
PRIMARY INDEX PI_PrtnshpAgrmnt (PartnershipAgreementId)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: PartnershipBusinessUnits 
--

CREATE MULTISET TABLE ATOMICDATA.PartnershipBusinessUnits, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    PartnershipAgreementId    VARCHAR(30)     NOT NULL,
    BusinessUnitID            INTEGER         NOT NULL,
    ValidPeriod    PERIOD(DATE) , --               DATE,
    StartDate                 DATE,
    EndDate                   DATE,
    CurrentInd                BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --              INTERVAL,
    ExtDttm                   TIMESTAMP(0),
    TblID                     INTEGER
)
PRIMARY INDEX PI_PartnershipBusinessUnits (PartnershipAgreementId)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: PartnershipCustAcct 
--

CREATE MULTISET TABLE ATOMICDATA.PartnershipCustAcct, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    PartnershipAgreementId    VARCHAR(30)     NOT NULL,
    CustAcctID                INTEGER         NOT NULL,
    ValidPeriod    PERIOD(DATE) , --               DATE,
    StartDate                 DATE,
    EndDate                   DATE,
    CurrentInd                BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --              INTERVAL,
    ExtDttm                   TIMESTAMP(0),
    TblID                     INTEGER
)
PRIMARY INDEX PI_PtspCust (CustAcctID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: Retailer 
--

CREATE MULTISET TABLE ATOMICDATA.Retailer, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    RetailerId      INTEGER         NOT NULL,
    RetailerName    VARCHAR(50),
    CurrentInd      BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --    INTERVAL,
    ExtDttm         TIMESTAMP(0),
    TblID           INTEGER
)
PRIMARY INDEX PI_Rtlr (RetailerId)
;



-- 
-- TABLE: RetailTrnAddLine 
--

CREATE MULTISET TABLE ATOMICDATA.RetailTrnAddLine, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    RetailTrnID            BIGINT          NOT NULL,
    RetailTrnAddLineNum    BYTEINT         NOT NULL,
    BusinessUnitID         INTEGER,
    AddInformationType     BIGINT,
    AddInformationValue    VARCHAR(30),
    AddInformationDesc     VARCHAR(50),
    AddInformationDesc1    VARCHAR(50),
    AddInformationDesc2    VARCHAR(50),
    CurrentInd             BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --           INTERVAL,
    ExtDttm                TIMESTAMP(0),
    TblID                  INTEGER
)
PRIMARY INDEX PI_RtlTrnAddLn (RetailTrnID)
PARTITION BY CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN)
;



-- 
-- TABLE: RetailTrnLoyaltyBreakdown 
--

CREATE MULTISET TABLE ATOMICDATA.RetailTrnLoyaltyBreakdown, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    RetailTrnID                      BIGINT            NOT NULL,
    RetailTrnLineSeqNum              SMALLINT          NOT NULL,
    CustAcctCardNum                  BIGINT,
    SalesAmt                         DECIMAL(13, 2)    DEFAULT 0,
    CustChainID                      SMALLINT,
    RetailTrnLoyaltyBrkpDt           DATE,
    CustAcctID                       INTEGER,
    CustGroupCode                    VARCHAR(10),
    MerchHierGroupId                 INTEGER            FORMAT '-(10)9',
    BusinessUnitID                   INTEGER,
    POSDEPARTMENTID                  INTEGER,
    CurrentInd                       BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --                     INTERVAL,
    ExtDttm                          TIMESTAMP(0),
    TblID                            INTEGER
)
PRIMARY INDEX PI_RtlTrnBrkp (RetailTrnID)
PARTITION BY (
RANGE_N(RetailTrnLoyaltyBrkpDt  BETWEEN DATE '2008-01-07' AND DATE '2014-12-31' EACH INTERVAL '7' DAY ),
CASE_N(
CustChainId =  3 ,
CustChainId =  4 ,
CustChainId =  5 ,
CustChainId =  7 ,
CustChainId =  8 ,
CustChainId =  11 ,
CustChainId =  20 ,
CustChainId =  24 ,
CustChainId =  26 ,
CustChainId =  30 ,
CustChainId =  31 ,
CustChainId =  32 ,
CustChainId =  33 ,
CustChainId =  37 ,
CustChainId =  38 ,
CustChainId =  47 ,
CustChainId =  51 ,
CustChainId =  68 ,
CustChainId =  82 ,
CustChainId =  83 ,
CustChainId =  86 ,
CustChainId =  131 ,
CustChainId =  133 ,
 NO CASE OR UNKNOWN)
, CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN) 
)

;



COMMENT ON TABLE RetailTrnLoyaltyBreakdown IS 'Loyalty reward data on item or merchandise hierarchy group level. '
;
-- 
-- TABLE: RetailTrnLoyaltyLine 
--

CREATE MULTISET TABLE ATOMICDATA.RetailTrnLoyaltyLine, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    RetailTrnID                      BIGINT            NOT NULL,
    RetailTrnLoyaltyLineSeqNum       SMALLINT          DEFAULT 0 NOT NULL,
    CustAcctCardNum                  BIGINT,
    TypeCode                         CHAR(2)           NOT NULL,
    RetailTrnLoyaltyDt               DATE,
    SalesAmt                         DECIMAL(13, 2)    DEFAULT 0,
    ProfitExclVAT                    DECIMAL(13, 2)    DEFAULT 0,
    VAT                              DECIMAL(13, 2)    DEFAULT 0,
    CustChainID                      SMALLINT,
    LoyaltyRewardSalesAmt            DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountAmt         DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDirectRedemption    DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardVIPDiscountAmt      DECIMAL(13, 2)    DEFAULT 0,
    CustGroupCode                    VARCHAR(30),
    CustAcctID                       INTEGER,
    BusinessUnitID                   INTEGER,
    CurrentInd                       BYTEINT,
    RecordPeriod   PERIOD(TIMESTAMP(0)), --                     INTERVAL,
    ExtDttm                          TIMESTAMP(0),
    TblID                            INTEGER
)
PRIMARY INDEX PI_RtlTrnLtyLn (RetailTrnID)
PARTITION BY (
 RANGE_N(RetailTrnLoyaltyDt  BETWEEN DATE '2008-01-07' AND DATE '2014-12-31' EACH INTERVAL '7' DAY ),
CASE_N(
CustChainId =  3 ,
CustChainId =  4 ,
CustChainId =  5 ,
CustChainId =  7 ,
CustChainId =  8 ,
CustChainId =  11 ,
CustChainId =  20 ,
CustChainId =  24 ,
CustChainId =  26 ,
CustChainId =  30 ,
CustChainId =  31 ,
CustChainId =  32 ,
CustChainId =  33 ,
CustChainId =  37 ,
CustChainId =  38 ,
CustChainId =  47 ,
CustChainId =  51 ,
CustChainId =  68 ,
CustChainId =  82 ,
CustChainId =  83 ,
CustChainId =  86 ,
CustChainId =  131 ,
CustChainId =  133 ,
 NO CASE OR UNKNOWN)
, CASE_N(CurrentInd=  1 ,CurrentInd=  0, NO CASE OR UNKNOWN) 
)
;




CREATE MULTISET TABLE AGGREGATEDATA.ReceiptLoyaltyBreakdownBU, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum              INTEGER           NOT NULL,
    BusinessUnitID            INTEGER           NOT NULL,
    CustChainID               INTEGER           NOT NULL,
    CustAcctID                INTEGER           NOT NULL,
    CustAcctCardNum           BIGINT            NOT NULL,
    CustClassSegment          VARCHAR(30),
    CustLANSEYSegment         VARCHAR(30),
    CustFoodstyleSegment      VARCHAR(30),
    CustPirkkaSegment         VARCHAR(30),
    CustLuomuSegment          VARCHAR(30),
    CustClassSegmentCode      VARCHAR(10),
    CustLANSEYSegmentCode     VARCHAR(10),
    CustFoodstyleSegmentCode  VARCHAR(10),
    CustPirkkaSegmentCode     VARCHAR(10),
    CustLuomuSegmentCode      VARCHAR(10),
    CustGroup                 VARCHAR(30),
    CustStatus                CHAR(1),
    CustGroupCode             VARCHAR(10)       NOT NULL,
    MerchHierGroupID          INTEGER,
    POSDepartmentID           INTEGER,
    MainPurchaseBUFlag        BYTEINT,
    SalesAmt                  DECIMAL(13, 2)    DEFAULT 0,
    LoadDttm                  TIMESTAMP(0),
    ModuleID                  INTEGER
)
PRIMARY INDEX PI_ReceiptLoyaltyBreakdownBU (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
;



-- 
-- TABLE: AGGREGATEDATA.ReceiptLoyaltyBreakdownChain 
--

CREATE MULTISET TABLE AGGREGATEDATA.ReceiptLoyaltyBreakdownChain , NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum                   INTEGER           NOT NULL,
    CustChainID                    INTEGER           NOT NULL,
    CustAcctID                     INTEGER           NOT NULL,
    CustAcctCardNum                BIGINT            NOT NULL,
    CustClassSegment               VARCHAR(30),
    CustLANSEYSegment              VARCHAR(30),
    CustFoodstyleSegment           VARCHAR(30),
    CustPirkkaSegment              VARCHAR(30),
    CustLuomuSegment               VARCHAR(30),
    CustClassSegmentCode           VARCHAR(10),
    CustLANSEYSegmentCode          VARCHAR(10),
    CustFoodstyleSegmentCode       VARCHAR(10),
    CustPirkkaSegmentCode          VARCHAR(10),
    CustLuomuSegmentCode           VARCHAR(10),
    RautaContractCategory          VARCHAR(30),
    RautaContractCategoryCode      VARCHAR(10)       NOT NULL,
    RautaContractCategoryStatus    CHAR(1)           NOT NULL,
    MerchHierGroupID               INTEGER,
    POSDepartmentID                INTEGER,
    MainPurchaseChainFlag          BYTEINT,
    SalesAmt                       DECIMAL(13, 2)    DEFAULT 0,
    LoadDttm                       TIMESTAMP(0),
    ModuleID                       INTEGER
)
PRIMARY INDEX PI_ReceiptLoyaltyBrkdwnChain (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
;



-- 
-- TABLE: AGGREGATEDATA.ReceiptLoyaltyRowChain 
--

CREATE MULTISET TABLE AGGREGATEDATA.ReceiptLoyaltyRowChain, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum                     INTEGER           NOT NULL,
    CustChainID                      INTEGER           NOT NULL,
    CustAcctID                       INTEGER           NOT NULL,
    CustAcctCardNum                  BIGINT            NOT NULL,
    RautaContractCategoryCode        VARCHAR(10)       NOT NULL,
    RautaContractCategoryStatus      CHAR(1)           NOT NULL,
	RautaContractCategory            VARCHAR(30),
    CustClassSegment                 VARCHAR(30),
    CustLANSEYSegment                VARCHAR(30),
    CustFoodstyleSegment             VARCHAR(30),
    CustPirkkaSegment                VARCHAR(30),
    CustLuomuSegment                 VARCHAR(30),
    CustClassSegmentCode             VARCHAR(10),
    CustLANSEYSegmentCode            VARCHAR(10),
    CustFoodstyleSegmentCode         VARCHAR(10),
    CustPirkkaSegmentCode            VARCHAR(10),
    CustLuomuSegmentCode             VARCHAR(10),
    SalesAmt                         DECIMAL(13, 2)    DEFAULT 0,
    ProfitExclVAT                    DECIMAL(13, 2)    DEFAULT 0,
    VAT                              DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardSalesAmt            DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountAmt         DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDirectRedemption    DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardVIPDiscountAmt      DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountQty         INTEGER,
    LoyaltyRewardDirectRedemptnQty   INTEGER,
    MainPurchaseChainFlag            BYTEINT,
    NoOfVisits                       INTEGER,
    LoadDttm                         TIMESTAMP(0),
    ModuleID                         INTEGER
)
PRIMARY INDEX PI_ReceiptLoyaltyRowChain (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
;



-- 
-- TABLE: AGGREGATEDATA.ReceiptLoyaltyRowBU 
--

CREATE MULTISET TABLE AGGREGATEDATA.ReceiptLoyaltyRowBU, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum                     INTEGER           NOT NULL,
    CustChainID                      INTEGER           NOT NULL,
    BusinessUnitID                   INTEGER           NOT NULL,
    CustAcctID                       INTEGER           NOT NULL,
    CustAcctCardNum                  BIGINT            NOT NULL,
    CustStatus                       CHAR(1),
    CustGroup                        VARCHAR(30),
    CustClassSegment                 VARCHAR(30),
    CustLANSEYSegment                VARCHAR(30),
    CustFoodstyleSegment             VARCHAR(30),
    CustPirkkaSegment                VARCHAR(30),
    CustLuomuSegment                 VARCHAR(30),
    CustClassSegmentCode             VARCHAR(10),
    CustLANSEYSegmentCode            VARCHAR(10),
    CustFoodstyleSegmentCode         VARCHAR(10),
    CustPirkkaSegmentCode            VARCHAR(10),
    CustLuomuSegmentCode             VARCHAR(10),
	CustGroupCode                    VARCHAR(10),
    SalesAmt                         DECIMAL(13, 2)    DEFAULT 0,
    ProfitExclVAT                    DECIMAL(13, 2)    DEFAULT 0,
    VAT                              DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardSalesAmt            DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountAmt         DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDirectRedemption    DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardVIPDiscountAmt      DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountQty         INTEGER,
    LoyaltyRewardDirectRedemptnQty   INTEGER,
    MainPurchaseBUFlag               BYTEINT,
    NoOfVisits                       SMALLINT,
    LoadDttm                         TIMESTAMP(0),
    ModuleID                         INTEGER
)
PRIMARY INDEX PI_ReceiptLoyaltyRowBU (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)

;

-- 
-- TABLE: AGGREGATEDATA.CustAcctSegmentInfo 
--

CREATE MULTISET TABLE AGGREGATEDATA.CustAcctSegmentInfo, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum                     INTEGER           NOT NULL,
    CustChainID                      INTEGER           NOT NULL,
    CustAcctID                       INTEGER           NOT NULL,
    CustClassSegment                 VARCHAR(30),
    CustLANSEYSegment                VARCHAR(30),
    CustFoodstyleSegment             VARCHAR(30),
    CustPirkkaSegment                VARCHAR(30),
    CustLuomuSegment                 VARCHAR(30),
    CustClassSegmentCode             VARCHAR(10),
    CustLANSEYSegmentCode            VARCHAR(10),
    CustFoodstyleSegmentCode         VARCHAR(10),
    CustPirkkaSegmentCode            VARCHAR(10),
    CustLuomuSegmentCode             VARCHAR(10),
    LoadDttm                         TIMESTAMP(0),
    ModuleID                         INTEGER
)
PRIMARY INDEX PI_CustAcctSegmentInfo (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
;



CREATE MULTISET TABLE AGGREGATEDATA.ReceiptLoyaltyRowSite, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum                      INTEGER           NOT NULL,
    CustChainID                       INTEGER           NOT NULL,
    SiteID                            INTEGER           NOT NULL,
    CustAcctID                        INTEGER           NOT NULL,
    CustAcctCardNum                   BIGINT            NOT NULL,
    RautaContractCategoryCode         VARCHAR(10)       NOT NULL,
    RautaContractCategoryStatus       CHAR(1),
	RautaContractCategory             VARCHAR(30),
    CustClassSegment                  VARCHAR(30),
    CustLANSEYSegment                 VARCHAR(30),
    CustFoodstyleSegment              VARCHAR(30),
    CustPirkkaSegment                 VARCHAR(30),
    CustLuomuSegment                  VARCHAR(30),
    CustClassSegmentCode              VARCHAR(10),
    CustLANSEYSegmentCode             VARCHAR(10),
    CustFoodstyleSegmentCode          VARCHAR(10),
    CustPirkkaSegmentCode             VARCHAR(10),
    CustLuomuSegmentCode              VARCHAR(10),
    SalesAmt                          DECIMAL(13, 2)    DEFAULT 0,
    ProfitExclVAT                     DECIMAL(13, 2)    DEFAULT 0,
    VAT                               DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardSalesAmt             DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountAmt          DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDirectRedemption     DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardVIPDiscountAmt       DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountQty          INTEGER,
    LoyaltyRewardDirectRedemptnQty    INTEGER,
    NoOfVisits                        SMALLINT,
    LoadDttm                          TIMESTAMP(0),
    ModuleID                          INTEGER
)
PRIMARY INDEX PI_ReceiptLoyaltyRowSite (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1);



-- 
-- TABLE: ATOMICDATA.CustAcctAffiliationAttr 
--

CREATE MULTISET TABLE ATOMICDATA.CustAcctAffiliationAttr,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    CustGroupID               INTEGER          NOT NULL,
    CustAcctID                INTEGER          NOT NULL,
    OrgLevelId                INTEGER          NOT NULL,
    OrgLevelTypeCode          CHAR(10)         NOT NULL,
    YearMonthNum              INTEGER          NOT NULL,
    CustAcctAffilAttrName     VARCHAR(50)      NOT NULL,
    CustAcctAffilAttrValue    DECIMAL(8, 6),
    ValidPeriod               PERIOD(DATE),
    CurrentInd                BYTEINT,
    RecordPeriod              PERIOD(TIMESTAMP(0)),
    ExtDttm                   TIMESTAMP(0),
    TblID                     INTEGER
)
PRIMARY INDEX PI_CusttAccrAfflAttr (CustAcctID)
PARTITION BY (
RANGE_N(YearMonthNum BETWEEN  200801 AND 201412 EACH 1)
,
RANGE_N(CustGroupID BETWEEN 1 AND 100 EACH 1 )
)
;

-- 
-- TABLE: ATOMICDATA.RetailTrnTenderLine 
--

CREATE MULTISET TABLE ATOMICDATA.RetailTrnTenderLine ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      RetailTrnID BIGINT NOT NULL,
      RetailTrnTenderLineNum SMALLINT NOT NULL,
      SrcTenderTypeCode INTEGER,
      VoucherID VARCHAR(16) CHARACTER SET LATIN NOT CASESPECIFIC,
      PaymentAmtEur DECIMAL(13,2) DEFAULT 0.00 ,
      RetailTrnTenderTypeCode BYTEINT,
      RetailTrnTenderSubTypeCode CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      ValidPeriod PERIOD(DATE),
      CurrencyCd CHAR(3) CHARACTER SET LATIN NOT CASESPECIFIC,
      CurrentInd BYTEINT,
      RecordPeriod PERIOD(TIMESTAMP(0)),
      ExtDttm TIMESTAMP(0),
      TblID INTEGER)
PRIMARY INDEX PI_RtlTrnPmtLn ( RetailTrnID )
PARTITION BY CASE_N(
CurrentInd =  1 ,
CurrentInd =  0 ,
 NO CASE OR UNKNOWN);
 
 

-- 
-- TABLE: AGGREGATEDATA.CustAcctChainUnitSegmentInfo 
--

CREATE MULTISET TABLE AGGREGATEDATA.CustAcctChainUnitSegmentInfo,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum                   INTEGER         NOT NULL,
    CustAcctID                     INTEGER         NOT NULL,
    ChainUnitID                    VARCHAR(50)     NOT NULL,
    CustClassSegment               VARCHAR(30),
    CustClassSegmentCode           VARCHAR(10),
    MainPurchaseBU                 INTEGER,
    LoyaltySegmentChainUnitFlag    CHAR(1),
    LoadDttm                       TIMESTAMP(0),
    ModuleID                       INTEGER
)
PRIMARY INDEX PI_CustAcctChnUnitSgmntInfo (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
;



-- 
-- TABLE: AGGREGATEDATA.CustAcctDivisionSegmentInfo 
--

CREATE MULTISET TABLE AGGREGATEDATA.CustAcctDivisionSegmentInfo,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO(
    YearMonthNum             INTEGER         NOT NULL,
    CustAcctID               INTEGER         NOT NULL,
    DivisionID               INTEGER         NOT NULL,
    CustClassSegmentCode     VARCHAR(10),
    CustClassSegment         VARCHAR(30),
    LoyaltySegmentDivFlag    CHAR(1),
    MainPurchaseChain        INTEGER,
    FirstSuppChain           INTEGER,
    SecondSuppChain          INTEGER,
    MainPurchaseBU           INTEGER,
    FirstSuppBU              INTEGER,
    SecondSuppBU             INTEGER,
    LoadDttm                 TIMESTAMP(0),
    ModuleID                 INTEGER
)
PRIMARY INDEX PI_CustAcctDvsnSgmntInfo (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
;




-- 
-- TABLE: ATOMICDATA.OrgLevelSegmentationRules 
--

CREATE TABLE ATOMICDATA.OrgLevelSegmentationRules(
    OrgLevelId          VARCHAR(10),
    OrgLevelName        VARCHAR(30),
    OrgLevelTypeCode    SMALLINT,
    TypeCode            VARCHAR(20),
    ResultTypeCode      VARCHAR(30),
    ResultValue         INTEGER,
    LowerLimit          INTEGER,
    UpperLimit          INTEGER,
    ValidPeriod         PERIOD(DATE),
	RecordPeriod        PERIOD(TIMESTAMP(0)),
    CurrentInd          BYTEINT
)
PRIMARY INDEX PI_OrgLevelSegmntnRules (OrgLevelId, OrgLevelName)
;


-- 
-- TABLE: BusinessUnitBusinessUnitGroup 
--

CREATE TABLE BusinessUnitBusinessUnitGrp, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
(
    BusinessUnitID         INTEGER         NOT NULL,
    BusinessUnitGroupID    INTEGER         NOT NULL,
    ValidPeriod            PERIOD(DATE),
    CurrentInd             BYTEINT,
    RecordPeriod           PERIOD(TIMESTAMP(0)),
    ExtDttm                TIMESTAMP(0),
    TblID                  INTEGER
)
PRIMARY INDEX PI_BUBUGroup (BusinessUnitID)
;


-- 
-- TABLE: AGGREGATEDATA.ReceiptLoyaltyRowChainUnit 
--
DROP TABLE AGGREGATEDATA.ReceiptLoyaltyRowChainUnit;
CREATE MULTISET TABLE AGGREGATEDATA.ReceiptLoyaltyRowChainUnit, NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO (
    YearMonthNum                      INTEGER           NOT NULL,
    ChainUnitID                       VARCHAR(50)       NOT NULL,
    CustChainID                       INTEGER           NOT NULL,
    CustAcctID                        INTEGER           NOT NULL,
    CustAcctCardNum                   BIGINT            NOT NULL,
    RautaContractCategoryCode         VARCHAR(10)       NOT NULL,
    RautaContractCategoryStatus       CHAR(1)           NOT NULL,
    RautaContractCategory             VARCHAR(30)       NOT NULL,
    CustClassSegment                  VARCHAR(30),
    CustLANSEYSegment                 VARCHAR(30),
    CustFoodstyleSegment              VARCHAR(30),
    CustPirkkaSegment                 VARCHAR(30),
    CustLuomuSegment                  VARCHAR(30),
    CustClassSegmentCode              VARCHAR(10),
    CustLANSEYSegmentCode             VARCHAR(10),
    CustFoodstyleSegmentCode          VARCHAR(10),
    CustPirkkaSegmentCode             VARCHAR(10),
    CustLuomuSegmentCode              VARCHAR(10),
    SalesAmt                          DECIMAL(13, 2)    DEFAULT 0,
    ProfitExclVAT                     DECIMAL(13, 2)    DEFAULT 0,
    VAT                               DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardSalesAmt             DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountAmt          DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDirectRedemption     DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardVIPDiscountAmt       DECIMAL(13, 2)    DEFAULT 0,
    LoyaltyRewardDiscountQty          INTEGER,
    LoyaltyRewardDirectRedemptnQty    INTEGER,
    NoOfVisits                        INTEGER,
    LoadDttm                          TIMESTAMP(0),
    ModuleID                          INTEGER
)
PRIMARY INDEX PI_RetailTrnLoyaltyLineChnUnt (CustAcctID)
PARTITION BY RANGE_N (YearMonthNum BETWEEN 200801 AND 201412 EACH 1)
;



-- 
-- VIEW: SLCAMPAIGN.Campaign 
--

REPLACE VIEW SLCAMPAIGN.Campaign AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID, Ca.OrderId, Ca.Name, Ca.StartDate, Ca.EndDate, Ca.SrcCampaignId, Ca.CampaignTypeCode, Ca.CampaignSourceSystemCd, Ca.StatusCode, Ca.IsTargeted, Ca.Description, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs, BEGIN(ca.ValidPeriod) AS ValidPeriodStartDt, END(ca.ValidPeriod) AS ValidPeriodEndDt
FROM ATOMICDATA.Campaign Ca
;

-- 
-- VIEW: SLCAMPAIGN.CampaignBusinessUnit
--

REPLACE VIEW SLCAMPAIGN.CampaignBusinessUnit AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID, Ca.BusinessUnitID, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignBusinessUnit Ca
;

-- 
-- VIEW: SLCAMPAIGN.CampaignOffering 
--

REPLACE VIEW SLCAMPAIGN.CampaignOffering AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID, Ca.CampaignOfferingID, Ca.ResponseEAN, BEGIN(Ca.ValidPeriod) ValidPeriodStartDt, END(Ca.ValidPeriod) ValidPeriodEndDt, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignOffering Ca
;

-- 
-- VIEW: SLCAMPAIGN.CampaignOrder 
--

REPLACE VIEW SLCAMPAIGN.CampaignOrder AS LOCKING ROW FOR ACCESS
SELECT Ca.OrderId, Ca.OrderHeader, Ca.InvoicingStatusCode, Ca.CampaignOrderStartDt, Ca.CampaignOrderEndDt, BEGIN(Ca.ValidPeriod) ValidPeriodStartDt, END(Ca.ValidPeriod) ValidPeriodEndDt, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignOrder Ca
;

-- 
-- VIEW: SLCAMPAIGN.CampaignResponse 
--

REPLACE VIEW SLCAMPAIGN.CampaignResponse AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID CampaignID, Ca.TargetGroupID TargetGroupID, Ca.CustomerId CustomerId, Ca.CustomerTypeCode CustomerTypeCode, Ca.CampaignResponseTypeCode CampaignResponseTypeCode, Ca.ResponseEAN ResponseEAN, Ca.ResponseDt ResponseDt, Ca.ResponseTm ResponseTm, Ca.RetailTrnID RetailTrnID, Ca.CampaignOfferingID CampaignOfferingID, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignResponse Ca
;

-- 
-- VIEW: SLCAMPAIGN.CampaignResponseCustAcct 
--

REPLACE VIEW SLCAMPAIGN.CampaignResponseCustAcct AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID, Ca.TargetGroupID, Ca.CustomerId CustAcctId, Ca.CampaignResponseTypeCode, Ca.ResponseEAN, Ca.ResponseDt, Ca.ResponseTm, Ca.RetailTrnID, Ca.CampaignOfferingID, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignResponse Ca
WHERE CustomerTypeCode = 1
;

-- 
-- VIEW: SLCAMPAIGN.CampaignResponseCustAcctCard 
--

REPLACE VIEW SLCAMPAIGN.CampaignResponseCustAcctCard AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID, Ca.TargetGroupID, Ca.CustomerId CustAcctCardNum, Ca.CustomerTypeCode, Ca.CampaignResponseTypeCode, Ca.ResponseEAN, Ca.ResponseDt, Ca.ResponseTm, Ca.RetailTrnID, Ca.CampaignOfferingID, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignResponse Ca
WHERE CustomerTypeCode = 3
;

-- 
-- VIEW: SLCAMPAIGN.CampaignResponseCustAcctPers 
--

REPLACE VIEW SLCAMPAIGN.CampaignResponseCustAcctPers AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID, Ca.TargetGroupID, Ca.CustomerId CustAcctPersId, Ca.CustomerTypeCode, Ca.CampaignResponseTypeCode, Ca.ResponseEAN, Ca.ResponseDt, Ca.ResponseTm, Ca.RetailTrnID, Ca.CampaignOfferingID, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignResponse Ca
WHERE CustomerTypeCode = 2
;

-- 
-- VIEW: SLCAMPAIGN.CampaignTargetGroup 
--

REPLACE VIEW SLCAMPAIGN.CampaignTargetGroup AS LOCKING ROW FOR ACCESS
SELECT Ca.CampaignID, Ca.TargetGroupID, Ca.TargetGroupClass, Ca.SrcTargetGroupId, Ca.Name, Ca.LevelId, Ca.SelectionLevelId, Ca.TypeCode, Ca.FollowUpInd, BEGIN(Ca.ValidPeriod) ValidPeriodStartDt, END(Ca.ValidPeriod) ValidPeriodEndDt, Ca.CurrentInd, BEGIN(Ca.RecordPeriod)  RecordPeriodStartTs, END(Ca.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignTargetGroup Ca
;

-- 
-- VIEW: SLCAMPAIGN.CampaignTargetGroupMember 
--

REPLACE VIEW SLCAMPAIGN.CampaignTargetGroupMember AS LOCKING ROW FOR ACCESS
SELECT ctgm.CampaignID, ctgm.TargetGroupID, ctgm.CustAcctID, ctgm.CustomerId, ctgm.CustomerTypeCode, ctgm.BusinessUnitID, ctgm.StatusCode, BEGIN(ctgm.ValidPeriod) ValidPeriodStartDt, END(ctgm.ValidPeriod) ValidPeriodEndDt, ctgm.CurrentInd, BEGIN(ctgm.RecordPeriod)  RecordPeriodStartTs, END(ctgm.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignTargetGroupMember ctgm
;

-- 
-- VIEW: SLCAMPAIGN.CampaignTargetGroupOffrng 
--

REPLACE VIEW SLCAMPAIGN.CampaignTargetGroupOffrng AS LOCKING ROW FOR ACCESS
SELECT ctgo.CampaignOfferingID, ctgo.CampaignID, ctgo.CustomerId, ctgo.CustomerTypeCode, ctgo.TargetGroupID, BEGIN(ctgo.RecordPeriod)  RecordPeriodStartTs, END(ctgo.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignTargetGroupOffrng ctgo
;

-- 
-- VIEW: SLCAMPAIGN.CampaignTgtGrpMbrCustAcct 
--

REPLACE VIEW SLCAMPAIGN.CampaignTgtGrpMbrCustAcct AS LOCKING ROW FOR ACCESS
SELECT ctgm.CampaignID, ctgm.TargetGroupID, ctgm.CustomerId CustAcctId, ctgm.BusinessUnitID, ctgm.StatusCode, BEGIN(ctgm.ValidPeriod) ValidPeriodStartDt, END(ctgm.ValidPeriod) ValidPeriodEndDt, ctgm.CurrentInd, BEGIN(ctgm.RecordPeriod)  RecordPeriodStartTs, END(ctgm.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignTargetGroupMember ctgm
WHERE CustomerTypeCode = 1
;

-- 
-- VIEW: SLCAMPAIGN.CampaignTgtGrpMbrCustAcctCard 
--

REPLACE VIEW SLCAMPAIGN.CampaignTgtGrpMbrCustAcctCard AS LOCKING ROW FOR ACCESS
SELECT ctgm.CampaignID, ctgm.BusinessUnitID, ctgm.CustomerId CustAcctCardNum, ctgm.TargetGroupID, ctgm.StatusCode, BEGIN(ctgm.ValidPeriod) ValidPeriodStartDt, END(ctgm.ValidPeriod) ValidPeriodEndDt, ctgm.CurrentInd, BEGIN(ctgm.RecordPeriod)  RecordPeriodStartTs, END(ctgm.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignTargetGroupMember ctgm
WHERE CustomerTypeCode = 3
;

-- 
-- VIEW: SLCAMPAIGN.CampaignTgtGrpMbrCustAcctPers 
--

REPLACE VIEW SLCAMPAIGN.CampaignTgtGrpMbrCustAcctPers AS LOCKING ROW FOR ACCESS
SELECT ctgm.CampaignID, ctgm.TargetGroupID, ctgm.CustomerId CustAcctPersId, ctgm.BusinessUnitID, ctgm.StatusCode, BEGIN(ctgm.ValidPeriod) ValidPeriodStartDt, END(ctgm.ValidPeriod) ValidPeriodEndDt, ctgm.CurrentInd, BEGIN(ctgm.RecordPeriod)  RecordPeriodStartTs, END(ctgm.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignTargetGroupMember ctgm
WHERE CustomerTypeCode = 2
;

-- 
-- VIEW: SLCAMPAIGN.LoyaltyProgramCampaign 
--

REPLACE VIEW SLCAMPAIGN.LoyaltyProgramCampaign AS LOCKING ROW FOR ACCESS
SELECT lpg.CampaignID, lpg.CustomerAccountTermCode, lpg.ResponseCouponDiscountAmount, lpg.AdditionalRewardPercentage, lpg.CampaignContactMethodGroup, lpg.OneTimePurchaseLimit, lpg.TotalPurchaseLimit, lpg.PurchaseFactor, lpg.AdditionalRewardBalance, lpg.LoyaltyRewardQuantity, lpg.PostalCodeID, lpg.CurrentInd, BEGIN(lpg.RecordPeriod)  RecordPeriodStartTs, END(lpg.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.LoyaltyProgramCampaign lpg
;

-- 
-- VIEW: SLCI.CampaignMemberStatistics 
--

REPLACE VIEW SLCI.CampaignMemberStatistics AS LOCKING ROW FOR ACCESS --LOCKING ROW FOR ACCESS
SELECT cms.CampaignID, cms.CustAcctID, cms.SiteID, cms.YearMonthNum, cms.SalesAmount, cms.LoyaltyRewardSalesAmountUnltd, cms.LoyaltyRewardSalesAmount, cms.VisitQuantity, cms.CampaignVisitQuantity, cms.CampaignResponseQuantity, cms.AdditionalRewardQuantity, cms.CampaignMatrix, BEGIN(cms.ValidPeriod) ValidPeriodStartDt, END(cms.ValidPeriod) ValidPeriodEndDt, cms.CurrentInd, BEGIN(cms.RecordPeriod)  RecordPeriodStartTs, END(cms.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CampaignMemberStatistics cms
;

-- 
-- VIEW: SLCI.CustAcctLoyaltyRewardEntry 
--

REPLACE VIEW SLCI.CustAcctLoyaltyRewardEntry AS LOCKING ROW FOR ACCESS
SELECT calre.CustAcctID, calre.RewardPeriodStartDt, calre.RewardPeriodEndDt, calre.TypeCode, calre.RewardModel, calre.StatementStartDt, calre.StatementEndDt, calre.TransactionDt, calre.RewardQty, calre.RewardFactor, calre.AdditionalRewardFactor, calre.PreviousRewardBalance, calre.CurrentInd, BEGIN(calre.RecordPeriod)  RecordPeriodStartTs, END(calre.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CustAcctLoyaltyRewardEntry calre
;

-- 
-- VIEW: SLCI.CustAcctLoyaltyStatement 
--

REPLACE VIEW SLCI.CustAcctLoyaltyStatement AS LOCKING ROW FOR ACCESS --LOCKING ROW FOR ACCESS
SELECT cals.CustAcctID, cals.StatementStartDt, cals.StatementEndDt, cals.StatementType, cals.BenefitID, cals.PersonalizationSqNr, cals.PersonalizationContent, cals.TransactionDt, cals.FormatID, cals.TransactionUserID, cals.TransaferDt, cals.CurrentInd, BEGIN(cals.RecordPeriod)  RecordPeriodStartTs, END(cals.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CustAcctLoyaltyStatement cals
;

-- 
-- VIEW: SLCI.CustAcctOrgAffiliation 
--

REPLACE VIEW SLCI.CustAcctOrgAffiliation AS LOCKING ROW FOR ACCESS
SELECT caoa.CustAcctID, caoa.CustAcctAffiliationTypeCode, caoa.OrgLevelId, caoa.OrgLevelTypeCode, caoa.CustAcctAfflnOrgLevelId, caoa.CustAcctAfflnOrgTypeCode, caoa.StatusCode, BEGIN(caoa.ValidPeriod) ValidPeriodStartDt, END(caoa.ValidPeriod) ValidPeriodEndDt, caoa.CurrentInd, BEGIN(caoa.RecordPeriod)  RecordPeriodStartTs, END(caoa.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.CustAcctOrgAffiliation caoa
;

-- 
-- VIEW: SLCI.PartnershipAgreement 
--

REPLACE VIEW SLCI.PartnershipAgreement AS LOCKING ROW FOR ACCESS
SELECT pa.PartnershipAgreementId,  pa.Description, pa.AgreementStartDate, pa.AgreementEndDate, pa.StatusCode, BEGIN(pa.ValidPeriod) ValidPeriodStartDt,END(pa.ValidPeriod) ValidPeriodEndDt, pa.CurrentInd, BEGIN(pa.RecordPeriod)  RecordPeriodStartTs, END(pa.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.PartnershipAgreement pa
;

-- 
-- VIEW: SLCI.PartnershipBusinessUnits 
--

REPLACE VIEW SLCI.PartnershipBusinessUnits AS LOCKING ROW FOR ACCESS
SELECT pbu.PartnershipAgreementId, pbu.BusinessUnitID, BEGIN(pbu.ValidPeriod) ValidPeriodStartDt, END(pbu.ValidPeriod) ValidPeriodEndDt, pbu.StartDate, pbu.EndDate, pbu.CurrentInd, BEGIN(pbu.RecordPeriod)  RecordPeriodStartTs, END(pbu.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.PartnershipBusinessUnits pbu
;

-- 
-- VIEW: SLCI.PartnershipCustAcct 
--

REPLACE VIEW SLCI.PartnershipCustAcct AS LOCKING ROW FOR ACCESS
SELECT pc.PartnershipAgreementId, pc.CustAcctID, pc.StartDate, pc.EndDate, BEGIN(pc.ValidPeriod) ValidPeriodStartDt, END(pc.ValidPeriod) ValidPeriodEndDt, pc.CurrentInd, BEGIN(pc.RecordPeriod)  RecordPeriodStartTs, END(pc.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.PartnershipCustAcct pc
;

-- 
-- VIEW: SLCI.ReceiptAddRow 
--

REPLACE VIEW SLCI.ReceiptAddRow AS LOCKING ROW FOR ACCESS
SELECT rtal.RetailTrnID, rtal.RetailTrnAddLineNum, rtal.AddInformationID, rtal.AddInformationValue, rtal.AddInformationDesc, rtal.AddInformationDesc1, rtal.AddInformationDesc2, BEGIN(rtal.RecordPeriod)  RecordPeriodStartTs, END(rtal.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.RetailTrnAddLine rtal
;

-- 
-- VIEW: SLCI.ReceiptLoyaltyBreakdown 
--

REPLACE VIEW SLCI.ReceiptLoyaltyBreakdown AS LOCKING ROW FOR ACCESS
SELECT rtlb.RetailTrnID, rtlb.RetailTrnLineSeqNum, rtlb.CustAcctCardNum, rtlb.SalesAmt, rtlb.CustChainID, rtlb.RetailTrnLoyaltyBrkpDt, rtlb.CustAcctID, rtlb.CustGroupCode, rtlb.MerchHierGroupId, rtlb.BusinessUnitID, rtlb.POSDEPARTMENTID, rtlb.CurrentInd, BEGIN(rtlb.RecordPeriod) RecordPeriodStartTs,END(rtlb.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.RetailTrnLoyaltyBreakdown rtlb
;

-- 
-- VIEW: SLCI.ReceiptLoyaltyRow 
--

REPLACE VIEW SLCI.ReceiptLoyaltyRow AS LOCKING ROW FOR ACCESS
SELECT rtll.RetailTrnID, rtll.BusinessUnitID, rtll.RetailTrnLoyaltyLineSeqNum, rtll.CustAcctCardNum, rtll.TypeCode, rtll.RetailTrnLoyaltyDt, rtll.SalesAmt, rtll.ProfitExclVAT, rtll.VAT, rtll.CustChainID, rtll.LoyaltyRewardSalesAmt, rtll.LoyaltyRewardDiscountAmt, rtll.LoyaltyRewardDirectRedemption, rtll.CustGroupCode, rtll.CustAcctID, rtll.LoyaltyRewardVIPDiscountAmt, rtll.CurrentInd, BEGIN(rtll.RecordPeriod)  RecordPeriodStartTs, END(rtll.RecordPeriod)  RecordPeriodEndTs
FROM ATOMICDATA.RetailTrnLoyaltyLine rtll
;

-- 
-- VIEW: SLCI.ReceiptTenderRow 
--

REPLACE VIEW SLCI.ReceiptTenderRow AS LOCKING ROW FOR ACCESS
SELECT rttl.RetailTrnID, rttl.RetailTrnTenderLineNum, rttl.SrcTenderTypeCode, rttl.VoucherID, rttl.PaymentAmtEur, BEGIN(rttl.ValidPeriod) ValidPeriodStartDt, END(rttl.ValidPeriod) ValidPeriodEndDt, rttl.CurrencyCd, rttl.RetailTrnTenderTypeCode, rttl.RetailTrnTenderSubTypeCode, BEGIN(rttl.RecordPeriod) RecordPeriodStartTs,END(rttl.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.RetailTrnTenderLine rttl
;

-- 
-- VIEW: SLCUSTOMER.CustAcctAffConsumptionStyleSeg 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffConsumptionStyleSeg AS LOCKING ROW FOR ACCESS
SELECT caa.CustAcctID, caa.CustGroupID, caa.OrgLevelId, caa.YearMonthNum, caa.OrgLevelTypeCode, caa.CustGroupTypeCode, caa.Amount, caa.Indx, caa.Probability, cgt.Name, BEGIN(caa.ValidPeriod) ValidPeriodStartDt, END(caa.ValidPeriod) ValidPeriodEndDt, caa.CurrentInd, BEGIN(caa.RecordPeriod) RecordPeriodStartTs,END(caa.RecordPeriod) RecordPeriodEndTs
FROM
    ATOMICDATA.CustAcctAffiliation caa
    , ATOMICDATA.CustomerGroup cg
    , ATOMICDATA.CustomerGroupType cgt
WHERE
    caa.CustGroupID = cg.CustGroupID
    AND cg.CustGroupTypeCode = cgt.CustGroupTypeCode
    AND cgt.Name IN ('Pirkka', 'Luomu') --Constant for CustClassSeg
;

-- 
-- VIEW: SLCUSTOMER.CustAcctAffCustClassSeg 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffCustClassSeg AS
LOCKING ROW FOR ACCESS
SELECT caa.CustAcctID, cg.Name, caa.CustGroupID, caa.OrgLevelId, caa.YearMonthNum, caa.OrgLevelTypeCode, caa.CustGroupTypeCode, caa.Amount, caa.Indx, caa.Probability, BEGIN(caa.ValidPeriod) ValidPeriodStartDt, END(caa.ValidPeriod) ValidPeriodEndDt, caa.CurrentInd, BEGIN(caa.RecordPeriod) RecordPeriodStartTs,END(caa.RecordPeriod) RecordPeriodEndTs
FROM
    ATOMICDATA.CustAcctAffiliation caa
    , ATOMICDATA.CustomerGroup cg
    , ATOMICDATA.CustomerGroupType cgt
WHERE
    caa.CustGroupID = cg.CustGroupID
    AND cg.CustGroupTypeCode = cgt.CustGroupTypeCode
    AND cgt.Name = 'CustClassSeg' --Constant for CustClassSeg
;

-- 
-- VIEW: SLCUSTOMER.CustAcctAffFoodstyleSeg 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffFoodstyleSeg AS
LOCKING ROW FOR ACCESS
SELECT caa.CustAcctID, cg.Name, caa.CustGroupID, caa.OrgLevelId, caa.YearMonthNum, caa.OrgLevelTypeCode, caa.CustGroupTypeCode, caa.Amount, caa.Indx, caa.Probability, BEGIN(caa.ValidPeriod) ValidPeriodStartDt, END(caa.ValidPeriod) ValidPeriodEndDt, caa.CurrentInd, BEGIN(caa.RecordPeriod) RecordPeriodStartTs,END(caa.RecordPeriod) RecordPeriodEndTs
FROM
    ATOMICDATA.CustAcctAffiliation caa
    , ATOMICDATA.CustomerGroup cg
    , ATOMICDATA.CustomerGroupType cgt
WHERE
    caa.CustGroupID = cg.CustGroupID
    AND cg.CustGroupTypeCode = cgt.CustGroupTypeCode
    AND cgt.name = 'FoodstyleSeg' --Constant for FoodstyleSeg
;

-- 
-- VIEW: SLCUSTOMER.CustAcctAffiliation 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffiliation AS LOCKING ROW FOR ACCESS
SELECT Cu.CustGroupID, Cu.CustAcctID, Cu.OrgLevelId, Cu.YearMonthNum, Cu.OrgLevelTypeCode, Cu.CustGroupTypeCode, Cu.Amount, Cu.Indx, Cu.Probability, BEGIN(Cu.ValidPeriod) ValidPeriodStartDt, END(Cu.ValidPeriod) ValidPeriodEndDt, Cu.CurrentInd, BEGIN(cu.RecordPeriod) RecordPeriodStartTs,END(cu.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.CustAcctAffiliation Cu
;

-- 
-- VIEW: SLCUSTOMER.CustAcctAffLANSEYSeg 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffLANSEYSeg AS
LOCKING ROW FOR ACCESS
SELECT caa.CustGroupID, cg.Name, caa.CustAcctID, caa.OrgLevelId, caa.YearMonthNum, caa.OrgLevelTypeCode, caa.CustGroupTypeCode, caa.Amount, caa.Indx, caa.Probability, BEGIN(caa.ValidPeriod) ValidPeriodStartDt, END(caa.ValidPeriod) ValidPeriodEndDt, caa.CurrentInd, BEGIN(caa.RecordPeriod) RecordPeriodStartTs,END(caa.RecordPeriod) RecordPeriodEndTs
FROM
    ATOMICDATA.CustAcctAffiliation caa
    , ATOMICDATA.CustomerGroup cg
    , ATOMICDATA.CustomerGroupType cgt
WHERE
    caa.CustGroupID = cg.CustGroupID
    AND cg.CustGroupTypeCode = cgt.CustGroupTypeCode
    AND cgt.Name = 'LANSEY' --Constant for LANSEYSeg
;

-- 
-- VIEW: SLCUSTOMER.CustAcctAffLoyaltySegChain 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffLoyaltySegChain AS
LOCKING ROW FOR ACCESS
SELECT caa.CustGroupID, cg.Name, caa.CustAcctID, caa.OrgLevelId, caa.YearMonthNum, caa.OrgLevelTypeCode, caa.CustGroupTypeCode, caa.Amount, caa.Indx, caa.Probability, BEGIN(caa.ValidPeriod) ValidPeriodStartDt, END(caa.ValidPeriod) ValidPeriodEndDt, caa.CurrentInd, BEGIN(caa.RecordPeriod) RecordPeriodStartTs,END(caa.RecordPeriod) RecordPeriodEndTs
FROM
    ATOMICDATA.CustAcctAffiliation caa
    , ATOMICDATA.CustomerGroup cg
    , ATOMICDATA.CustomerGroupType cgt
WHERE
    caa.CustGroupID = cg.CustGroupID
    AND cg.CustGroupTypeCode = cgt.CustGroupTypeCode
    AND cgt.Name = 'LoyaltySegChain' --Constant for LoyaltySegChain
;

-- 
-- VIEW: SLCUSTOMER.CustAcctAffLoyaltySegDivision 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffLoyaltySegDivision AS
LOCKING ROW FOR ACCESS
SELECT caa.CustGroupID, cg.Name, caa.CustAcctID, caa.OrgLevelId, caa.YearMonthNum, caa.OrgLevelTypeCode, caa.CustGroupTypeCode, caa.Amount, caa.Indx, caa.Probability, BEGIN(caa.ValidPeriod) ValidPeriodStartDt, END(caa.ValidPeriod) ValidPeriodEndDt, caa.CurrentInd, BEGIN(caa.RecordPeriod) RecordPeriodStartTs,END(caa.RecordPeriod) RecordPeriodEndTs
FROM
    ATOMICDATA.CustAcctAffiliation caa
    , ATOMICDATA.CustomerGroup cg
    , ATOMICDATA.CustomerGroupType cgt
WHERE
    caa.CustGroupID = cg.CustGroupID
    AND cg.CustGroupTypeCode = cgt.CustGroupTypeCode
    AND cgt.Name = 'LoyaltySegDivision' --Constant for LoyaltySegDivision
;



-- 
-- VIEW: SLCUSTOMER.CustAcctAssoc 
--

REPLACE VIEW SLCUSTOMER.CustAcctAssoc AS LOCKING ROW FOR ACCESS
SELECT Cu.ParentCustAcctID, Cu.ChildCustAcctID, Cu.TransactionDate, BEGIN(Cu.ValidPeriod) ValidPeriodStartDt, END(Cu.ValidPeriod) ValidPeriodEndDt, Cu.CurrentInd, BEGIN(cu.RecordPeriod) RecordPeriodStartTs,END(cu.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.CustAcctAssoc Cu
;

-- 
-- VIEW: SLCUSTOMER.CustAcctChild 
--

REPLACE VIEW SLCUSTOMER.CustAcctChild AS LOCKING ROW FOR ACCESS
SELECT Cu.CustAcctID, Cu.ChildId, Cu.BirthYear, BEGIN(Cu.ValidPeriod) ValidPeriodStartDt, END(Cu.ValidPeriod) ValidPeriodEndDt, Cu.CurrentInd, BEGIN(cu.RecordPeriod) RecordPeriodStartTs,END(cu.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.CustAcctChild Cu
;

-- 
-- VIEW: SLCUSTOMER.CustAcctShareOfWallet 
--

REPLACE VIEW SLCUSTOMER.CustAcctShareOfWallet AS LOCKING ROW FOR ACCESS
SELECT Cu.CustAcctID, Cu.SowTypeCode, Cu.YearMonthNum, Cu.TotalPurchasingPowerAmount, Cu.PurchaseAmount, Cu.ShareOfWalletPercentage, Cu.ShareOfWalletCategory, Cu.PotentialAmount, BEGIN(Cu.ValidPeriod) ValidPeriodStartDt, END(cu.ValidPeriod) ValidPeriodEndDt, Cu.CurrentInd, BEGIN(cu.RecordPeriod) RecordPeriodStartTs,END(cu.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.CustAcctShareOfWallet Cu
;

-- 
-- VIEW: SLCUSTOMER.CustContactAge 
--
REPLACE VIEW SLCUSTOMER.CustContactAge AS LOCKING ROW FOR ACCESS --LOCKING ROW FOR ACCESS
SELECT Cu.CustContactId, Cu.CustContactSrcSysId, Cu.SrcCustContactId, Cu.GenderCd, Cu.BirthYearNum, Cu.BirthMonthNum, Cu.BirthDt, Cu.LangCd, Cu.CountryId, Cu.ValidPhoneNumInd, Cu.ValidEmailAddrInd, BEGIN(Cu.ValidPeriod) ValidPeriodStartDt, END(Cu.ValidPeriod) ValidPeriodEndDt, Cu.CurrentInd, BEGIN(cu.RecordPeriod) RecordPeriodStartTs,END(cu.RecordPeriod) RecordPeriodEndTs, Cu.MonthNameFI, CASE WHEN BirthDt = '1900-01-01' THEN 0.0 ELSE (DATE - BirthDt)/(365.25) END CurrentAge, CASE WHEN CurrentAge = 0.0 THEN 'NA    '     WHEN CurrentAge < 15 THEN '<15   ' WHEN CurrentAge >= 15 AND CurrentAge < 25 THEN '15-24' WHEN CurrentAge >= 25 AND CurrentAge < 35 THEN '25-34'WHEN CurrentAge >= 35 AND CurrentAge < 45 THEN '35-44'WHEN CurrentAge >= 45 AND CurrentAge < 55 THEN '45-54'WHEN CurrentAge >= 55 AND CurrentAge < 65 THEN '55-64'ELSE '>65   ' END AgeGroup
FROM ATOMICDATA.CustContact Cu
;

-- 
-- VIEW: SLCUSTOMER.CustomerGroup 
--

REPLACE VIEW SLCUSTOMER.CustomerGroup AS LOCKING ROW FOR ACCESS
SELECT cg.CustGroupID, cg.CustGroupTypeCode, cg.SrcCustGroupId, cg.Name, cg.Description, cg.GroupOwner, cg.GroupOwnerTypeCode, BEGIN(cg.ValidPeriod) ValidPeriodStartDt, END(cg.ValidPeriod) ValidPeriodEndDt, cg.CurrentInd, BEGIN(cg.RecordPeriod) RecordPeriodStartTs,END(cg.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.CustomerGroup cg
;

-- 
-- VIEW: SLCUSTOMER.CustomerGroupAssoc 
--

REPLACE VIEW SLCUSTOMER.CustomerGroupAssoc AS LOCKING ROW FOR ACCESS
SELECT cga.CustGroupID, cga.SubCustGroupId, BEGIN(cga.ValidPeriod) ValidPeriodStartDt, END(cga.ValidPeriod) ValidPeriodEndDt, cga.CurrentInd, BEGIN(cga.RecordPeriod) RecordPeriodStartTs,END(cga.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.CustomerGroupAssociation cga
;

-- 
-- VIEW: SLCUSTOMER.CustomerGroupType 
--

REPLACE VIEW SLCUSTOMER.CustomerGroupType AS LOCKING ROW FOR ACCESS
SELECT cgt.CustGroupTypeCode, cgt.Name, BEGIN(cgt.ValidPeriod) ValidPeriodStartDt, END(cgt.ValidPeriod) ValidPeriodEndDt, cgt.CurrentInd, BEGIN(cgt.RecordPeriod) RecordPeriodStartTs,END(cgt.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.CustomerGroupType cgt
;

-- 
-- VIEW: SLCUSTOMER.CustAcctSegmentInfo 
--

REPLACE VIEW SLCUSTOMER.CustAcctSegmentInfo AS
SELECT casi.YearMonthNum, casi.CustChainId, casi.CustAcctID, casi.CustClassSegment, casi.CustLANSEYSegment, casi.CustFoodstyleSegment, casi.CustPirkkaSegment, casi.CustLuomuSegment, Casi.CustClassSegmentCode, Casi.CustLANSEYSegmentCode, Casi.CustFoodstyleSegmentCode, Casi.CustPirkkaSegmentCode, Casi.CustLuomuSegmentCode
FROM AGGREGATEDATA.CustAcctSegmentInfo casi
;


-- 
-- VIEW: SLORGANIZATION.AssociatedBusinessUnitGrp 
--

REPLACE VIEW SLORGANIZATION.AssociatedBusinessUnitGrp AS LOCKING ROW FOR ACCESS
SELECT abug.BusinessUnitGroupFunctionID, abug.ParentBusinessUnitGroupLevelID, abug.ParentBusinessUnitGroupID, abug.ChildBusinessUnitGroupID, abug.EffectiveDate, abug.ExpirationDate, BEGIN(abug.ValidPeriod) ValidPeriodStartDt, END(abug.ValidPeriod) ValidPeriodEndDt, abug.CurrentInd, BEGIN(abug.RecordPeriod) RecordPeriodStartTs,END(abug.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.AssociatedBusinessUnitGrp abug
;

-- 
-- VIEW: SLORGANIZATION.BricConsumerCategorization 
--

REPLACE VIEW SLORGANIZATION.BricConsumerCategorization AS LOCKING ROW FOR ACCESS
SELECT bric.MapSquareId, bric.CityId, bric.TerritoryId, bric.ISOCountryCode, bric.ResidentialAreaCategory, bric.EducationLevelCategory, bric.HomeOwnershipCategory, bric.HousingCategory, bric.PaymentDefaultProbabilityCtgy, bric.PurchasingPowerAmount, bric.LifestyleYoungCouplesNoKidsQty, bric.LifestyleFamiliesQty, bric.LifestyleAdultHouseholdsQty, bric.LifestyleSeniorHouseholdsQty, bric.ResidentialAreaRuralQuantity, bric.ResidentialAreaSuburbsQuantity, bric.ResidentialAreaCitiesQuantity, bric.ResidentialAreaMajorCitiesQty, bric.ResidentialAreaCapitalAreaQty, bric.EducationBasicQuantity, bric.EducationMiddleQuantity, bric.EducationHighQuantity, bric.HousingRentalQuantity, bric.HousingOwnedQuantity, bric.HousingDetachedHouseQuantity, bric.HousingApartmentQuantity, bric.PaymentDefaultQuantity, bric.PopulationDensity, bric.PurchasingPowerProportionedAmt, bric.PurchasingPowerCategory, bric.PurchasingPowerSubcategory, bric.LifestyleCategory, bric.LifestyleSubcategory, bric.HouseholdQuantity, bric.PopulationQuantity, BEGIN(bric.ValidPeriod) ValidPeriodStartDt, END(bric.ValidPeriod) ValidPeriodEndDt, bric.CurrentInd, BEGIN(bric.RecordPeriod) RecordPeriodStartTs,END(bric.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.BricConsumerCategorization bric
;

-- 
-- VIEW: SLORGANIZATION.BusinessUnitGroup 
--

REPLACE VIEW SLORGANIZATION.BusinessUnitGroup AS LOCKING ROW FOR ACCESS
SELECT bungrp.BusinessUnitGroupID, MAX(CASE WHEN languageId = 1 THEN  bugrpnm.BusinessUnitGroupName ELSE NULL END) BusinessUnitGroupName, 
MAX(CASE WHEN languageId = 2 THEN  bugrpnm.BusinessUnitGroupName ELSE NULL END)BusinessUnitGroupNameFI, 
MAX(CASE WHEN languageId = 3 THEN  bugrpnm.BusinessUnitGroupName ELSE NULL END) BusinessUnitGroupNameSE, 
MAX(CASE WHEN languageId = 4 THEN  bugrpnm.BusinessUnitGroupName ELSE NULL END) BusinessUnitGroupNameRU, 
MAX(CASE WHEN languageId = 5 THEN  bugrpnm.BusinessUnitGroupName ELSE NULL END) BusinessUnitGroupNameEN,
bungrp.SrcBusinessUnitGroupID, bungrp.Logo, bungrp.TypeCode, BEGIN(bungrp.ValidPeriod) ValidPeriodStartDt, END(bungrp.ValidPeriod) ValidPeriodEndDt, bungrp.CurrentInd, BEGIN(bungrp.RecordPeriod) RecordPeriodStartTs,END(bungrp.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.BusinessUnitGroup bungrp,
ATOMICDATA.BusinessUnitGroupName bugrpnm
WHERE bungrp.BusinessUnitGroupID = bugrpnm.BusinessUnitGroupID
GROUP BY bungrp.BusinessUnitGroupID, bungrp.SrcBusinessUnitGroupID, bungrp.Logo, bungrp.TypeCode, bungrp.ValidPeriod, bungrp.CurrentInd, bungrp.RecordPeriod
;

-- 
-- VIEW: SLORGANIZATION.BusinessUnitLoyaltyProgram 
--

REPLACE VIEW SLORGANIZATION.BusinessUnitLoyaltyProgram AS LOCKING ROW FOR ACCESS
SELECT bulp.BusinessUnitID, bulp.InformationText, bulp.IsIncluded, bulp.SortCategory, bulp.IsSiteShowed, BEGIN(bulp.ValidPeriod) ValidPeriodStartDt, END(bulp.ValidPeriod) ValidPeriodEndDt, bulp.CurrentInd, BEGIN(bulp.RecordPeriod) RecordPeriodStartTs,END(bulp.RecordPeriod) RecordPeriodEndTs, bulp.PlussaMbrAppElig
FROM ATOMICDATA.BusinessUnitLoyaltyProgram bulp
;

-- 
-- VIEW: SLORGANIZATION.BusinessUnitSrcIdentifcn 
--

REPLACE VIEW SLORGANIZATION.BusinessUnitSrcIdentifcn AS LOCKING ROW FOR ACCESS
SELECT busi.BusinessUnitID, busi.IdentificationType, busi.SrcBusinessId, BEGIN(busi.ValidPeriod) ValidPeriodStartDt, END(busi.ValidPeriod) ValidPeriodEndDt, busi.CurrentInd, BEGIN(busi.RecordPeriod) RecordPeriodStartTs,END(busi.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.BusinessUnitSrcIdentifcn busi
;

-- 
-- VIEW: SLORGANIZATION.OperPtyLyltyPrgmAgmt 
--

REPLACE VIEW SLORGANIZATION.OperPtyLyltyPrgmAgmt AS LOCKING ROW FOR ACCESS
SELECT oplpa.BusinessUnitID, oplpa.OperationalPartyID, oplpa.TypeCode, oplpa.AccountNumber, oplpa.BankAccountNumber, oplpa.BankAccountTypeCode, oplpa.RetailerName, oplpa.StatementCode, oplpa.ReferenceNumber, oplpa.InvoicingNumber, oplpa.ExternalInvoicingNumber, oplpa.LedgerNumber, oplpa.StartDt, oplpa.EndDt, BEGIN(oplpa.ValidPeriod) ValidPeriodStartDt, END(oplpa.ValidPeriod) ValidPeriodEndDt, oplpa.CurrentInd, BEGIN(oplpa.RecordPeriod) RecordPeriodStartTs,END(oplpa.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.OperPtyLyltyPrgmAgmt oplpa
;

-- 
-- VIEW: SLORGANIZATION.OperPtyLyltyPrgmAgmtApndx 
--

REPLACE VIEW SLORGANIZATION.OperPtyLyltyPrgmAgmtApndx AS LOCKING ROW FOR ACCESS
SELECT Op.BusinessUnitID, Op.OperationalPartyID, Op.TypeCode, Op.Agreementid, Op.StartDt, Op.EndDt, BEGIN(Op.ValidPeriod) ValidPeriodStartDt, END(Op.ValidPeriod) ValidPeriodEndDt, Op.CurrentInd, BEGIN(op.RecordPeriod) RecordPeriodStartTs,END(op.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.OperPtyLyltyPrgmAgmtApndx Op
;

-- 
-- VIEW: SLORGANIZATION.Retailer 
--

REPLACE VIEW SLORGANIZATION.Retailer AS LOCKING ROW FOR ACCESS
SELECT rtlr.RetailerId, rtlr.RetailerName, rtlr.CurrentInd, BEGIN(rtlr.RecordPeriod) RecordPeriodStartTs,END(rtlr.RecordPeriod) RecordPeriodEndTs
FROM ATOMICDATA.Retailer rtlr
;

-- 
-- VIEW: SLCI.CustAcctOrgAffMainChain 
--

REPLACE VIEW SLCI.CustAcctOrgAffMainChain AS LOCKING ROW FOR ACCESS
SELECT Cu.CustAcctID, Cu.CustAcctAffiliationTypeCode, Cu.OrgLevelId, Cu.OrgLevelTypeCode, Cu.isNew, Cu.ValidPeriodStartDt, Cu.ValidPeriodEndDt
FROM SLCI.CustAcctOrgAffiliation Cu
WHERE CustAcctAffiliationTypeCode = 1 -- This is constant for Main Org level
AND OrgLevelTypeCode = 2 -- This is constant for Chain
;

-- 
-- VIEW: SLCI.CustAcctOrgAffMainSite 
--

REPLACE VIEW SLCI.CustAcctOrgAffMainSite AS LOCKING ROW FOR ACCESS
SELECT Cu.CustAcctID, Cu.CustAcctAffiliationTypeCode, Cu.OrgLevelId, Cu.OrgLevelTypeCode,  Cu.isNew, Cu.ValidPeriodStartDt, Cu.ValidPeriodEndDt
FROM SLCI.CustAcctOrgAffiliation Cu
WHERE CustAcctAffiliationTypeCode = 1 -- This is constant for Main Org level
AND OrgLevelTypeCode = 1 -- This is constant for Main Site
;

-- 
-- VIEW: SLCI.CustAcctOrgAffRautaContracts 
--

REPLACE VIEW SLCI.CustAcctOrgAffRautaContracts AS LOCKING ROW FOR ACCESS
SELECT Cu.CustAcctID, Cu.CustAcctAffiliationTypeCode, Cu.OrgLevelId, Cu.OrgLevelTypeCode, Cu.isNew, Cu.ValidPeriodStartDt, Cu.ValidPeriodEndDt
FROM SLCI.CustAcctOrgAffiliation Cu
WHERE CustAcctAffiliationTypeCode = 2 -- This is constant for Rauta Contracts
AND OrgLevelTypeCode = 1 -- This is constant for Business Unit under contracts
;

-- 
-- VIEW: SLCI.ReceiptLoyaltyBreakdownBU 
--


REPLACE VIEW SLCI.ReceiptLoyaltyBreakdownBU AS
SELECT Re.YearMonthNum, Re.BusinessUnitID, Re.CustChainID, Re.CustAcctID, Re.CustAcctCardNum, Re.CustClassSegment, Re.CustLANSEYSegment, Re.CustFoodstyleSegment, Re.CustPirkkaSegment, Re.CustLuomuSegment, Re.CustClassSegmentCode, Re.CustLANSEYSegmentCode, Re.CustFoodstyleSegmentCode, Re.CustPirkkaSegmentCode, Re.CustLuomuSegmentCode, Re.CustGroup, Re.CustStatus, Re.CustGroupCode, Re.MerchHierGroupID, Re.POSDepartmentId, Re.MainPurchaseBUFlag, Re.SalesAmt
FROM AGGREGATEDATA.ReceiptLoyaltyBreakdownBU Re
INNER JOIN 
SL_SECDB.SECURITYASSOCIATION S 
ON 
CAST(S.COLUMNVALUE AS INTEGER)=CAST(Re.BusinessUnitID AS INTEGER) 
 AND UPPER(JOINCOLUMN)=UPPER('BusinessUnitID');
;


-- 
-- VIEW: SLCI.ReceiptLoyaltyBreakdownChain 
--


REPLACE VIEW SLCI.ReceiptLoyaltyBreakdownChain AS
SELECT Re.YearMonthNum, Re.CustChainID, Re.CustAcctID, Re.CustAcctCardNum, Re.CustClassSegment, Re.CustLANSEYSegment, Re.CustFoodstyleSegment, Re.CustPirkkaSegment, Re.CustLuomuSegment, Re.CustClassSegmentCode, Re.CustLANSEYSegmentCode, Re.CustFoodstyleSegmentCode, Re.CustPirkkaSegmentCode, Re.CustLuomuSegmentCode, Re.RautaContractCategory, Re.RautaContractCategoryCode, Re.RautaContractCategoryStatus, Re.MerchHierGroupID, Re.POSDepartmentId, Re.MainPurchaseChainFlag, Re.SalesAmt
FROM AGGREGATEDATA.ReceiptLoyaltyBreakdownChain Re

;



-- 
-- VIEW: SLCI.ReceiptLoyaltyRowBU 
--

REPLACE VIEW SLCI.ReceiptLoyaltyRowBU AS
SELECT Re.YearMonthNum, Re.CustChainId, Re.BusinessUnitID, Re.CustAcctID, Re.CustAcctCardNum, Re.CustStatus, Re.CustGroup, Re.CustClassSegment, Re.CustLANSEYSegment, Re.CustFoodstyleSegment, Re.CustPirkkaSegment, Re.CustLuomuSegment, Re.CustClassSegmentCode, Re.CustLANSEYSegmentCode, Re.CustFoodstyleSegmentCode, Re.CustPirkkaSegmentCode, Re.CustLuomuSegmentCode, Re.CustGroupCode, Re.SalesAmt, Re.ProfitExclVAT, Re.VAT, Re.LoyaltyRewardSalesAmt, Re.LoyaltyRewardDiscountAmt, Re.LoyaltyRewardDirectRedemption, Re.LoyaltyRewardVIPDiscountAmt, Re.LoyaltyRewardDiscountQty, Re.LoyaltyRewardDirectRedemptnQty, Re.MainPurchaseBUFlag, Re.NoOfVisits
FROM AGGREGATEDATA.ReceiptLoyaltyRowBU Re
INNER JOIN 
SL_SECDB.SECURITYASSOCIATION S 
ON 
CAST(S.COLUMNVALUE AS INTEGER)=CAST(Re.BusinessUnitID AS INTEGER) 
 AND UPPER(S.JOINCOLUMN)=UPPER('BusinessUnitID');
;


-- 
-- VIEW: SLCI.ReceiptLoyaltyRowChain 
--

REPLACE VIEW SLCI.ReceiptLoyaltyRowChain AS
SELECT Re.YearMonthNum, Re.CustChainID, Re.CustAcctID, Re.CustAcctCardNum, Re.RautaContractCategory, Re.RautaContractCategoryStatus, Re.RautaContractCategoryCode, Re.CustClassSegment, Re.CustLANSEYSegment, Re.CustFoodstyleSegment, Re.CustPirkkaSegment, Re.CustLuomuSegment, Re.CustClassSegmentCode, Re.CustLANSEYSegmentCode, Re.CustFoodstyleSegmentCode, Re.CustPirkkaSegmentCode, Re.CustLuomuSegmentCode, Re.SalesAmt, Re.ProfitExclVAT, Re.VAT, Re.LoyaltyRewardSalesAmt, Re.LoyaltyRewardDiscountAmt, Re.LoyaltyRewardDirectRedemption, Re.LoyaltyRewardVIPDiscountAmt, Re.LoyaltyRewardDiscountQty, Re.LoyaltyRewardDirectRedemptnQty, Re.MainPurchaseChainFlag, Re.NoOfVisits
FROM AGGREGATEDATA.ReceiptLoyaltyRowChain Re
;


-- 
-- VIEW: SLORGANIZATION.CustAcctConsumerCtgrzn 
--
REPLACE VIEW SLORGANIZATION.CustAcctConsumerCtgrzn AS
SELECT cacc.MapSquareId, cacc.CustAcctID, cacc.CurrentInd, BEGIN(cacc.RecordPeriod) AS RecordPeriodStartTs, END(cacc.RecordPeriod) AS RecordPeriodEndTs
FROM ATOMICDATA.CustAcctConsumerCtgrzn cacc
;


-- 
-- VIEW: SLCOMMON.TerritoryCity 
--

REPLACE VIEW SLCOMMON.TerritoryCity AS
SELECT tc.TerritoryId, tc.CityId, tc.CountryId, tc.TerritoryTypeCodeId, BEGIN(RecordPeriod) AS RecordPeriodStartTs, END(RecordPeriod) AS RecordPeriodEndTs, tc.CurrentInd
FROM ATOMICDATA.TerritoryCity tc
;

-- 
-- VIEW: SLCI.ReceiptLoyaltyRowSite 
--

REPLACE VIEW SLCI.ReceiptLoyaltyRowSite AS
SELECT Re.YearMonthNum, Re.CustChainID, Re.SiteID, Re.CustAcctID, Re.CustAcctCardNum, Re.RautaContractCategory, Re.RautaContractCategoryStatus, Re.RautaContractCategoryCode, Re.CustClassSegment, Re.CustLANSEYSegment, Re.CustFoodstyleSegment, Re.CustPirkkaSegment, Re.CustLuomuSegment, Re.CustClassSegmentCode, Re.CustLANSEYSegmentCode, Re.CustFoodstyleSegmentCode, Re.CustPirkkaSegmentCode, Re.CustLuomuSegmentCode, Re.SalesAmt, Re.ProfitExclVAT, Re.VAT, Re.LoyaltyRewardSalesAmt, Re.LoyaltyRewardDiscountAmt, Re.LoyaltyRewardDirectRedemption, Re.LoyaltyRewardVIPDiscountAmt, Re.LoyaltyRewardDiscountQty, Re.LoyaltyRewardDirectRedemptnQty, Re.NoOfVisits, Re.LoadDttm, Re.ModuleID
FROM AGGREGATEDATA.ReceiptLoyaltyRowSite Re



-- 
-- VIEW: SLCI.ReceiptLoyaltyRowChainUnit 
--

CREATE VIEW SLCI.ReceiptLoyaltyRowChainUnit AS
SELECT r.YearMonthNum, r.ChainUnitID, r.CustAcctID, r.CustAcctCardNum, r.RautaContractCategoryCode, r.RautaContractCategoryStatus, r.RautaContractCategory, r.CustClassSegment, r.CustLANSEYSegment, r.CustFoodstyleSegment, r.CustPirkkaSegment, r.CustLuomuSegment, r.CustClassSegmentCode, r.CustLANSEYSegmentCode, r.CustFoodstyleSegmentCode, r.CustPirkkaSegmentCode, r.CustLuomuSegmentCode, r.SalesAmt, r.ProfitExclVAT, r.VAT, r.LoyaltyRewardSalesAmt, r.LoyaltyRewardDiscountAmt, r.LoyaltyRewardDirectRedemption, r.LoyaltyRewardVIPDiscountAmt, r.LoyaltyRewardDiscountQty, r.LoyaltyRewardDirectRedemptnQty, r.NoOfVisits
FROM AGGREGATEDATA.ReceiptLoyaltyRowChainUnit r
;





-- 
-- VIEW: SLCUSTOMER.CustAcctAffiliationAttr 
--

REPLACE VIEW SLCUSTOMER.CustAcctAffiliationAttr AS
SELECT caaa.CustGroupID, caaa.CustAcctID, caaa.OrgLevelId, caaa.OrgLevelTypeCode, caaa.YearMonthNum, caaa.CustAcctAffilAttrName, caaa.CustAcctAffilAttrValue, caaa.CurrentInd, BEGIN(ValidPeriod) AS ValidPeriodStartDt, END(ValidPeriod) AS ValidPeriodEndDt, BEGIN(RecordPeriod) AS RecordPeriodStartTs, END(RecordPeriod) AS RecordPeriodEndTs
FROM ATOMICDATA.CustAcctAffiliationAttr caaa
;



-- 
-- VIEW: SLCUSTOMER.CustAcctChainUnitSegmentInfo 
--

REPLACE VIEW SLCUSTOMER.CustAcctChainUnitSegmentInfo AS
SELECT cacusi.YearMonthNum, cacusi.CustAcctID, cacusi.ChainUnitID, cacusi.CustClassSegment, cacusi.CustClassSegmentCode, cacusi.MainPurchaseBU, cacusi.LoyaltySegmentChainUnitFlag
FROM AGGREGATEDATA.CustAcctChainUnitSegmentInfo cacusi
;




-- 
-- VIEW: SLCUSTOMER.CustAcctDivisionSegmentInfo 
--

REPLACE VIEW SLCUSTOMER.CustAcctDivisionSegmentInfo AS
SELECT cadsi.YearMonthNum, cadsi.CustAcctID, cadsi.DivisionID, cadsi.CustClassSegmentCode, cadsi.CustClassSegment, cadsi.LoyaltySegmentDivFlag, cadsi.MainPurchaseChain, cadsi.FirstSuppChain, cadsi.SecondSuppChain, cadsi.MainPurchaseBU, cadsi.FirstSuppBU, cadsi.SecondSuppBU
FROM AGGREGATEDATA.CustAcctDivisionSegmentInfo cadsi
;

-- 
-- VIEW: SLPRODUCT.MerchGroupHierarchyPLS 
--

REPLACE VIEW SLPRODUCT.MerchGroupHierarchyPLS AS LOCKING ROW FOR ACCESS
SELECT

    mh.MerchHierId
    , mh.DescFI
    , l1.MerchHierGroupId AS L1_MerchHierGroupId
    , l1.DescFI AS L1_DescFI
    , l1.MerchHierGroupCd AS L1_MerchHierGroupCd 
    , l2.MerchHierGroupId AS L2_MerchHierGroupId
    , l2.DescFI AS L2_DescFI
    , l2.MerchHierGroupCd AS L2_MerchHierGroupCd
    , l3.MerchHierGroupId AS L3_MerchHierGroupId
    , l3.DescFI AS L3_DescFI
    , l3.MerchHierGroupCd AS L3_MerchHierGroupCd
    , l4.MerchHierGroupId AS L4_MerchHierGroupId
    , l4.DescFI AS L4_DescFI
    , l4.MerchHierGroupCd AS L4_MerchHierGroupCd
    , l5.MerchHierGroupId AS L5_MerchHierGroupId
    , l5.DescFI AS L5_DescFI
    , l5.MerchHierGroupCd AS L5_MerchHierGroupCd
    , CASE WHEN L5.LevelNum IS NOT NULL THEN L5.LevelNum
           WHEN L4.LevelNum IS NOT NULL THEN L4.LevelNum
           WHEN L3.LevelNum IS NOT NULL THEN L3.LevelNum
           WHEN L2.LevelNum IS NOT NULL THEN L2.LevelNum
           WHEN L1.LevelNum IS NOT NULL THEN L1.LevelNum
       END AS Max_LevelNum
    , CASE WHEN L5_MerchHierGroupId IS NOT NULL THEN L5_MerchHierGroupId
           WHEN L4_MerchHierGroupId IS NOT NULL THEN L4_MerchHierGroupId
           WHEN L3_MerchHierGroupId IS NOT NULL THEN L3_MerchHierGroupId
           WHEN L2_MerchHierGroupId IS NOT NULL THEN L2_MerchHierGroupId
           WHEN L1_MerchHierGroupId IS NOT NULL THEN L1_MerchHierGroupId
       END AS MerchHierGroupId
    , CASE WHEN BEGIN(l1.RecordPeriod) >= BEGIN(l2.RecordPeriod) AND BEGIN(l1.RecordPeriod) >= BEGIN(l3.RecordPeriod) AND BEGIN(l1.RecordPeriod) >= BEGIN(l4.RecordPeriod) AND BEGIN(l1.RecordPeriod) >= BEGIN(l5.RecordPeriod) THEN BEGIN(l1.RecordPeriod)
            WHEN BEGIN(l2.RecordPeriod) >= BEGIN(l1.RecordPeriod) AND BEGIN(l2.RecordPeriod) >= BEGIN(l3.RecordPeriod) AND BEGIN(l2.RecordPeriod) >= BEGIN(l4.RecordPeriod) AND BEGIN(l2.RecordPeriod) >= BEGIN(l5.RecordPeriod) THEN BEGIN(l2.RecordPeriod)
            WHEN BEGIN(l3.RecordPeriod) >= BEGIN(l1.RecordPeriod) AND BEGIN(l3.RecordPeriod) >= BEGIN(l2.RecordPeriod) AND BEGIN(l3.RecordPeriod) >= BEGIN(l4.RecordPeriod) AND BEGIN(l3.RecordPeriod) >= BEGIN(l5.RecordPeriod) THEN BEGIN(l3.RecordPeriod)
            WHEN BEGIN(l4.RecordPeriod) >= BEGIN(l1.RecordPeriod) AND BEGIN(l4.RecordPeriod) >= BEGIN(l2.RecordPeriod) AND BEGIN(l4.RecordPeriod) >= BEGIN(l3.RecordPeriod) AND BEGIN(l4.RecordPeriod) >= BEGIN(l5.RecordPeriod) THEN BEGIN(l4.RecordPeriod)
      ELSE BEGIN(l5.RecordPeriod) END AS RecordPeriodStartTs
    , CASE WHEN END(l1.RecordPeriod) <= END(l2.RecordPeriod) AND END(l1.RecordPeriod) <= END(l3.RecordPeriod) AND END(l1.RecordPeriod) <= END(l4.RecordPeriod) AND END(l1.RecordPeriod) <= END(l5.RecordPeriod) THEN END(l1.RecordPeriod)
            WHEN END(l2.RecordPeriod) <= END(l1.RecordPeriod) AND END(l2.RecordPeriod) <= END(l3.RecordPeriod) AND END(l2.RecordPeriod) <= END(l4.RecordPeriod) AND END(l2.RecordPeriod) <= END(l5.RecordPeriod) THEN END(l2.RecordPeriod)
            WHEN END(l3.RecordPeriod) <= END(l1.RecordPeriod) AND END(l3.RecordPeriod) <= END(l2.RecordPeriod) AND END(l3.RecordPeriod) <= END(l4.RecordPeriod) AND END(l3.RecordPeriod) <= END(l5.RecordPeriod) THEN END(l3.RecordPeriod)
            WHEN END(l4.RecordPeriod) <= END(l1.RecordPeriod) AND END(l4.RecordPeriod) <= END(l2.RecordPeriod) AND END(l4.RecordPeriod) <= END(l3.RecordPeriod) AND END(l4.RecordPeriod) <= END(l5.RecordPeriod) THEN END(l4.RecordPeriod)
      ELSE END(l5.RecordPeriod) END AS RecordPeriodEndTs
    , CASE WHEN l1.CurrentInd = 1
                AND l2.CurrentInd = 1
                AND l3.CurrentInd = 1
                AND l4.CurrentInd = 1
                AND l5.CurrentInd = 1
            THEN 1 ELSE 0 END AS CurrentInd   
FROM
    ATOMICDATA.MerchHierGroup l1
    INNER JOIN ATOMICDATA.MerchHier mh
    ON
    l1.MerchHierId = mh.MerchHierId
    --AND BEGIN(mh.RecordPeriod) <= END  (l1.RecordPeriod)
    --AND END  (mh.RecordPeriod) >= BEGIN(l1.RecordPeriod)
    LEFT OUTER JOIN ATOMICDATA.MerchHierGroup l2
    ON
    l2.ParentMerchHierGroupId = l1.MerchHierGroupId
    AND BEGIN(l2.RecordPeriod) <= END  (l1.RecordPeriod)
    AND END  (l2.RecordPeriod) >= BEGIN(l1.RecordPeriod)
    --AND BEGIN(l2.RecordPeriod) <= END  (mh.RecordPeriod)
    --AND END  (l2.RecordPeriod) >= BEGIN(mh.RecordPeriod)
    LEFT OUTER JOIN ATOMICDATA.MerchHierGroup l3
    ON
    l3.ParentMerchHierGroupId = l2.MerchHierGroupId
    AND BEGIN(l3.RecordPeriod) <= END  (l2.RecordPeriod)
    AND END  (l3.RecordPeriod) >= BEGIN(l2.RecordPeriod)
    AND BEGIN(l3.RecordPeriod) <= END  (l1.RecordPeriod)
    AND END  (l3.RecordPeriod) >= BEGIN(l1.RecordPeriod)
    --AND BEGIN(l3.RecordPeriod) <= END  (mh.RecordPeriod)
    --AND END  (l3.RecordPeriod) >= BEGIN(mh.RecordPeriod)
    LEFT OUTER JOIN ATOMICDATA.MerchHierGroup l4
    ON
    l4.ParentMerchHierGroupId = l3.MerchHierGroupId
    AND BEGIN(l4.RecordPeriod) <= END  (l3.RecordPeriod)
    AND END  (l4.RecordPeriod) >= BEGIN(l3.RecordPeriod)
    AND BEGIN(l4.RecordPeriod) <= END  (l2.RecordPeriod)
    AND END  (l4.RecordPeriod) >= BEGIN(l2.RecordPeriod)
    AND BEGIN(l4.RecordPeriod) <= END  (l1.RecordPeriod)
    AND END  (l4.RecordPeriod) >= BEGIN(l1.RecordPeriod)
    --AND BEGIN(l4.RecordPeriod) <= END  (mh.RecordPeriod)
    --AND END  (l4.RecordPeriod) >= BEGIN(mh.RecordPeriod)
    LEFT OUTER JOIN ATOMICDATA.MerchHierGroup l5
    ON
    l5.ParentMerchHierGroupId = l4.MerchHierGroupId
    AND BEGIN(l5.RecordPeriod) <= END  (l4.RecordPeriod)
    AND END  (l5.RecordPeriod) >= BEGIN(l4.RecordPeriod)
    AND BEGIN(l5.RecordPeriod) <= END  (l3.RecordPeriod)
    AND END  (l5.RecordPeriod) >= BEGIN(l3.RecordPeriod)
    AND BEGIN(l5.RecordPeriod) <= END  (l2.RecordPeriod)
    AND END  (l5.RecordPeriod) >= BEGIN(l2.RecordPeriod)
    AND BEGIN(l5.RecordPeriod) <= END  (l1.RecordPeriod)
    AND END  (l5.RecordPeriod) >= BEGIN(l1.RecordPeriod)
    --AND BEGIN(l5.RecordPeriod) <= END  (mh.RecordPeriod)
    --AND END  (l5.RecordPeriod) >= BEGIN(mh.RecordPeriod)
WHERE
    l1.levelnum = 1    
;





-------       ALTER TABLE SCRIPT  ----------------



ALTER TABLE atomicdata.OperationalParty
ADD RetailerId                INTEGER,
ADD LanguageID                CHAR(1)
;


ALTER TABLE atmoicdata.site
    ADD xCoordinate              VARCHAR(20),
    ADD yCoordinate              VARCHAR(20)
;


ALTER TABLE ATOMICDATA.RetailTrn
	ADD TrnNum BIGINT
;

