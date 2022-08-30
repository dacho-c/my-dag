def sql_ET_machine_cust(st):
    return """select (A.UR_MODEL || A.UR_ID) as item_id, A.UR_MODEL, A.UR_ID, A.UR_CUS, A.UR_REMARK, A.UR_SPECIAL_SERVICE, A.UR_SPEC, A.UR_NAME, BUR.BR_SNAME as MC_BRANCH, BUR.BR_REP_MEN as MC_BRANCH_REGION, A.UR_DATE, A.UR_FDJ_MODEL, A.UR_FDJ_CODE, A.UR_ZC_MODEL, A.UR_ZC_CODE, 
A.UR_SMR, A.UR_DELIVER_DATE, A.UR_SMR_SOURCE, A.UR_LOCAL_DATE, A.UR_CREATEDTIME, A.UR_SHIPPING_DATE, A.UR_GCPS_DATE, A.UR_USED_DELIVER_DATE, A.UR_SERVICE_TYPE, 
A.UR_SALES_MAN, ke.emp_name as SALES_NAME, A.UR_GPS_ADDRESS, BGPS.BR_SNAME as MC_GPS_BRANCH, BGPS.BR_REP_MEN as MC_GPS_BRANCH_REGION,
B.CUS_ID, B.CUS_NAME, B.CUS_SNAME, B.CUS_ENAME, B.BR_SNAME as cust_branch, B.BR_REP_MEN as cust_region, B.CA_ADDRESS, B.PROVINCE, B.CITY, B.TYPE, B.TRADE, B.CUS_CREATEDTIME, A.MC_FY, A.YEARMONTH, B.CUST_FY,
0 as mc_bfp, 0 as inv_bfp, '' as cus_group 
from (select A.UR_MODEL, A.UR_ID, A.UR_CUS, A.UR_REMARK, A.UR_SPECIAL_SERVICE, UR_SPEC, UR_NAME, UR_ORG_ID, UR_DATE, UR_FDJ_MODEL, UR_FDJ_CODE, UR_ZC_MODEL, UR_ZC_CODE, UR_SMR,
UR_DELIVER_DATE, UR_SMR_SOURCE, UR_LOCAL_DATE, UR_CREATEDTIME, UR_SHIPPING_DATE, UR_GCPS_DATE, UR_USED_DELIVER_DATE, UR_SERVICE_TYPE, UR_SALES_MAN, UR_GPS_ADDRESS, UR_GPS_BRANCH,
EXTRACT(YEAR from A.UR_DELIVER_DATE) || (case when EXTRACT(month from A.UR_DELIVER_DATE) < 10 then '0' || EXTRACT(month from A.UR_DELIVER_DATE)::text else EXTRACT(month from A.UR_DELIVER_DATE)::text end) as YEARMONTH,
(case when EXTRACT(month from A.UR_DELIVER_DATE) < 4 then EXTRACT(YEAR from A.UR_DELIVER_DATE) - 1 else EXTRACT(YEAR from A.UR_DELIVER_DATE) end) MC_FY 
from kp_machine A where UR_MACHINE_CATEGORY = '1' and UR_STATUS = '1'""" + st + """) A
join (select C.CUS_ID, C.CUS_NAME, C.CUS_SNAME, C.CUS_ENAME, BR.BR_SNAME, BR.BR_REP_MEN, CA.CA_ADDRESS, P.ID_NAME as Province, B.ID_NAME as City, TY.ID_NAME as TYPE, TR.ID_NAME as TRADE, C.CUS_CREATEDTIME,
(case when EXTRACT(month from C.CUS_CREATEDTIME) < 4 then EXTRACT(YEAR from C.CUS_CREATEDTIME) - 1 else EXTRACT(YEAR from C.CUS_CREATEDTIME) end) CUST_FY
from kp_customer C
join kp_branch BR on (C.CUS_BRANCH = BR.BR_ID)
left outer join (select * from kp_idbooks ki where ID_ATTRIBUTE = 10) TR on (C.CUS_TRADE = TR.ID_CODE)
left outer join (select * from kp_idbooks ki where ID_ATTRIBUTE = 12) TY on (C.CUS_TYPE = TY.ID_CODE)
left outer join (select b.ID_CODE, b.ID_NAME, b.ID_TAG2 from kp_idbooks b where ID_ATTRIBUTE = 6) B on (C.CUS_CITY = B.ID_CODE)
left outer join (select b.ID_CODE, b.ID_NAME, b.ID_TAG2 from kp_idbooks b where ID_ATTRIBUTE = 5) P on (C.CUS_PROVINCE = P.ID_CODE)
left outer join (select * from kp_customer_address kca where CA_ADDR_ID = '0001') CA on(C.CUS_ID = CA.CA_CUS_ID)
) B on(A.UR_CUS = B.CUS_ID)
left outer join kp_branch BUR on (A.UR_ORG_ID = BUR.BR_ID)
left outer join kp_branch BGPS on (A.UR_GPS_BRANCH = BGPS.BR_ID)
left outer join kp_employee ke on (A.UR_SALES_MAN = ke.emp_id)"""

def sql_ET_machine():
    return """select km.UR_MODEL, km.UR_ID, km.UR_CUS, km.UR_DELIVER_DATE,
(case when EXTRACT(month from km.UR_DELIVER_DATE) < 4 then EXTRACT(YEAR from km.UR_DELIVER_DATE) - 1 else EXTRACT(YEAR from km.UR_DELIVER_DATE) end) mcfy 
from kp_machine km where km.UR_MACHINE_CATEGORY = '1' and km.UR_STATUS = '1'"""

def sql_ET_invoice():
    return """select kih.pih_cus_id,
(case when EXTRACT(month from kih.pih_inv_date) < 4 then EXTRACT(YEAR from kih.pih_inv_date) - 1 else EXTRACT(YEAR from kih.pih_inv_date) end) invfy 
from kp_invoice_head kih"""