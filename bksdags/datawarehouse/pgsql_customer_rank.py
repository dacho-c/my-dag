def sql_ET_cust_rank(st):
    return """select A.PROVINCE, A.cus_province__c as province_sf, A.CUS_ID, A.CUS_NAME, A.CUS_ENAME, A.BRANCH, A.REGION as BRANCH_REGION, A.major_brand__c as major_brand, A.customer_type__c as TYPE_SF, A.region_by_user__c as SALES_REGION, A.sales_name as SALES_PERSON, A.department, A.BUSINESS, A.MC as TOTAL_MC, A.KOM as KOMTRAX, A.NKOM as NON_KOMTRAX,
COALESCE(A.mc19,0) as mc_22,
COALESCE(A.mc18,0) as mc_21,
COALESCE(A.mc17,0) as mc_20,
COALESCE(A.mc16,0) as mc_19,
COALESCE(A.mc15,0) as mc_18,
COALESCE(A.mctotal,0) as MC_TOTAL,
(A.AMT19 + A.SAMT19) as amount_22,
(A.AMT18 + A.SAMT18) as amount_21, 
(A.AMT17 + A.SAMT17) as amount_20,
(A.AMT16 + A.SAMT16) as amount_19,
(A.AMT15 + A.SAMT15) as amount_18,
A.STAMT as Service_amount, A.TAMT as part_amount,
A.TTAMT as total_amount
from (select C.CUS_ID, C.CUS_OLDCODE, C.CUS_NAME, C.CUS_ENAME, B.BR_SNAME as branch, B.BR_REP_MEN as region, P.ID_NAME as province, T.ID_NAME as Type, T1.ID_NAME as Business, COALESCE(U.MC,0) as mc, COALESCE(U.kom,0) as kom, COALESCE(U.nkom,0) as nkom,
COALESCE(part.AMT19,0) as amt19, COALESCE(sA.amt19,0) as samt19, 
COALESCE(part.AMT18,0) as amt18, COALESCE(sA.amt18,0) as samt18, 
COALESCE(part.AMT17,0) as amt17, COALESCE(sA.amt17,0) as samt17, 
COALESCE(part.AMT16,0) as amt16, COALESCE(sA.amt16,0) as samt16, 
COALESCE(part.AMT15,0) as amt15, COALESCE(sA.amt15,0) as samt15, 
COALESCE(part.TAMT,0) as tamt, COALESCE(sA.tamt,0) as stamt, (COALESCE(part.TAMT,0) + COALESCE(sA.tamt,0)) as ttamt,
mc15, mc16, mc17, mc18, mc19, mctotal, sB.customer_type__c, sB.region_by_user__c, case when sB.sales_name = 'Admin Mintel' then null else sB.sales_name end sales_name, sB.cus_province__c, sB.major_brand__c, sB.department_by_user__c as department 
from kp_customer C 
join kp_branch B on(B.BR_ID = C.CUS_BRANCH)
join (select ID_CODE, ID_NAME, ID_TAG2 from kp_idbooks ki where ID_ATTRIBUTE = 5) P on(P.ID_CODE = C.CUS_PROVINCE)
left outer join (select ID_CODE, ID_NAME from kp_idbooks ki where ID_ATTRIBUTE = 12) T on(T.ID_CODE = C.CUS_TYPE)
left outer join (select ID_CODE, ID_NAME from kp_idbooks ki where ID_ATTRIBUTE = 10) T1 on(T1.ID_CODE = C.CUS_TRADE)
left outer join (select A.UR_CUS, sum(A.KOM) as kom, sum(A.NKOM) as nkom, sum(A.MC) as mc from (select km.UR_CUS, case when km.UR_SMR_SOURCE = 'KOMTRAX' then 1 else 0 end kom, 
case when km.UR_SMR_SOURCE = 'KOMTRAX' then 0 else 1 end nkom, 1 as mc from kp_machine km where km.ur_machine_category = '1') A group by A.UR_CUS) U on(C.CUS_ID = U.UR_CUS)
-- MC last Delivery
left outer join (select A.UR_CUS, sum(A.MC15) as mc15, sum(A.MC16) as mc16, sum(A.MC17) as mc17, sum(A.MC18) as mc18, sum(A.MC19) as mc19, sum(A.MC) as mctotal
from (select A.UR_CUS, 
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 5 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 end) then MC else 0 end mc15,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 3 end) then MC else 0 end mc16,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 3 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 2 end) then MC else 0 end mc17,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 2 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 1 end) then MC else 0 end mc18,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 1 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) end) then MC else 0 end mc19, MC
from (select A.UR_CUS, case when EXTRACT(MONTH FROM A.UR_DELIVER_DATE) < 4 then EXTRACT(YEAR FROM A.UR_DELIVER_DATE) - 1 else EXTRACT(YEAR FROM A.UR_DELIVER_DATE) end Y, 1 as MC
from kp_machine A where A.ur_machine_category = '1' and UR_STATUS <> '9' and A.UR_DELIVER_DATE >= ((case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then 
EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 5 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 end)::varchar(255) || '-04-01')::date) A) A group by A.UR_CUS) M on (C.CUS_ID = M.UR_CUS)
-- End MC
-- Part Amount
left outer join (select PIH_INVOICED_CUS_ID, sum(AMOUNT15) as amt15, sum(AMOUNT16) as amt16,
sum(AMOUNT17) as amt17, sum(AMOUNT18) as amt18, sum(AMOUNT19) as amt19, sum(amount) as Tamt
from (select PIH_INVOICED_CUS_ID,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 5 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 end) then AMOUNT else 0 end amount15,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 3 end) then AMOUNT else 0 end amount16,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 3 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 2 end) then AMOUNT else 0 end amount17,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 2 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 1 end) then AMOUNT else 0 end amount18,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 1 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) end) then AMOUNT else 0 end amount19,
AMOUNT
from (select A.PIH_INVOICED_CUS_ID, A.Y, sum(A.PID_BASIC_TTLAMOUNT) as AMOUNT
from (select A.PIH_ACCOUNT_MONTH, A.PIH_INVOICED_CUS_ID, case when EXTRACT(MONTH FROM PIH_FINANCE_DATE) < 4 then EXTRACT(YEAR FROM PIH_FINANCE_DATE) - 1 else EXTRACT(YEAR FROM PIH_FINANCE_DATE) end Y, PID_TOTAL_COST, PID_BASIC_TTLAMOUNT
from (select A.PIH_ACCOUNT_MONTH, A.PIH_INVOICED_CUS_ID, A.PIH_INVCUS, B.PID_TOTAL_COST, B.PID_PRO_KOMCODE, B.PID_BASIC_TTLAMOUNT, A.PIH_FINANCE_DATE from kp_invoice_head A
join kp_invoice_detail B on(A.PIH_TICKET_ID = B.PID_TICKET_ID)
where PIH_ACCOUNT_MONTH >= (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 5 else  EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 end) || '04' and PIH_ACCOUNT_MONTH <= to_char(current_date - INTERVAL '1 day','YYYYMM') 
and not A.PIH_INV_TYPE in('99','98') and A.PIH_TICKET_ID like 'XF%' and A.PIH_CREDIT_NOTE_STATUS = '') A) A group by A.PIH_INVOICED_CUS_ID, A.Y) A) A
group by PIH_INVOICED_CUS_ID) part on(C.CUS_ID = part.PIH_INVOICED_CUS_ID)
-- End Part
-- Service Amount
left outer join (select PIH_INVOICED_CUS_ID, sum(AMOUNT15) as amt15, sum(AMOUNT16) as amt16, sum(AMOUNT17) as amt17, sum(AMOUNT18) as amt18, sum(AMOUNT19) as amt19, sum(AMOUNT) as tamt
from (select PIH_INVOICED_CUS_ID,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 5 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 end) then AMOUNT else 0 end amount15,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 3 end) then AMOUNT else 0 end amount16,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 3 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 2 end) then AMOUNT else 0 end amount17,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 2 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 1 end) then AMOUNT else 0 end amount18,
case when Y = (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 1 else EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) end) then AMOUNT else 0 end amount19,
AMOUNT
from (select A.PIH_INVOICED_CUS_ID, A.Y, sum(A.PIH_BASIC_TTLAMOUNT) as AMOUNT
from (select A.PIH_ACCOUNT_MONTH, A.PIH_INVOICED_CUS_ID, case when EXTRACT(MONTH FROM PIH_FINANCE_DATE) < 4 then EXTRACT(YEAR FROM PIH_FINANCE_DATE) - 1 else EXTRACT(YEAR FROM PIH_FINANCE_DATE) end Y, PIH_BASIC_TTLAMOUNT FROM kp_invoice_head A
where PIH_ACCOUNT_MONTH >= (case when EXTRACT(MONTH FROM (current_date - INTERVAL '1 day')) < 4 then EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 5 else  EXTRACT(YEAR FROM (current_date - INTERVAL '1 day')) - 4 end) || '04' and PIH_ACCOUNT_MONTH <= to_char(current_date - INTERVAL '1 day','YYYYMM') 
and not A.PIH_INV_TYPE in('99','98') and A.PIH_TICKET_ID like 'FF%' and A.PIH_CREDIT_NOTE_STATUS = '') A group by A.PIH_INVOICED_CUS_ID, A.Y) A) A 
group by PIH_INVOICED_CUS_ID) sA on(C.CUS_ID = sA.PIH_INVOICED_CUS_ID)
-- End Service Amount
left outer join (select sa.eg_kopen_id__c, sa.cus_province__c, sa.customer_type__c, sa.major_brand__c, sa.region_by_user__c , su."name" as sales_name, (su.firstname_th__c || ' ' || su.lastname_th__c) as name_th, sa.department_by_user__c 
from sf_account sa join sf_user su on (sa.ownerid = su.id) where not sa.eg_kopen_id__c is Null) sB on(C.CUS_ID = sB.eg_kopen_id__c)
where CUS_STATUS <> '9' and U.MC > 0 and not C.CUS_NAME like 'TF-%' ) A order by MC desc, MCTOTAL desc, TTAMT desc """ + st

