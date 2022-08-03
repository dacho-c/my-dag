def sql_ET_salesbyitem(lym):
    return ("""select REGION,PID_TICKET_ID,PIH_ORIGINAL_TKTID,PID_SERIAL_NO,CUSTOMER_ORDER,PID_PRO_KOMCODE,PRO_NAME,PID_SERVICE_NAME,
DEP_NAME,PRICE_LIST,DISCOUNT,BILLING_RATE,SALEPRICE,UNIT_PRICE,PID_QTY,PID_NOTAX_PRICE,PRICE_VAT,PID_TOTAL_AMOUNT,PID_TOTAL_MONEY,
PID_TOTAL_TAX,PID_TOTAL_COST,PID_TOTAL_COST_DB,PID_TKTID,PID_BASIC_TTLMONEY,PID_BASIC_TTLAMOUNT,PID_BASIC_TTLTAX,PID_ORI_SERIALNO,
PID_CHARGE_AMOUNT,PID_EXCHANGE_RATE,PID_ALL_TAX,PIH_ORG_ID,PID_REMARK,BR_NAME,PIH_DEP_ID,DEP_NAME1,PSS_CODE_DB,PSS_NAME_DB,PIH_INV_NO,
PIH_ORIGINAL_INVNO,PIH_INV_TYPE,INV_TYPE_NAME,REVENUE_TYPE,REVENUE_CODE,UNKNOW,PART_GROUP_DB,PART_GROUP_BKS,PART_MONITOR_NAME,
PART_STATUS,UNIT,PART_MONITOR_TYPE,REAL_UNIT,FILTER_MONITOR_ITEM,PMC_KOPEN,PMC_BKS,CUSTOMER_CODE,CUS_NAME,CUSTOMER_INV_CODE,INV_CUS_NAME,
INV_DATE,VERIFIED_DATE,STATUS,REVENUE_TYPE1,PIH_UNIT_MODEL,PIH_UNIT_ID,PIH_PROJECT_ID,SMM_SD_TICKET_ID,SMM_DESCRIPTION,FY,MFY,QUOTATION_NO,
QUOTATION_REF,SMM_LINKMAN,SMM_TEL,PIH_ACCOUNT_MONTH 
from (SELECT CASE WHEN PIH_APPROVER IN ('018580', '003634', '009313') THEN 'PARTS DIVISION'
            WHEN PIH_APPROVER IN ('S00024') THEN 'COUNTERSALES' ELSE BR_REP_MEN END REGION,
        B.PID_TICKET_ID,PIH_ORIGINAL_TKTID,PID_SERIAL_NO,poh_s_tktid AS CUSTOMER_ORDER,PID_PRO_KOMCODE,PRO_NAME,PID_SERVICE_NAME,
        DEP_NAME,0 AS PRICE_LIST,0 AS DISCOUNT,0 AS BILLING_RATE,PID_NOTAX_PRICE AS UNIT_PRICE,B.PID_LIST_PRICE AS UNIT_PRICE_LIST,
        PID_QTY,PID_NOTAX_PRICE,PID_NOTAX_PRICE as SALEPRICE,PID_PRICE AS PRICE_VAT,PID_TOTAL_AMOUNT,PID_TOTAL_MONEY,PID_TOTAL_TAX,
        PID_TOTAL_COST,PID_TOTAL_COST_zB AS PID_TOTAL_COST_DB,b.PID_TKTID,PID_BASIC_TTLMONEY,PID_BASIC_TTLAMOUNT,PID_BASIC_TTLTAX,
        PID_ORI_SERIALNO,PID_CHARGE_AMOUNT,PID_EXCHANGE_RATE,PID_ALL_TAX,PIH_ORG_ID,PID_REMARK,BR_NAME,PIH_DEP_ID,DEP_NAME as DEP_NAME1,
        PIH_APPROVER AS PSS_CODE_DB,emp_name AS PSS_NAME_DB,PIH_INV_NO,PIH_ORIGINAL_INVNO,PIH_INV_TYPE,INV_TYPE_NAME,REVENUE_TYPE,REVENUE_CODE,
        CASE
            WHEN LEFT(B.PID_TICKET_ID, 2) = 'XF'
            AND PIH_APPROVER IN('022919', '100002') THEN 'PART TPI'
            WHEN LEFT(B.PID_TICKET_ID, 2) = 'XF'
            AND NOT PIH_APPROVER IN('022919', '100002') THEN 'PART'
            WHEN LEFT(b.PID_TKTID, 2) IN('SL', 'SK')
            AND LEFT(b.PID_TICKET_ID , 2 ) = 'FF' THEN 'P2S'
            WHEN LEFT(b.PID_TKTID, 2) IN('FB')
            AND LEFT(B.PID_TICKET_ID , 2 ) = 'FF'
            AND PID_SERVICE_NAME LIKE '%ค่าแรง%' THEN 'LABOR'
            WHEN LEFT(b.PID_TKTID, 2) IN('FB')
            AND LEFT(B.PID_TICKET_ID , 2 ) = 'FF'
            AND PID_SERVICE_NAME LIKE '%ค่าพาหนะ%' THEN 'MILEAGE'
            WHEN LEFT(b.PID_TKTID, 2) IN('FB')
            AND LEFT(B.PID_TICKET_ID , 2 ) = 'FF'
            AND (PID_SERVICE_NAME NOT LIKE '%ค่าพาหนะ%'
            OR PID_SERVICE_NAME NOT LIKE '%ค่าแรง%' ) THEN 'OTHER'
        END UNKNOW,
        ID_NAME AS PART_GROUP_DB,
        bpm_type AS PART_GROUP_BKS,
        PRO_NAME AS PART_MONITOR_NAME,
        PART_STATUS AS PART_STATUS,
        PART_REAL_UNIT AS UNIT,
        PART_KGO AS PART_MONITOR_TYPE,
        (CASE WHEN PART_KGO = 'KGO' THEN (CASE WHEN PART_REAL_UNIT = '' THEN 1 WHEN PART_REAL_UNIT IS NULL THEN 1 ELSE PART_REAL_UNIT::integer END) * PID_QTY ELSE 0 END) AS Real_Unit,
        main_mark AS FILTER_MONITOR_ITEM ,
        '' AS PMC_KOPEN ,
        (SELECT demand_rank FROM kp_bks_part_demand
        WHERE
            demand_part_no = PID_PRO_KOMCODE FETCH FIRST 1 ROWS ONLY) AS PMC_BKS,
        PIH_CUS_ID AS CUSTOMER_CODE,
        CUS_NAME AS CUS_NAME,
        pih_invoiced_cus_id AS CUSTOMER_INV_CODE ,
        pih_invcus AS INV_CUS_NAME ,
        PIH_INV_DATE AS INV_DATE ,
        PIH_CHECK_DATE AS VERIFIED_DATE ,
        PIH_STATUS AS STATUS,
        CASE
            WHEN REVENUE_CODE IN('P', 'S') THEN 'REVENUE'
            WHEN REVENUE_CODE = 'I' THEN 'NON REVENUE'
            ELSE REVENUE_CODE
        END AS REVENUE_TYPE1,
        PIH_UNIT_MODEL,
        PIH_UNIT_ID,
        PIH_PROJECT_ID,
        SMM_SD_TICKET_ID,
        TRANSLATE(TRANSLATE(SMM_DESCRIPTION, '', CHR(10)), '', CHR(13)) AS SMM_DESCRIPTION,
        CASE
            WHEN EXTRACT(MONTH from PIH_INV_DATE) < 4 THEN EXTRACT(YEAR from PIH_INV_DATE) - 1
            ELSE EXTRACT(YEAR from PIH_INV_DATE)
        END FY,
        CASE
            WHEN EXTRACT(MONTH from PIH_INV_DATE) < 4 THEN EXTRACT(MONTH from PIH_INV_DATE) + 9
            else EXTRACT(MONTH from PIH_INV_DATE) - 3
        END MFY,
        AA.PQH_TICKET_ID AS Quotation_No,
        AA.PQH_CUS_LINKMAN AS Quotation_Ref,
        SMM_LINKMAN,
        SMM_TEL, PIH_ACCOUNT_MONTH
    FROM kp_invoice_head A
    LEFT JOIN kp_customer kc ON CUS_ID = PIH_CUS_ID
    LEFT join kp_revenue_type krt ON INV_CODE = PIH_INV_TYPE
    LEFT JOIN kp_invoice_detail B ON A.PIH_TICKET_ID = B.PID_TICKET_ID
    LEFT JOIN kp_part kp ON PRO_KOMCODE = PID_PRO_KOMCODE
    LEFT JOIN kp_part_picking_head kpph ON b.PID_TKTID = POH_TICKET_ID
    LEFT JOIN kp_branch kb ON BR_ID = PIH_ORG_ID
    LEFT JOIN kp_department kd ON DEP_CODE = PIH_DEP_ID
    LEFT JOIN kp_employee ke ON EMP_ID = PIH_APPROVER
    LEFT JOIN kp_service_job ksj ON PIH_PROJECT_ID = SMM_TICKET_ID
    LEFT JOIN (SELECT DISTINCT A.PART_NUMBER,A.PART_NAME,A.PART_STATUS,A.PART_REAL_UNIT,A.PART_KGO
        FROM kp_BKS_PART_MONITOR A) pm ON PID_PRO_KOMCODE = PART_NUMBER
    LEFT JOIN (SELECT DISTINCT PART_NO AS BPM_NO,CASE WHEN PART_TYPE = '1' THEN 'Consumable'
    WHEN PART_TYPE = '2' THEN 'Functional' ELSE '' END BPM_TYPE FROM kp_BKS_SOK_PART WHERE PART_TYPE IN('1', '2')) sp ON PID_PRO_KOMCODE = BPM_NO
    LEFT JOIN (SELECT DISTINCT main_part_no,main_mark FROM kp_bks_all_main_filter) amf ON PID_PRO_KOMCODE = main_part_no
    LEFT JOIN (SELECT ID_CODE,ID_NAME FROM kp_IDBOOKS
        WHERE ID_ATTRIBUTE = 84 AND ID_CONTROL = 2) idb ON PRO_SALES_TYPE = ID_CODE
    LEFT OUTER JOIN (SELECT DISTINCT B.PID_TICKET_ID, B.PID_TKTID, q.PQH_TICKET_ID, q.PQH_CUS_LINKMAN
        FROM kp_invoice_head A
        JOIN kp_invoice_detail B ON (A.PIH_TICKET_ID = B.PID_TICKET_ID)
        JOIN kp_part_picking_head os ON (B.PID_TKTID = os.POH_TICKET_ID)
        JOIN kp_part_quote_head q ON (os.POH_S_TKTID = q.PQH_S_TKTID)
        WHERE A.PIH_ACCOUNT_MONTH >= '%s' 
                AND LEFT(A.PIH_TICKET_ID, 2) = 'XF'
                    AND NOT A.PIH_INV_TYPE IN('99', '98')
                        AND PQH_CUS_LINKMAN <> '') AA ON (B.PID_TICKET_ID = AA.PID_TICKET_ID AND B.PID_TKTID = AA.PID_TKTID)
    WHERE
        LEFT(B.PID_TICKET_ID, 2 ) IN ('XF', 'FF' ) AND PIH_STATUS = '1'
        AND LEFT(b.PID_TKTID, 2) IN ('FB', 'SL', 'SK', 'XC', 'XR')
        AND PIH_ACCOUNT_MONTH >= '%s' ORDER BY B.PID_TICKET_ID ,B.PID_SERIAL_NO) A""") % (lym, lym)

def sql_DEL_salesbyitem(lym):
    return ("DELETE FROM sales_by_item s where s.pih_account_month >= '%s'") % (lym)