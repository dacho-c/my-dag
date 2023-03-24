import pyarrow as pa

def sql_detail_select(tb, lym):
    if tb == 'part_inv_detail':
        return """SELECT db2admin.PART_INV_DETAIL.* 
            FROM db2admin.PART_INV_DETAIL 
            LEFT JOIN db2admin.PART_INV_HEAD ON db2admin.PART_INV_DETAIL.PID_TICKET_ID = db2admin.PART_INV_HEAD.PIH_TICKET_ID 
            WHERE db2admin.PART_INV_HEAD.PIH_ACCOUNT_MONTH >= '%s'""" % (lym)
    elif tb == 'PART_OUTSALE_DETAIL':
        return """SELECT db2admin.PART_OUTSALE_DETAIL.* 
            FROM db2admin.PART_OUTSALE_DETAIL 
            LEFT JOIN db2admin.PART_OUTSALE_HEAD ON db2admin.PART_OUTSALE_DETAIL.POD_TICKET_ID = db2admin.PART_OUTSALE_HEAD.POH_TICKET_ID 
            WHERE db2admin.PART_OUTSALE_HEAD.POH_ACCOUNT_MONTH >= '%s'""" % (lym)
    elif tb == 'PART_QUOTE_DETAIL':
        return """SELECT db2admin.PART_QUOTE_DETAIL.* 
            FROM db2admin.PART_QUOTE_DETAIL 
            LEFT JOIN db2admin.PART_QUOTE_HEAD ON db2admin.PART_QUOTE_DETAIL.PQD_TICKET_ID = db2admin.PART_QUOTE_HEAD.PQH_TICKET_ID 
            WHERE db2admin.PART_QUOTE_HEAD.PQH_ACCOUNT_MONTH >= '%s'
            UNION ALL 
            SELECT db2admin.PART_QUOTE_DETAIL.* 
            FROM db2admin.PART_QUOTE_DETAIL 
            LEFT JOIN db2admin.SERVICE_QUOTE_HEAD ON db2admin.PART_QUOTE_DETAIL.PQD_TICKET_ID = db2admin.SERVICE_QUOTE_HEAD.SQH_TICKET_ID 
            WHERE db2admin.SERVICE_QUOTE_HEAD.SQH_ACCOUNT_MONTH >= '%s'""" % (lym,lym)
    elif tb == 'SERVICE_QUOTE_DTL':
        return """SELECT db2admin.SERVICE_QUOTE_DTL.* 
            FROM db2admin.SERVICE_QUOTE_DTL 
            LEFT JOIN db2admin.SERVICE_QUOTE_HEAD ON db2admin.SERVICE_QUOTE_DTL.SQD_TICKET_ID = db2admin.SERVICE_QUOTE_HEAD.SQH_TICKET_ID 
            WHERE db2admin.SERVICE_QUOTE_HEAD.SQH_ACCOUNT_MONTH >= '%s'""" % (lym)
    elif tb == 'SERV_PART_OUT_DTL':
        return """SELECT db2admin.SERV_PART_OUT_DTL.* 
            FROM db2admin.SERV_PART_OUT_DTL 
            LEFT JOIN db2admin.SERV_PART_OUTSTOCK ON db2admin.SERV_PART_OUT_DTL.SPOD_TICKET_ID = db2admin.SERV_PART_OUTSTOCK.SPO_TICKET_ID 
            WHERE db2admin.SERV_PART_OUTSTOCK.SPO_ACCOUNT_MONTH >= '%s'""" % (lym)
    else: 
        return ""

def sql_detail_delete(tb, lym):
    if tb == 'kp_invoice_detail':
        return """DELETE FROM kp_invoice_detail 
            WHERE kp_invoice_detail.PID_TICKET_ID IN (SELECT PIH_TICKET_ID FROM kp_invoice_head WHERE PIH_ACCOUNT_MONTH >= '%s');""" % (lym)
    elif tb == 'kp_part_picking_detail':
        return """DELETE FROM kp_part_picking_detail 
            WHERE kp_part_picking_detail.POD_TICKET_ID IN (SELECT POH_TICKET_ID FROM kp_part_picking_head WHERE POH_ACCOUNT_MONTH >= '%s');""" % (lym)
    elif tb == 'kp_part_quote_detail':
        return """DELETE FROM kp_part_quote_detail 
            WHERE kp_part_quote_detail.PQD_TICKET_ID IN (SELECT PQH_TICKET_ID FROM kp_part_quote_head WHERE PQH_ACCOUNT_MONTH >= '%s');
            DELETE FROM kp_part_quote_detail 
            WHERE kp_part_quote_detail.PQD_TICKET_ID IN (SELECT SQH_TICKET_ID FROM kp_service_quote_head WHERE SQH_ACCOUNT_MONTH >= '%s');""" % (lym,lym)
    elif tb == 'kp_service_quote_detail':
        return """DELETE FROM kp_service_quote_detail 
            WHERE kp_service_quote_detail.SQD_TICKET_ID IN (SELECT SQH_TICKET_ID FROM kp_service_quote_head WHERE SQH_ACCOUNT_MONTH >= '%s');""" % (lym)
    elif tb == 'kp_service_part_supply_detail':
        return """DELETE FROM kp_service_part_supply_detail 
            WHERE kp_service_part_supply_detail.SPOD_TICKET_ID IN (SELECT SPO_TICKET_ID FROM kp_service_part_supply_head WHERE SPO_ACCOUNT_MONTH >= '%s');""" % (lym)
    else:
        return ""

def sql_stock():
    return """select (db2admin.stock.ST_ORG_ID || db2admin.stock.ST_WH_ID || db2admin.stock.ST_SR_ID || db2admin.stock.ST_LO_ID || db2admin.stock.ST_PRO_KOMCODE) as item_id, db2admin.stock.ST_ORG_ID, db2admin.stock.ST_WH_ID, 
    db2admin.stock.ST_SR_ID, db2admin.stock.ST_LO_ID, db2admin.stock.ST_PRO_ID, db2admin.stock.ST_PRO_KOMCODE, db2admin.stock.ST_DEP_ID, db2admin.stock.ST_HOLD_PUR, 
    db2admin.stock.ST_HOLD_CUS, db2admin.stock.ST_HOLD_REP, db2admin.stock.ST_OUT_CUS, db2admin.stock.ST_OUT_REP, db2admin.stock.ST_ENABLE_STOCK, db2admin.stock.ST_BALANCE_STOCK, 
    db2admin.stock.ST_RETURN_STOCK, db2admin.stock.ST_IN_TOTAL, db2admin.stock.ST_OUT_TOTAL, db2admin.stock.ST_COST_NOTAX_PRICE, db2admin.stock.ST_LASTUSERID, db2admin.stock.ST_LASTTIME, 
    db2admin.stock.ST_STATUS, db2admin.stock.ST_LASTINTIME, db2admin.stock.ST_LASTOUTTIME, db2admin.stock.ST_HOLD_TRANS, db2admin.stock.ST_OUT_TRANS, db2admin.stock.ST_IN_DATE, 
    db2admin.stock.ST_SALE_DATE, db2admin.stock.ST_PICKED_QTY, DATE(db2admin.stock.ST_STARTDATE) as ST_STARTDATE, DATE(db2admin.stock.ST_ENDDATE) as ST_ENDDATE, DATE(db2admin.stock.ST_ADJUSTMENT_DATE) as ST_ADJUSTMENT_DATE, db2admin.stock.ST_SR_QTY, 
    db2admin.stock.ST_HOLD_PUR_PO, db2admin.stock.ST_HOLD_REP_PO from db2admin.stock """

def sql_part_sale_head(fy,fy1):
    return """select A.PSH_TICKET_ID, A.PSH_S_TKTID, A.PSH_ORG_ID, A.PSH_DEP_ID, A.PSH_FINANCE_DATE, A.PSH_ACCOUNT_MONTH, A.PSH_WH_ID, A.PSH_CUS_ID, A.PSH_VALID_DATE, A.PSH_CUS_ADDRID, 
    A.PSH_OUT_TAXRATE, A.PSH_DISCOUNT, A.PSH_TOTAL_MONEY, A.PSH_TOTAL_AMOUNT, A.PSH_TOTAL_TAX, A.PSH_WH_TKTID, A.PSH_WH_STATUS, A.PSH_INV_TYPE, A.PSH_INV_NO, A.PSH_INV_DATE, 
    A.PSH_INV_MONEY, A.PSH_REC_DATE, A.PSH_REC_TKTID, A.PSH_REC_MONEY, A.PSH_PASSED, A.PSH_IS_PUR, A.PSH_PUR_TKTID, A.PSH_E_TKTID, A.PSH_REMARK, A.PSH_PRN_CNT, A.PSH_CREATER_ID, 
    A.PSH_CREATE_DATE, A.PSH_CHECKER_ID, A.PSH_CHECK_DATE, A.PSH_LASTUSERID, A.PSH_LASTTIME, A.PSH_STATUS, A.PSH_TRANS_TYPE, A.PSH_TRANS_FEE, A.PSH_TRANS_REMARK, A.PSH_PAY_DAY, 
    A.PSH_REC_MODE, A.PSH_SALES, A.PSH_OLD_TICKET_ID, A.PSH_OLD_TICKETID, A.PSH_INVCUS, A.PSH_IS_INT, A.PSH_PZ_TYPE, A.PSH_SALE_TYPE, A.PSH_UNIT_MODEL, A.PSH_UNIT_ID, A.PSH_CURRENCY, 
    A.PSH_EXCHANGE_RATE, A.PSH_BASIC_TTLMONEY, A.PSH_BASIC_TTLAMOUNT, A.PSH_BASIC_TTLTAX, A.PSH_ORDER_TYPE, A.PSH_OUT_STATUS, A.PSH_CHARGE_AMOUNT, A.PSH_LP_INFO, A.PSH_PREPAY_MONEY, 
    A.PSH_PAY_TERMS, A.PSH_USANCE_DAYS, A.PSH_CREDIT_TYPE, A.PSH_OR_TKTID, A.PSH_DIRECT, A.PSH_INDENT_ORDER, A.PSH_TTLTAX_NEW, A.PSH_XML_DATE, A.PSH_DISCOUNT_TYPE, A.PSH_XS_TKTID, 
    A.PSH_APPROVER, A.PSH_DOC_PATH from db2admin.PART_SALE_HEAD A where left(PSH_ACCOUNT_MONTH,4) in ('%s','%s')""" % (fy,fy1)

def sql_create_part_sale_head(tb_to, primary_key):
    return """CREATE TABLE IF NOT EXISTS public.kp_part_sale_head (
	            psh_ticket_id text NULL,
	            psh_s_tktid text NULL,
	            psh_org_id text NULL,
	            psh_dep_id text NULL,
	            psh_finance_date date NULL,
	            psh_account_month text NULL,
	            psh_wh_id text NULL,
	            psh_cus_id text NULL,
	            psh_valid_date date NULL,
	            psh_out_taxrate float8 NULL,
	            psh_discount float8 NULL,
	            psh_wh_tktid text NULL,
	            psh_wh_status text NULL,
	            psh_rec_date date NULL,
	            psh_passed text NULL,
	            psh_is_pur text NULL,
	            psh_remark text NULL,
	            psh_creater_id text NULL,
	            psh_create_date timestamp NULL,
	            psh_checker_id text NULL,
	            psh_check_date timestamp NULL,
	            psh_lastuserid text NULL,
	            psh_lasttime timestamp NULL,
	            psh_status text NULL,
	            psh_pay_day date NULL,
	            psh_sales text NULL,
	            psh_old_ticket_id text NULL,
	            psh_old_ticketid text NULL,
	            psh_invcus text NULL,
	            psh_is_int text NULL,
	            psh_pz_type text NULL,
	            psh_sale_type text NULL,
	            psh_unit_model text NULL,
	            psh_unit_id text NULL,
	            psh_currency text NULL,
	            psh_exchange_rate float8 NULL,
	            psh_basic_ttlmoney float8 NULL,
	            psh_basic_ttlamount float8 NULL,
	            psh_basic_ttltax float8 NULL,
	            psh_order_type text NULL,
	            psh_lp_info text NULL,
	            psh_pay_terms text NULL,
	            psh_usance_days int8 NULL,
	            psh_credit_type text NULL,
	            psh_ttltax_new float8 NULL,
                CONSTRAINT %s_pkey PRIMARY KEY (%s)
            );""" % (tb_to, primary_key)

def schema_part_sale_head():
    return pa.schema([
        pa.field('psh_ticket_id',pa.string()),
        pa.field('psh_s_tktid',pa.string()),
        pa.field('psh_org_id',pa.string()),
        pa.field('psh_dep_id',pa.string()),
        pa.field('psh_finance_date',pa.date32()),
        pa.field('psh_account_month',pa.string()),
        pa.field('psh_wh_id',pa.string()),
        pa.field('psh_cus_id',pa.string()),
        pa.field('psh_valid_date',pa.date32()),
        pa.field('psh_cus_addrid',pa.string()),
        pa.field('psh_out_taxrate',pa.float64()),
        pa.field('psh_discount',pa.float64()),
        pa.field('psh_total_money',pa.float64()),
        pa.field('psh_total_amount',pa.float64()),
        pa.field('psh_total_tax',pa.float64()),
        pa.field('psh_wh_tktid',pa.string()),
        pa.field('psh_wh_status',pa.string()),
        pa.field('psh_inv_type',pa.string()),
        pa.field('psh_inv_no',pa.string()),
        pa.field('psh_inv_date',pa.date32()),
        pa.field('psh_inv_money',pa.float64()),
        pa.field('psh_rec_date',pa.date32()),
        pa.field('psh_rec_tktid',pa.string()),
        pa.field('psh_rec_money',pa.float64()),
        pa.field('psh_passed',pa.string()),
        pa.field('psh_is_pur',pa.string()),
        pa.field('psh_pur_tktid',pa.string()),
        pa.field('psh_e_tktid',pa.string()),
        pa.field('psh_remark',pa.string()),
        pa.field('psh_prn_cnt',pa.int64()),
        pa.field('psh_creater_id',pa.string()),
        pa.field('psh_create_date',pa.timestamp('us')),
        pa.field('psh_checker_id',pa.string()),
        pa.field('psh_check_date',pa.timestamp('us')),
        pa.field('psh_lastuserid',pa.string()),
        pa.field('psh_lasttime',pa.timestamp('us')),
        pa.field('psh_status',pa.string()),
        pa.field('psh_trans_type',pa.string()),
        pa.field('psh_trans_fee',pa.float64()),
        pa.field('psh_trans_remark',pa.string()),
        pa.field('psh_pay_day',pa.date32()),
        pa.field('psh_rec_mode',pa.string()),
        pa.field('psh_sales',pa.string()),
        pa.field('psh_old_ticket_id',pa.string()),
        pa.field('psh_old_ticketid',pa.string()),
        pa.field('psh_invcus',pa.string()),
        pa.field('psh_is_int',pa.string()),
        pa.field('psh_pz_type',pa.string()),
        pa.field('psh_sale_type',pa.string()),
        pa.field('psh_unit_model',pa.string()),
        pa.field('psh_unit_id',pa.string()),
        pa.field('psh_currency',pa.string()),
        pa.field('psh_exchange_rate',pa.float64()),
        pa.field('psh_basic_ttlmoney',pa.float64()),
        pa.field('psh_basic_ttlamount',pa.float64()),
        pa.field('psh_basic_ttltax',pa.float64()),
        pa.field('psh_order_type',pa.string()),
        pa.field('psh_out_status',pa.string()),
        pa.field('psh_charge_amount',pa.float64()),
        pa.field('psh_lp_info',pa.string()),
        pa.field('psh_prepay_money',pa.float64()),
        pa.field('psh_pay_terms',pa.string()),
        pa.field('psh_usance_days',pa.int64()),
        pa.field('psh_credit_type',pa.string()),
        pa.field('psh_or_tktid',pa.string()),
        pa.field('psh_direct',pa.string()),
        pa.field('psh_indent_order',pa.string()),
        pa.field('psh_ttltax_new',pa.float64()),
        pa.field('psh_xml_date',pa.timestamp('us')),
        pa.field('psh_discount_type',pa.string()),
        pa.field('psh_xs_tktid',pa.string()),
        pa.field('psh_approver',pa.string()),
        pa.field('psh_doc_path',pa.string()),
    ])

def columns_part_sale_head():
    return ['psh_ticket_id', 'psh_s_tktid', 'psh_org_id', 'psh_dep_id',
       'psh_finance_date', 'psh_account_month', 'psh_wh_id', 'psh_cus_id',
       'psh_valid_date', 'psh_out_taxrate', 'psh_discount', 'psh_wh_tktid',
       'psh_wh_status', 'psh_rec_date', 'psh_passed', 'psh_is_pur', 
       'psh_remark', 'psh_creater_id', 'psh_create_date',
       'psh_checker_id', 'psh_check_date', 'psh_lastuserid', 'psh_lasttime',
       'psh_status', 'psh_pay_day', 'psh_sales', 'psh_old_ticket_id',
       'psh_old_ticketid', 'psh_invcus', 'psh_is_int', 'psh_pz_type',
       'psh_sale_type', 'psh_unit_model', 'psh_unit_id', 'psh_currency',
       'psh_exchange_rate', 'psh_basic_ttlmoney', 'psh_basic_ttlamount',
       'psh_basic_ttltax', 'psh_order_type', 'psh_lp_info', 'psh_pay_terms',
       'psh_usance_days', 'psh_credit_type', 'psh_ttltax_new']

def sql_part_sale_detail(fy, fy1):
    return """select (A.PSD_TICKET_ID || A.PSD_SERIAL_NO) as item_id, A.PSD_TICKET_ID, A.PSD_SERIAL_NO, A.PSD_PRO_ID, A.PSD_PRO_KOMCODE, A.PSD_PRO_KOMCODE_O, A.PSD_UNITID, A.PSD_OUT_TAXRATE, A.PSD_QTY, A.PSD_QTY1, 
    A.PSD_QTY2, A.PSD_QTY3, A.PSD_PRICE_NO, A.PSD_PRICE, A.PSD_NOTAX_PRICE, A.PSD_TOTAL_MONEY, A.PSD_TOTAL_AMOUNT, A.PSD_TOTAL_TAX, A.PSD_REF_PRICE, A.PSD_DISCOUNT, 
    A.PSD_PRM_TKTID, A.PSD_WH_STATUS, A.PSD_REMARK, A.PSD_WH_ID, A.PSD_ORG_ID, A.PSD_QTY4, A.PSD_SR_ID, A.PSD_LO_ID, A.PSD_COST_PRICE, A.PSD_TOTAL_COST, 
    A.PSD_COSTPRICE_ZB, A.PSD_PRICE_FLAG, A.PSD_TOTAL_COST_ZB, A.PSD_PRO_KOMCODE_TH, A.PSD_TH_STATUS, A.PSD_OUTST_SERIALNO, A.PSD_LACK_STATUS, A.PSD_BASIC_TTLMONEY, 
    A.PSD_BASIC_TTLAMOUNT, A.PSD_BASIC_TTLTAX, A.PSD_WET, A.PSD_STALLOC_QTY, A.PSD_WAIT_QTY, A.PSD_PROCESS_QTY, A.PSD_HOLD_QTY, A.PSD_CHARGE_PRICE, A.PSD_CHARGE_KG, 
    A.PSD_CHARGE_RATE, A.PSD_CHARGE_AMOUNT, A.PSD_HO_QTY, A.PSD_TTLTAX_NEW, A.PSD_SALES_TYPE, A.PSD_DISCOUNT_RATE, A.PSD_AP_AMOUNT, A.PSD_AP_MONEY, A.PSD_CASE_PROBLEM, 
    A.PSD_GUIDELINES, A.PSD_LP_AMOUNT, A.PSD_PROMOTION_CODE, B.PSH_ACCOUNT_MONTH from db2admin.PART_SALE_DETAIL A
    join (select PSH_ACCOUNT_MONTH, PSH_TICKET_ID FROM db2admin.PART_SALE_HEAD where left(PSH_ACCOUNT_MONTH,4) in ('%s','%s')) B on(A.PSD_TICKET_ID = B.PSH_TICKET_ID)""" % (fy,fy1)

def sql_create_part_sale_detail(tb_to, primary_key):
    return """CREATE TABLE IF NOT EXISTS public.kp_part_sale_detail (
	        item_id text NULL,
	        psd_ticket_id text NULL,
	        psd_serial_no int8 NULL,
	        psd_pro_komcode text NULL,
	        psd_pro_komcode_o text NULL,
	        psd_out_taxrate float8 NULL,
	        psd_qty float8 NULL,
	        psd_qty1 float8 NULL,
	        psd_qty2 float8 NULL,
	        psd_qty3 float8 NULL,
	        psd_price_no int8 NULL,
	        psd_price float8 NULL,
	        psd_notax_price float8 NULL,
	        psd_total_money float8 NULL,
	        psd_total_amount float8 NULL,
	        psd_total_tax float8 NULL,
	        psd_ref_price float8 NULL,
	        psd_discount float8 NULL,
	        psd_wh_status text NULL,
	        psd_remark text NULL,
	        psd_wh_id text NULL,
	        psd_org_id text NULL,
	        psd_qty4 float8 NULL,
	        psd_sr_id text NULL,
	        psd_lo_id text NULL,
	        psd_cost_price float8 NULL,
	        psd_total_cost float8 NULL,
	        psd_costprice_zb float8 NULL,
	        psd_price_flag text NULL,
	        psd_total_cost_zb float8 NULL,
	        psd_outst_serialno int8 NULL,
	        psd_lack_status text NULL,
	        psd_basic_ttlmoney float8 NULL,
	        psd_basic_ttlamount float8 NULL,
	        psd_basic_ttltax float8 NULL,
	        psd_wet float8 NULL,
	        psd_stalloc_qty float8 NULL,
	        psd_wait_qty float8 NULL,
	        psd_process_qty float8 NULL,
	        psd_hold_qty float8 NULL,
	        psd_ttltax_new float8 NULL,
	        psd_sales_type text NULL,
	        psd_discount_rate float8 NULL,
	        psd_ap_amount float8 NULL,
	        psd_ap_money float8 NULL,
	        psd_case_problem text NULL,
	        psd_guidelines text NULL,
	        psd_lp_amount float8 NULL,
	        psd_promotion_code text NULL,
	        psh_account_month text NULL,
                CONSTRAINT %s_pkey PRIMARY KEY (%s)
            );""" % (tb_to, primary_key)

def schema_part_sale_detail():
    return pa.schema([
        pa.field('item_id',pa.string()),
        pa.field('psd_ticket_id',pa.string()),
        pa.field('psd_serial_no',pa.int64()),
        pa.field('psd_pro_id',pa.string()),
        pa.field('psd_pro_komcode',pa.string()),
        pa.field('psd_pro_komcode_o',pa.string()),
        pa.field('psd_unitid',pa.string()),
        pa.field('psd_out_taxrate',pa.float64()),
        pa.field('psd_qty',pa.float64()),
        pa.field('psd_qty1',pa.float64()),
        pa.field('psd_qty2',pa.float64()),
        pa.field('psd_qty3',pa.float64()),
        pa.field('psd_price_no',pa.int64()),
        pa.field('psd_price',pa.float64()),
        pa.field('psd_notax_price',pa.float64()),
        pa.field('psd_total_money',pa.float64()),
        pa.field('psd_total_amount',pa.float64()),
        pa.field('psd_total_tax',pa.float64()),
        pa.field('psd_ref_price',pa.float64()),
        pa.field('psd_discount',pa.float64()),
        pa.field('psd_prm_tktid',pa.string()),
        pa.field('psd_wh_status',pa.string()),
        pa.field('psd_remark',pa.string()),
        pa.field('psd_wh_id',pa.string()),
        pa.field('psd_org_id',pa.string()),
        pa.field('psd_qty4',pa.float64()),
        pa.field('psd_sr_id',pa.string()),
        pa.field('psd_lo_id',pa.string()),
        pa.field('psd_cost_price',pa.float64()),
        pa.field('psd_total_cost',pa.float64()),
        pa.field('psd_costprice_zb',pa.float64()),
        pa.field('psd_price_flag',pa.string()),
        pa.field('psd_total_cost_zb',pa.float64()),
        pa.field('psd_pro_komcode_th',pa.string()),
        pa.field('psd_th_status',pa.string()),
        pa.field('psd_outst_serialno',pa.int64()),
        pa.field('psd_lack_status',pa.string()),
        pa.field('psd_basic_ttlmoney',pa.float64()),
        pa.field('psd_basic_ttlamount',pa.float64()),
        pa.field('psd_basic_ttltax',pa.float64()),
        pa.field('psd_wet',pa.float64()),
        pa.field('psd_stalloc_qty',pa.float64()),
        pa.field('psd_wait_qty',pa.float64()),
        pa.field('psd_process_qty',pa.float64()),
        pa.field('psd_hold_qty',pa.float64()),
        pa.field('psd_charge_price',pa.float64()),
        pa.field('psd_charge_kg',pa.float64()),
        pa.field('psd_charge_rate',pa.float64()),
        pa.field('psd_charge_amount',pa.float64()),
        pa.field('psd_ho_qty',pa.float64()),
        pa.field('psd_ttltax_new',pa.float64()),
        pa.field('psd_sales_type',pa.string()),
        pa.field('psd_discount_rate',pa.float64()),
        pa.field('psd_ap_amount',pa.float64()),
        pa.field('psd_ap_money',pa.float64()),
        pa.field('psd_case_problem',pa.string()),
        pa.field('psd_guidelines',pa.string()),
        pa.field('psd_lp_amount',pa.float64()),
        pa.field('psd_promotion_code',pa.string()),
        pa.field('psh_account_month',pa.string()),
    ])

def columns_part_sale_detail():
    return ['item_id', 'psd_ticket_id', 'psd_serial_no', 'psd_pro_komcode', 'psd_pro_komcode_o',
    'psd_out_taxrate', 'psd_qty', 'psd_qty1', 'psd_qty2', 'psd_qty3', 'psd_price_no', 'psd_price',
    'psd_notax_price', 'psd_total_money', 'psd_total_amount',
    'psd_total_tax', 'psd_ref_price', 'psd_discount',
    'psd_wh_status', 'psd_remark', 'psd_wh_id', 'psd_org_id', 'psd_qty4',
    'psd_sr_id', 'psd_lo_id', 'psd_cost_price', 'psd_total_cost',
    'psd_costprice_zb', 'psd_price_flag', 'psd_total_cost_zb',
    'psd_outst_serialno', 'psd_lack_status', 'psd_basic_ttlmoney', 'psd_basic_ttlamount',
    'psd_basic_ttltax', 'psd_wet', 'psd_stalloc_qty', 'psd_wait_qty',
    'psd_process_qty', 'psd_hold_qty', 'psd_ttltax_new',
    'psd_sales_type', 'psd_discount_rate', 'psd_ap_amount', 'psd_ap_money',
    'psd_case_problem', 'psd_guidelines', 'psd_lp_amount', 'psd_promotion_code', 'psh_account_month']