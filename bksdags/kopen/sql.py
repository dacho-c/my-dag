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
            WHERE db2admin.PART_QUOTE_HEAD.PQH_ACCOUNT_MONTH >= '%s'""" % (lym)
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
            WHERE kp_part_quote_detail.PQD_TICKET_ID IN (SELECT PQH_TICKET_ID FROM kp_part_quote_head WHERE PQH_ACCOUNT_MONTH >= '%s');""" % (lym)
    elif tb == 'kp_service_part_supply_detail':
        return """DELETE FROM kp_service_part_supply_detail 
            WHERE kp_service_part_supply_detail.SPOD_TICKET_ID IN (SELECT SPO_TICKET_ID FROM kp_service_part_supply_head WHERE SPO_ACCOUNT_MONTH >= '%s');""" % (lym)
    else:
        return ""

def sql_stock():
    return """select (A.ST_ORG_ID || A.ST_WH_ID || ST_PRO_KOMCODE) as item_id, A.ST_ORG_ID, A.ST_WH_ID, A.ST_SR_ID, A.ST_LO_ID, A.ST_PRO_ID, A.ST_PRO_KOMCODE, A.ST_DEP_ID, A.ST_HOLD_PUR, A.ST_HOLD_CUS, A.ST_HOLD_REP, A.ST_OUT_CUS, A.ST_OUT_REP, 
A.ST_ENABLE_STOCK, A.ST_BALANCE_STOCK, A.ST_RETURN_STOCK, A.ST_IN_TOTAL, A.ST_OUT_TOTAL, A.ST_COST_NOTAX_PRICE, A.ST_LASTUSERID, A.ST_LASTTIME, A.ST_STATUS, A.ST_LASTINTIME, 
A.ST_LASTOUTTIME, A.ST_HOLD_TRANS, A.ST_OUT_TRANS, A.ST_IN_DATE, A.ST_SALE_DATE, A.ST_PICKED_QTY, A.ST_STARTDATE, A.ST_ENDDATE, A.ST_ADJUSTMENT_DATE, A.ST_SR_QTY, A.ST_HOLD_PUR_PO, A.ST_HOLD_REP_PO 
from db2admin.stock as A """