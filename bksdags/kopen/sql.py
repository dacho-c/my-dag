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
    return """select (db2admin.stock.ST_ORG_ID || db2admin.stock.ST_WH_ID || db2admin.stock.ST_SR_ID || db2admin.stock.ST_LO_ID || db2admin.stock.ST_PRO_KOMCODE) as item_id, db2admin.stock.ST_ORG_ID, db2admin.stock.ST_WH_ID, 
    db2admin.stock.ST_SR_ID, db2admin.stock.ST_LO_ID, db2admin.stock.ST_PRO_ID, db2admin.stock.ST_PRO_KOMCODE, db2admin.stock.ST_DEP_ID, db2admin.stock.ST_HOLD_PUR, 
    db2admin.stock.ST_HOLD_CUS, db2admin.stock.ST_HOLD_REP, db2admin.stock.ST_OUT_CUS, db2admin.stock.ST_OUT_REP, db2admin.stock.ST_ENABLE_STOCK, db2admin.stock.ST_BALANCE_STOCK, 
    db2admin.stock.ST_RETURN_STOCK, db2admin.stock.ST_IN_TOTAL, db2admin.stock.ST_OUT_TOTAL, db2admin.stock.ST_COST_NOTAX_PRICE, db2admin.stock.ST_LASTUSERID, db2admin.stock.ST_LASTTIME, 
    db2admin.stock.ST_STATUS, db2admin.stock.ST_LASTINTIME, db2admin.stock.ST_LASTOUTTIME, db2admin.stock.ST_HOLD_TRANS, db2admin.stock.ST_OUT_TRANS, db2admin.stock.ST_IN_DATE, 
    db2admin.stock.ST_SALE_DATE, db2admin.stock.ST_PICKED_QTY, db2admin.stock.ST_STARTDATE, db2admin.stock.ST_ENDDATE, db2admin.stock.ST_ADJUSTMENT_DATE, db2admin.stock.ST_SR_QTY, 
    db2admin.stock.ST_HOLD_PUR_PO, db2admin.stock.ST_HOLD_REP_PO from db2admin.stock """