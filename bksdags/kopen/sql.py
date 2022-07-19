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
    else:
        return ""