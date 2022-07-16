def sql_detail_select(tb, lym):
    if tb == 'part_inv_detail':
        return """SELECT db2admin.PART_INV_DETAIL.* 
            FROM db2admin.PART_INV_DETAIL 
            LEFT JOIN db2admin.PART_INV_HEAD ON db2admin.PART_INV_DETAIL.PID_TICKET_ID = db2admin.PART_INV_HEAD.PIH_TICKET_ID 
            WHERE db2admin.PART_INV_HEAD.PIH_ACCOUNT_MONTH >= '%s'""" % (lym)
    else:
        return ""

def sql_detail_delete(tb, lym):
    if tb == 'kp_invoice_detail':
        return """DELETE FROM kp_invoice_detail 
            WHERE kp_invoice_detail.PID_TICKET_ID IN (SELECT PIH_TICKET_ID FROM kp_invoice_head WHERE PIH_ACCOUNT_MONTH >= '%s');""" % (lym)
    else:
        return ""