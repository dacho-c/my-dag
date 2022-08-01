def sql_ET_dblake(tb, lym):
    if tb == 'sales_by_item':
        return """SELECT db2admin.PART_INV_DETAIL.* 
            FROM db2admin.PART_INV_DETAIL 
            LEFT JOIN db2admin.PART_INV_HEAD ON db2admin.PART_INV_DETAIL.PID_TICKET_ID = db2admin.PART_INV_HEAD.PIH_TICKET_ID 
            WHERE db2admin.PART_INV_HEAD.PIH_ACCOUNT_MONTH >= '%s'""" % (lym)
    elif tb == 'machine':
        return """'%s'""" % (lym)
    else: 
        return ""

def sql_L_warehouse(tb, lym):
    if tb == 'sales_by_item':
        return """DELETE FROM kp_invoice_detail 
            WHERE kp_invoice_detail.PID_TICKET_ID IN (SELECT PIH_TICKET_ID FROM kp_invoice_head WHERE PIH_ACCOUNT_MONTH >= '%s');""" % (lym)
    elif tb == 'machine':
        return """'%s'""" % (lym)
    else:
        return ""