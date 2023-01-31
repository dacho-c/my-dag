def sql_ETL_stockendmonth():
    strsql = """set timezone = 'Asia/Bangkok';
    select to_char(now() - INTERVAL '1 DAY','YYYYMM') || ks.st_org_id || ks.st_wh_id || ks.st_sr_id || ks.st_lo_id || ks.st_pro_komcode as item_id
    ,to_char(now() - INTERVAL '1 DAY','YYYYMM') ym ,ks.st_org_id ,kb.br_sname ,ks.st_dep_id ,ks.st_wh_id ,kw.wh_name ,ks.st_sr_id ,ks.st_lo_id ,ks.st_pro_komcode
    ,kp.pro_name ,ks.st_enable_stock ,to_char(ks.st_lastintime,'YYYY-MM-DD') as last_instock ,to_char(ks.st_lastouttime,'YYYY-MM-DD') as last_outstock ,to_char(now() - INTERVAL '1 DAY','YYYY-MM-DD') as date_check
    ,case when ks.st_lastouttime < to_date('2000-01-01','YYYY-MM-DD') then EXTRACT(DAY FROM (now() - INTERVAL '1 DAY')-ks.st_lastintime) else EXTRACT(DAY FROM (now() - INTERVAL '1 DAY')-ks.st_lastouttime) end st_date
    from kp_stock ks 
    join kp_branch kb on(ks.st_org_id = kb.br_id)
    join kp_warehouse kw on(ks.st_wh_id = kw.wh_id)
    join kp_part kp on(ks.st_pro_komcode = kp.pro_komcode)
    where ks.st_status = '1' and ks.st_enable_stock <> 0;"""
    return strsql
    