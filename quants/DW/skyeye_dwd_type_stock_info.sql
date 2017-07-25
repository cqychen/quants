use ods_data;
select
a.`code`
,a.`name`
,a.c_name
,b.area
,c.c_name
,case when d.`code` is not null then '创业板' else '非创业板' end  isgem
,case when e.`code` is not null then '沪深300' else '非沪深300' end  ishs300
,case when f.`code` is not null then '中小板' else '非中小板' end  issme
,case when g.`code` is not null then 'st' else 'nost' end  isst
,case when h.`code` is not null then 'sz50' else 'nosz50' end  issz50
,case when j.`code` is not null then 'zz500' else 'nozz500' end  iszz500
from ods_classified_basic a
left JOIN ods_classified_area b on(a.`code`=b.`code`)
LEFT JOIN ods_classified_concept c ON(a.`code`=c.`code`)
LEFT JOIN ods_classified_gem d  ON(a.`code`=d.`code`)
LEFT JOIN ods_classified_hs300 e ON(a.`code`=e.`code`)
LEFT JOIN ods_classified_sme f  on(a.`code`=f.`code`)
LEFT JOIN ods_classified_st g on(a.`code`=g.`code`)
LEFT JOIN ods_classified_sz50s h ON(a.`code`=h.`code`)
LEFT JOIN ods_classified_zz500s j ON(a.`code`=j.`code`)
;