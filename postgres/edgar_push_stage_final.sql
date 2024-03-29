insert into edgar.sub
select 
coalesce(adsh, 'nvs')
,cik
,name
,sic
,countryba
,stprba
,cityba
,zipba
,former
,to_date(changed::text, 'yyyymmdd')
,afs
,cast(wksi as boolean)
,fye
,form
,to_date(period::text, 'yyyymmdd')
,fy
,fp
,to_date(filed::text, 'yyyymmdd')
,cast(prevrpt as boolean)
,detail
,instance
,nciks
,aciks
,sec_qtr
from edgar.sub_stage;



insert into edgar.tag_bad
select *
from edgar.tag_stage as s
where exists (
	select 'x'
	from edgar.tag as f
	where f.tag = s.tag
	and f.version = s.version
	);



insert into edgar.tag
select 
coalesce(tag, 'NVS')
,coalesce(version, 'NVS')
,cast(custom as boolean)
,cast(abstract as boolean)
,datatype
,iord
,crdr
,tlabel
,doc
,sec_qtr
from edgar.tag_stage
on conflict (tag, version) 
do nothing;



insert into edgar.num
select 
adsh
,tag
,version
,to_date(ddate::text, 'yyyymmdd')
,qtrs
,uom
,coalesce(coreg, 'NVS')
,value
,footnote
,sec_qtr
from edgar.num_stage;