select sec_qtr, count(*) from edgar.tag group by 1 order by 1;
select sec_qtr, count(*) from edgar.tag_stage group by 1 order by 1;

select * from edgar.tag_bad where tag = 'ZA';
select * from edgar.tag where tag = 'ZA';

select sec_qtr, count(*) from edgar.num where tag = 'EntityCommonStockSharesOutstanding' group by 1; -- limit 50;

select count(*) from (
	select tag from edgar.tag where sec_qtr = '2018q1'
	union all
	select tag from edgar.tag_bad where sec_qtr = '2018q1'
	) t1;

SELECT tlabel FROM edgar.tag limit 50;