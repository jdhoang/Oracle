set linesize 200
set pagesize 1000

col mb heading 'Size (Mb)'   heading 999,999,999

col lin format a200
select ddf.tablespace_name||','||file_name||','|| bytes/1024/1024||','||
       dt.extent_management ||','||dt.segment_space_management lin
  from dba_data_files ddf
       inner join dba_tablespaces dt on dt.tablespace_name = ddf.tablespace_name
union
select tablespace_name||','||file_name||','|| bytes/1024/1024 lin 
  from dba_temp_files
 order by 1
/

select tablespace_name, sum(bytes)/1024/1024 mb
  from dba_data_files
 group by tablespace_name
union
select tablespace_name, sum(bytes)/1024/1024
  from dba_temp_files
 group by tablespace_name
/

