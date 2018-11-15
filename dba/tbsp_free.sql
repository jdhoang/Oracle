col tablespace_name format a30 heading "Tablespace"
col sizemb          heading "Size (Mb)"
col freemb          heading "Free (Mb)"

set numformat 999,999,999,999
select tbsp.tablespace_name, tbs_size SizeMb, free.free_space FreeMb
  from (select tablespace_name, round(sum(bytes)/1024/1024 ,2) as free_space
          from dba_free_space group by tablespace_name) free,
       (select tablespace_name, sum(bytes)/1024/1024 as tbs_size
          from dba_data_files group by tablespace_name
         UNION
        select tablespace_name, sum(bytes)/1024/1024 tbs_size
          from dba_temp_files group by tablespace_name ) tbsp
 where free.tablespace_name(+)= tbsp.tablespace_name
 order by 1;

