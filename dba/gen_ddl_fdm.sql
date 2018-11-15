rem ============================================================================
rem Name:   gen_ddl_fdm.sql
rem
rem Description:
rem Generate DDL from FDM Data Dictionary.  
rem
rem Assumptions:
rem
rem ============================================================================
rem Revision History:
rem Date       By        Comment
rem ---------- --------- -----------------------------------------------------
rem 11/16/2017 jhoang    Original release.
rem
rem ============================================================================


set trimspool on linesize 1000
set serverout on size 100000 format wrapped
set verify off feedback off timing off escape on

accept tbn prompt "Generate DDL for Table: "
spool cr_&&tbn..sql

declare

   type VcharArrTyp is table of varchar2(100);

   UserArr                        VcharArrTyp := VcharArrTyp
                                  ('alumam','gaummj','alurdr','gauxdz','g3us3r','fpepamd'
                                  ,'fpedevl','APPL_FPE_SF_WRITE');
   colcnt                         binary_integer := 0;
   tbname                         varchar2 (128) := upper ('&&tbn');
   coltyp                         varchar2 (2000);
   colsep                         varchar2 (200) := '(';
   tbl_id                         number;
   schema_nm                      varchar2 (128);
   seq                            varchar2 (128);
   sch                            varchar2 (100) := '\&schema';
   fpe_sch                        varchar2 (100) := '\&fpe_sch';
   sel_env                        varchar2 (4000) :=
'col fpe_schema  new_value fpe_sch noprint '||chr(13)||
'col global_name new_value schema  noprint '||chr(13)||
'set scan on verify off feedback off '||chr(13)||chr(13)||
'select case substr (global_name, 1, instr (global_name, ''.'')-1) '||chr(13)||
'          when ''DOFPE100'' then ''fpergd'' '||chr(13)||
'          when ''TOFPE100'' then ''fpergt'' '||chr(13)||
'          when ''AOFPE100'' then ''fperga'' '||chr(13)||
'          when ''AOFPE001'' then ''fpergr'' '||chr(13)||
'       end  global_name '||chr(13)||
'      ,case substr (global_name, 1, instr (global_name, ''.'')-1) '||chr(13)||
'          when ''DOFPE100'' then ''fpedevl'' '||chr(13)||
'          when ''TOFPE100'' then ''fpetest'' '||chr(13)||
'          when ''AOFPE100'' then ''fpeacpt'' '||chr(13)||
'          when ''AOFPE001'' then ''fpeperf'' '||chr(13)||
'       end  fpe_schema '||chr(13)||
'  from global_name;'||chr(13)||chr(13)||
'set feedback on';


   cursor cur_obj_id is
      select fdm_col_nme
        from fdm_col  col
             inner join fdm_tbl t on (t.fdm_tbl_nme = tbname and t.id = col.fdm_tbl_id)
       where fdm_col_nme like tbname||'_OBJ_ID';

   cursor get_tb is
      select tbl.id
            ,case sch.schema_nme
                when 'Single Family' then 'fpesfd'
                when 'Multi Family'  then 'fpemfd'
                else 'fpergd'
             end     schema_nme
        from fpergd.fdm_tbl          tbl
             inner join fpergd.fdm_schema sch
                   on (sch.id = tbl.fdm_schema_id)
       where tbl.fdm_tbl_nme = tbname;

   -- ======================================================
   -- ======================================================

   procedure pr (ilin varchar2) is
      idx                            binary_integer;
      lin                            varchar2 (32767) := ilin;
   begin
      while (lin is not null) loop
         idx := instr (lin, chr(13));
         if idx > 0 then
            dbms_output.put_line (substr (lin, 1, idx-1));
         else
            dbms_output.put_line (lin);
         end if;
         exit when idx = 0;
         lin := substr (lin, idx+1);
      end loop;
   end;

begin

   -- ================================================================
   -- Verify Table in Data Dictionary
   -- ================================================================

   open get_tb;
   fetch get_tb into tbl_id, schema_nm;
   if get_tb%notfound then
      close get_tb;
      dbms_output.put_line ('Table '||tbname||' not found in Data Dictionary');
      return;
   end if;
   close get_tb;

   -- ================================================================
   -- Determine whether to create sequence
   -- ================================================================

   pr (sel_env);

   open cur_obj_id;
   fetch cur_obj_id into seq;
   if cur_obj_id%found then
      seq := tbname || '_SEQ';
      dbms_output.put_line ('create sequence '||sch||'..'||seq||';');
      dbms_output.put_line ('grant select on '||sch||'..'||seq||' to appl_fpe_sf_write;');
      dbms_output.put_line ('grant select on '||sch||'..'||seq||' to '||fpe_sch||';'||chr(10));
   end if;
   close cur_obj_id;

   -- ================================================================
   -- Generate DDL
   -- ================================================================

   for rec in (select fdm_col_nme
                     ,col_data_type
                     ,col_len
                     ,col_precision
                     ,(case col_data_type
                         when 'VARCHAR2'  then 'VARCHAR2 ('||col_len||')'
                         when 'VARCHAR'   then 'VARCHAR2 ('||col_len||')'
                         when 'NUMERIC'   then 'NUMBER'
                         when 'NUMBER'    then 'NUMBER'
                         else col_data_type
                       end)   data_type
                     ,decode (col_nullable, 'NN', 'not null') col_nullable
                 from fpergd.fdm_col
                where fdm_tbl_id = tbl_id
                order by col_order) loop

      colcnt := colcnt + 1;
      if colcnt = 1 then
         dbms_output.put_line (rpad ('rem =',50,'='));
         dbms_output.put_line ('rem '||tbname);
         dbms_output.put_line (rpad ('rem =',50,'=')||chr(10));
         dbms_output.put_line ('create table '||sch||'..'||tbname);
      end if;

      if rec.data_type = 'NUMBER' and (rec.col_precision is null or rec.col_precision = 0) then
         coltyp := rpad ('integer', 15)||rec.col_nullable;
      else
         coltyp := rpad (lower(rec.data_type),15)||rec.col_nullable;
      end if;
      if rec.fdm_col_nme like tbname||'_OBJ_ID' then
         coltyp := rpad (coltyp,19) || 'default fpesfd.'||tbname||'_SEQ.nextval';
      end if;
      dbms_output.put_line (colsep||rpad(rec.fdm_col_nme,31)||coltyp);
      colsep := ',';

   end loop;

   if colcnt > 0 then

      dbms_output.put_line (',CREATED_DT                     date               default sysdate');
      dbms_output.put_line (',CREATED_BY                     varchar2 (100)     default user');
      dbms_output.put_line (') tablespace fpe_data_01;'||chr(10));
      dbms_output.put_line ('create public synonym '||tbname||' for '||sch||'..'||tbname||';'||chr(10));

      /*
      for usr in Userarr.first..UserArr.last loop
         dbms_output.put_line ('grant select,insert,update,delete on '||sch||'..'||tbname||
                               ' to '||UserArr(usr)||';');
      end loop;
      */
      dbms_output.put_line ('grant select,insert,update,delete on '||sch||'..'||tbname||' to APPL_FPE_SF_WRITE;');
      dbms_output.put_line ('grant select,insert,update,delete on '||sch||'..'||tbname||' to '||fpe_sch||';');
      dbms_output.put_line ('grant select on '||sch||'..'||tbname||' to APPL_FPE_SF_READ;');

   else
      dbms_output.put_line ('-- No Columns Defined in Data Dictionary');
   end if;

end;
/

spool off

prompt ========================================================================
prompt Spooled output to cr_&&tbn..sql
prompt ========================================================================

set linesize 80 feedback on

