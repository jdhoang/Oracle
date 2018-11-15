rem ============================================================================
rem Name:   gen_view.sql
rem
rem Description:
rem Generate Views DDL from FDM Data Dictionary.
rem
rem Assumptions:
rem
rem ============================================================================
rem Revision History:
rem Date       By        Comment
rem ---------- --------- -----------------------------------------------------
rem 11/16/2017 jhoang    Original release.
rem 08/22/2018 jhoang    Add CASTing to match SAS data types.
rem 08/28/2018 jhoang    Add lookup to match SAS data types.
rem
rem ============================================================================

set trimspool on linesize 1000
set serverout on size 1000000 format wrapped
set verify off feedback off timing off escape on

accept vnm prompt "Generate DDL for View (Source Name): "
spool vw_&&vnm..sql

declare

   type map_rec_type is record (
      field_nme         varchar2 (128)
     ,fdm_col_nme       varchar2 (128)
     ,col_len           number
     ,src_data_type     varchar2 (100)
     ,src_len           number
     ,src_precision     number
      );

   type ArrMapRecTyp is table of map_rec_type index by binary_integer;

   cursor cur_tb (src varchar2) is
      select id
            ,upper (trim (fdm_tbl_nme))
        from fdm_tbl
       where upper (src_tbl_nme) = src;

   cursor cur_mapping (tid number, tnm varchar2) is
      select field_nme
            ,upper (fdm_col_nme)
            ,col_len
            ,src_data_type
            ,src_len
            ,src_precision
        from fdm_col
       where fdm_tbl_id = tid
         and fdm_col_nme not like tnm||'_OBJ_ID'
         and field_nme is not null
       order by col_order;

   MapArr            ArrMapRecTyp;
   tbid              number;
   vwcnt             binary_integer := 0;
   vwnm              varchar2 (128) := upper (trim ('&&vnm'));
   tblnm             varchar2 (128);
   ctype             varchar2 (500);
   lsep              varchar2 (10)  := '(';
   sch                            varchar2 (100) := '\&schema';
   fpe_sch                        varchar2 (100) := '\&fpe_sch';
   sel_env                        varchar2 (4000) :=
'col fpe_schema  new_value fpe_sch noprint '||chr(13)||
'col global_name new_value schema  noprint '||chr(13)||
'set scan on verify off feedback off '||chr(13)||chr(13)||
'select case substr (global_name, 1, instr (global_name, ''.'')-1) '||chr(13)||
'          when ''DOFPE100'' then ''fpesfd'' '||chr(13)||
'          when ''TOFPE100'' then ''fpesft'' '||chr(13)||
'          when ''AOFPE100'' then ''fpesfa'' '||chr(13)||
'          when ''AOFPE001'' then ''fpesfr'' '||chr(13)||
'       end  global_name '||chr(13)||
'      ,case substr (global_name, 1, instr (global_name, ''.'')-1) '||chr(13)||
'          when ''DOFPE100'' then ''fpedevl'' '||chr(13)||
'          when ''TOFPE100'' then ''fpetest'' '||chr(13)||
'          when ''AOFPE100'' then ''fpeacpt'' '||chr(13)||
'          when ''AOFPE001'' then ''fpeperf'' '||chr(13)||
'       end  fpe_schema '||chr(13)||
'  from global_name;'||chr(13)||chr(13)||
'set feedback 20';

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

   open cur_tb (vwnm);
   fetch cur_tb into tbid, tblnm;
   if cur_tb%notfound then
      close cur_tb;
      dbms_output.put_line ('Table '||vwnm||' does not exist in Data Dictionary');
      return;
   end if;
   close cur_tb;

   pr (sel_env);

   dbms_output.put_line (rpad ('rem =',50,'='));
   dbms_output.put_line ('rem VW_'||vwnm);
   dbms_output.put_line (rpad ('rem =',50,'=')||chr(10));
   dbms_output.put_line ('create or replace view '||sch||'..VW_'||vwnm);

   open cur_mapping (tbid, vwnm);
   fetch cur_mapping bulk collect into MapArr;
   close cur_mapping;

   for col in 1..MapArr.count loop
      dbms_output.put_line ('      '||lsep||nvl(MapArr(col).field_nme,'--Missing for '||MapArr(col).fdm_col_nme));
      lsep := ',';
   end loop;

   dbms_output.put_line (')');
   dbms_output.put_line (' as');
   dbms_output.put_line ('select ');

   lsep := ' ';
   for col in 1..MapArr.count loop
      if MapArr(col).src_data_type is not null and
         MapArr(col).fdm_col_nme not like '%_DT' and
         MapArr(col).fdm_col_nme not like '%_DTTM' then

         case
            when MapArr(col).src_data_type = 'Char' then
               ctype := 'varchar2 ('||MapArr(col).src_len||')';
            else
               ctype := 'number';
         end case;

         dbms_output.put_line ('      '||lsep||'cast('||MapArr(col).fdm_col_nme||
                               ' as '||ctype||')');
      else
         dbms_output.put_line ('      '||lsep||upper(MapArr(col).fdm_col_nme));
      end if;
      lsep := ',';
   end loop;
   dbms_output.put_line ('  from '||sch||'..'||tblnm||';'||chr(10));

   dbms_output.put_line ('create or replace public synonym VW_'||vwnm||' for '||sch||'..VW_'||vwnm||';'||chr(10));
   dbms_output.put_line ('grant select on '||sch||'..VW_'||vwnm||' to APPL_FPE_SF_READ;');
   dbms_output.put_line ('grant select on '||sch||'..VW_'||vwnm||' to APPL_FPE_SF_WRITE;');
   dbms_output.put_line ('grant select on '||sch||'..VW_'||vwnm||' to '||fpe_sch||';');

end;
/
spool off

prompt ========================================================================
prompt Spooled output to vw_&&vnm..sql
prompt ========================================================================

set linesize 80 feedback on
