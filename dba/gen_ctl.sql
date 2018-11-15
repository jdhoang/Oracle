set serverout on size 1000000 format wrapped
set linesize 200 trimspool on
set pagesize 1000
set verify off

prompt ===========================================
prompt Generate SQL*Loader Control Script
prompt ===========================================
prompt

accept tbnm prompt "Prompt Table Name: "
rem accept cstr prompt "Prompt CSV Header: "

declare

   idx                            number;
   tbl_id                         number;
   cnt                            binary_integer := 0;
   prefix                         varchar2 (1)   := '(';
   vcol                           varchar2 (100);
   pcol                           varchar2 (100);
   coltype                        varchar2 (100);

   tbname                         varchar2 (100) := upper (trim('&&tbnm'));
   schema_nm                      varchar2 (100);
   str                            varchar2 (32000) := 
'FNMA_LN,MOD_START_DTE,DLQ_VALID_DTE,MOD_CURR_DTE,PRE_MOD_PMT,POST_MOD_PMT,PRE_MOD_REMTRM,POST_MOD_REMTRM,TRIAL_OUTCOME,TRIAL_END_DTE,UPB_FORBORN,pre_mod_rate,post_mod_rate,PRE_MOD_UPB,POST_MOD_UPB,mod_type,STEP_RATE_INI_FIXED_MTHS,STEP_RATE_PER_ADJ_MTHS,STEP_RATE_ADJ_AMT,STEP_RATE_TARGET,dlqpmod,n_hamp,n_hamp_s,n_alt,n_alt_s,n_hsa,n_hsf,n_hsf_s,n_ltf,n_ltf_s,n_rpp,n_rpp_s,n_std,n_alt2011,n_alt2011_s,n_fnmamod,n_fnmamod_s,n_flexmod,n_flexmod_s,n_mod24,n_mod24_s,n_alt30,n_alt30_s,prv_hamp_paych_gt_10pct,last_hamp_start_date,Mod_Count_S';



   cursor cur_get_phycol (vcol varchar2) is
      select fdm_col_nme
            ,col_data_type
            ,(case when col_data_type like 'DATE%' then 'date ''mm/dd/yyyy'''
                   else 'char' end)  coltype
        from fpergd.fdm_col
       where fdm_tbl_id = tbl_id
         and trim(upper(field_nme)) = trim(upper(vcol));

   phyrow                         cur_get_phycol%rowtype;

   cursor get_tb is
      select tbl.id
            ,case sch.schema_nme
                when 'Single Family' then 'FPESFD'
                when 'Multi Family'  then 'FPESFD'
                else 'FPERGD'
             end     schema_nme
        from fpergd.fdm_tbl          tbl
             inner join fpergd.fdm_schema sch
                   on (sch.id = tbl.fdm_schema_id)
       where tbl.fdm_tbl_nme = tbname;

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

   dbms_output.put_line ('options (skip=1)');
   dbms_output.put_line ('load data');
   dbms_output.put_line ('infile      '''||tbname||'.csv''');
   dbms_output.put_line ('badfile     '''||tbname||'.bad''');
   dbms_output.put_line ('discardfile '''||tbname||'.dis''');
   dbms_output.put_line ('append into table fpesfd.'||tbname);
   dbms_output.put_line ('fields terminated by "," optionally enclosed by ''"''');
   dbms_output.put_line ('trailing nullcols');
   
   while (str is not null) loop

      idx  := instr (str, ',');
      if idx > 0 then
         vcol := substr (str, 1, idx-1);
      else
         vcol := str;
      end if;

      open cur_get_phycol (vcol);
      fetch cur_get_phycol into phyrow;
      if cur_get_phycol%notfound then
         dbms_output.put_line (prefix||rpad (vcol,31)||'filler');
      else
         if upper (phyrow.col_data_type) in ('NUMBER','NUMERIC') then
            coltype := '"REGEXP_REPLACE(:'||phyrow.fdm_col_nme||',''\\$|,|\\(|\\)|\\%'','''')"';
         else
            coltype := phyrow.coltype;
         end if;
         if length(phyrow.fdm_col_nme) < 31 then
            dbms_output.put_line (prefix||rpad (phyrow.fdm_col_nme,31)||coltype);
         else
            dbms_output.put_line (prefix||phyrow.fdm_col_nme||'--'||coltype);
         end if;
      end if;
      close cur_get_phycol;

      exit when idx = 0;
      cnt := cnt + 1;
      str := substr (str, idx+1);
      prefix := ',';

   end loop;

   dbms_output.put_line (prefix||rpad('CASE_ID',31)||'constant 1001');
   dbms_output.put_line (prefix||rpad('CREATED_DT',31)||'"sysdate"');
   dbms_output.put_line (prefix||rpad('CREATED_BY',31)||'constant ''fpedevl''');
   dbms_output.put_line (')');
   
end;
/
sho errors

