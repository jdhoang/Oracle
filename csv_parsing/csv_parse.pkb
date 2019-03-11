REM ============================================================================
REM Name: csv_parse.pkb
REM
REM Description:
REM Package Body for CSV_PARSE which parse CLOB csv files into appropriate
REM staging tables.
REM
REM Assumptions:
REM
REM ============================================================================
REM Revision History:
REM Date       By        Comment
REM ---------- --------- -----------------------------------------------------
REM 02/22/2019 jhoang    Original release.
REM 02/25/2019 jhoang    Add pipelined function to return load status.
REM 02/27/2019 jhoang    Add procedure to load all Staing tables.
REM 03/08/2019 jhoang    Add capability to handle quoted cells and add mapping
REM                      for checklists files.
REM ============================================================================


-- =====================================================================
-- CSV_PARSE Package Body
-- =====================================================================

create or replace package body csv_parse as

   type MapRecTyp is record
      (fil_name                   varchar2 (132)
      ,tbl_name                   varchar2 (132)
      );

   type ColRecTyp is record
      (col_name                   varchar2 (128)
      ,data_type                  varchar2 (128)
      );

   type MapArrTyp   is table of MapRecTyp        index by binary_integer;
   type ColArrTyp   is table of ColRecTyp        index by binary_integer;
   type VCharArrTyp is table of varchar2 (32000) index by binary_integer;

   MapArr                         MapArrTyp;
   DT_FMT                         varchar2 (100) := '''yyyy-mm-dd hh24:mi:ss''';
   TS_FMT                         varchar2 (100) := '''yyyy-mm-dd hh24:mi:ss.ff''';


-- =====================================================================
-- GET_TB: Map File Name to Oracle Table
-- =====================================================================

function get_tb
        (file_name                      in     varchar2) return varchar2 is

   tb_name                        varchar2 (132);

begin

   for idx in 1..MapArr.count loop
      if lower (MapArr(idx).fil_name) = lower (file_name) then
         tb_name := upper (MapArr(idx).tbl_name);
         exit;
      end if;
   end loop;
   return tb_name;

end get_tb;


-- =====================================================================
-- GET_STG_COLS: Get Staging Table Columns and Data Types
-- =====================================================================

procedure get_stg_cols
         (tb_name                        in     varchar2
         ,col_arr                        in out ColArrTyp) is

   cnt                            binary_integer := 0;

begin

   for col in (select column_name
                     ,data_type
                     ,data_length
                 from user_tab_columns
                where table_name = tb_name
                order by column_id) loop
      cnt := cnt + 1;
      col_arr (cnt).col_name  := col.column_name;
      case
         when col.data_type = 'CLOB' then
            col_arr (cnt).data_type := 'CLOB';
         when col.data_type = 'VARCHAR2' and col.data_length = 4000 then
            col_arr (cnt).data_type := 'VARCHAR4';
         else
            col_arr (cnt).data_type := col.data_type;
      end case;
   end loop;

end get_stg_cols;


-- =====================================================================
-- Load: Load CLOB and Parse into Staging Tables
-- =====================================================================

procedure load
         (file_name                      in     varchar2
         ,status                         in out varchar2
         ,delimiter                      in     varchar2 default '|'
         ,debug                          in     varchar2 default null) is

   inp_clob                       clob;
   stg_cols                       ColArrTyp;
   col_vals                       VCharArrTyp;

   coff                           number;
   clob_len                       number;
   elin                           number;
   nlin                           number := 0;
   nins                           number := 0;
   num_cols                       number;
   idx                            number;
   nval                           number;
   cnt_delim                      number;
   ccnt                           binary_integer;
   vcnt                           binary_integer;

   quotes                         boolean;
   last_flg                       boolean := false;
   tb_name                        varchar2 (132);
   lin                            varchar2 (32000);
   val                            varchar2 (32000);
   sql_stmt                       varchar2 (32000);
   val_stmt                       clob;
   clob_val1                      clob;
   clob_val2                      clob;
   clob_val3                      clob;
   clob_val4                      clob;
   vchr1                          varchar2 (32000);
   vchr2                          varchar2 (32000);
   vchr3                          varchar2 (32000);
   vchr4                          varchar2 (32000);

   cursor get_clob (fname varchar2) is
      select fil_contents
        from stg_clob
       where lower (fil_name) = lower (fname) || '.txt'
          or lower (fil_name) = lower (fname);
          

begin

   -- =====================================================================
   -- Get Oracle Table
   -- =====================================================================

   tb_name := get_tb (file_name);
   if tb_name is null then
      status := '**Error - File Mapping to Table not found';
      return;
   end if;

   get_stg_cols (tb_name, stg_cols);
   num_cols := stg_cols.count;


   -- =====================================================================
   -- Retrieve CLOB
   -- =====================================================================

   open get_clob (file_name);
   fetch get_clob into inp_clob;
   if get_clob%notfound then
      close get_clob;
      status := 'Error - CLOB not found';
      return;
   end if;
   close get_clob;
   clob_len := dbms_lob.getlength (inp_clob);


   -- =====================================================================
   -- Loop through each line(LF)
   -- =====================================================================

   coff := 1;
   while (coff <= clob_len) loop

      elin := instr (inp_clob, chr(10), coff, 1);
      if elin = 0 then
         lin := dbms_lob.substr (inp_clob, 32767, coff);
         last_flg := true;
      else
         lin := dbms_lob.substr (inp_clob, elin - coff, coff);
         if debug is not null then
            dbms_output.put_line (rpad('=',80,'='));
            dbms_output.put_line (lin);
            dbms_output.put_line (rpad('=',80,'='));
         end if;
      end if;

      --cnt_delim := regexp_count (lin, '\'||delimiter);
      --dbms_output.put_line ('Num Delimiter = '||cnt_delim);

      nlin := nlin + 1;
      if nlin > 1 then

         -- =====================================================================
         -- Parse values
         -- =====================================================================

         nval := 0;
         if substr (lin, 1, 1) = '"' then
            quotes := true;
         else
            quotes := false;
         end if;

         while (nval < num_cols) loop
            if quotes then
               idx  := instr (lin, '"');
            else
               idx  := instr (lin, delimiter);
            end if;

            if idx > 0 then
               val := substr (lin, 1, idx-1);
            else

               -- ===============================================================
               -- If not last value, value has continued on next line
               -- ===============================================================

               val := lin;
               if nval+1 < num_cols or quotes then

                  loop
                     coff := elin + 1;
                     elin := instr (inp_clob, chr(10), coff, 1);
                     if elin = 0 then
                        lin := dbms_lob.substr (inp_clob, 32767, coff);
                        val := val || chr(10) || lin;
                        last_flg := true;
                        exit;
                     else
                        lin := dbms_lob.substr (inp_clob, elin - coff, coff);
                     end if;
                     if quotes then
                        idx  := instr (lin, '"');
                     else
                        idx  := instr (lin, delimiter);
                     end if;

                     if idx > 0 then
                        val := val || chr(10) || substr (lin, 1, idx-1);
                        if quotes then
                           idx := idx + 1;
                        end if;
                        exit;
                     else
                        val := val || chr(10) || lin;
                     end if;
                  end loop;

               end if;
            end if;

            nval := nval + 1;
            if substr (val, length (val), 1) = chr(13) then
               val := substr (val, 1, length (val)-1);
            end if;
            col_vals (nval) := trim (replace (val, '''',''''''));

            if quotes then
               lin := substr (lin, idx+2);
            else
               lin := substr (lin, idx+1);
            end if;
            if substr (lin, 1, 1) = '"' then
               quotes := true;
               lin := substr (lin, 2);
            else
               quotes := false;
            end if;
            --if nlin = 2 then
               --dbms_output.put_line ('-- Val = *'||col_vals(nval)||'*');
               --dbms_output.put_line (lin);
            --end if;
         end loop;


         -- =====================================================================
         -- Create INSERT DML and dynamically insert
         -- =====================================================================

         sql_stmt := 'insert into '||tb_name||' values (';
         val_stmt := ''''||col_vals(1)||'''';

         ccnt := 0;
         vcnt := 0;
         for idx in 2..col_vals.count loop

            if col_vals(idx) is null and stg_cols(idx).data_type not in ('CLOB','VARCHAR4') then
               val := 'null';
            else

               case stg_cols(idx).data_type
                  when 'VARCHAR2' then
                     val := ''''||col_vals (idx)||'''';
                  when 'CLOB' then
                     ccnt := ccnt + 1;
                     val  := ':clob_val'||ccnt;
                     case ccnt
                        when 1 then
                           clob_val1 := col_vals (idx);
                        when 2 then
                           clob_val2 := col_vals (idx);
                        when 3 then
                           clob_val3 := col_vals (idx);
                        when 4 then
                           clob_val4 := col_vals (idx);
                     end case;

                  when 'VARCHAR4' then
                     vcnt := vcnt + 1;
                     val  := ':vchr'||vcnt;
                     case vcnt
                        when 1 then
                           vchr1 := col_vals (idx);
                        when 2 then
                           vchr2 := col_vals (idx);
                        when 3 then
                           vchr3 := col_vals (idx);
                        when 4 then
                           vchr4 := col_vals (idx);
                     end case;

                  when 'DATE' then
                     if instr (col_vals(idx),'.') > 0 then
                        val := 'to_timestamp('''||col_vals(idx)||''','||ts_fmt||')';
                     else
                        val := 'to_date('''||col_vals(idx)||''','||dt_fmt||')';
                     end if;

                  else
                     val := col_vals (idx);
               end case;
            end if;
            val_stmt := val_stmt || ',' || val;

         end loop;

         if debug is not null then
            dbms_output.put_line (sql_stmt);
            dbms_output.put_line (val_stmt || ')');
         else
            begin
               if ccnt = 0 and vcnt = 0 then
                  execute immediate sql_stmt || val_stmt || ')';
               elsif ccnt > 0 then
                  case ccnt
                     when 1 then
                        execute immediate sql_stmt || val_stmt || ')' using clob_val1;
                     when 2 then
                        execute immediate sql_stmt || val_stmt || ')' using clob_val1, clob_val2;
                     when 3 then
                        execute immediate sql_stmt || val_stmt || ')' using clob_val1, clob_val2, clob_val3;
                     when 4 then
                        execute immediate sql_stmt || val_stmt || ')' using clob_val1, clob_val2, clob_val3, clob_val4;
                  end case;
               elsif vcnt > 0 then
                  case vcnt
                     when 1 then
                        execute immediate sql_stmt || val_stmt || ')' using vchr1;
                     when 2 then
                        execute immediate sql_stmt || val_stmt || ')' using vchr1, vchr2;
                     when 3 then
                        execute immediate sql_stmt || val_stmt || ')' using vchr1, vchr2, vchr3;
                     when 4 then
                        execute immediate sql_stmt || val_stmt || ')' using vchr1, vchr2, vchr3, vchr4;
                  end case;
               end if;
               nins := nins + 1;
            exception
               when others then
                  dbms_output.put_line (sql_stmt);
                  dbms_output.put_line (val_stmt || ')');
                  dbms_output.put_line (sqlerrm);
            end;
         end if;

      end if;
      if last_flg then
         exit;
      end if;
      coff := elin + 1;
      col_vals.delete;

   end loop;

   status := 'Number of Lines Read = '||nlin||' (# Rec Ins: '||nins||')';

exception
   when others then
      status := sqlerrm;

end load;


-- =====================================================================
-- STG_TBL_CNT: Output Staging Tables Row Count
-- =====================================================================

procedure stg_tbl_cnt is
   cnt                            number;
begin
   for idx in 1..MapArr.count loop
      execute immediate 'select /*+ parallel (t) */ count(*) from '||
         MapArr (idx).tbl_name||' t' into cnt;
      dbms_output.put_line (rpad (MapArr (idx).fil_name, 31)||' :'||
      rpad(MapArr (idx).tbl_name,35)||': '||cnt);
   end loop;
end stg_tbl_cnt;


-- =====================================================================
-- STG_TBL_STATUS: Output Staging Tables Row Count via Pipeline
-- =====================================================================

function stg_tbl_status return LoadStatArrTyp pipelined is
   cnt                            number;
begin
   for idx in 1..MapArr.count loop
      execute immediate 'select /*+ parallel (t) */ count(*) from '||
         MapArr (idx).tbl_name||' t' into cnt;
      pipe row (LoadStatRecTyp (MapArr (idx).fil_name,
                                MapArr (idx).tbl_name, cnt));
   end loop;
end stg_tbl_status;


-- =====================================================================
-- LOAD_STG: Load all Staging Tables
-- =====================================================================

procedure load_stg is
   cnt                            binary_integer := 0;
   strt_dt                        date;
   end_dt                         date;
   tim_diff                       varchar2 (100);
   status                         varchar2 (200);
begin
   for idx in 1..MapArr.count loop
      strt_dt := sysdate;
      load (MapArr (idx).fil_name, status);
      end_dt := sysdate;
      tim_diff := to_char(to_date('1970-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss')+(end_dt-strt_dt),'hh24:mi:ss');
      dbms_output.put_line (status||' Elapsed: '||tim_diff);
   end loop;
end load_stg;


-- =====================================================================
-- =====================================================================

begin
   MapArr (1).fil_name  := 'BSA_QUESTIONNAIRE';
   MapArr (1).tbl_name  := 'STG_BSA_QUESTIONNAIRE';
   MapArr (2).fil_name  := 'ComplianceViolations';
   MapArr (2).tbl_name  := 'STG_COMPLIANCEVIOLATIONS';
   MapArr (3).fil_name  := 'CreditUnionWorkload';
   MapArr (3).tbl_name  := 'STG_CREDITuNIONwORKLOAD';
   MapArr (4).fil_name  := 'CU030';
   MapArr (4).tbl_name  := 'STG_CU030';
   MapArr (5).fil_name  := 'CUSO_CUR_CUSOSERVICES';
   MapArr (5).tbl_name  := 'STG_CUSO_CUR_CUSOSERVICES';
   MapArr (6).fil_name  := 'CUSO_CUR_CUSOS';
   MapArr (6).tbl_name  := 'STG_CUSO_CUR_CUSOS';
   MapArr (7).fil_name  := 'CuSO_CUR_CUSTOMERS';
   MapArr (7).tbl_name  := 'STG_CuSO_CUR_CUSTOMERS';
   MapArr (8).fil_name  := 'CUSO_CUR_ENTITYTYPES';
   MapArr (8).tbl_name  := 'STG_CUSO_CUR_ENTITYTYPES';
   MapArr (9).fil_name  := 'CUSO_CUR_GENERAL_INFO';
   MapArr (9).tbl_name  := 'STG_CUSO_CUR_GENERALINFORMATIONS';
   MapArr (10).fil_name := 'CUSO_CUR_NONCUSTOMEROWNERS';
   MapArr (10).tbl_name := 'STG_CUSO_CUR_NONCUSTOMEROWNERS';
   MapArr (11).fil_name := 'CUSO_CUR_SERVICEITEMS';
   MapArr (11).tbl_name := 'STG_CUSO_CUR_SERVICEITEMS';
   MapArr (12).fil_name := 'DOIFOMExpansion';
   MapArr (12).tbl_name := 'STG_DOIFOMExpansion';
   MapArr (13).fil_name := 'DOIMain';
   MapArr (13).tbl_name := 'STG_DOIMain';
   MapArr (14).fil_name := 'EmployeeDistrict';
   MapArr (14).tbl_name := 'STG_EmployeeDistrict';
   MapArr (15).fil_name := 'Employee';
   MapArr (15).tbl_name := 'STG_Employee';
   MapArr (16).fil_name := 'EX640a';
   MapArr (16).tbl_name := 'STG_EX640a';
   MapArr (17).fil_name := 'EX640c';
   MapArr (17).tbl_name := 'STG_EX640c';
   MapArr (18).fil_name := 'EX640_State';
   MapArr (18).tbl_name := 'STG_EX640_State';
   MapArr (19).fil_name := 'Ex640';
   MapArr (19).tbl_name := 'STG_Ex640';
   MapArr (20).fil_name := 'IRR_WorkBook';
   MapArr (20).tbl_name := 'STG_IRR_WorkBook';
   MapArr (21).fil_name := 'IST';
   MapArr (21).tbl_name := 'STG_IST';
   MapArr (22).fil_name := 'LRQ';
   MapArr (22).tbl_name := 'STG_LRQ';
   MapArr (23).fil_name := 'PRDORPROBLEM';
   MapArr (23).tbl_name := 'STG_PRDORPROBLEM';
   MapArr (24).fil_name := 'PRProblemRecord';
   MapArr (24).tbl_name := 'STG_PRProblemRecord';
   MapArr (25).fil_name := 'ResourceAllocation';
   MapArr (25).tbl_name := 'STG_ResourceAllocation';
   MapArr (26).fil_name := 'RiskSumThresholds';
   MapArr (26).tbl_name := 'STG_RiskSumThresholds';
   MapArr (27).fil_name := 'SDBudgetHours';
   MapArr (27).tbl_name := 'STG_SDBudgetHours';
   MapArr (28).fil_name := 'SDBudgetPlans';
   MapArr (28).tbl_name := 'STG_SDBudgetPlans';
   MapArr (29).fil_name := 'SDERA';
   MapArr (29).tbl_name := 'STG_SDERA';
   MapArr (30).fil_name := 'SDExamScope';
   MapArr (30).tbl_name := 'STG_SDExamScope';
   MapArr (31).fil_name := 'SDNEXTEXAMRISK';
   MapArr (31).tbl_name := 'STG_SDNEXTEXAMRISK';
   MapArr (32).fil_name := 'SDPEPQANSWER';
   MapArr (32).tbl_name := 'STG_SDPEPQANSWER';
   MapArr (33).fil_name := 'SDReviewStateExam';
   MapArr (33).tbl_name := 'STG_SDReviewStateExam';
   MapArr (34).fil_name := 'SDScopeDoc';
   MapArr (34).tbl_name := 'STG_SDScopeDoc';
   MapArr (35).fil_name := 'STATIC_PeerRatios';
   MapArr (35).tbl_name := 'STG_STATIC_PeerRatios';

   MapArr (36).fil_name := 'ck_names.csv';
   MapArr (36).tbl_name := 'CK_STG_CHECKLIST';
   MapArr (37).fil_name := 'ck_questions.csv';
   MapArr (37).tbl_name := 'CK_STG_CHECKLIST_QST';

end csv_parse;
/
sho errors
