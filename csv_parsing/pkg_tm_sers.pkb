REM ============================================================================
REM Name:   pkg_tm_sers.pkb
REM
REM Description:
REM Package Body for PKG_TM_SERS which parse BLOB csv files into appropriate
REM time series table.
REM
REM Assumptions:
REM
REM ============================================================================
REM Revision History:
REM Date       By        Comment
REM ---------- --------- ------------------------------------------------------
REM 06/26/2018 jhoang    Original release.
REM 10/30/2018 jhoang    Add additional category to handle multiple time series
REM
REM ============================================================================

col fpe_schema  new_value fpe_sch noprint
col global_name new_value schema  noprint
set scan on verify off feedback off

select case substr (global_name, 1, instr (global_name, '.')-1)
          when 'DOFPE100' then 'fpergd'
          when 'TOFPE100' then 'fpergt'
          when 'AOFPE100' then 'fperga'
          when 'AOFPE001' then 'fpergr'
       end  global_name
      ,case substr (global_name, 1, instr (global_name, '.')-1)
          when 'DOFPE100' then 'fpedevl'
          when 'TOFPE100' then 'fpetest'
          when 'AOFPE100' then 'fpeacpt'
          when 'AOFPE001' then 'fpeperf'
       end  fpe_schema
  from global_name;

set feedback on


prompt =============================================================================
prompt PKG_TM_SERS Package Body
prompt =============================================================================

create or replace package body &schema..pkg_tm_sers as

-- =============================================================================
-- Query for SF_MF_DATA_ID
-- =============================================================================

function get_sfmf_id
        (fil_id                         in number
        ,styp                           in varchar2 default null) return varchar2 is

   id                  varchar2 (38);
   asmp_id             varchar2 (38);

   cursor get_id is
      select sf_mf_data_id
        from sf_mf_data_assc
       where file_id = fil_id
         and nvl (appl_asmp_data_typ,'xx') = nvl (styp, 'xx');

   cursor get_asmp_id is
      select appl_asmp_id
        from appl_file_asmp
       where file_id = fil_id;

begin
   open get_id;
   fetch get_id into id;
   if get_id%notfound then
      open get_asmp_id;
      fetch get_asmp_id into asmp_id;
      if get_asmp_id%found then
         insert into sf_mf_data_assc
            values (sf_mf_data_assc_seq.nextval, asmp_id, fil_id, 'SFM', styp)
            returning sf_mf_data_id into id;
      end if;
      close get_asmp_id;
   end if;
   close get_id;
   return id;
end get_sfmf_id;


-- =============================================================================
-- Parse Line Delimited by Comma and Pass Back in Array
-- =============================================================================

procedure parse_lin
         (ilin                           in out varchar2
         ,arr                            in out VCharArrTyp
         ,delimit                        in     varchar2 default ',') is
    idx                 number;
    nval                binary_integer  := 0;
    lin                 varchar2 (4000) := trim (ilin);
    val                 varchar2 (4000);
begin

   if delimit = ',' then
      lin := trim (ilin);
   else
      lin := regexp_replace (trim (ilin), '[[:space:]]+',',');
   end if;

   arr.delete;
   while (lin is not null) loop
      idx := instr (lin, ',');
      if idx > 0 then
         val := trim (substr (lin, 1, idx-1));
      else
         val := trim (lin);
      end if;
      nval := nval + 1;
      if val = '.' then
         val := 'null';
      end if;
      arr (nval) := trim (replace (val, chr(13)));
      arr (nval) := nvl (arr (nval), 'null');

      exit when idx = 0;
      lin := substr (lin, idx+1);
      if idx > 0 and lin is null then
         arr (nval+1) := 'null';
      end if;

   end loop;

end parse_lin;


-- =============================================================================
-- Fetch BLOB, convert and return CLOB
-- =============================================================================

procedure ld_blob
         (fil_id                         in     number
         ,o_clob                         in out clob
         ,status                         out    varchar2) is

    inp_blob            blob := empty_blob ();
    doffset             integer := 1;
    soffset             integer := 1;
    maxsize             integer := dbms_lob.lobmaxsize;
    csid                number  := dbms_lob.default_csid;
    lang                number  := dbms_lob.default_lang_ctx;
    warning             integer;

    cursor get_blob (fid varchar2) is
       select file_blob_img
         from file_dtl
        where file_id = fid;

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   status := '';
   open get_blob (fil_id);
   fetch get_blob into inp_blob;
   if get_blob%notfound then
      close get_blob;
      status := 'Error - File ID '||fil_id||' not found';
      return;
   end if;
   close get_blob;

   dbms_lob.CreateTemporary (o_clob, true);
   dbms_lob.ConvertToClob (o_clob, inp_blob, maxsize, doffset, soffset, csid, lang, warning);

exception
   when others then
      status := sqlerrm;
end ld_blob;


-- =============================================================================
-- Determine CSV file type
-- =============================================================================

procedure prs_csv
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is

    inp_clob            clob;
    elin                number;
    csv_category        number;

    VarArr              VCharArrTyp;
    ValArr              VCharArrTyp;

    nvar                binary_integer := 0;
    lin                 varchar2 (32000);
    vals                varchar2 (4000);
    vals_str            varchar2 (4000);
    delim               varchar2 (1);

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   -- ================================================================
   -- Extract 1st line of CSV file to determine type
   -- ================================================================

   elin := instr (inp_clob, chr(10), 1, 1);
   lin  := dbms_lob.substr (inp_clob, elin-1, 1);
   if debug_flg is not null then
      dbms_output.put_line ('1: '||lin);
   end if;

   parse_lin (lin, VarArr);

   case
      when VarArr.count = 2  then csv_category := 1;
      when VarArr.count = 2  then csv_category := 1;
      when VarArr.count > 10 then csv_category := 3;
      else csv_category := 4;
   end case;
   status := 'Processed CSV for '||fil_id;

end prs_csv;


-- =============================================================================
-- Process MFCW CSV Files
-- Format: <AssumptionID>,<Date>,<Period>,<Data>
-- =============================================================================

procedure csv_mfcw
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is

    inp_clob            clob;
    clen                number;
    coff                number;
    elin                number;

    VarArr              VCharArrTyp;
    ValArr              VCharArrTyp;

    nlin                binary_integer := 0;
    nvar                binary_integer := 0;
    nrec                binary_integer := 0;
    lin                 varchar2 (32000);
    stat                varchar2 (4000);
    vals                varchar2 (4000);
    vals_str            varchar2 (4000);
    delim               varchar2 (1);
    sql_stmt            varchar2 (4000) :=
                        'insert into appl_tm_sers_data '||
                        '(sf_mf_data_id,appl_snro_dt,appl_asmp_prd_id,appl_asmp_data_val) '||
                        'values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;


   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);
   sql_stmt := sql_stmt || ''''||fil_id||'''';

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to determine CSV category
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, VarArr);
      else

      -- =============================================================
      -- Parse subsequent lines for CSV values
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);

         for v in 2..ValArr.count loop
            if upper (VarArr(v)) like '%DATE%' then
                vals := 'to_date('''||ValArr(v)||''',''mm/dd/yyyy'')';
            else
                vals := ValArr(v);
            end if;
            vals_str := vals_str || ',' || vals;
         end loop;

         -- ==========================================================
         -- Execute SQL to load CSV values
         -- ==========================================================

         begin
            execute immediate sql_stmt||vals_str||')';
            nrec := nrec + 1;
         exception
            when others then
               dbms_output.put_line (sqlerrm);
         end;

      end if;

      coff := elin + 1;

   end loop;

   status := 'Processed CSV for '||fil_id||' (Loaded: '||nrec||')';

end csv_mfcw;


-- =============================================================================
-- Process SF Cat 1 CSV Files
-- Format: <YYYYMM>,<Value>
-- =============================================================================

procedure csv_cat1
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is


   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             varchar2 (38);

   VarArr              VCharArrTyp;
   ValArr              VCharArrTyp;
   col_nme             varchar2 (200);

   nlin                binary_integer := 0;
   nvar                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
                        'insert into appl_tm_sers_data '||
                        '(sf_mf_data_id,appl_asmp_col_nme,appl_snro_dt,appl_asmp_data_val) '||
                        'values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   sfmf_id := get_sfmf_id (fil_id);
   if sfmf_id is null then
      status := 'Unable to find SF_MF_DATA_ID';
      dbms_output.put_line (status);
      return;
   end if;


   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to obtain headers
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, VarArr);
         col_nme := upper (VarArr(2));
         sql_stmt := sql_stmt || ''''||sfmf_id||''','''||col_nme||'''';
      else

      -- =============================================================
      -- Parse subsequent lines for CSV values
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);

         for v in 1..ValArr.count loop
            if upper (VarArr(v)) like '%DTE%' then
                vals := 'to_date('''||ValArr(v)||''',''yyyymm'')';
            else
                vals := ValArr(v);
            end if;
            vals_str := vals_str || ',' || vals;
         end loop;

         -- ==========================================================
         -- Execute SQL to load CSV values
         -- ==========================================================

         if debug_flg is not null then
            dbms_output.put_line (sql_stmt);
            dbms_output.put_line (vals_str||')');
         else
            begin
               execute immediate sql_stmt||vals_str||')';
               nrec := nrec + 1;
            exception
               when others then
                  dbms_output.put_line (sqlerrm);
            end;
         end if;
      end if;
      coff := elin + 1;

   end loop;
   status := 'Processed CSV for '||fil_id||' (Lines Parsed: '||nlin||' - KV Loaded: '||nrec||')';

end csv_cat1;


-- =============================================================================
-- Process SF Cat 2 CSV Files
-- Format: <Year>,<Month>,<Value>,<Value>,...
-- =============================================================================

procedure csv_cat2
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is


   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             varchar2 (38);

   VarArr              VCharArrTyp;
   ValArr              VCharArrTyp;
   col_nme             varchar2 (200);

   nlin                binary_integer := 0;
   nvar                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   snro_dt             date;
   sql_stmt            varchar2 (4000) :=
                        'insert into appl_tm_sers_data '||
                        '(sf_mf_data_id,appl_snro_dt,appl_asmp_col_nme,appl_asmp_data_val) '||
                        'values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   sfmf_id := get_sfmf_id (fil_id);
   if sfmf_id is null then
      status := 'Unable to find SF_MF_DATA_ID';
      dbms_output.put_line (status);
      return;
   end if;

   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to obtain CSV header/variables
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, VarArr);
         col_nme := upper (VarArr(1));
         sql_stmt := sql_stmt || ''''||sfmf_id||'''';
      else

      -- =============================================================
      -- Parse subsequent lines for CSV values
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);

         vals := ',to_date('''||ValArr(1)||lpad(ValArr(2),2,'0')||''',''yyyymm''),';

         for v in 3..ValArr.count loop
            vals_str := vals || ''''||VarArr(v)||''','||ValArr(v);

            -- ==========================================================
            -- Execute SQL to load CSV values
            -- ==========================================================

            begin
               execute immediate sql_stmt||vals_str||')';
               nrec := nrec + 1;
            exception
               when others then
                  dbms_output.put_line (sqlerrm);
                  dbms_output.put_line(sql_stmt||vals_str||')');
            end;
         end loop;
      end if;
      coff := elin + 1;

   end loop;
   status := 'Processed CSV for '||fil_id||' (Loaded: '||nrec||')';

end csv_cat2;


-- =============================================================================
-- Process SF Cat 3 CSV Files
-- Format: <Year>,<Value>,<Value>,<Value>,...
-- =============================================================================

procedure csv_cat3
         (fil_id                         in     number
         ,status                         in out varchar2
         ,delim                          in     varchar2 default ','
         ,debug_flg                      in     varchar2 default null) is


   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             varchar2 (38);

   VarArr              VCharArrTyp;
   ValArr              VCharArrTyp;
   col_nme             varchar2 (200);

   nlin                binary_integer := 0;
   nvar                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   snro_dt             date;
   sql_stmt            varchar2 (4000) :=
                        'insert into appl_tm_sers_data '||
                        '(sf_mf_data_id,appl_snro_dt,appl_asmp_col_nme,appl_asmp_data_val) '||
                        'values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   sfmf_id := get_sfmf_id (fil_id);
   if sfmf_id is null then
      status := 'Unable to find SF_MF_DATA_ID';
      dbms_output.put_line (status);
      return;
   end if;


   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to obtain CSV header/variables
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, VarArr, delim);
         col_nme := upper (VarArr(1));
         sql_stmt := sql_stmt || ''''||sfmf_id||'''';
      else

      -- =============================================================
      -- Parse subsequent lines for CSV values
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr, delim);

         vals := ',to_date('''||ValArr(1)||''',''mm/dd/yyyy''),';
         --vals := ',to_date('''||ValArr(1)||'0101'',''yyyymmdd''),';

         for v in 2..ValArr.count loop
            vals_str := vals || ''''||VarArr(v)||''','||ValArr(v);

            if debug_flg is not null then
               dbms_output.put_line (vals_str);
            else
               begin
                  execute immediate sql_stmt||vals_str||')';
                  nrec := nrec + 1;
               exception
                  when others then
                     dbms_output.put_line (sqlerrm);
                     dbms_output.put_line(sql_stmt||vals_str||')');
               end;
            end if;
         end loop;
      end if;
      coff := elin + 1;

   end loop;
   status := 'Processed CSV for '||fil_id||' (# Lines: '||nlin||' - Time Series: '||nrec||')';

end csv_cat3;


-- =============================================================================
-- Process SF Cat 4 CSV Files
-- Format: <yyyymm>,<Value>,<Value>
-- =============================================================================

procedure csv_cat4
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is


   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             varchar2 (38);

   VarArr              VCharArrTyp;
   ValArr              VCharArrTyp;
   snro_dt             varchar2 (200);

   nlin                binary_integer := 0;
   nvar                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
                        'insert into appl_tm_sers_data '||
                        '(sf_mf_data_id,appl_snro_dt,appl_asmp_col_nme,appl_asmp_data_val) '||
                        'values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   sfmf_id := get_sfmf_id (fil_id);
   if sfmf_id is null then
      status := 'Unable to find SF_MF_DATA_ID';
      dbms_output.put_line (status);
      return;
   end if;

   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to obtain headers
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, VarArr);
         sql_stmt := sql_stmt || sfmf_id||',';
      else

         -- =============================================================
         -- Parse subsequent lines for CSV values
         -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);
         for v in 1..ValArr.count loop
            if v = 1 then
               snro_dt := 'to_date('''||ValArr(1)||''',''yyyymm'')';
            else
               vals_str := snro_dt || ','''||VarArr(v)||''','||trim (ValArr(v));

               -- ==========================================================
               -- Execute SQL to load CSV values
               -- ==========================================================

               if debug_flg is not null then
                  dbms_output.put_line (sql_stmt);
                  dbms_output.put_line (vals_str);
               else
                  begin
                     execute immediate sql_stmt||vals_str||')';
                     nrec := nrec + 1;
                  exception
                     when others then
                        dbms_output.put_line (sqlerrm);
                  end;
               end if;
            end if;
         end loop;

      end if;
      coff := elin + 1;

   end loop;
   status := 'Processed CSV for '||fil_id||' (# Lines: '||nlin||' - Time Series: '||nrec||')';

end csv_cat4;


-- =============================================================================
-- Process TS w/variable Date
-- Format: <date>,<Value>,<Value>
-- =============================================================================

procedure csv_dtval
         (fil_id                         in     number
         ,status                         in out varchar2
         ,datefm                         in     varchar2 default 'yyyymm'
         ,debug_flg                      in     varchar2 default null) is


   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             varchar2 (38);

   VarArr              VCharArrTyp;
   ValArr              VCharArrTyp;
   snro_dt             varchar2 (200);

   nlin                binary_integer := 0;
   nvar                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
                       'insert into appl_tm_sers_data '||
                       '(sf_mf_data_id,appl_snro_dt,appl_asmp_col_nme,appl_asmp_data_val) '||
                       'values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   sfmf_id := get_sfmf_id (fil_id);
   if sfmf_id is null then
      status := 'Unable to find SF_MF_DATA_ID';
      dbms_output.put_line (status);
      return;
   end if;

   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to obtain headers
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, VarArr);
         sql_stmt := sql_stmt || sfmf_id||',';
      else

         -- =============================================================
         -- Parse subsequent lines for CSV values
         -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);
         for v in 1..ValArr.count loop
            if v = 1 then
               snro_dt := 'to_date('''||ValArr(1)||''','''||datefm||''')';
            else
               vals_str := snro_dt || ','''||VarArr(v)||''','||trim (ValArr(v));

               -- ==========================================================
               -- Execute SQL to load CSV values
               -- ==========================================================

               if debug_flg is not null then
                  dbms_output.put_line (sql_stmt);
                  dbms_output.put_line (vals_str);
               else
                  begin
                     execute immediate sql_stmt||vals_str||')';
                     nrec := nrec + 1;
                  exception
                     when others then
                        dbms_output.put_line (sqlerrm);
                        dbms_output.put_line (sql_stmt);
                        dbms_output.put_line (vals_str);
                  end;
               end if;
            end if;
         end loop;

      end if;
      coff := elin + 1;

   end loop;
   status := 'Processed CSV for '||fil_id||' (# Lines: '||nlin||' - Time Series: '||nrec||')';

end csv_dtval;


-- =============================================================================
-- Process CSV file to split into multiple time series
-- Format: <yyyymm>,<Value>,<Value>,...,<TimeSeriesType>
-- =============================================================================

procedure csv_mts
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is


   type IdArrTyp is table of number index by varchar2 (255);

   IdArr               IdArrTyp;
   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             varchar2 (38);

   HdrArr              VCharArrTyp;
   ValArr              VCharArrTyp;
   snro_dt             varchar2 (200);

   nlin                binary_integer := 0;
   nvar                binary_integer := 0;
   nrec                binary_integer := 0;
   nts                 binary_integer := 0;
   lin                 varchar2 (32000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
                        'insert into appl_tm_sers_data '||
                                '(sf_mf_data_id,appl_snro_dt,appl_asmp_col_nme,appl_asmp_data_val) '||
                                'values (';

   function get_assc_id (styp varchar2) return varchar2 is
      id                  varchar2 (38);
   begin
      if IdArr.exists (styp) then
         id := IdArr (styp);
      else
         IdArr (styp) := get_sfmf_id (fil_id, styp);
      end if;
      return IdArr (styp);
   end get_assc_id;

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to obtain headers
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, HdrArr);
      else

         -- =============================================================
         -- Parse subsequent lines for CSV values
         -- =============================================================

         parse_lin (lin, ValArr);
         sfmf_id := get_assc_id (ValArr (ValArr.count));

         for v in 1..ValArr.count-1 loop
            if v = 1 then
               snro_dt := ',to_date('''||ValArr(1)||''',''yyyymm'')';
            else
               vals_str := sfmf_id || snro_dt || ','''||HdrArr(v)||''','||trim (ValArr(v));

               -- ==========================================================
               -- Execute SQL to load CSV values
               -- ==========================================================

               if debug_flg is not null then
                  dbms_output.put_line (sql_stmt);
                  dbms_output.put_line (vals_str);
                  nrec := nrec + 1;
               else
                  begin
                     execute immediate sql_stmt||vals_str||')';
                     nrec := nrec + 1;
                  exception
                     when others then
                        dbms_output.put_line (sqlerrm);
                  dbms_output.put_line (vals_str);
                  end;
               end if;
            end if;
         end loop;

      end if;
      coff := elin + 1;

   end loop;
   status := 'Processed CSV for '||fil_id||' (# Lines: '||nlin||' - Time Series: '||nrec||')';

end csv_mts;


-- =============================================================================
-- Process DFLT SPD CSV file
-- =============================================================================

procedure csv_dflt_spd
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is


   type IdArrTyp is table of number index by varchar2 (255);

   IdArr               IdArrTyp;
   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             varchar2 (38);

   HdrArr              VCharArrTyp;
   ValArr              VCharArrTyp;
   snro_dt             varchar2 (200);

   nlin                binary_integer := 0;
   nvar                binary_integer := 0;
   nrec                binary_integer := 0;
   nts                 binary_integer := 0;
   nerr                binary_integer := 0;
   lin                 varchar2 (32000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
                        'insert into ln_dflt_spd_rt '||
                        '(sf_mf_data_assc_id,ln_dflt_spd_yr_no,ln_dflt_spd_mth_no,ln_dlq_mth_cnt'||
                        ',prop_drvd_st_cd,ln_dflt_spd_typ,ln_dflt_spd_rt,ln_dflt_spd_ver_desc) '||
                        'values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, status);
   if status is not null then
      dbms_output.put_line (status);
      return;
   end if;

   sfmf_id := get_sfmf_id (fil_id);
   if sfmf_id is null then
      status := 'Unable to find SF_MF_DATA_ID';
      dbms_output.put_line (status);
      return;
   end if;
   sql_stmt := sql_stmt ||sfmf_id;

   -- ================================================================
   -- Loop through each line in CLOB
   -- ================================================================

   coff := 1;
   clen := dbms_lob.getlength (inp_clob);

   while (coff <= clen) loop
      elin := instr (inp_clob, chr(10), coff, 1);
      exit when elin = 0;

      nlin := nlin + 1;
      lin  := dbms_lob.substr (inp_clob, elin - coff, coff);
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to obtain headers
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, HdrArr);
      else

         -- =============================================================
         -- Parse subsequent lines for CSV values
         -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);
         if ValArr.count = 8 then
            for v in 1..ValArr.count loop
               vals_str := vals_str || ',''' || replace (ValArr(v),'"') || '''';
            end loop;

            -- ==========================================================
            -- Execute SQL to load CSV values
            -- ==========================================================

            if debug_flg is not null then
               dbms_output.put_line (sql_stmt);
               dbms_output.put_line (vals_str);
               nrec := nrec + 1;
            else
               begin
                  execute immediate sql_stmt||vals_str||')';
                  nrec := nrec + 1;
               exception
                  when others then
                     dbms_output.put_line (sqlerrm);
                     dbms_output.put_line (sql_stmt);
                     dbms_output.put_line (vals_str);
               end;
            end if;
         else
            dbms_output.put_line (nlin||'(<8): '||lin);
            nerr := nerr + 1;
         end if;
      end if;
      coff := elin + 1;

   end loop;
   status := 'Processed CSV for '||fil_id||' (# Lines: '||nlin||' - Time Series: '||nrec||')';

end csv_dflt_spd;

-- =============================================================================
-- =============================================================================

end pkg_tm_sers;
/
sho errors

rem create or replace public synonym PKG_TM_SERS for &schema..PKG_TM_SERS;
rem grant execute on &schema..PKG_TM_SERS to &fpe_sch;

