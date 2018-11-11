REM ============================================================================
REM Name:   pkg_kv.pkb
REM
REM Description:
REM Package Body for PKG_KV which parse BLOB csv files into appropriate
REM Key Value table.
REM
REM Assumptions:
REM
REM ============================================================================
REM Revision History:
REM Date       By        Comment
REM ---------- --------- -----------------------------------------------------
REM 10/26/2018 jhoang    Original release.
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
prompt PKG_KV Package Body
prompt =============================================================================

create or replace package body &schema..pkg_kv as

-- =============================================================================
-- Query for SF_MF_DATA_ID
-- =============================================================================

function get_sfmf_id 
        (fil_id                         in number) return number is

   id                  number;
   asmp_id             varchar2 (38);

   cursor get_id is
      select sf_mf_data_id
        from sf_mf_data_assc
       where file_id = fil_id;

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
            values (sf_mf_data_assc_seq.nextval, asmp_id, fil_id, 'SFM', null)
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
         ,comments                       in     varchar2 default 'n'
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
      /*
      if comments = 'y' and nval = 2 then
         arr (3) := replace (lin, '''','''''');
         arr (3) := replace (arr (3), chr(13));
         exit;
      end if;
      */

      idx := instr (lin, ',');
      if idx > 0 then
         val := substr (lin, 1, idx - 1);
      else
         val := lin;
      end if;
      nval := nval + 1;
      arr (nval) := trim (replace (val, chr(13)));
      arr (nval) := replace (arr (nval), '''','''''');

      exit when idx = 0;
      lin := substr (lin, idx + 1);
      if idx > 0 and lin is null then
         arr (nval+1) := '';
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
-- Process Key Value File Type 1
-- Format: <Parameter>,<Value>
-- =============================================================================

procedure prs_kv_1
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is

   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             number;

   HdrArr              VCharArrTyp;
   ValArr              VCharArrTyp;

   nlin                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   stat                varchar2 (4000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   last_flg            boolean := false;
   sql_stmt            varchar2 (4000) :=
      'insert into appl_key_val_data '||
      '(sf_mf_data_id,appl_asmp_row_seq_id,appl_asmp_key_col_nme,appl_asmp_val_col_nme,appl_asmp_key_nme'||
      ',appl_asmp_data_val)'||
      ' values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, stat);
   if stat is not null then
      dbms_output.put_line (stat);
      status := 'Unable to load BLOB - '||stat;
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
      if elin = 0 then
         lin  := dbms_lob.substr (inp_clob, 32767, coff);
         dbms_output.put_line (length (lin)||'-'||lin);
         last_flg := true;
      else
         lin := dbms_lob.substr (inp_clob, elin - coff, coff);
      end if;

      nlin := nlin + 1;
      if debug_flg is not null then
         dbms_output.put_line (nlin||': '||lin);
      end if;

      -- =============================================================
      -- Parse 1st line of CSV to get KV columns
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, HdrArr);
      else

      -- =============================================================
      -- Parse subsequent lines for KV columns
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);

         vals_str := sfmf_id ||','||nrec||','''||HdrArr(1)||''','''||HdrArr(2)||''','''||nvl (ValArr(1),'9999')||''','''||
                     ValArr(2)||'''';


         -- ==========================================================
         -- Execute SQL to load KV columns
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
      if last_flg then
         exit;
      end if;
      coff := elin + 1;

   end loop;

   status := 'Processed CSV for '||fil_id||' (Lines Parsed: '||nlin||' - KV Loaded: '||nrec||')';

exception
   when others then
      status := sqlerrm;
      dbms_output.put_line (sqlerrm);
end prs_kv_1;


-- =============================================================================
-- Process Key Value File Type 2
-- Format: <MontCnt>,<Key>,<Key>
-- =============================================================================

procedure prs_kv_2
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is

   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             number;

   HdrArr              VCharArrTyp;
   ValArr              VCharArrTyp;

   nlin                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   stat                varchar2 (4000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
      'insert into appl_key_val_data '||
      '(sf_mf_data_id,appl_asmp_row_seq_id,appl_asmp_key_col_nme,appl_asmp_val_col_nme,appl_asmp_key_nme'||
      ',appl_asmp_data_val)'||
      ' values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, stat);
   if stat is not null then
      dbms_output.put_line (stat);
      status := 'Unable to load BLOB - '||stat;
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
      -- Parse 1st line of CSV to get KV columns
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, HdrArr);
      else

      -- =============================================================
      -- Parse subsequent lines for KV columns
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);

         for v in 2..ValArr.count loop
            vals_str := sfmf_id ||','||nrec||','''||HdrArr(1)||''','''||
                        HdrArr(v)||''','''||ValArr(1)||''','''||ValArr(v)||'''';

            -- ==========================================================
            -- Execute SQL to load KV columns
            -- ==========================================================

            if debug_flg is not null then
               dbms_output.put_line (v||'-'||vals_str);
               null;

            else
               begin
                  execute immediate sql_stmt||vals_str||')';
                  nrec := nrec + 1;
               exception
                  when others then
                     dbms_output.put_line (sqlerrm);
               end;
            end if;
         end loop;

      end if;
      coff := elin + 1;

   end loop;

   status := 'Processed CSV for '||fil_id||' (Lines Parsed: '||nlin||' - KV Loaded: '||nrec||')';

exception
   when others then
      status := sqlerrm;
      dbms_output.put_line (sqlerrm);
end prs_kv_2;


-- =============================================================================
-- Process Key Value File Type 3 (Concatenated Keys)
-- Format: <Key>,<Col>,<Value>
-- =============================================================================

procedure prs_kv_3
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is

   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             number;

   HdrArr              VCharArrTyp;
   ValArr              VCharArrTyp;

   nlin                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   stat                varchar2 (4000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   key_col             varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
      'insert into appl_key_val_data '||
      '(sf_mf_data_id,appl_asmp_row_seq_id,appl_asmp_key_col_nme,appl_asmp_val_col_nme,appl_asmp_key_nme'||
      ',appl_asmp_data_val)'||
      ' values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, stat);
   if stat is not null then
      dbms_output.put_line (stat);
      status := 'Unable to load BLOB - '||stat;
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
      -- Parse 1st line of CSV to get KV columns
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, HdrArr);
         key_col := HdrArr(1)||','||HdrArr(2)||','||HdrArr(4);
      else

      -- =============================================================
      -- Parse subsequent lines for KV columns
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr);

         vals_str := sfmf_id ||','||nrec||','''||key_col||''','''||HdrArr(3)||''','''||
                     ValArr(1)||','||ValArr(2)||','||ValArr(4)||''','''||ValArr(3)||'''';

         -- ==========================================================
         -- Execute SQL to load KV columns
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

exception
   when others then
      status := sqlerrm;
      dbms_output.put_line (sqlerrm);
end prs_kv_3;


-- =============================================================================
-- Process Key Value File Type 4
-- Format: <Parameter>,<Value>,<Comment>
-- =============================================================================

procedure prs_kv_4
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is

   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             number;

   HdrArr              VCharArrTyp;
   ValArr              VCharArrTyp;

   nlin                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   stat                varchar2 (4000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
      'insert into appl_key_val_data '||
      '(sf_mf_data_id,appl_asmp_row_seq_id,appl_asmp_key_col_nme,appl_asmp_val_col_nme,appl_asmp_key_nme'||
      ',appl_asmp_data_val,appl_asmp_key_val_cmnt)'||
      ' values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, stat);
   if stat is not null then
      dbms_output.put_line (stat);
      status := 'Unable to load BLOB - '||stat;
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
      -- Parse 1st line of CSV to get KV columns
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, HdrArr);
      else

      -- =============================================================
      -- Parse subsequent lines for KV columns
      -- =============================================================

         vals_str := '';
         parse_lin (lin, ValArr, 'y');

         vals_str := sfmf_id ||','||nrec||','''||HdrArr(1)||''','''||HdrArr(2)||''','''||ValArr(1)||''','''||
                     ValArr(2)||''','''||nvl(ValArr(3),'')||'''';
         if debug_flg is not null then
            dbms_output.put_line (sql_stmt);
            dbms_output.put_line (vals_str||')');
         end if;

         -- ==========================================================
         -- Execute SQL to load KV columns
         -- ==========================================================

         if debug_flg is null then
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

exception
   when others then
      status := sqlerrm;
      dbms_output.put_line (sqlerrm);
end prs_kv_4;


-- =============================================================================
-- Process Key Value File Type 5 (2 Concatenated Keys, Multiple Values)
-- Format: <Key>,<Col>,<Value>
-- =============================================================================

procedure prs_kv_5
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null) is

   inp_clob            clob;
   clen                number;
   coff                number;
   elin                number;
   sfmf_id             number;

   HdrArr              VCharArrTyp;
   ValArr              VCharArrTyp;

   nlin                binary_integer := 0;
   nrec                binary_integer := 0;
   lin                 varchar2 (32000);
   stat                varchar2 (4000);
   vals                varchar2 (4000);
   vals_str            varchar2 (4000);
   key_col             varchar2 (4000);
   key_val             varchar2 (4000);
   delim               varchar2 (1);
   sql_stmt            varchar2 (4000) :=
      'insert into appl_key_val_data '||
      '(sf_mf_data_id,appl_asmp_row_seq_id,appl_asmp_key_col_nme,appl_asmp_key_nme,appl_asmp_val_col_nme,'||
      'appl_asmp_data_val)'||
      ' values (';

begin

   -- ================================================================
   -- Fetch BLOB and convert to CLOB
   -- ================================================================

   ld_blob (fil_id, inp_clob, stat);
   if stat is not null then
      dbms_output.put_line (stat);
      status := 'Unable to load BLOB - '||stat;
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
      -- Parse 1st line of CSV to get KV columns
      -- =============================================================

      if nlin = 1 then
         parse_lin (lin, HdrArr);
         key_col := HdrArr(1)||','||HdrArr(2);
      else

      -- =============================================================
      -- Parse subsequent lines for KV columns
      -- =============================================================

         parse_lin (lin, ValArr);
         vals_str := '';
         key_val  := ValArr(1)||','||ValArr(2);
         for v in 3..ValArr.count loop

            vals_str := sfmf_id ||','||nrec||','''||key_col||''','''||key_val||''','''||
                        HdrArr(v)||''','''||ValArr(v)||'''';

            -- ==========================================================
            -- Execute SQL to load KV columns
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
         end loop;

      end if;
      coff := elin + 1;

   end loop;

   status := 'Processed CSV for '||fil_id||' (Lines Parsed: '||nlin||' - KV Loaded: '||nrec||')';

exception
   when others then
      status := sqlerrm;
      dbms_output.put_line (sqlerrm);
end prs_kv_5;

-- =============================================================================
-- =============================================================================

end pkg_kv;
/
sho errors

create or replace public synonym PKG_KV for &schema..PKG_KV;
grant execute on &schema..PKG_KV to &fpe_sch;

