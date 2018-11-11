REM ============================================================================
REM Name:   pkg_kv.pks
REM
REM Description:
REM Package Specification for PKG_KV which parse BLOB csv files into appropriate
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
prompt PKG_KV Package Specification
prompt =============================================================================

create or replace package &schema..pkg_kv as

    type VCharArrTyp is table of varchar2 (1000) index by binary_integer;

-- =============================================================================
-- Query for SF_MF_DATA_ID
-- =============================================================================

function get_sfmf_id 
        (fil_id                         in number) return number;

-- =============================================================================
-- Process Key Value File Type 1
-- Format: <Parameter>,<Value>
-- =============================================================================

procedure prs_kv_1
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process Key Value File Type 2
-- Format: <Key>,<Col>,<Value>
-- =============================================================================

procedure prs_kv_2
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process Key Value File Type 3
-- Format: <Key>,<Col>,<Value>
-- =============================================================================

procedure prs_kv_3
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process Key Value File Type 4
-- Format: <Parameter>,<Value>,<Comment>
-- =============================================================================

procedure prs_kv_4
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process Key Value File Type 5 (2 Concatenated Keys, Multiple Values)
-- Format: <Key>,<Col>,<Value>
-- =============================================================================

procedure prs_kv_5
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

end pkg_kv;
/
sho errors

