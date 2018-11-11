REM ============================================================================
REM Name:   pkg_tm_sers.pks
REM
REM Description:
REM Package Spec for PKG_TM_SERS.
REM
REM Assumptions:
REM
REM ============================================================================
REM Revision History:
REM Date       By        Comment
REM ---------- --------- -----------------------------------------------------
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
prompt PKG_TM_SERS Package Specification
prompt =============================================================================

create or replace package &schema..pkg_tm_sers as

    type VCharArrTyp is table of varchar2 (100) index by binary_integer;

-- =============================================================================
-- Process MFCW CSV Files
-- =============================================================================

procedure csv_mfcw
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process SF Cat 1 CSV Files
-- =============================================================================

procedure csv_cat1
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process SF Cat 2 CSV Files
-- =============================================================================

procedure csv_cat2
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process SF Cat 3 CSV Files
-- =============================================================================

procedure csv_cat3
         (fil_id                         in     number
         ,status                         in out varchar2
         ,delim                          in     varchar2 default ','
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process SF Cat 4 CSV Files
-- Format: <yyyymm>,<Value>,<Value>
-- =============================================================================

procedure csv_cat4
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process TS w/variable Date
-- Format: <date>,<Value>,<Value>
-- =============================================================================

procedure csv_dtval
         (fil_id                         in     number
         ,status                         in out varchar2
         ,datefm                         in     varchar2 default 'yyyymm'
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process CSV file to split into multiple time series
-- Format: <yyyymm>,<Value>,<Value>,...,<TimeSeriesType>
-- =============================================================================

procedure csv_mts
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);

-- =============================================================================
-- Process DFLT SPD CSV file
-- =============================================================================

procedure csv_dflt_spd
         (fil_id                         in     number
         ,status                         in out varchar2
         ,debug_flg                      in     varchar2 default null);


end pkg_tm_sers;
/
sho errors

