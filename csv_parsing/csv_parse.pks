REM ============================================================================
REM Name: csv_parse.pks
REM
REM Description:
REM Package Specficiation for CSV_PARSE which parse CLOB csv files into appropriate
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
REM
REM ============================================================================


drop type LoadStatArrTyp;
drop type LoadStatRecTyp;

create or replace type LoadStatRecTyp as object
(file_name                 varchar2(50)
,stg_tbl_name              varchar2(50)
,load_cnt                  number)
/

create or replace type LoadStatArrTyp is table of LoadStatRecTyp
/


-- =====================================================================
-- CSV_PARSE Package Specification
-- =====================================================================

create or replace package csv_parse as


function get_tb
        (file_name                      in     varchar2) return varchar2;

procedure load
         (file_name                      in     varchar2
         ,status                         in out varchar2
         ,delimiter                      in     varchar2 default '|'
         ,debug                          in     varchar2 default null);

procedure stg_tbl_cnt;

function stg_tbl_status return LoadStatArrTyp pipelined;

procedure load_stg;

end csv_parse;
/
sho errors
