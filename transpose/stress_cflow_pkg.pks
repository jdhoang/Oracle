rem ============================================================================
rem Name:   stress_cflow_pkg.pks
rem
rem Description:
rem Create Package Specification for Stress CashFlow staging.
rem
rem Assumptions:
rem The following tables are used for Stress CashFlow staging:
rem STS_UNIQ      Contains unique keys to run parallel jobs
rem STG_STS       View on MF_CRDT_WORKS_CFLW_INPT_STS
rem ISTMT         Store INSERT DML for debugging
rem
rem ============================================================================
rem Revision History:
rem Date       By        Comment
rem ---------- --------- -----------------------------------------------------
rem 03/29/2018 jhoang    Original release.
rem
rem ============================================================================

prompt =========================================================================
prompt Create Package Specification for STRESS_CFLOW_PKG
prompt =========================================================================

set scan off
create or replace package fpemfd.stress_cflow_pkg as

-- =============================================================================
-- =============================================================================

procedure proc_sts
         (start_rowid                    in     rowid
         ,end_rowid                      in     rowid);
procedure trunc
         (tbname                         in     varchar2);

procedure pop_uniq;
procedure setup_parallel_task;
procedure run_parallel_task;
procedure run_staging
         (dparam                         in  varchar2
         ,start_dt                       out varchar2
         ,end_dt                         out varchar2);

-- =============================================================================
-- =============================================================================

end stress_cflow_pkg;
/
sho errors

set scan on

