rem ============================================================================
rem Name:   stress_cflow_pkg.sql
rem
rem Description:
rem Create Package for Stress CashFlow staging.
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
prompt Create Package Body for STRESS_CFLOW_PKG
prompt =========================================================================

set scan off
create or replace package body fpemfd.stress_cflow_pkg as


-- =============================================================================
-- Merge Stress CashFlow
-- =============================================================================

procedure proc_sts
         (start_rowid                    in rowid
         ,end_rowid                      in rowid) is

   type ValRecTyp is record
      (ln_frct_avg_actv_upb_amt       number
      ,ln_frct_refi_actv_upb_amt      number
      ,ln_frct_prd                    number);

   type FilArrTyp is table of varchar2 (128);
   type InsArrTyp is table of varchar2 (32000) index by binary_integer;
   type ValArrTyp is table of ValRecTyp        index by binary_integer;


   ValArr                         ValArrTyp;
   InsArr                         InsArrTyp;
   FileArr                        FilArrTyp := FilArrTyp
   ('StressActiveUPB.csv' ,'StressAmortUPBAvg.csv' ,'StressDefaultedUPB.csv'
   ,'StressDQ60UPB.csv' ,'StressDSCR.csv' ,'StressEGI.csv' ,'StressFloatIncome.csv'
   ,'StressG&AExpenses.csv' ,'StressGrossGfeeIncome.csv' ,'StressGrossLoss.csv'
   ,'StressGrossPFSLoss.csv' ,'StressGrossPFSLossUPB.csv' ,'StressGrossREOLoss.csv'
   ,'StressGrossREOLossCarryCost.csv' ,'StressGrossREOLossStoppedInterest.csv'
   ,'StressGrossREOLossUPB.csv' ,'StressInterestIncome.csv' ,'StressIVCashflow.csv'
   ,'StressMLTV.csv' ,'StressNetGfeeIncome.csv' ,'StressNetLoss.csv' ,'StressNOI.csv'
   ,'StressNoteRate.csv' ,'StressPrepaymentDollars.csv' ,'StressProbabilityofDefault.csv'
   ,'StressProbabilityofPFS.csv' ,'StressProbabilityofPrepay.csv' ,'StressProbabilityofREO.csv'
   ,'StressPVFactor.csv' ,'StressYMRevenue-Fannie.csv' ,'StressYMRevenue-Investor.csv'
   ,'StressYMRevenue-Lender.csv' ,'StressMissingDefaultUPB.csv');

   nprd                           binary_integer;
   lcnt                           binary_integer := 0;
   ins_stmt                       varchar2 (32000);
   prd0                           boolean;

   cursor get_val (snro_id varchar2, ln_id varchar2, dt date, fname varchar2) is
      select ln_frct_avg_actv_upb_amt
            ,ln_frct_refi_actv_upb_amt
            ,ln_frct_prd
       from fpemfd.stg_mf_crdt_works_cflw_sts
      where appl_snro_id = snro_id
        and fnm_ln_id    = ln_id
        and ln_frct_dt   = dt
        and file_name    = fname
      order by ln_frct_prd;

begin

   -- =====================================================
   -- Retrieve each Loan in chunks defined by rowid
   -- =====================================================

   for ln in (select appl_snro_id
                    ,fnm_ln_id
                    ,to_char (ln_frct_dt, 'mm/dd/yyyy') ln_frct_dt
                from fpemfd.sts_uniq
               where rowid between start_rowid and end_rowid) loop

      for i in 0..361 loop
         InsArr(i) := '';
      end loop;

      prd0 := true;
      lcnt := lcnt + 1;

      ins_stmt := 'insert into fpemfd.stg_sts values ('||
                  ''''||ln.appl_snro_id||''','||ln.fnm_ln_id||','||
                      'to_date('''||ln.ln_frct_dt||''',''mm/dd/yyyy'')';

      -- =====================================================
      -- Loop through File in Order
      -- =====================================================

      for fil in FileArr.first .. FileArr.last loop

         -- =====================================================
         -- Retrieve Cash Flow by Period to generate Insert DML
         -- =====================================================

         open get_val (ln.appl_snro_id, ln.fnm_ln_id, ln.ln_frct_dt, FileArr(fil));
         fetch get_val bulk collect into ValArr;
         close get_val;
         if ValArr.count = 0 then
            insert into istmt values (sysdate,ln.fnm_ln_id||'-'||FileArr(fil));
         end if;

         nprd := ValArr.count;
         for val in 1..ValArr.count loop

            if prd0 then
               InsArr (ValArr(val).ln_frct_prd) := InsArr (ValArr(val).ln_frct_prd) ||','||ValArr(val).ln_frct_prd;
            end if;

            InsArr (ValArr(val).ln_frct_prd) := InsArr (ValArr(val).ln_frct_prd) ||
                  ','||nvl(to_char(ValArr(val).ln_frct_avg_actv_upb_amt),'null') || ',' ||
                  nvl(to_char(ValArr(val).ln_frct_refi_actv_upb_amt),'null');

         end loop;
         prd0 := false;

      end loop;

      -- =====================================================
      -- Execute Generated Insert DMLs
      -- =====================================================

      for lin in 0..nprd-1 loop
         if InsArr(lin) is not null then
            begin
               execute immediate ins_stmt || InsArr(lin)||')';
            exception
               when others then
                  insert into istmt values (sysdate,ins_stmt||InsArr(lin));
                  commit;
                  raise;
            end;
         end if;
      end loop;

   end loop;

end proc_sts;


-- =============================================================================
-- Truncate Table
-- =============================================================================

procedure trunc (tbname in varchar2) is
begin
   execute immediate 'truncate table '||tbname;
end trunc;


-- =============================================================================
-- Obtain Unique Keys to Prepare for Parallelism
-- =============================================================================

procedure pop_uniq is
begin
   insert into fpemfd.sts_uniq
      select distinct appl_snro_id, fnm_ln_id, ln_frct_dt 
        from fpemfd.stg_mf_crdt_works_cflw_sts;
end pop_uniq;


-- =============================================================================
-- Create Parallel Task and Setup Chunks for Parallelism
-- =============================================================================

procedure setup_parallel_task is
begin

   dbms_parallel_execute.create_chunks_by_rowid
      (task_name     => 'StressCF'
      ,table_owner   => 'FPEMFD'
      ,table_name    => 'STS_UNIQ'
      ,by_row        => false
      ,chunk_size    => 10000);

end setup_parallel_task;

-- =============================================================================
-- =============================================================================

procedure run_parallel_task is
begin
   dbms_parallel_execute.run_task
      (task_name      => 'StressCF'
      ,sql_stmt       => 'begin stress_cflow_pkg.proc_sts (:start_id, :end_id); end;'
      ,language_flag  => DBMS_SQL.NATIVE
      ,parallel_level => 4);
end;


-- =============================================================================
-- =============================================================================

procedure run_staging
         (dparam                         in  varchar2
         ,start_dt                       out varchar2
         ,end_dt                         out varchar2) is
begin

   -- ================================================================
   -- Truncate Key and Input Table before Processing
   -- ================================================================

   start_dt := user||' - '||to_char (sysdate, 'mm/dd/yyyy hh:mi:sspm');
   trunc ('STS_UNIQ');
   trunc ('MF_CRDT_WORKS_CFLW_INPT_STS');

   -- ================================================================
   -- Populate Key Table, Create, Setup and Run Parallel Task.
   -- ================================================================

   pop_uniq;
   dbms_parallel_execute.create_task ('StressCF');
   setup_parallel_task;
   run_parallel_task;
   dbms_parallel_execute.drop_task ('StressCF');
   end_dt := to_char (sysdate, 'mm/dd/yyyy hh:mi:sspm');

end;

-- =============================================================================
-- =============================================================================

end stress_cflow_pkg;
/
sho errors

set scan on

