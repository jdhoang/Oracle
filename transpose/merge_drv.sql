
create or replace
procedure fpemfr.merge_drv is

   clf_cnt                        binary_integer;
   avg_cnt                        binary_integer;
   sts_cnt                        binary_integer;
   sql_stmt                       varchar2 (32000);
   frm_table                      varchar2 (1000);
   cnt_stmt                       varchar2 (1000) := 'select count (distinct (ln_frct_prd)) from ';
   main_cols                      varchar2 (1000) := 'x.APPL_SNRO_ID, x.FNM_LN_ID, x.LN_FRCT_DT, x.LN_FRCT_PRD,';
   audit_cols                     varchar2 (1000) := ',sysdate,user';
   cols                           varchar2 (32000) :=
     'LN_FRCT_AVG_ACTV_UPB_AMT,LN_FRCT_REFI_ACTV_UPB_AMT,LN_FRCT_AMRTD_UPB_AMT,'||
     'LN_FRCT_REFI_AMRTD_UPB_AMT,LN_FRCT_REFI_BLN_UPB_AMT,LN_FRCT_DFLTD_UPB_AMT,'||
     'LN_FRCT_REFI_DFLTD_UPB_AMT,LN_FRCT_60DDLQ_UPB_AMT,LN_FRCT_REFI_60DDLQ_UPB_AMT,'||
     'LN_FRCT_DSCR_RT,LN_FRCT_REFI_DSCR_RT,LN_FRCT_EGI_AMT,LN_FRCT_REFI_EGI_AMT,'||
     'LN_FRCT_FLT_INCM_AMT,LN_FRCT_REFI_FLT_INCM_AMT,LN_FRCT_GA_EXP_AMT,'||
     'LN_FRCT_REFI_GA_EXP_AMT,LN_FRCT_GRS_GFEE_INCM_AMT,LN_FRCT_REFI_GRS_GFEE_AMT,'||
     'LN_FRCT_GRS_LOSS_AMT,LN_FRCT_REFI_GRS_LOSS_AMT,LN_FRCT_PFS_GRS_LOSS_AMT,'||
     'LN_FRCT_REFI_PFS_GRS_LOSS_AMT,LN_FRCT_PFS_GRS_LOSS_UPB_AMT,LN_FRCT_REFI_PFS_LOSS_UPB_AMT,'||
     'LN_FRCT_REO_GRS_LOSS_AMT,LN_FRCT_REFI_REO_GRS_LOSS_AMT,LN_FRCT_REO_CCOST_LOSS_AMT,'||
     'LN_FRCT_REFI_CCOST_LOSS_AMT,LN_FRCT_REO_GRS_SI_LOSS_AMT,LN_FRCT_REFI_SI_LOSS_AMT,'||
     'LN_FRCT_LOSS_UPB_AMT,LN_FRCT_REFI_REO_LOSS_UPB_AMT,LN_FRCT_AVG_INT_INCM_AMT,'||
     'LN_FRCT_REFI_INT_INCM_AMT,LN_FRCT_AVG_IDX_VAL_AMT,LN_FRCT_REFI_IDX_VAL_AMT,'||
     'LN_FRCT_AVG_MTM_LTV_RT,LN_FRCT_REFI_MTM_LTV_RT,LN_FRCT_AVG_NET_GFEE_INCM_AMT,'||
     'LN_FRCT_REFI_NET_GFEE_AMT,LN_FRCT_AVG_NTLSS_AMT,LN_FRCT_REFI_AVG_NTLSS_AMT,'||
     'LN_FRCT_AVG_NOI_AMT,LN_FRCT_REFI_AVG_NOI_AMT,LN_FRCT_AVG_NOTE_RT,'||
     'LN_FRCT_REFI_AVG_NOTE_RT,LN_FRCT_AVG_PPAY_AMT,LN_FRCT_REFI_AVG_PPAY_AMT,'||
     'LN_FRCT_AVG_PODFLT_RT,LN_FRCT_REFI_AVG_PODFLT_RT,LN_FRCT_AVG_PBBY_OF_PFS_RT,'||
     'LN_FRCT_REFI_PBBY_OF_PFS_RT,LN_FRCT_AVG_POPPAY_RT,LN_FRCT_REFI_AVG_POPPAY_RT,'||
     'LN_FRCT_AVG_PBBY_OF_REO_RT,LN_FRCT_REFI_PBBY_OF_REO_RT,LN_FRCT_AVG_PV_FCTR,'||
     'LN_FRCT_REFI_AVG_PV_FCTR,LN_FRCT_AVG_FNM_YLD_MANT_AMT,LN_FRCT_REFI_AVG_FNM_YMT_AMT,'||
     'LN_FRCT_AVG_IVSR_YLD_MANT_AMT,LN_FRCT_REFI_AVG_IVSR_YMT_AMT,LN_FRCT_AVG_LNDR_YLD_MANT_AMT,'||
     'LN_FRCT_REFI_AVG_LNDR_YMT_AMT,LN_FRCT_FNM_FEE_EC_AMT,LN_FRCT_REFI_FNM_FEE_EC_AMT,'||
     'LN_FRCT_FULL_CHRGD_FEE_EC_AMT,LN_FRCT_REFI_FULL_FEE_EC_AMT,'||
     'c.LN_FRCT_CLF_BLN_DFLTD_UPB_AMT,c.LN_FRCT_CLF_60DDLQ_BLN_AMT,'||
     'c.LN_FRCT_CLF_BLN_GRS_LOSS_AMT,c.LN_FRCT_CLF_BLN_NTLSS_AMT,'||
     'c.LN_FRCT_CLF_DFLTD_UPB_AMT,c.LN_FRCT_CLF_GRS_LOSS_AMT,'||
     's.LN_FRCT_STRS_ACTV_UPB_AMT,s.LN_FRCT_REFI_STRS_ACTV_AMT,'||
     's.LN_FRCT_STRS_AMRTD_UPB_AMT,s.LN_FRCT_REFI_STRS_AMRTD_AMT,'||
     's.LN_FRCT_REFI_STRS_BLN_UPB_AMT,s.LN_FRCT_STRS_DFLTD_UPB_AMT,'||
     's.LN_FRCT_REFI_STRS_DFLTD_AMT,s.LN_FRCT_STRS_60DDLQ_UPB_AMT,'||
     's.LN_FRCT_REFI_STRS_60DDLQ_AMT,s.LN_FRCT_STRS_DSCR_RT,'||
     's.LN_FRCT_REFI_STRS_DSCR_RT,s.LN_FRCT_STRS_EGI_AMT,'||
     's.LN_FRCT_REFI_STRS_EGI_AMT,s.LN_FRCT_STRS_FLT_INCM_AMT,'||
     's.LN_FRCT_REFI_STRS_FLT_AMT,s.LN_FRCT_STRS_GA_AMT,'||
     's.LN_FRCT_REFI_STRS_GA_AMT,s.LN_FRCT_STRS_GRS_GFEE_AMT,'||
     's.LN_FRCT_REFI_STRS_GFEE_AMT,s.LN_FRCT_STRS_GRS_LOSS_AMT,'||
     's.LN_FRCT_REFI_STRS_LOSS_AMT,s.LN_FRCT_STRS_PFS_LOSS_AMT,'||
     's.LN_FRCT_REFI_STRS_PFS_LOSS_AMT,s.LN_FRCT_STRS_PFS_LOSS_UPB_AMT,'||
     's.LN_FRCT_REFI_STRS_PFSLOSS_UPBA,s.LN_FRCT_STRS_REO_GRS_LOSS_AMT,'||
     's.LN_FRCT_REFI_STRS_REO_AMT,s.LN_FRCT_STRS_REO_CCOST_AMT,'||
     's.LN_FRCT_REFI_STRS_CCOST_AMT,s.LN_FRCT_STRS_REO_SI_LOSS_AMT,'||
     's.LN_FRCT_REFI_STRS_SI_LOSS_AMT,s.LN_FRCT_STRS_REO_LOSS_UPB_AMT,'||
     's.LN_FRCT_REFI_STRS_REOLOSS_UPBA,s.LN_FRCT_STRS_INT_INCM_AMT,'||
     's.LN_FRCT_REFI_STRS_INT_AMT,s.LN_FRCT_STRS_IDX_VAL_AMT,'||
     's.LN_FRCT_REFI_STRS_IDX_VAL_AMT,s.LN_FRCT_STRS_MTM_LTV_RT,'||
     's.LN_FRCT_REFI_STRS_MTM_LTV_RT,s.LN_FRCT_STRS_NET_GFEE_AMT,'||
     's.LN_FRCT_REFI_STRS_NET_GFEE_AMT,s.LN_FRCT_STRS_NTLSS_AMT,'||
     's.LN_FRCT_REFI_STRS_NTLSS_AMT,s.LN_FRCT_STRS_NOI_AMT,'||
     's.LN_FRCT_REFI_STRS_NOI_AMT,s.LN_FRCT_STRS_NOTE_RT,'||
     's.LN_FRCT_REFI_STRS_NOTE_RT,s.LN_FRCT_STRS_PPAY_AMT,'||
     's.LN_FRCT_REFI_STRS_PPAY_AMT,s.LN_FRCT_STRS_PODFLT_RT,'||
     's.LN_FRCT_REFI_STRS_PODFLT_RT,s.LN_FRCT_STRS_PFS_PBBY_RT,'||
     's.LN_FRCT_REFI_STRS_PFS_PBBY_RT,s.LN_FRCT_STRS_POPPAY_RT,'||
     's.LN_FRCT_REFI_STRS_POPPAY_RT,s.LN_FRCT_STRS_REO_PBBY_RT,'||
     's.LN_FRCT_REFI_STRS_REO_PBBY_RT,s.LN_FRCT_STRS_PV_FCTR,'||
     's.LN_FRCT_REFI_STRS_PV_FCTR,s.LN_FRCT_STRS_FNM_YLD_MANT_AMT,'||
     's.LN_FRCT_REFI_STRS_FNM_YMT_AMT,s.LN_FRCT_STRS_IVSR_YMT_AMT,'||
     's.LN_FRCT_REFI_STRS_IVSR_YMT_AMT,s.LN_FRCT_STRS_LNDR_YMT_AMT,'||
     's.LN_FRCT_REFI_STRS_LNDR_YMT_AMT,a.LN_FRCT_MDFLT_UPBA,'||
     'a.LN_FRCT_REFI_MDFLT_UPBA,s.LN_FRCT_STRS_MDFLT_UPBA,'||
     's.LN_FRCT_REFI_STRS_MDFLT_UPBA,c.LN_FRCT_CLF_60DDLQ_UPB_AMT,'||
     'c.LN_FRCT_CLF_NTLSS_AMT';

begin

   execute immediate cnt_stmt || 'mf_crdt_works_cflw_inpt_avg' into avg_cnt;
   execute immediate cnt_stmt || 'mf_crdt_works_cflw_inpt_clf' into clf_cnt;
   execute immediate cnt_stmt || 'mf_crdt_works_cflw_inpt_sts' into sts_cnt;

   dbms_output.put_line ('AVG: '||avg_cnt);
   dbms_output.put_line ('CLF: '||clf_cnt);
   dbms_output.put_line ('STS: '||sts_cnt);

   if clf_cnt > 0 then
      main_cols := replace (main_cols, 'x', 'c');
      frm_table := ' from mf_crdt_works_cflw_inpt_clf c'||
                    ' left outer join mf_crdt_works_cflw_inpt_avg a'||
                    ' on (a.fnm_ln_id = c.fnm_ln_id and a.ln_frct_dt = c.ln_frct_dt and a.ln_frct_prd = c.ln_frct_prd)'||
                    ' left outer join mf_crdt_works_cflw_inpt_sts s'||
                    ' on (s.fnm_ln_id = c.fnm_ln_id and s.ln_frct_dt = c.ln_frct_dt and s.ln_frct_prd = c.ln_frct_prd)';
   elsif avg_cnt >= clf_cnt and avg_cnt >= sts_cnt then
      main_cols := replace (main_cols, 'x', 'a');
      frm_table := ' from mf_crdt_works_cflw_inpt_avg a'||
                    ' left outer join mf_crdt_works_cflw_inpt_clf c'||
                    ' on (c.fnm_ln_id = a.fnm_ln_id and c.ln_frct_dt = a.ln_frct_dt and c.ln_frct_prd = a.ln_frct_prd)'||
                    ' left outer join mf_crdt_works_cflw_inpt_sts s'||
                    ' on (s.fnm_ln_id = a.fnm_ln_id and s.ln_frct_dt = a.ln_frct_dt and s.ln_frct_prd = a.ln_frct_prd)';
   else
      main_cols := replace (main_cols, 'x', 's');
      frm_table := ' from mf_crdt_works_cflw_inpt_sts s'||
                    ' left outer join mf_crdt_works_cflw_inpt_clf c'||
                    ' on (c.fnm_ln_id = s.fnm_ln_id and c.ln_frct_dt = s.ln_frct_dt and c.ln_frct_prd = s.ln_frct_prd)'||
                    ' left outer join mf_crdt_works_cflw_inpt_avg a'||
                    ' on (a.fnm_ln_id = s.fnm_ln_id and a.ln_frct_dt = s.ln_frct_dt and a.ln_frct_prd = s.ln_frct_prd)';
   end if;

   sql_stmt := 'insert into mf_crdt_works_cflw_inpt '||
               'select '||main_cols||cols||audit_cols||frm_table;
   --insert into istmt values (sql_stmt);
   execute immediate sql_stmt; 

   dbms_output.put_line ('select '||main_cols||audit_cols||frm_table);

end merge_drv;
/
sho errors

