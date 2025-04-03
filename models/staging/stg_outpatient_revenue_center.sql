select
      cast(desy_sort_key as {{ dbt.type_string() }} ) as desy_sort_key
    , cast(claim_no as {{ dbt.type_string() }} ) as claim_no
    , cast(clm_line_num as {{ dbt.type_string() }} ) as clm_line_num
    , {{ try_to_cast_date('clm_thru_dt', 'YYYYMMDD') }} as clm_thru_dt
    , cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as nch_clm_type_cd
    , cast(rev_cntr as {{ dbt.type_string() }} ) as rev_cntr
    , {{ try_to_cast_date('rev_cntr_dt', 'YYYYMMDD') }} as rev_cntr_dt
    , cast(rev_cntr_apc_hipps_cd as {{ dbt.type_string() }} ) as rev_cntr_apc_hipps_cd
    , cast(hcpcs_cd as {{ dbt.type_string() }} ) as hcpcs_cd
    , cast(hcpcs_1st_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_1st_mdfr_cd
    , cast(hcpcs_2nd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_2nd_mdfr_cd
    , cast(hcpcs_3rd_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_3rd_mdfr_cd
    , cast(hcpcs_4th_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_4th_mdfr_cd
    , cast(rev_cntr_pmt_mthd_ind_cd as {{ dbt.type_string() }} ) as rev_cntr_pmt_mthd_ind_cd
    , cast(rev_cntr_dscnt_ind_cd as {{ dbt.type_string() }} ) as rev_cntr_dscnt_ind_cd
    , cast(rev_cntr_packg_ind_cd as {{ dbt.type_string() }} ) as rev_cntr_packg_ind_cd
    , cast(rev_cntr_otaf_pmt_cd as {{ dbt.type_string() }} ) as rev_cntr_otaf_pmt_cd
    , cast(rev_cntr_ide_ndc_upc_num as {{ dbt.type_string() }} ) as rev_cntr_ide_ndc_upc_num
    , cast(regexp_substr(rev_cntr_unit_cnt, '.') as integer) as rev_cntr_unit_cnt
    , cast(rev_cntr_rate_amt as {{ dbt.type_numeric() }} ) as rev_cntr_rate_amt
    , cast(rev_cntr_blood_ddctbl_amt as {{ dbt.type_numeric() }} ) as rev_cntr_blood_ddctbl_amt
    , cast(rev_cntr_cash_ddctbl_amt as {{ dbt.type_numeric() }} ) as rev_cntr_cash_ddctbl_amt
    , cast(rev_cntr_coinsrnc_wge_adjstd_c as {{ dbt.type_string() }} ) as rev_cntr_coinsrnc_wge_adjstd_c
    , cast(rev_cntr_rdcd_coinsrnc_amt as {{ dbt.type_numeric() }} ) as rev_cntr_rdcd_coinsrnc_amt
    , cast(rev_cntr_1st_msp_pd_amt as {{ dbt.type_numeric() }} ) as rev_cntr_1st_msp_pd_amt
    , cast(rev_cntr_2nd_msp_pd_amt as {{ dbt.type_numeric() }} ) as rev_cntr_2nd_msp_pd_amt
    , cast(rev_cntr_prvdr_pmt_amt as {{ dbt.type_numeric() }} ) as rev_cntr_prvdr_pmt_amt
    , cast(rev_cntr_bene_pmt_amt as {{ dbt.type_numeric() }} ) as rev_cntr_bene_pmt_amt
    , cast(rev_cntr_ptnt_rspnsblty_pmt as {{ dbt.type_string() }} ) as rev_cntr_ptnt_rspnsblty_pmt
    , cast(rev_cntr_pmt_amt_amt as {{ dbt.type_numeric() }} ) as rev_cntr_pmt_amt_amt
    , cast(rev_cntr_tot_chrg_amt as {{ dbt.type_numeric() }} ) as rev_cntr_tot_chrg_amt
    , cast(rev_cntr_ncvrd_chrg_amt as {{ dbt.type_numeric() }} ) as rev_cntr_ncvrd_chrg_amt
    , cast(rev_cntr_stus_ind_cd as {{ dbt.type_string() }} ) as rev_cntr_stus_ind_cd
    , cast(rev_cntr_pricng_ind_cd as {{ dbt.type_string() }} ) as rev_cntr_pricng_ind_cd
    , cast(rev_cntr_rndrng_physn_upin as {{ dbt.type_string() }} ) as rev_cntr_rndrng_physn_upin
    , cast(rev_cntr_rndrng_physn_npi as {{ dbt.type_string() }} ) as rev_cntr_rndrng_physn_npi
    , cast(rev_cntr_rndrng_physn_spclty_cd as {{ dbt.type_string() }} ) as rev_cntr_rndrng_physn_spclty_cd
    , cast(rev_cntr_ddctbl_coinsrnc_cd as {{ dbt.type_string() }} ) as rev_cntr_ddctbl_coinsrnc_cd
    , cast(thrpy_cap_ind_cd1 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd1
    , cast(thrpy_cap_ind_cd2 as {{ dbt.type_string() }} ) as thrpy_cap_ind_cd2
    , cast(rc_ptnt_add_on_pymt_amt as {{ dbt.type_numeric() }} ) as rc_ptnt_add_on_pymt_amt
    , cast(file_name as {{ dbt.type_string() }} ) as file_name
    , cast(null as {{ dbt.type_string() }} ) as ingest_datetime
from {{ source('medicare_lds','outpatient_revenue_center') }}