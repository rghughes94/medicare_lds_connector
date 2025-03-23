select
      cast(desy_sort_key as bigint) as desy_sort_key
    , cast(claim_no as bigint) as claim_no
    , cast(clm_line_num as integer) as clm_line_num
    , cast(clm_thru_dt as date) as clm_thru_dt
    , cast(nch_clm_type_cd as char(2)) as nch_clm_type_cd
    , cast(rev_cntr as char(4)) as rev_cntr
    , cast(rev_cntr_dt as date) as rev_cntr_dt
    , cast(rev_cntr_apc_hipps_cd as char(5)) as rev_cntr_apc_hipps_cd
    , cast(hcpcs_cd as char(5)) as hcpcs_cd
    , cast(hcpcs_1st_mdfr_cd as char(2)) as hcpcs_1st_mdfr_cd
    , cast(hcpcs_2nd_mdfr_cd as char(2)) as hcpcs_2nd_mdfr_cd
    , cast(hcpcs_3rd_mdfr_cd as char(2)) as hcpcs_3rd_mdfr_cd
    , cast(rev_cntr_pmt_mthd_ind_cd as char(1)) as rev_cntr_pmt_mthd_ind_cd
    , cast(rev_cntr_unit_cnt as integer) as rev_cntr_unit_cnt
    , cast(rev_cntr_rate_amt as decimal(9,2)) as rev_cntr_rate_amt
    , cast(rev_cntr_pmt_amt_amt as decimal(9,2)) as rev_cntr_pmt_amt_amt
    , cast(rev_cntr_tot_chrg_amt as decimal(9,2)) as rev_cntr_tot_chrg_amt
    , cast(rev_cntr_ncvrd_chrg_amt as decimal(9,2)) as rev_cntr_ncvrd_chrg_amt
    , cast(rev_cntr_ddctbl_coinsrnc_cd as char(1)) as rev_cntr_ddctbl_coinsrnc_cd
    , cast(rev_cntr_stus_ind_cd as char(2)) as rev_cntr_stus_ind_cd
    , cast(rev_cntr_rndrng_physn_upin as char(6)) as rev_cntr_rndrng_physn_upin
    , cast(rev_cntr_rndrng_physn_npi as char(10)) as rev_cntr_rndrng_physn_npi
    , cast(rev_cntr_rndrng_physn_spclty_cd as char(2)) as rev_cntr_rndrng_physn_spclty_cd
    , cast(rev_cntr_dscnt_ind_cd as char(1)) as rev_cntr_dscnt_ind_cd
    , cast(rev_cntr_ide_ndc_upc_num as char(11)) as rev_cntr_ide_ndc_upc_num
    , cast(rev_cntr_prvdr_pmt_amt as decimal(9,2)) as rev_cntr_prvdr_pmt_amt
    , cast(rev_cntr_ptnt_rspnsblty_pmt as decimal(9,2)) as rev_cntr_ptnt_rspnsblty_pmt
    , cast(rev_cntr_prcng_ind_cd as char(1)) as rev_cntr_prcng_ind_cd
    , cast(thrpy_cap_ind_cd1 as char(1)) as thrpy_cap_ind_cd1
    , cast(thrpy_cap_ind_cd2 as char(1)) as thrpy_cap_ind_cd2
    , cast(file_name as varchar(255)) as file_name
    , cast(ingest_datetime as timestamp) as ingest_datetime
from {{ source('medicare_lds','hha_revenue_center') }}