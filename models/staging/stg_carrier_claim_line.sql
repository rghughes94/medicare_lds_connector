select
      cast(desy_sort_key as {{ dbt.type_string() }} ) as 
    , cast(claim_no as {{ dbt.type_string() }} ) as 
    , cast(line_num as integer) as 
    , {{ try_to_cast_date('c.clm_thru_dt', 'YYYYMMDD') }} as 
    , cast(nch_clm_type_cd as {{ dbt.type_string() }} ) as 
    , cast(carr_prfrng_pin_num as {{ dbt.type_string() }} ) as 
    , cast(prf_physn_upin as {{ dbt.type_string() }} ) as 
    , cast(prf_physn_npi as {{ dbt.type_string() }} ) as 
    , cast(org_npi_num as {{ dbt.type_string() }} ) as 
    , cast(carr_line_prvdr_type_cd as {{ dbt.type_string() }} ) as 
    , cast(prvdr_state_cd as {{ dbt.type_string() }} ) as 
    , cast(prvdr_spclty as {{ dbt.type_string() }} ) as 
    , cast(prtcptng_ind_cd as {{ dbt.type_string() }} ) as 
    , cast(carr_line_rdcd_pmt_phys_astn_c as {{ dbt.type_string() }} ) as 
    , cast(regexp_substr(line_srvc_cnt,'.') as integer) as
    , cast(line_cms_type_srvc_cd as {{ dbt.type_string() }} ) as 
    , cast(line_place_of_srvc_cd as {{ dbt.type_string() }} ) as 
    , cast(carr_line_prcng_lclty_cd as {{ dbt.type_string() }} ) as 
    , {{ try_to_cast_date('line_last_expns_dt', 'YYYYMMDD') }} as 
    , cast(hcpcs_cd as {{ dbt.type_string() }} ) as 
    , cast(hcpcs_1st_mdfr_cd as {{ dbt.type_string() }} ) as 
    , cast(hcpcs_2nd_mdfr_cd as {{ dbt.type_string() }} ) as 
    , cast(betos_cd as {{ dbt.type_string() }} ) as 
    , cast(line_nch_pmt_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_bene_pmt_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_prvdr_pmt_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_bene_ptb_ddctbl_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_bene_prmry_pyr_cd as {{ dbt.type_string() }} ) as 
    , cast(line_bene_prmry_pyr_pd_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_coinsrnc_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_sbmtd_chrg_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_alowd_chrg_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_prcsg_ind_cd as {{ dbt.type_string() }} ) as 
    , cast(line_pmt_80_100_cd as {{ dbt.type_string() }} ) as 
    , cast(line_service_deductible as {{ dbt.type_numeric() }} ) as 
    , cast(carr_line_mtus_cnt as {{ dbt.type_numeric() }} ) as 
    , cast(carr_line_mtus_cd as {{ dbt.type_string() }} ) as 
    , cast(line_icd_dgns_cd as {{ dbt.type_string() }} ) as 
    , cast(line_icd_dgns_vrsn_cd as {{ dbt.type_string() }} ) as 
    , cast(line_hct_hgb_rslt_num as {{ dbt.type_string() }} ) as 
    , cast(line_hct_hgb_type_cd as {{ dbt.type_string() }} ) as 
    , cast(line_ndc_cd as {{ dbt.type_string() }} ) as 
    , cast(carr_line_clia_lab_num as {{ dbt.type_string() }} ) as 
    , cast(carr_line_ansthsa_unit_cnt as {{ dbt.type_numeric() }} ) as 
    , cast(carr_line_cl_chrg_amt as {{ dbt.type_numeric() }} ) as 
    , cast(line_othr_apld_ind_cd1 as {{ dbt.type_string() }} ) as 
    , cast(line_othr_apld_ind_cd2 as {{ dbt.type_string() }} ) as 
    , cast(line_othr_apld_ind_cd3 as {{ dbt.type_string() }} ) as 
    , cast(line_othr_apld_ind_cd4 as {{ dbt.type_string() }} ) as 
    , cast(line_othr_apld_ind_cd5 as {{ dbt.type_string() }} ) as 
    , cast(line_othr_apld_ind_cd6 as {{ dbt.type_string() }} ) as 
    , cast(line_othr_apld_ind_cd7 as {{ dbt.type_string() }} ) as 
    , cast(line_othr_apld_amt1 as {{ dbt.type_numeric() }} ) as 
    , cast(line_othr_apld_amt2 as {{ dbt.type_numeric() }} ) as 
    , cast(line_othr_apld_amt3 as {{ dbt.type_numeric() }} ) as 
    , cast(line_othr_apld_amt4 as {{ dbt.type_numeric() }} ) as 
    , cast(line_othr_apld_amt5 as {{ dbt.type_numeric() }} ) as 
    , cast(line_othr_apld_amt6 as {{ dbt.type_numeric() }} ) as 
    , cast(line_othr_apld_amt7 as {{ dbt.type_numeric() }} ) as 
    , cast(thrpy_cap_ind_cd1 as {{ dbt.type_string() }} ) as 
    , cast(thrpy_cap_ind_cd2 as {{ dbt.type_string() }} ) as 
    , cast(thrpy_cap_ind_cd3 as {{ dbt.type_string() }} ) as 
    , cast(thrpy_cap_ind_cd4 as {{ dbt.type_string() }} ) as 
    , cast(thrpy_cap_ind_cd5 as {{ dbt.type_string() }} ) as 
    , cast(clm_next_gnrtn_aco_ind_cd1 as {{ dbt.type_string() }} ) as 
    , cast(clm_next_gnrtn_aco_ind_cd2 as {{ dbt.type_string() }} ) as 
    , cast(clm_next_gnrtn_aco_ind_cd3 as {{ dbt.type_string() }} ) as 
    , cast(clm_next_gnrtn_aco_ind_cd4 as {{ dbt.type_string() }} ) as 
    , cast(clm_next_gnrtn_aco_ind_cd5 as {{ dbt.type_string() }} ) as 
    , cast(file_name as {{ dbt.type_string() }} ) as 
    , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime 
from {{ source('medicare_lds','carrier_claim_line') }}