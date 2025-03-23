select
      cast(desy_sort_key as integer) as desy_sort_key
    , cast(claim_no as {{ dbt.type_string() }}) as claim_no
    , cast(prvdr_num as {{ dbt.type_string() }}) as prvdr_num
    , cast(clm_thru_dt as date) as clm_thru_dt
    , cast(nch_near_line_rec_ident_cd as {{ dbt.type_string() }}) as nch_near_line_rec_ident_cd
    , cast(nch_clm_type_cd as {{ dbt.type_string() }}) as nch_clm_type_cd
    , cast(clm_fac_type_cd as {{ dbt.type_string() }}) as clm_fac_type_cd
    , cast(clm_srvc_clsfctn_type_cd as {{ dbt.type_string() }}) as clm_srvc_clsfctn_type_cd
    , cast(clm_freq_cd as {{ dbt.type_string() }}) as clm_freq_cd
    , cast(fi_num as {{ dbt.type_string() }}) as fi_num
    , cast(clm_mdcr_non_pmt_rsn_cd as {{ dbt.type_string() }}) as clm_mdcr_non_pmt_rsn_cd
    , cast(clm_pmt_amt as numeric(10, 2)) as clm_pmt_amt
    , cast(nch_prmry_pyr_clm_pd_amt as numeric(10, 2)) as nch_prmry_pyr_clm_pd_amt
    , cast(nch_prmry_pyr_cd as {{ dbt.type_string() }}) as nch_prmry_pyr_cd
    , cast(prvdr_state_cd as {{ dbt.type_string() }}) as prvdr_state_cd
    , cast(org_npi_num as {{ dbt.type_string() }}) as org_npi_num
    , cast(srvc_loc_npi_num as {{ dbt.type_string() }}) as srvc_loc_npi_num
    , cast(at_physn_upin as {{ dbt.type_string() }}) as at_physn_upin
    , cast(at_physn_npi as {{ dbt.type_string() }}) as at_physn_npi
    , cast(at_physn_spclty_cd as {{ dbt.type_string() }}) as at_physn_spclty_cd
    , cast(op_physn_npi as {{ dbt.type_string() }}) as op_physn_npi
    , cast(op_physn_spclty_cd as {{ dbt.type_string() }}) as op_physn_spclty_cd
    , cast(ot_physn_npi as {{ dbt.type_string() }}) as ot_physn_npi
    , cast(ot_physn_spclty_cd as {{ dbt.type_string() }}) as ot_physn_spclty_cd
    , cast(rndrng_physn_npi as {{ dbt.type_string() }}) as rndrng_physn_npi
    , cast(rndrng_physn_spclty_cd as {{ dbt.type_string() }}) as rndrng_physn_spclty_cd
    , cast(rfr_physn_npi as {{ dbt.type_string() }}) as rfr_physn_npi
    , cast(rfr_physn_spclty_cd as {{ dbt.type_string() }}) as rfr_physn_spclty_cd
    , cast(ptnt_dschrg_stus_cd as {{ dbt.type_string() }}) as ptnt_dschrg_stus_cd
    , cast(clm_pps_ind_cd as {{ dbt.type_string() }}) as clm_pps_ind_cd
    , cast(clm_tot_chrg_amt as numeric(10, 2)) as clm_tot_chrg_amt
    , cast(prncpal_dgns_cd as {{ dbt.type_string() }}) as prncpal_dgns_cd
    , cast(icd_dgns_cd1 as {{ dbt.type_string() }}) as icd_dgns_cd1
    , cast(icd_dgns_cd2 as {{ dbt.type_string() }}) as icd_dgns_cd2
    , cast(icd_dgns_cd3 as {{ dbt.type_string() }}) as icd_dgns_cd3
    , cast(icd_dgns_cd4 as {{ dbt.type_string() }}) as icd_dgns_cd4
    , cast(icd_dgns_cd5 as {{ dbt.type_string() }}) as icd_dgns_cd5
    , cast(icd_dgns_cd6 as {{ dbt.type_string() }}) as icd_dgns_cd6
    , cast(icd_dgns_cd7 as {{ dbt.type_string() }}) as icd_dgns_cd7
    , cast(icd_dgns_cd8 as {{ dbt.type_string() }}) as icd_dgns_cd8
    , cast(icd_dgns_cd9 as {{ dbt.type_string() }}) as icd_dgns_cd9
    , cast(icd_dgns_cd10 as {{ dbt.type_string() }}) as icd_dgns_cd10
    , cast(icd_dgns_cd11 as {{ dbt.type_string() }}) as icd_dgns_cd11
    , cast(icd_dgns_cd12 as {{ dbt.type_string() }}) as icd_dgns_cd12
    , cast(icd_dgns_cd13 as {{ dbt.type_string() }}) as icd_dgns_cd13
    , cast(icd_dgns_cd14 as {{ dbt.type_string() }}) as icd_dgns_cd14
    , cast(icd_dgns_cd15 as {{ dbt.type_string() }}) as icd_dgns_cd15
    , cast(icd_dgns_cd16 as {{ dbt.type_string() }}) as icd_dgns_cd16
    , cast(icd_dgns_cd17 as {{ dbt.type_string() }}) as icd_dgns_cd17
    , cast(icd_dgns_cd18 as {{ dbt.type_string() }}) as icd_dgns_cd18
    , cast(icd_dgns_cd19 as {{ dbt.type_string() }}) as icd_dgns_cd19
    , cast(icd_dgns_cd20 as {{ dbt.type_string() }}) as icd_dgns_cd20
    , cast(icd_dgns_cd21 as {{ dbt.type_string() }}) as icd_dgns_cd21
    , cast(icd_dgns_cd22 as {{ dbt.type_string() }}) as icd_dgns_cd22
    , cast(icd_dgns_cd23 as {{ dbt.type_string() }}) as icd_dgns_cd23
    , cast(icd_dgns_cd24 as {{ dbt.type_string() }}) as icd_dgns_cd24
    , cast(icd_dgns_cd25 as {{ dbt.type_string() }}) as icd_dgns_cd25
    , cast(fst_dgns_e_cd as {{ dbt.type_string() }}) as fst_dgns_e_cd
    , cast(icd_dgns_e_cd1 as {{ dbt.type_string() }}) as icd_dgns_e_cd1
    , cast(icd_dgns_e_cd2 as {{ dbt.type_string() }}) as icd_dgns_e_cd2
    , cast(icd_dgns_e_cd3 as {{ dbt.type_string() }}) as icd_dgns_e_cd3
    , cast(icd_dgns_e_cd4 as {{ dbt.type_string() }}) as icd_dgns_e_cd4
    , cast(icd_dgns_e_cd5 as {{ dbt.type_string() }}) as icd_dgns_e_cd5
    , cast(icd_dgns_e_cd6 as {{ dbt.type_string() }}) as icd_dgns_e_cd6
    , cast(icd_dgns_e_cd7 as {{ dbt.type_string() }}) as icd_dgns_e_cd7
    , cast(icd_dgns_e_cd8 as {{ dbt.type_string() }}) as icd_dgns_e_cd8
    , cast(icd_dgns_e_cd9 as {{ dbt.type_string() }}) as icd_dgns_e_cd9
    , cast(icd_dgns_e_cd10 as {{ dbt.type_string() }}) as icd_dgns_e_cd10
    , cast(icd_dgns_e_cd11 as {{ dbt.type_string() }}) as icd_dgns_e_cd11
    , cast(icd_dgns_e_cd12 as {{ dbt.type_string() }}) as icd_dgns_e_cd12
    , cast(clm_hha_lupa_ind_cd as {{ dbt.type_string() }}) as clm_hha_lupa_ind_cd
    , cast(clm_hha_rfrl_cd as {{ dbt.type_string() }}) as clm_hha_rfrl_cd
    , cast(clm_hha_tot_visit_cnt as integer) as clm_hha_tot_visit_cnt
    , cast(clm_admsn_dt as date) as clm_admsn_dt
    , cast(dob_dt as date) as dob_dt
    , cast(gndr_cd as {{ dbt.type_string() }}) as gndr_cd
    , cast(bene_race_cd as {{ dbt.type_string() }}) as bene_race_cd
    , cast(bene_cnty_cd as {{ dbt.type_string() }}) as bene_cnty_cd
    , cast(bene_state_cd as {{ dbt.type_string() }}) as bene_state_cd
    , cast(cwf_bene_mdcr_stus_cd as {{ dbt.type_string() }}) as cwf_bene_mdcr_stus_cd
    , cast(clm_query_cd as {{ dbt.type_string() }}) as clm_query_cd
    , cast(fi_clm_actn_cd as {{ dbt.type_string() }}) as fi_clm_actn_cd
    , cast(clm_mco_pd_sw as {{ dbt.type_string() }}) as clm_mco_pd_sw
    , cast(nch_bene_dschrg_dt as date) as nch_bene_dschrg_dt
    , cast(clm_trtmt_authrztn_num as {{ dbt.type_string() }}) as clm_trtmt_authrztn_num
    , cast(clm_prcr_rtrn_cd as {{ dbt.type_string() }}) as clm_prcr_rtrn_cd
    , cast(clm_next_gnrtn_aco_ind_cd1 as {{ dbt.type_string() }}) as clm_next_gnrtn_aco_ind_cd1
    , cast(clm_next_gnrtn_aco_ind_cd2 as {{ dbt.type_string() }}) as clm_next_gnrtn_aco_ind_cd2
    , cast(clm_next_gnrtn_aco_ind_cd3 as {{ dbt.type_string() }}) as clm_next_gnrtn_aco_ind_cd3
    , cast(clm_next_gnrtn_aco_ind_cd4 as {{ dbt.type_string() }}) as clm_next_gnrtn_aco_ind_cd4
    , cast(clm_next_gnrtn_aco_ind_cd5 as {{ dbt.type_string() }}) as clm_next_gnrtn_aco_ind_cd5
    , cast(aco_id_num as {{ dbt.type_string() }}) as aco_id_num
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(ingest_datetime as timestamp) as ingest_datetime
from {{ source('medicare_lds','hha_base_claim') }}