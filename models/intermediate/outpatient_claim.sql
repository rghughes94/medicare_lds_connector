with outpatient_base_claim as (

    select *
         , left(clm_thru_dt,4) as clm_thru_dt_year
    from {{ ref('stg_outpatient_base_claim') }}
    where clm_mdcr_non_pmt_rsn_cd is null
    /** filter out denied claims **/

)

, header_payment as (

    select
          claim_no as claim_id
        , clm_pmt_amt as paid_amount
        , /** medicare payment **/
          clm_pmt_amt
              /** beneficiary payment **/
              + nch_bene_ptb_coinsrnc_amt
              + nch_bene_ptb_ddctbl_amt
              + nch_bene_blood_ddctbl_lblty_am
              /** primary payer payment **/
              + nch_prmry_pyr_clm_pd_amt
          as total_cost_amount
        , clm_tot_chrg_amt as charge_amount
    from outpatient_base_claim
)

, claim_start_date as (

    select
          claim_no
        , min(coalesce(rev_cntr_dt,clm_thru_dt)) as claim_start_date
    from {{ ref('stg_outpatient_revenue_center') }}
    group by claim_no

)

select
      /* Claim ID is not unique across claim types.  Concatenating original claim ID, claim year, and claim type. */
      b.claim_no
        || b.clm_thru_dt_year
        || b.nch_clm_type_cd
      as claim_id
    , l.clm_line_num as claim_line_number
    , cast('institutional'  as {{ dbt.type_string() }} ) as claim_type
    , b.desy_sort_key as person_id
    , b.desy_sort_key as member_id
    , cast('medicare' as {{ dbt.type_string() }} ) as payer
    , cast('medicare' as {{ dbt.type_string() }} ) as plan
    , c.claim_start_date as claim_start_date
    , b.clm_thru_dt as claim_end_date
    , l.rev_cntr_dt as claim_line_start_date
    , l.rev_cntr_dt as claim_line_end_date
    , date(NULL) as admission_date
    , date(NULL) as discharge_date
    , cast(NULL as {{ dbt.type_string() }} ) as admit_source_code
    , cast(NULL as {{ dbt.type_string() }} ) as admit_type_code
    , b.ptnt_dschrg_stus_cd as discharge_disposition_code
    , cast(NULL as {{ dbt.type_string() }} ) as place_of_service_code
    , b.clm_fac_type_cd
        || b.clm_srvc_clsfctn_type_cd
        || b.clm_freq_cd
      as bill_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as apr_drg_code
    , l.rev_cntr as revenue_center_code
    , l.rev_cntr_unit_cnt as service_unit_quantity
    , l.hcpcs_cd as hcpcs_code
    , l.hcpcs_1st_mdfr_cd as hcpcs_modifier_1
    , l.hcpcs_2nd_mdfr_cd as hcpcs_modifier_2
    , l.hcpcs_3rd_mdfr_cd as hcpcs_modifier_3
    , l.hcpcs_4th_mdfr_cd as hcpcs_modifier_4
    , cast(NULL as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , b.rndrng_physn_npi as rendering_npi
    , cast(NULL as {{ dbt.type_string() }} ) as rendering_tin
    , b.org_npi_num as billing_npi
    , cast(NULL as {{ dbt.type_string() }} ) as billing_tin
    , coalesce(b.org_npi_num,b.srvc_loc_npi_num) as facility_npi
    , date(NULL) as paid_date
    , coalesce(
            p.paid_amount
            , cast(0 as {{ dbt.type_numeric() }})
      ) as paid_amount
    , cast(NULL as {{ dbt.type_numeric() }}) as allowed_amount
    , p.charge_amount as charge_amount
    , cast(null as {{ dbt.type_numeric() }}) as coinsurance_amount
    , cast(null as {{ dbt.type_numeric() }}) as copayment_amount
    , cast(null as {{ dbt.type_numeric() }}) as deductible_amount
    , p.total_cost_amount as total_cost_amount
    , cast('icd-10-cm'  as {{ dbt.type_string() }} ) as diagnosis_code_type
    , b.prncpal_dgns_cd as diagnosis_code_1
    , b.icd_dgns_cd2 as diagnosis_code_2
    , b.icd_dgns_cd3 as diagnosis_code_3
    , b.icd_dgns_cd4 as diagnosis_code_4
    , b.icd_dgns_cd5 as diagnosis_code_5
    , b.icd_dgns_cd6 as diagnosis_code_6
    , b.icd_dgns_cd7 as diagnosis_code_7
    , b.icd_dgns_cd8 as diagnosis_code_8
    , b.icd_dgns_cd9 as diagnosis_code_9
    , b.icd_dgns_cd10 as diagnosis_code_10
    , b.icd_dgns_cd11 as diagnosis_code_11
    , b.icd_dgns_cd12 as diagnosis_code_12
    , b.icd_dgns_cd13 as diagnosis_code_13
    , b.icd_dgns_cd14 as diagnosis_code_14
    , b.icd_dgns_cd15 as diagnosis_code_15
    , b.icd_dgns_cd16 as diagnosis_code_16
    , b.icd_dgns_cd17 as diagnosis_code_17
    , b.icd_dgns_cd18 as diagnosis_code_18
    , b.icd_dgns_cd19 as diagnosis_code_19
    , b.icd_dgns_cd20 as diagnosis_code_20
    , b.icd_dgns_cd21 as diagnosis_code_21
    , b.icd_dgns_cd22 as diagnosis_code_22
    , b.icd_dgns_cd23 as diagnosis_code_23
    , b.icd_dgns_cd24 as diagnosis_code_24
    , b.icd_dgns_cd25 as diagnosis_code_25
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , cast('icd-10-pcs' as {{ dbt.type_string() }} ) as procedure_code_type
    , b.icd_prcdr_cd1 as procedure_code_1
    , b.icd_prcdr_cd2 as procedure_code_2
    , b.icd_prcdr_cd3 as procedure_code_3
    , b.icd_prcdr_cd4 as procedure_code_4
    , b.icd_prcdr_cd5 as procedure_code_5
    , b.icd_prcdr_cd6 as procedure_code_6
    , b.icd_prcdr_cd7 as procedure_code_7
    , b.icd_prcdr_cd8 as procedure_code_8
    , b.icd_prcdr_cd9 as procedure_code_9
    , b.icd_prcdr_cd10 as procedure_code_10
    , b.icd_prcdr_cd11 as procedure_code_11
    , b.icd_prcdr_cd12 as procedure_code_12
    , b.icd_prcdr_cd13 as procedure_code_13
    , b.icd_prcdr_cd14 as procedure_code_14
    , b.icd_prcdr_cd15 as procedure_code_15
    , b.icd_prcdr_cd16 as procedure_code_16
    , b.icd_prcdr_cd17 as procedure_code_17
    , b.icd_prcdr_cd18 as procedure_code_18
    , b.icd_prcdr_cd19 as procedure_code_19
    , b.icd_prcdr_cd20 as procedure_code_20
    , b.icd_prcdr_cd21 as procedure_code_21
    , b.icd_prcdr_cd22 as procedure_code_22
    , b.icd_prcdr_cd23 as procedure_code_23
    , b.icd_prcdr_cd24 as procedure_code_24
    , b.icd_prcdr_cd25 as procedure_code_25
    , b.prcdr_dt1 as procedure_date_1
    , b.prcdr_dt2 as procedure_date_2
    , b.prcdr_dt3 as procedure_date_3
    , b.prcdr_dt4 as procedure_date_4
    , b.prcdr_dt5 as procedure_date_5
    , b.prcdr_dt6 as procedure_date_6
    , b.prcdr_dt7 as procedure_date_7
    , b.prcdr_dt8 as procedure_date_8
    , b.prcdr_dt9 as procedure_date_9
    , b.prcdr_dt10 as procedure_date_10
    , b.prcdr_dt11 as procedure_date_11
    , b.prcdr_dt12 as procedure_date_12
    , b.prcdr_dt13 as procedure_date_13
    , b.prcdr_dt14 as procedure_date_14
    , b.prcdr_dt15 as procedure_date_15
    , b.prcdr_dt16 as procedure_date_16
    , b.prcdr_dt17 as procedure_date_17
    , b.prcdr_dt18 as procedure_date_18
    , b.prcdr_dt19 as procedure_date_19
    , b.prcdr_dt20 as procedure_date_20
    , b.prcdr_dt21 as procedure_date_21
    , b.prcdr_dt22 as procedure_date_22
    , b.prcdr_dt23 as procedure_date_23
    , b.prcdr_dt24 as procedure_date_24
    , b.prcdr_dt25 as procedure_date_25
    , cast(1 as int) as in_network_flag
    , cast('medicare_lds' as {{ dbt.type_string() }} ) as data_source
    , b.file_name
    , b.ingest_datetime
from outpatient_base_claim as b
    inner join {{ ref('stg_outpatient_revenue_center') }} as l
        on b.claim_no = l.claim_no
    /* Payment is provided at the header level only.  Populating on revenue center 001 to avoid duplication. */
    left join header_payment as p
        on b.claim_no = p.claim_id
        and l.rev_cntr = '0001'
    left join claim_start_date as c
        on b.claim_no = c.claim_no