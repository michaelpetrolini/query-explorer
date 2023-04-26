--------------------------------------------------------------------------------
-- Description:
-- Epic Link: https://agile.at.sky/browse/DPAEP-534
-- DM: Giuseppe Messina
-- Documentation: https://wiki.at.sky/pages/viewpage.action?spaceKey=DAI&title=F.1.3.3+Centralized+Sales+Channel and
--                https://wiki.at.sky/pages/viewpage.action?pageId=363627505
-- KPI:
--------------------------------------------------------------------------------
WITH
extra_promo as (
    select
        distinct
        pro.cod_promo as cod_promo
     from `$source_project.order_management.promo_extrainfo` pro
     where date(pro.partition_date) in (select max(date(partition_date)) from `$source_project.order_management.promo_extrainfo`)
     and promo_category LIKE ("%TEST 3P%")
),

sales_daily_tv as (
    SELECT
        id_order_salesforce,
        id_order,
        scenario_sales_order,
        id_account,
        id_egon_number,
        sales_promotion_contract,
        order_type,
        order_status,
        order_sub_status,
        order_action,
        order_offer_type,
        order_platform_entry,
        order_sales_channel,
        promo_name,
        promo_macrocategory,
        cod_promo,
        cod_promo_ext,
        promo_category,
        des_promo,
        cod_contract,
        cod_parent_contract,
        id_parent,
        id_contract_bb,
        shipping_city,
        shipping_address,
        shipping_street,
        shipping_province,
        shipping_country,
        cod_shipping_postal,
        shipping_region,
        contract_status,
        cod_fiscal_payer,
        payment_method,
        payment_status,
        billing_frequency,
        payment_reason_fail,
        payment_sub_status,
        payment_card_type,
        cod_product_list,
        date_contract_creation,
        date_order_creation,
        date_first_activation,
        cod_dealer,
        cod_vendor,
        des_dealer_channel_l1,
        des_dealer_channel_l2,
        des_dealer_channel_l3,
        vendor_channel_qualification,
        cod_first_seller,
        des_first_seller_channel_l1,
        des_first_seller_channel_l2,
        des_first_seller_channel_l3,
        seller_first_channel_qualification,
        cod_second_seller,
        des_second_seller_channel_l1,
        des_second_seller_channel_l2,
        des_second_seller_channel_l3,
        seller_second_channel_qualification,
        flg_check_contract_status,
        flg_check_payment,
        flg_check_fiscal_code,
        flg_check_order_promo,
        flg_sales_daily,
        flg_promo_bundle_tnb_multi_order,
        flg_trybuy,
        partition_date,
        des_dealer_commission_fees_plan ,
        des_dealer_commission,
        cube_owner,
        serial_number_first_voucher
    FROM `$source_project.trading.sales_daily_tv`
    WHERE DATE(date_order_creation) >= DATE('2021-11-01')
),

sales_daily_skiwifi as (
    SELECT
      id_contract_bb,
      id_account,
      id_egon_number ,
      date_contract_creation ,
      flg_promo_bundle_tnb_multi_order ,
      scenario_sales_order ,
      id_order ,
      cod_contract ,
      partition_date ,
      id_order_salesforce ,
      sales_promotion_contract ,
      order_type ,
      order_status ,
      order_sub_status,
      order_action ,
      order_offer_type ,
      order_platform_entry ,
      order_sales_channel ,
      promo_name ,
      cod_promo ,
      des_promo,
      cod_parent_contract ,
      id_parent ,
      shipping_city ,
      shipping_address ,
      shipping_street,
      shipping_province,
      shipping_country,
      shipping_region,
      cod_shipping_postal,
      contract_status ,
      cod_fiscal_payer ,
      payment_method ,
      payment_status ,
      billing_frequency ,
      payment_reason_fail ,
      payment_sub_status,
      payment_card_type ,
      cod_product_list ,
      date_order_creation ,
      date_first_activation,
      cod_dealer,
      cod_vendor,
      des_dealer_channel_l1,
      des_dealer_channel_l2,
      des_dealer_channel_l3,
      vendor_channel_qualification,
      cod_first_seller,
      des_first_seller_channel_l1,
      des_first_seller_channel_l2,
      des_first_seller_channel_l3,
      seller_first_channel_qualification,
      cod_second_seller,
      des_second_seller_channel_l1,
      des_second_seller_channel_l2,
      des_second_seller_channel_l3,
      seller_second_channel_qualification,
      flg_check_contract_status,
      flg_check_payment,
      flg_check_fiscal_code,
      flg_check_order_promo,
      flg_sales_daily,
      des_commission_fees_plan ,
      des_commission
    FROM `$source_project.trading.sales_skywifi_daily`
    where scenario_sales_order <> '3P'
    and DATE(partition_date) <= DATE("2022-12-31")
),

sales_glass_daily as (
    SELECT * EXCEPT (record_type_name)
    FROM (
        SELECT
            id_order_salesforce,
            id_order,
            scenario_sales_order,
            id_account,
            id_egon_number_cntr AS id_egon_number,
            sales_promotion_contract,
            order_type,
            order_status,
            order_sub_status,
            order_action,
            order_offer_type,
            order_platform_entry,
            order_sales_channel,
            promo_name,
            promo_macrocategory,
            cod_promo,
            cod_promo_ext,
            promo_category,
            des_promo,
            contract_code AS cod_contract,
            contract_code_parent AS cod_parent_contract,
            id_parent,
            IF(record_type_name = "MA", id_contract, NULL) AS id_contract_ma,
            IF(record_type_name = "HW", id_contract, NULL) AS id_contract_hw,
            id_contract_bb,
            contract_code_media_aggregator AS contract_code_media_aggregator_glass,
            contract_code_hardware_glass,
            contract_code_bb AS contract_code_bb_glass,
            shipping_city,
            shipping_address,
            shipping_street,
            shipping_state AS shipping_province,
            shipping_country,
            postal_code_shipping AS cod_shipping_postal,
            des_region AS shipping_region,
            contract_status,
            fiscal_code_payer AS cod_fiscal_payer,
            payment_method,
            payment_status,
            billing_frequency,
            payment_reason_fail,
            payment_sub_status,
            pay_card_type AS payment_card_type,
            cod_product_list,
            date_create_ts_cntr AS date_contract_creation,
            date_order_creation_ts AS date_order_creation,
            date(date_first_activation_cntr) AS date_first_activation,
            cod_dealer_odt AS cod_dealer,
            cod_vendor,
            dealer_des_channel_l1 AS des_dealer_channel_l1,
            dealer_des_channel_l2 AS des_dealer_channel_l2,
            dealer_des_channel_l3 des_dealer_channel_l3,
            vendor_channel_qualification,
            cod_first_seller_odt AS cod_first_seller,
            des_first_seller_channel_l1,
            des_first_seller_channel_l2,
            des_first_seller_channel_l3,
            first_seller_channel_qualification AS seller_first_channel_qualification,
            cod_second_seller,
            des_second_seller_channel_l1,
            des_second_seller_channel_l2,
            des_second_seller_channel_l3,
            second_seller_channel_qualification AS seller_second_channel_qualification,
            flg_check_contract_status,
            flg_check_payment,
            flg_check_fiscal_code,
            flg_promo_bundle_tnb_multi_order,
            flg_trybuy,
            partition_date,
            des_commission_fees_plan AS des_dealer_commission_fees_plan ,
            des_commission AS des_dealer_commission,
            NULL AS cube_owner,
            NULL AS serial_number_first_voucher,
            NULL AS flg_check_order_promo,
            CASE WHEN (flg_migration_from_skytv = 0 AND flg_customer_trial = 0 AND flg_staff_trial = 0) THEN 1 ELSE 0 END AS flg_sales_daily,
            sales_channel AS glass_business_sales_channel,
            sales_channel_detail AS glass_business_sales_channel_detail,
            record_type_name
        FROM `$source_project.trading.sales_glass_daily`
        WHERE DATE(partition_date) = DATE("2022-12-31")
    )
    WHERE record_type_name = "MA"
),

sales_tv_bundle_tnb_multi_order as (
select tmp.*
  from
       (
       select sub.* except (bb_id_contract_bb,id_contract_bb),
              if(rn = 1, bb_id_contract_bb, null) as id_contract_bb,
              case
                when rn = 1 then 1
                else 0
               end as  flg_bundle,
              case
                when rn = 1 then 'BUNDLE BB+TV TNB MULTI-ORDER'
                else NULL
               end as bundle_type
         from
              (
              select tv.*,
                     row_number()
                       over (partition by tv.id_account, tv.id_egon_number, date(tv.date_contract_creation)
                                 order by tv.date_contract_creation, bb.date_contract_creation) as rn,
                     bb.id_contract_bb as bb_id_contract_bb
                from sales_daily_tv tv
                      inner join
                     sales_daily_skiwifi bb
                        on tv.id_account = bb.id_account
                       and tv.id_egon_number = bb.id_egon_number
                       and date(tv.date_contract_creation, "Europe/Rome") = date(bb.date_contract_creation, "Europe/Rome")
    where tv.flg_promo_bundle_tnb_multi_order = 1
                       and bb.flg_promo_bundle_tnb_multi_order = 1
                 and UPPER(TRIM(tv.scenario_sales_order)) = 'TV'
                 and UPPER(TRIM(bb.scenario_sales_order)) = 'SABB'
                 and tv.flg_sales_daily = 1
              ) sub
       ) as tmp
 where rn = 1
),

sales_tv_tmp as (
select tv1.id_order_salesforce,
       tv1.id_order,
       tv1.scenario_sales_order,
       tv1.id_account,
       tv1.sales_promotion_contract,
       tv1.order_type,
       tv1.order_status,
       tv1.order_sub_status,
       tv1.order_action,
       tv1.order_offer_type,
       tv1.order_platform_entry,
       tv1.order_sales_channel,
       tv1.promo_name,
       tv1.promo_macrocategory,
       tv1.cod_promo,
       tv1.cod_promo_ext,
       tv1.promo_category,
       tv1.des_promo,
       tv1.cod_contract,
       tv1.cod_parent_contract,
       tv1.id_parent,
       tv1.shipping_city,
       tv1.shipping_address,
       tv1.shipping_street,
       tv1.shipping_province,
       tv1.shipping_country,
       tv1.cod_shipping_postal,
       tv1.shipping_region,
       tv1.contract_status,
       tv1.cod_fiscal_payer,
       tv1.payment_method,
       tv1.payment_status,
       tv1.billing_frequency,
       tv1.payment_reason_fail,
       tv1.payment_sub_status,
       tv1.payment_card_type,
       tv1.cod_product_list,
       tv1.date_contract_creation,
       tv1.date_order_creation,
       tv1.date_first_activation,
       tv1.cod_dealer,
       tv1.cod_vendor,
       tv1.des_dealer_channel_l1,
       tv1.des_dealer_channel_l2,
       tv1.des_dealer_channel_l3,
       tv1.vendor_channel_qualification,
       tv1.cod_first_seller,
       tv1.des_first_seller_channel_l1,
       tv1.des_first_seller_channel_l2,
       tv1.des_first_seller_channel_l3,
       tv1.seller_first_channel_qualification,
       tv1.cod_second_seller,
       tv1.des_second_seller_channel_l1,
       tv1.des_second_seller_channel_l2,
       tv1.des_second_seller_channel_l3,
       tv1.seller_second_channel_qualification,
       tv1.flg_check_contract_status,
       tv1.flg_check_payment,
       tv1.flg_check_fiscal_code,
       tv1.flg_check_order_promo,
       tv1.flg_sales_daily,
       tv1.flg_trybuy,
       tv1.des_dealer_commission_fees_plan,
       tv1.des_dealer_commission,
       tv1.partition_date,
       tv1.cube_owner,
       tv1.serial_number_first_voucher,
       coalesce(tv2.id_contract_bb, tv1.id_contract_bb) as id_contract_bb,
       case
         when ep.cod_promo is not null and UPPER(TRIM(tv1.scenario_sales_order)) = 'TV' then 1
         when tv2.id_order is not null then 1
         when UPPER(TRIM(tv1.scenario_sales_order)) = '3P' then 1
         else 0
        end as  flg_bundle,
       case
         when ep.cod_promo is not null and UPPER(TRIM(tv1.scenario_sales_order)) = 'TV' then 'BUNDLE BB+TV SEQUENTIAL ORDERS'
         when tv2.id_order is not null then 'BUNDLE BB+TV TNB MULTI-ORDER'
         when UPPER(TRIM(tv1.scenario_sales_order)) = '3P' then 'BUNDLE BB+TV REGULAR'
         else NULL
        end as bundle_type
  from
       sales_daily_tv tv1
        left join
       sales_tv_bundle_tnb_multi_order tv2
          on tv1.id_order = tv2.id_order
        left join
       sales_daily_skiwifi bb
          on tv1.id_order = bb.id_order
        left join extra_promo ep on tv1.cod_promo=ep.cod_promo
),

sales_bb_tmp as (
select bb.* except(cod_contract),
            bb.cod_contract as cod_contract,
            case
            when tv.bundle_type = 'BUNDLE BB+TV SEQUENTIAL ORDERS' then 1
            else 0
            end as flg_bundle,
       case
            when tv.bundle_type = 'BUNDLE BB+TV SEQUENTIAL ORDERS' then 'BUNDLE BB+TV SEQUENTIAL ORDERS'
            else null
            end as bundle_type
        from sales_daily_skiwifi bb
            left join
            sales_tv_tmp tv
        on  bb.cod_contract = tv.cod_contract
        where UPPER(bb.scenario_sales_order) = 'UPSELLING BB ON TV' -- WA52
    union distinct
    select  bb.* except(cod_contract),
            case
        when UPPER(tv.bundle_type) = 'BUNDLE BB+TV TNB MULTI-ORDER' then tv.cod_contract
        end as cod_contract,
       case
         when tv.flg_bundle = 1 then 1
         else 0
        end as flg_bundle,
       case
        when UPPER(tv.bundle_type) = 'BUNDLE BB+TV TNB MULTI-ORDER' then 'BUNDLE BB+TV TNB MULTI-ORDER'
        ELSE NULL
        end as bundle_type
  from sales_daily_skiwifi bb
  left join
       sales_tv_tmp tv
        on  bb.id_contract_bb = tv.id_contract_bb -- BL275/3P
        where UPPER(TRIM(bb.scenario_sales_order)) = 'SABB'
),

sales_daily_skywifi_id_order as (
    SELECT
       id_order
    FROM sales_bb_tmp bb -- same trading.sales_skywifi_daily bb
    WHERE DATE(partition_date) = DATE("2022-12-31")
    EXCEPT DISTINCT
    SELECT
       id_order
    FROM sales_daily_tv
),

union_sales_daily as (
   SELECT
       tv.id_order_salesforce,
       tv.id_order,
       tv.scenario_sales_order,
       tv.id_account,
       tv.sales_promotion_contract,
       tv.order_type,
       tv.order_status,
       tv.order_sub_status,
       tv.order_action,
       tv.order_offer_type,
       tv.order_platform_entry,
       tv.order_sales_channel,
       tv.promo_name,
       tv.promo_macrocategory,
       tv.cod_promo,
       tv.cod_promo_ext,
       tv.promo_category,
       tv.des_promo,
       tv.cod_contract,
       tv.cod_parent_contract,
       tv.id_parent,
       tv.id_contract_bb,
       tv.shipping_city,
       tv.shipping_address,
       tv.shipping_street,
       tv.shipping_province,
       tv.shipping_country,
       tv.cod_shipping_postal,
       tv.shipping_region,
       tv.contract_status,
       tv.cod_fiscal_payer,
       tv.payment_method,
       tv.payment_status,
       tv.billing_frequency,
       tv.payment_reason_fail,
       tv.payment_sub_status,
       tv.payment_card_type,
       tv.cod_product_list,
       tv.date_contract_creation,
       tv.date_order_creation,
       tv.date_first_activation,
       tv.cod_dealer,
       tv.cod_vendor,
       tv.des_dealer_channel_l1,
       tv.des_dealer_channel_l2,
       tv.des_dealer_channel_l3,
       tv.vendor_channel_qualification,
       tv.cod_first_seller,
       tv.des_first_seller_channel_l1,
       tv.des_first_seller_channel_l2,
       tv.des_first_seller_channel_l3,
       tv.seller_first_channel_qualification,
       tv.cod_second_seller,
       tv.des_second_seller_channel_l1,
       tv.des_second_seller_channel_l2,
       tv.des_second_seller_channel_l3,
       tv.seller_second_channel_qualification,
       tv.flg_check_contract_status,
       tv.flg_check_payment,
       tv.flg_check_fiscal_code,
       tv.flg_check_order_promo,
       tv.flg_sales_daily,
       tv.flg_bundle,
       tv.bundle_type,
       tv.flg_trybuy,
       tv.des_dealer_commission_fees_plan,
       tv.des_dealer_commission,
       tv.cube_owner,
       tv.serial_number_first_voucher
    FROM sales_tv_tmp tv
    LEFT JOIN
    (select *
     from`$source_b2c_project.trading.sales_common`
    where DATE(partition_date) < DATE("2022-12-31")
    ) common
    ON tv.cod_contract=common.cod_contract AND UPPER(TRIM(common.scenario_sales_order)) in ('3P', 'TV')
WHERE DATE(tv.partition_date) = DATE("2022-12-31") and common.id_order is null
  UNION ALL
    SELECT
       id_order_salesforce,
       bb.id_order,
       scenario_sales_order,
       id_account,
       sales_promotion_contract,
       order_type,
       order_status,
       order_sub_status,
       order_action,
       order_offer_type,
       order_platform_entry,
       order_sales_channel,
       promo_name,
       NULL as promo_macrocategory,
       cod_promo,
       NULL as cod_promo_ext,
       NULL as promo_category,
       des_promo,
       cod_contract,
       cod_parent_contract,
       id_parent,
       id_contract_bb,
       shipping_city,
       shipping_address,
       shipping_street,
       shipping_province,
       shipping_country,
       cod_shipping_postal,
       shipping_region,
       contract_status,
       cod_fiscal_payer,
       payment_method,
       payment_status,
       billing_frequency,
       payment_reason_fail,
       payment_sub_status,
       payment_card_type,
       cod_product_list,
       date_contract_creation,
       date_order_creation,
       date_first_activation,
       cod_dealer,
       cod_vendor,
       des_dealer_channel_l1,
       des_dealer_channel_l2,
       des_dealer_channel_l3,
       vendor_channel_qualification,
       cod_first_seller,
       des_first_seller_channel_l1,
       des_first_seller_channel_l2,
       des_first_seller_channel_l3,
       seller_first_channel_qualification,
       cod_second_seller,
       des_second_seller_channel_l1,
       des_second_seller_channel_l2,
       des_second_seller_channel_l3,
       seller_second_channel_qualification,
       flg_check_contract_status,
       flg_check_payment,
       flg_check_fiscal_code,
       flg_check_order_promo,
       flg_sales_daily,
       flg_bundle,
       bundle_type,
       NULL AS flg_trybuy,
       des_commission as des_dealer_commission,
       null as cube_owner,
       null as serial_number_first_voucher
    FROM sales_bb_tmp bb
    INNER JOIN sales_daily_skywifi_id_order new_bb ON bb.id_order=new_bb.id_order
    WHERE DATE(partition_date) = DATE("2022-12-31") -- not necessary only to understand union logic
),
all_user_daily as (
    select id_user_sf,
    sales_team,
    date(partition_date) as partition_date
  from `$source_project.account_snapshot.user_daily`
)
,
last_snapshot_order_salesforce as (
  select
    id_order_sf,
    id_created_by_user,
    date(date_created_ltz) as date_created_ltz
  from `$source_project.order_management_snapshot.order_salesforce_daily`
  WHERE date(partition_date) = DATE("2022-12-31")
),

 sales_sub_channel as (
select
 union_tbl.id_order_salesforce,
 union_tbl.id_order,
 aud.sales_team,

    case
      when  upper(trim(des_dealer_commission_fees_plan)) IN ('TV MEDIA - TELEOUT 1 NO RA', 'TELEOUT 1 NO RA') THEN 'TV_TELEOUT' --1
      when  upper(trim(des_dealer_commission_fees_plan)) = 'BB - TELEOUT 1 NO RA' THEN 'BB_TELEOUT' --2
      when  upper(trim(des_dealer_commission_fees_plan)) = 'HR - TELEOUT 1 NO RA' THEN 'HR_TELEOUT' --3
      when  (upper(trim(des_dealer_channel_l1))  = 'TELESELLING ESTERNO' AND upper(trim(des_dealer_channel_l2)) = 'TELESELLING ESTERNO'
            AND upper(trim(des_dealer_channel_l3)) = 'TELESEL. ESTERNO OUT' AND  upper(trim(des_dealer_commission)) = 'AGENZIA COMPARATRICE'
            AND upper(trim(des_dealer_commission_fees_plan)) = 'AGENZIA COMPARATRICE') then "OUTBOUND COMPARATORI" --4
      when    (upper(trim(des_dealer_channel_l1))  = 'TELESELLING ESTERNO'AND upper(trim(des_dealer_channel_l2)) = 'TELESELLING ESTERNO'
            AND upper(trim(des_dealer_channel_l3)) = 'TELESEL. ESTERNO OUT') then  "TELESELLING OUTBOUND"  --5
      when  upper(trim(des_dealer_commission_fees_plan)) = 'TV MEDIA - CALL ME NOW 1' THEN 'TV_CALL ME NOW' --6
      when  upper(trim(des_dealer_commission_fees_plan)) = 'BB - CALL ME NOW 1' THEN 'BB_CALL ME NOW' --7
      when  upper(trim(des_dealer_commission_fees_plan)) = 'DIGITAL HLF' THEN 'HLF_DIGITAL' --8
      when  upper(trim(des_dealer_commission_fees_plan)) = 'DIGITAL HARD RETENTION' then "HR_DIGITAL" --9
      when  (upper(trim(des_dealer_commission)) ='WEB OUTSOURCED'
            AND  upper(trim(des_dealer_commission_fees_plan)) IN ('CALL ME NOW 1', 'CALL ME NOW 2', 'CALL ME NOW 3')) then "CALL ME NOW" --10
      when (upper(trim(des_dealer_commission_fees_plan)) ='AGENZIA COMPARATRICE DIGITAL' ) then 'WEB COMPARATORI' --11

          when upper(trim(des_dealer_commission_fees_plan)) = 'WEB SELLING'
          and upper(trim(vendor_channel_qualification)) ='AOL MOBILE'
            and upper(trim(des_dealer_commission))='WEB'
          then 'AOL MOBILE' --12

      when upper(trim(des_dealer_commission_fees_plan)) = 'WEB SELLING'
          and upper(trim(vendor_channel_qualification)) ='WEBSELLING'
            and upper(trim(des_dealer_commission)) ='WEB'
          then 'AOL' --13
    when (upper(trim(des_first_seller_channel_l3)) ='WEBSELLING' and upper(trim(seller_first_channel_qualification)) ='WEB C2C') then 'WEB C2C' --14

  -- new AGENZIE
when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER'
        AND upper(trim(des_dealer_channel_l2)) = 'DISTR. TRADIZIONALE'
        AND upper(trim(des_dealer_channel_l3)) = 'DEALER TRADIZIONALE'
        AND upper(trim(vendor_channel_qualification)) ='AGENZIA DOOR TO DOOR' then "AGENZIE" --15
  -- new MULTIBRAND
      when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER'
      AND upper(trim(des_dealer_channel_l2)) = 'DISTR. TRADIZIONALE'
      AND upper(trim(des_dealer_channel_l3)) = 'MULTIBRAND'
              and ( cod_first_seller is null or trim(cod_first_seller)='' )
              and upper(trim(vendor_channel_qualification))	<>	 'AGENZIA DOOR TO DOOR' then "MULTIBRAND" --16
  -- new DISTRIBUZIONE TRADIZONALE
      when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER'
          AND upper(trim(des_dealer_channel_l2)) = 'DISTR. TRADIZIONALE'
          AND ( cod_first_seller is null or trim(cod_first_seller)='' )
          and upper(trim(vendor_channel_qualification))	<>	 'AGENZIA DOOR TO DOOR'      then "DISTRIBUZIONE TRADIZIONALE" --17
  -- new DISTRIBUZIONE MODERNA
      when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER' AND upper(trim(des_dealer_channel_l2)) = 'DISTR. MODERNA'
              AND ( cod_first_seller is null or trim(cod_first_seller)='' )   then "DISTRIBUZIONE MODERNA" --18
  -- new SSA SIS":
  when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER'
        AND upper(trim(des_dealer_channel_l2)) in  ('DISTR. TRADIZIONALE', 'DISTR. MODERNA')
        AND ( cod_first_seller is not  null or trim(cod_first_seller)<>'' )
        and upper(trim(vendor_channel_qualification))	<>	 'AGENZIA DOOR TO DOOR'   then "SSA SIS" --19
    ---"SSA MALLS":
    when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER'
        AND upper(trim(des_dealer_channel_l2)) = 'MALLS'  then "SSA MALLS" --20
    ---SSA OUT OF SHOP
      when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER' AND upper(trim(des_dealer_channel_l2)) = 'OUT OF SHOP' then "SSA OUT OF SHOP" --21

      --OTHER CENTER
      when   upper(trim(des_dealer_channel_l1))  = 'SKY CENTER' then "OTHER CENTER" --22

      when  (upper(trim(des_dealer_channel_l1))  = 'SKY SERVICE'
            AND upper(trim(des_first_seller_channel_l2)) ='UNITA TECNICO COMMERCIALE' ) then 'EVENTI' --23
      when  (upper(trim(des_dealer_channel_l1))  = 'SKY SERVICE'
            AND upper(trim(des_first_seller_channel_l2)) ='SKY INSTALLER' ) then 'SKY INSTALLER' --24
      when  (upper(trim(des_dealer_channel_l1))  = 'SKY SERVICE'
            AND upper(trim(des_first_seller_channel_l2)) ='AFFILIATO' ) then 'MASTER DEALER' --25
      when  (upper(trim(des_dealer_channel_l1))  = 'SKY SERVICE'
            AND upper(trim(des_dealer_channel_l2)) ='SERVICE POST VENDITA'
            AND upper(trim(des_dealer_channel_l3)) ='MONOBRAND' ) then 'MONOBRAND' --26
      when (upper(trim(des_dealer_channel_l1))  = 'SKY SERVICE' ) then  'SOAS' --27
      when upper(trim(des_dealer_commission_fees_plan)) ='VALUE - VENDITORE OUTSOURCED' then 'VALUE TELESALES' --28
      when upper(trim(des_dealer_commission_fees_plan)) ='HR - VENDITORE OUTSOURCED' then "HR_TELEIN" --29
      when upper(trim(des_dealer_commission_fees_plan))  in ('TV MEDIA - VENDITORE OUTSOURCED', 'VENDITORE OUTSOURCED') then "TV_TELEIN" --30
      when upper(trim(des_dealer_commission_fees_plan)) ='BB - VENDITORE OUTSOURCED' then "BB_TELEIN" --31
      when (upper(trim(des_dealer_channel_l1))  = 'CODICI SKY' ) then 'TELESELLING INBOUND' --32
      when (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO'
            AND  upper(trim(vendor_channel_qualification)) !='TELESELLING INTERNO CAGLIARI' ) then 'TELESELLING INBOUND' --33
      when  (upper(trim( order_sales_channel)) ='WSC'
            AND upper(trim(order_platform_entry))='WEB' AND cod_dealer ='' ) then 'WSC'--34
        when  (upper(trim( order_sales_channel)) ='MOBILE'
            AND upper(trim(order_platform_entry))='WEB' and cod_dealer ='' ) then 'MSA'--35

      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              OR (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            AND upper(trim(aud.sales_team)) ='VALUE_TS' ) then 'VALUE TELESALES' --36
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and upper(trim(aud.sales_team)) ='RETENTION_TS' ) then 'RETENTION TELESALES'--37
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and upper(trim(aud.sales_team)) ='TELESALES INBOUND' ) then 'TELESELLING INBOUND' --38
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and (upper(trim(aud.sales_team)) like '%INB%'  or upper(trim(aud.sales_team)) like '%INH%'  )  and upper(trim(aud.sales_team)) like '%VALUE%' ) then 'INBOUND CRM – VALUE' --39
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and (upper(trim(aud.sales_team)) like '%INB%'  or upper(trim(aud.sales_team)) like '%INH%'  )  and upper(trim(aud.sales_team)) like '%SERV%' ) then 'INBOUND CRM - SERVICE' --40
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and (upper(trim(aud.sales_team)) like '%INB%'  or upper(trim(aud.sales_team)) like '%INH%'  )  and upper(trim(aud.sales_team)) like '%TURN%' ) then 'INBOUND CRM - TURNAROUND' -- 41
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and (upper(trim(aud.sales_team)) like '%INB%'  or upper(trim(aud.sales_team)) like '%INH%'  )  and upper(trim(aud.sales_team)) like '%RET%' ) then 'INBOUND CRM – RETENTION' --42
     when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and (upper(trim(aud.sales_team)) like '%INB%'  or upper(trim(aud.sales_team)) like '%INH%'  )  and upper(trim(aud.sales_team)) like '%TIGER%' ) then 'INBOUND CRM – TIGER'--43
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and upper(trim(aud.sales_team)) like '%VALUE%' ) then 'OUTBOUND CRM – VALUE' --44
      when ( ((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and upper(trim(aud.sales_team)) like '%SERV%' ) then 'OUTBOUND CRM - SERVICE' --45
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and upper(trim(aud.sales_team)) like '%TURN%' ) then 'OUTBOUND CRM - TURNAROUND' --46
      when (((upper(trim(cod_vendor))  = ''  or cod_vendor is null)
              or (upper(trim(des_dealer_channel_l1))  = 'TELESELLING INTERNO' and upper(trim(vendor_channel_qualification)) ='TELESELLING INTERNO CAGLIARI') )
            and upper(trim(aud.sales_team)) like '%RET%' ) then 'OUTBOUND CRM – RETENTION' --47
      else if(union_tbl.id_order is not null,'OTHER', 'NO ORDINE' ) end   --48
      as business_sales_channel_detail

from union_sales_daily union_tbl
left join last_snapshot_order_salesforce os on ( union_tbl.id_order_salesforce = os.id_order_sf)
left join all_user_daily aud  on os.id_created_by_user= aud.id_user_sf and date(aud.partition_date) = date(os.date_created_ltz)
)
,

sales_channel as
(
  select id_order_salesforce,
  id_order,
  sales_team,
  business_sales_channel_detail,
      case when (upper(trim(business_sales_channel_detail))='MONOBRAND') then 'MONOBRAND' --1
      when upper(trim(business_sales_channel_detail)) IN ('OUTBOUND COMPARATORI','TELESELLING OUTBOUND','HR_TELEOUT', 'TV_TELEOUT', 'BB_TELEOUT') then 'TELESELLING OUTBOUND' --2
      when upper(trim(business_sales_channel_detail)) IN ('AGENZIE','MULTIBRAND','DISTRIBUZIONE TRADIZIONALE','DISTRIBUZIONE MODERNA','SSA SIS','SSA MALLS','SSA OUT OF SHOP','OTHER CENTER') then 'SKY CENTER' --3
      when upper(trim(business_sales_channel_detail)) IN ('EVENTI','MASTER DEALER','SKY INSTALLER' ,'SOAS' ) then 'SKY SERVICE' --4
      when upper(trim(business_sales_channel_detail)) IN ('CALL ME NOW', 'AOL MOBILE','MSA' ,'WSC','AOL', 'WEB C2C', 'WEB COMPARATORI', 'HR_DIGITAL', 'TV_CALL ME NOW', 'BB_CALL ME NOW', 'HLF_DIGITAL')  then 'WEBSELLING' --5
      when upper(trim(business_sales_channel_detail)) IN ('VALUE TELESALES', 'RETENTION TELESALES', 'TELESELLING INBOUND', 'HR_TELEIN', 'TV_TELEIN', 'BB_TELEIN', 'OTHER', 'NO ORDINE') then 'TELESELLING INBOUND' --6
      when upper(trim(business_sales_channel_detail)) IN ('OUTBOUND CRM – VALUE','OUTBOUND CRM - SERVICE','OUTBOUND CRM - TURNAROUND' ,'OUTBOUND CRM – RETENTION' ) then 'OUTBOUND CRM' --7
      when upper(trim(business_sales_channel_detail)) IN ('INBOUND CRM – VALUE' ,'INBOUND CRM - SERVICE','INBOUND CRM - TURNAROUND' ,'INBOUND CRM – RETENTION','INBOUND CRM – TIGER' ) then 'INBOUND CRM'--8
      when IFNULL(business_sales_channel_detail,'') ='' then null --9
      else business_sales_channel_detail --10
      END business_sales_channel
  from sales_sub_channel
),

union_sales_glass as (
   select
     sales.id_order_salesforce,
     sales.id_order,
     scenario_sales_order,
     id_account,
     sales_promotion_contract,
     order_type,
     order_status,
     order_sub_status,
     order_action,
     order_offer_type,
     order_platform_entry,
     order_sales_channel,
     promo_name,
     promo_macrocategory,
     cod_promo,
     cod_promo_ext,
     promo_category,
     des_promo,
     cod_contract,
     cod_parent_contract,
     id_parent,
     id_contract_bb,
     NULL AS id_contract_ma,
     NULL AS id_contract_hw,
     NULL AS contract_code_media_aggregator_glass,
     NULL AS contract_code_hardware_glass,
     NULL AS contract_code_bb_glass,
     shipping_city,
     shipping_address,
     shipping_street,
     shipping_province,
     shipping_country,
     cod_shipping_postal,
     shipping_region,
     contract_status,
     cod_fiscal_payer,
     payment_method,
     payment_status,
     billing_frequency,
     payment_reason_fail,
     payment_sub_status,
     payment_card_type,
     cod_product_list,
     date_contract_creation,
     date_order_creation,
     date_first_activation,
     cod_dealer,
     cod_vendor,
     des_dealer_channel_l1,
     des_dealer_channel_l2,
     des_dealer_channel_l3,
     vendor_channel_qualification,
     cod_first_seller,
     des_first_seller_channel_l1,
     des_first_seller_channel_l2,
     des_first_seller_channel_l3,
     seller_first_channel_qualification,
     cod_second_seller,
     des_second_seller_channel_l1,
     des_second_seller_channel_l2,
     des_second_seller_channel_l3,
     seller_second_channel_qualification,
     flg_check_contract_status,
     flg_check_payment,
     flg_check_fiscal_code,
     flg_check_order_promo,
     flg_sales_daily,
     flg_bundle,
     bundle_type,
     case when flg_bundle = 1 and UPPER(ifnull(bundle_type,'nd')) like '%REGULAR%' AND flg_trybuy = 1 THEN 1
     ELSE 0 END AS flg_3p_regular_trybuy_tv,
     case when (flg_bundle = 1 and UPPER(ifnull(bundle_type,'nd')) like '%REGULAR%' AND flg_trybuy = 1)
         or (scenario_sales_order = 'TV' and bundle_type = 'BUNDLE BB+TV TNB MULTI-ORDER')
     THEN 1 ELSE 0 END AS flg_bundle_tnb,
     business_sales_channel_detail,
     business_sales_channel,
     NULL AS glass_business_sales_channel_detail,
     NULL AS glass_business_sales_channel,
     des_dealer_commission_fees_plan,
     des_dealer_commission,
     sales_team,
     cube_owner,
     serial_number_first_voucher
   from union_sales_daily sales
   LEFT JOIN sales_channel chan ON sales.id_order = chan.id_order
   UNION ALL
   SELECT
     sgd.id_order_salesforce,
     sgd.id_order,
     sgd.scenario_sales_order,
     sgd.id_account,
     sgd.sales_promotion_contract,
     sgd.order_type,
     sgd.order_status,
     sgd.order_sub_status,
     sgd.order_action,
     sgd.order_offer_type,
     sgd.order_platform_entry,
     sgd.order_sales_channel,
     sgd.promo_name,
     sgd.promo_macrocategory,
     sgd.cod_promo,
     sgd.cod_promo_ext,
     sgd.promo_category,
     sgd.des_promo,
     sgd.cod_contract,
     sgd.cod_parent_contract,
     sgd.id_parent,
     sgd.id_contract_bb,
     sgd.id_contract_ma,
     sgd.id_contract_hw,
     sgd.contract_code_media_aggregator_glass,
     sgd.contract_code_hardware_glass,
     sgd.contract_code_bb_glass,
     sgd.shipping_city,
     sgd.shipping_address,
     sgd.shipping_street,
     sgd.shipping_province,
     sgd.shipping_country,
     sgd.cod_shipping_postal,
     sgd.shipping_region,
     sgd.contract_status,
     sgd.cod_fiscal_payer,
     sgd.payment_method,
     sgd.payment_status,
     sgd.billing_frequency,
     sgd.payment_reason_fail,
     sgd.payment_sub_status,
     sgd.payment_card_type,
     sgd.cod_product_list,
     sgd.date_contract_creation,
     sgd.date_order_creation,
     sgd.date_first_activation,
     sgd.cod_dealer,
     sgd.cod_vendor,
     sgd.des_dealer_channel_l1,
     sgd.des_dealer_channel_l2,
     sgd.des_dealer_channel_l3,
     sgd.vendor_channel_qualification,
     sgd.cod_first_seller,
     sgd.des_first_seller_channel_l1,
     sgd.des_first_seller_channel_l2,
     sgd.des_first_seller_channel_l3,
     sgd.seller_first_channel_qualification,
     sgd.cod_second_seller,
     sgd.des_second_seller_channel_l1,
     sgd.des_second_seller_channel_l2,
     sgd.des_second_seller_channel_l3,
     sgd.seller_second_channel_qualification,
     sgd.flg_check_contract_status,
     sgd.flg_check_payment,
     sgd.flg_check_fiscal_code,
     sgd.flg_check_order_promo,
     sgd.flg_sales_daily,
     IF(sgd.order_offer_type = "MEDIA;BB;LLAMA", 1, 0) AS flg_bundle,
     IF(sgd.order_offer_type = "MEDIA;BB;LLAMA", "BUNDLE BB+GLASS", NULL) AS bundle_type,
     NULL AS flg_3p_regular_trybuy_tv,
     CASE WHEN flg_promo_bundle_tnb_multi_order = 1 THEN 1 ELSE 0 END AS flg_bundle_tnb,
     NULL AS business_sales_channel_detail,
     NULL AS business_sales_channel,
     sgd.glass_business_sales_channel_detail, --CAMPO TEMPORANEO FINCHE' LA GESTIONE DEI CANALI GLASS NON VERRA' CALCOLATA QUI CON ALBERO CENTRALIZZATO E NON PIU' IN LETTURA DA SALES_GLASS_DAILY
     sgd.glass_business_sales_channel, --CAMPO TEMPORANEO FINCHE' LA GESTIONE DEI CANALI GLASS NON VERRA' CALCOLATA QUI CON ALBERO CENTRALIZZATO E NON PIU' IN LETTURA DA SALES_GLASS_DAILY
     sgd.des_dealer_commission_fees_plan,
     sgd.des_dealer_commission,
     NULL AS sales_team,
     SAFE_CAST(sgd.cube_owner AS STRING) AS cube_owner,
     SAFE_CAST(sgd.serial_number_first_voucher AS STRING) AS serial_number_first_voucher
   FROM sales_glass_daily sgd
)

Select
       id_order_salesforce,
       id_order,
       scenario_sales_order,
       id_account,
       sales_promotion_contract,
       order_type,
       order_status,
       order_sub_status,
       order_action,
       order_offer_type,
       order_platform_entry,
       order_sales_channel,
       promo_name,
       promo_macrocategory,
       cod_promo,
       cod_promo_ext,
       promo_category,
       des_promo,
       cod_contract,
       cod_parent_contract,
       id_parent,
       id_contract_bb,
       id_contract_ma,
       id_contract_hw,
       contract_code_media_aggregator_glass,
       contract_code_hardware_glass,
       contract_code_bb_glass,
       shipping_city,
       shipping_address,
       shipping_street,
       shipping_province,
       shipping_country,
       cod_shipping_postal,
       shipping_region,
       contract_status,
       cod_fiscal_payer,
       payment_method,
       payment_status,
       billing_frequency,
       payment_reason_fail,
       payment_sub_status,
       payment_card_type,
       cod_product_list,
       date_contract_creation,
       date_order_creation,
       date_first_activation,
       cod_dealer,
       cod_vendor,
       des_dealer_channel_l1,
       des_dealer_channel_l2,
       des_dealer_channel_l3,
       vendor_channel_qualification,
       cod_first_seller,
       des_first_seller_channel_l1,
       des_first_seller_channel_l2,
       des_first_seller_channel_l3,
       seller_first_channel_qualification,
       cod_second_seller,
       des_second_seller_channel_l1,
       des_second_seller_channel_l2,
       des_second_seller_channel_l3,
       seller_second_channel_qualification,
       flg_check_contract_status,
       flg_check_payment,
       flg_check_fiscal_code,
       flg_check_order_promo,
       flg_sales_daily,
       flg_bundle,
       bundle_type,
       flg_3p_regular_trybuy_tv,
       flg_bundle_tnb,
       business_sales_channel_detail,
       business_sales_channel,
       glass_business_sales_channel_detail,
       glass_business_sales_channel,
       des_dealer_commission_fees_plan,
       des_dealer_commission,
       sales_team,
       cube_owner,
       serial_number_first_voucher
from union_sales_glass sales