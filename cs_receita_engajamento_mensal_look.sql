WITH core_joinpaths AS (SELECT 'sessions' as path
      UNION ALL
      SELECT 'daily_subscriptions' as path
      UNION ALL
      SELECT 'daily_professional_subscriptions' as path
      UNION ALL
      SELECT 'daily_validation_code' as path
      UNION ALL
      SELECT 'daily_contract' as path
      UNION ALL
      SELECT 'pro_searches' as path
      UNION ALL
      SELECT 'rating' as path
      UNION ALL
      SELECT 'b2b_saas' as path
      UNION ALL
      SELECT 'gmv_b2b_fixed_eligible' as path
      UNION ALL
      SELECT 'gmv_b2b_fixed_activation' as path
      UNION ALL
      SELECT 'gmv_b2b_fixed_member' as path
      UNION ALL
      SELECT 'gmv_b2b_fixed_session_member' as path
      UNION ALL
      SELECT 'gmv_fixed_professionals' as path
      UNION ALL
      SELECT 'business_charge' as path
      UNION ALL
      SELECT 'mood_tracker' as path
      UNION ALL
      SELECT 'mood_tracker_weekly' as path
      UNION ALL
      SELECT 'wellbeing_index_answer' as path
      UNION ALL
      SELECT 'onboarding' as path
      UNION ALL
      SELECT 'professional_facts' as path
      UNION ALL
      SELECT 'client_facts' as path
      UNION ALL
      SELECT 'subscription_invoices' as path
      UNION ALL
      SELECT 'content' as path
      UNION ALL
      SELECT 'ibc' as path
      UNION ALL
      SELECT 'ibc_fact' as path
      UNION ALL
      SELECT 'ranking_history_v3' as path
      UNION ALL
      SELECT 'product_rule' as path
      UNION ALL
      SELECT 'journey' as path
      UNION ALL
      SELECT 'home' as path
      UNION ALL
      SELECT 'payment_actions' as path
      UNION ALL
      SELECT 'welcome_actions' as path
      UNION ALL
      SELECT 'mood_tracker_actions' as path
      UNION ALL
      SELECT 'session_actions' as path
      UNION ALL
      SELECT 'access_actions' as path
      UNION ALL
      SELECT 'typeform' as path
      UNION ALL
      SELECT 'zm_actions' as path
      UNION ALL
      SELECT 'search_conversions' as path
      UNION ALL
      SELECT 'professional_avg_rank' as path
      UNION ALL
      SELECT 'ndr_b2b_sessions' as path
      UNION ALL
      SELECT 'content_completion' as path
      UNION ALL
      SELECT 'chat_actions' as path
      UNION ALL
      SELECT 'triage' as path
      UNION ALL
      SELECT 'daily_gmv_b2b_fixed_saas' as path
      UNION ALL
      SELECT 'gmv_b2b_fixed_saas_expansion' as path
      UNION ALL
      SELECT 'ndr_b2b_sessions_by_csm' as path
      UNION ALL
      SELECT 'professional_charges' as path
      UNION ALL
      SELECT 'gmv_b2b_fixed_saas_daily_by_csm' as path
      UNION ALL
      SELECT 'professional_person' as path
      UNION ALL
      SELECT 'core_credits_transactions' as path
    )
  ,  core_credits_transactions AS (SELECT
      s.created_at
      ,((p.price/p.credits) * s.value) expired_credits
      ,s.value
      ,s.subscription_id
      ,s.plan_id
      ,s.profile_id
      ,generate_uuid() pk

      FROM `zenklub-business.dump.subscription_credit_transaction` s
      LEFT JOIN `zenklub-business.dump.plans` p
      ON s.plan_id = p.id

      WHERE transaction_type = 'expired'
      )
  ,  client_facts AS (WITH
  sessions AS (
  SELECT
    DATE_TRUNC(day, MONTH) AS base_month,
    profileId,
    contract_id,
    subscription_subscriptionId AS subscription_id,
    COUNT(DISTINCT event_id) AS qty_sessions
  FROM
    `zenklub-business.core.sessions`
  WHERE
    status = 'paid'
    AND DATE(day) < CURRENT_DATE()
  GROUP BY
    1,
    2,
    3,
    4),
  content AS (
  SELECT
    DATE_TRUNC(base_date, MONTH) AS base_month,
    user_id AS profileId,
    contract_id,
    subscription_id,
    COUNT(DISTINCT content_id) AS qty_contents
  FROM
    core.content_actions
    WHERE DATE(base_date) < CURRENT_DATE()
  GROUP BY
    1,
    2,
    3,
    4)

    SELECT
    s.*,
    c.qty_contents
    FROM
    sessions s
    FULL JOIN
    content c
    ON
    c.base_month = s.base_month
    AND c.profileId = s.profileId)
  ,  gmv_b2b_fixed_saas_expansion AS (with calc_variation_value as (
  with company_sum as (
  SELECT
  DATE_TRUNC(base_payment_date, MONTH) as base_month,
  company_id,
  company_name,
  SUM(monthly_fixed_fee) as company_monthly_fixed_fee
   FROM core.gmv_b2b_fixed_saas_daily
  GROUP BY
  DATE_TRUNC(base_payment_date, MONTH),
  company_id,
  company_name
  ),
  previous_saas as (
  SELECT
  *,
  COALESCE(LAG(company_monthly_fixed_fee)
    OVER( PARTITION  BY company_id ORDER BY base_month ASC) , 0 )
    as previous_company_monthly_fixed_fee
  FROM company_sum
  order by base_month
  )
  select *,
  company_monthly_fixed_fee - previous_company_monthly_fixed_fee as diff_company_level
  from previous_saas
),
calc_variation_date as (
  with company_sum_02 as (
  SELECT
  base_payment_date,
  DATE_TRUNC(base_payment_date, MONTH) as base_month,
  company_id,
  company_name,
  contract_id,
  SUM(monthly_fixed_fee) as contract_monthly_fixed_fee,
  FROM core.gmv_b2b_fixed_saas_daily
  GROUP BY
  base_payment_date,
  company_id,
  company_name,
  contract_id
  ),
  previous_saas_02 as (
  SELECT
  base_payment_date,
  base_month,
  company_id,
  company_name,
  contract_id,
  contract_monthly_fixed_fee,
  SUM(contract_monthly_fixed_fee) over(partition by company_id, base_payment_date order by base_payment_date) as cumulative_contract_monthly_fixed_fee,
  COALESCE(LAG(contract_monthly_fixed_fee)
    OVER( PARTITION  BY company_id, contract_id ORDER BY base_payment_date ASC) , 0 )
    as previous_contract_monthly_fixed_fee
  FROM company_sum_02
  order by base_payment_date
  ),
  base_day_02 as (
  select *,
  contract_monthly_fixed_fee - previous_contract_monthly_fixed_fee as diff_contract_level,
  COALESCE(LAG(cumulative_contract_monthly_fixed_fee)
    OVER( PARTITION  BY company_id ORDER BY base_payment_date ASC) , 0 )
    as previous_cumulative_contract_monthly_fixed_fee,
  cumulative_contract_monthly_fixed_fee
  - COALESCE(LAG(cumulative_contract_monthly_fixed_fee)
    OVER( PARTITION  BY company_id ORDER BY base_payment_date ASC) , 0 )
    as diff_cum_contract_level,
  CASE WHEN (contract_monthly_fixed_fee - previous_contract_monthly_fixed_fee) != 0
    THEN EXTRACT(DAY FROM base_payment_date)
    ELSE NULL
  END payment_day_temp
  from previous_saas_02
  ),
  base_day_03 as (
  select *,
  LAST_VALUE(payment_day_temp IGNORE NULLS)
  OVER(PARTITION BY company_id ORDER BY base_payment_date ASC)
  as payment_day,
    LAST_DAY(base_month, MONTH) as last_day_of_base_month,
  EXTRACT(YEAR FROM base_month) as base_year_number,
    EXTRACT(MONTH FROM  base_month) as base_month_number,
    DENSE_RANK () OVER(PARTITION BY company_name ORDER BY base_month) as saas_company_counter,
    DENSE_RANK () OVER(PARTITION BY company_name, contract_id ORDER BY base_payment_date) as saas_contract_counter,
    COUNT(contract_id) OVER(PARTITION BY company_id, base_month) as number_contracts_month,
    SUM(CASE WHEN diff_contract_level > 0.1 OR diff_contract_level < -0.1 THEN 1 ELSE 0 END) OVER (PARTITION BY company_id, base_month) as number_diff_contracts
  from base_day_02
  )
  select * EXCEPT(payment_day),
  COALESCE(payment_day, EXTRACT(DAY FROM base_payment_date)) as payment_day
  from base_day_03
),
final_table_1 as (
select DISTINCT v.*,
d.contract_id,
d.saas_company_counter,
d.saas_contract_counter,
d.contract_monthly_fixed_fee,
d.diff_contract_level,
d.cumulative_contract_monthly_fixed_fee,
d.diff_cum_contract_level,
d.number_contracts_month,
d.number_diff_contracts,
CASE
  WHEN d.payment_day = 31
  THEN last_day_of_base_month
  WHEN d.payment_day = 30 AND base_month_number = 2
  THEN last_day_of_base_month
  WHEN d.payment_day = 29 AND base_month_number = 2
  THEN last_day_of_base_month
  ELSE DATE(
      base_year_number,
      base_month_number,
      d.payment_day
      )
END as   base_payment_date ,
CASE
  WHEN saas_contract_counter = 1 THEN 1 ELSE 0
END as new_contract_flag,
SUM(CASE WHEN saas_contract_counter = 1 THEN 1 ELSE 0 END) OVER(PARTITION BY d.company_name, d.base_month) as number_new_contracts_month
from calc_variation_value v
LEFT JOIN calc_variation_date d
ON v.company_id = d.company_id
  AND d.base_month = v.base_month
ORDER BY v.base_month ASC
),
final_table_2 as (
select
GENERATE_UUID() as uuid,
*,
CASE
  -- SE TODOS CONTRATOS SÃO NOVOS RATEAR IGUALMENTE A DIFERENÇA
  WHEN number_contracts_month = number_new_contracts_month
  THEN ROUND((diff_company_level / number_contracts_month),2)
  -- SE NÃO HOUVE NENHUM NOVO CONTRATO MAS ALGUM CONTRATO DO MES ANTERIOR FOI CANCELADO
    WHEN number_new_contracts_month = 0 AND diff_company_level < -0.1
    THEN ROUND((diff_company_level / number_contracts_month),2)
    -- SE APENAS ALGUNS CONTRATOS SAO NOVOS PORÉM NO SALDO DO MES HOUVE REDUÇÃO
    WHEN number_contracts_month > number_new_contracts_month AND diff_company_level < -0.1
    THEN ROUND((diff_company_level / number_contracts_month),2)
    -- TRATAMENTO DE EXCEÇÕES
    WHEN company_name = 'Jusbrasil'
        AND (diff_contract_level > 0.1 OR diff_contract_level < -0.1)
        AND  number_contracts_month > number_new_contracts_month
        AND number_diff_contracts = 1
        THEN ROUND((diff_company_level / number_diff_contracts),2)
    -- SE APENAS ALGUNS CONTRATOS SÃO NOVOS e houve expansão a nivel de COMPANY DIFF_CONTRACT_LEVEL PARA OS EXISTENTES
  WHEN number_contracts_month > number_new_contracts_month
  THEN ROUND(diff_contract_level,2)
END expansion_reduction_value,
from final_table_1
),
final_table_3 as (
select
final_table_2.*,

    COALESCE(SUM(CASE WHEN (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN sessions.charge_value  ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN sessions.price  ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN sessions.price  ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN ((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))) THEN business_charge.value ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN sessions.b2b_paid_value  ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN (( business_charge.charge_type  ) = 'other') THEN business_charge.value ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN (( business_charge.charge_type  ) = 'live') THEN business_charge.value ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN sessions.price  ELSE NULL END), 0) + COALESCE(SUM(CASE WHEN (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN sessions.charge_value  ELSE NULL END), 0) AS core_metrics_gmv_b2b_variable,


CASE
    when saas_company_counter = 1 then 'New Company'
    when expansion_reduction_value > 0.1  and saas_company_counter > 1 then 'Expansion'
    when expansion_reduction_value < -0.1 and saas_company_counter > 1 then 'Reduction'
end as saas_variation
from final_table_2
LEFT JOIN `zenklub-business.core.events` sessions
ON sessions.company_id = CAST(final_table_2.company_id AS STRING)
AND sessions.contract_id = final_table_2.contract_id
AND DATE(sessions.day) = final_table_2.base_payment_date

LEFT JOIN `zenklub-business.dump.business_charge` business_charge
ON sessions.company_id = CAST(business_charge.company_id AS STRING)
AND sessions.contract_id = business_charge.contract_id
AND DATE(sessions.day) = DATE(business_charge.charge_date)

LEFT JOIN `zenklub-business.core.contract` contract
ON contract.id = sessions.contract_id OR contract.id = business_charge.contract_id OR final_table_2.contract_id = contract.id

GROUP BY
1,2,3,4,5,6,7,8,9,10, 11,12,13,14,15,16,17,18,19,20
)

SELECT *,
FIRST_VALUE( base_month ) OVER (PARTITION BY company_id ORDER BY base_month ) as cohort_date,
DATE_DIFF(base_month, FIRST_VALUE( base_month ) OVER (PARTITION BY company_id ORDER BY base_month ), MONTH) as m_duration
FROM final_table_3
--where company_name = 'Ambev'
order by base_payment_date  )
SELECT
    (FORMAT_DATE('%Y-%m', core_metrics.date)) AS core_metrics_base_month,
    company.id AS company_id,
    company.name  AS company_name,
    COUNT(DISTINCT CASE WHEN ( (IF((plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL))) IS TRUE,
          (plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL))),
          daily_validation_code.active)) ) AND ( plans.active ) THEN ( COALESCE(
            subscription_validationCode,
            mood_tracker.validation_code,
            content.validation_code,
            wellbeing_index_answer.validation_code,
            rating.validation_code,
            onboarding.validation_code,
            ibc.validation_code,
            journey.validation_code,
            home.validation_code,
            payment_actions.validation_code,
            welcome_actions.validation_code,
            mood_tracker_actions.validation_code,
            session_actions.validation_code,
            access_actions.validation_code,
            typeform.validation_code,
            content_completion.validation_code,
            chat_actions.validation_code,
            triage.validation_code,
            daily_validation_code.code,
            daily_subscriptions.validation_code
            )  )  ELSE NULL END) AS validation_code_qty_b2b_eligible,
    COUNT(DISTINCT CASE WHEN ( plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL))) THEN clients._id  ELSE NULL END) AS clients_qty_b2b_users,
    COUNT(DISTINCT CASE WHEN (( plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL)) )) AND (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) THEN clients._id  ELSE NULL END) AS clients_qty_b2b_session_users,
    COUNT(DISTINCT CASE WHEN (( ((content.subscope='player') IS TRUE AND (content.interaction='open') IS TRUE AND (content.content_id IS NOT NULL) AND content.user_id IS NOT NULL) IS TRUE AND (plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL))) IS TRUE )) AND (( content.contract_id is not null  )) THEN clients._id  ELSE NULL END) AS clients_qty_b2b_content_users,
    COUNT(DISTINCT CASE WHEN (( mood_tracker.user_id IS NOT NULL )) AND (( plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL)) )) THEN mood_tracker.user_id ELSE NULL END) AS clients_qty_b2b_mood_tracker_users,
    COUNT(DISTINCT CASE WHEN ( (plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL))) IS TRUE AND (((IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)) IS NOT NULL) IS TRUE
          OR (mood_tracker.user_id IS NOT NULL) IS TRUE
          OR ((content.subscope='player') IS TRUE AND (content.interaction='open') IS TRUE AND (content.content_id IS NOT NULL) AND content.user_id IS NOT NULL) IS TRUE
          OR (COALESCE(typeform.user_id, wellbeing_index_answer.profile_id) IS NOT NULL) IS TRUE
          OR journey_facts.journey_start IS TRUE
          OR (chat_actions.subscope='talk' AND chat_actions.items='professional' AND chat_actions.interaction='open' AND chat_actions.professional_id IS NOT NULL) IS TRUE
          OR (rating.id IS NOT NULL) IS TRUE) IS TRUE) THEN clients._id  ELSE NULL END) AS clients_qty_b2b_engaged_users,
    COUNT(DISTINCT CASE WHEN (( sessions.b2_type  ) = 'B2B') THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL) ) ELSE NULL END) AS sessions_qty_b2b_paid_sessions,
    COUNT(DISTINCT CASE WHEN ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL) ) ELSE NULL END) AS sessions_qty_b2b_sponsored_sessions,
    COUNT(DISTINCT CASE WHEN ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL) ) ELSE NULL END) AS sessions_qty_b2b_regular_sessions,
    COUNT(DISTINCT CASE WHEN ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL) ) ELSE NULL END) AS sessions_qty_b2b_employee_sessions,
    COUNT(DISTINCT CASE WHEN ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL) ) ELSE NULL END) AS sessions_qty_b2b_coparticipation_sessions,
    COUNT(DISTINCT CASE WHEN ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL) ) ELSE NULL END) AS sessions_qty_b2b_distribution_sessions,
    COUNT(DISTINCT CASE WHEN ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))) THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL) ) ELSE NULL END) AS sessions_qty_b2b_partnership_sessions,
    COUNT(DISTINCT CASE WHEN ( content.interaction='open' ) THEN content.content_id  ELSE NULL END) AS content_qty_unique_opened_content,
    COUNT(DISTINCT mood_tracker.id ) AS mood_tracker_qty_moods_tracked,
    COUNT(DISTINCT CASE WHEN NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE) THEN COALESCE( zm_actions.user_id , zm_actions.user_pseudo_id  )  ELSE NULL END) AS zm_actions_qty_users,
    COUNT(DISTINCT CASE WHEN NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE) THEN company.id ELSE NULL END) AS zm_actions_qty_company,
    COUNT(DISTINCT CASE WHEN ((( zm_actions.scope  ) = 'insights')) AND (NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE)) THEN COALESCE( zm_actions.user_id , zm_actions.user_pseudo_id  )  ELSE NULL END) AS zm_actions_qty_insights_users,
    COUNT(DISTINCT CASE WHEN ((( zm_actions.scope  ) = 'atlas')) AND (NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE)) THEN COALESCE( zm_actions.user_id , zm_actions.user_pseudo_id  )  ELSE NULL END) AS zm_actions_qty_atlas_users,
    COUNT(DISTINCT CASE WHEN ((( zm_actions.scope  ) = 'ibc')) AND (NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE)) THEN COALESCE( zm_actions.user_id , zm_actions.user_pseudo_id  )  ELSE NULL END) AS zm_actions_qty_ibc_users,
    COUNT(DISTINCT CASE WHEN ((( zm_actions.scope  ) = 'sessions')) AND (NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE)) THEN COALESCE( zm_actions.user_id , zm_actions.user_pseudo_id  )  ELSE NULL END) AS zm_actions_qty_sessions_users,
    COUNT(DISTINCT CASE WHEN ((( zm_actions.scope  ) = 'employee')) AND (NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE)) THEN COALESCE( zm_actions.user_id , zm_actions.user_pseudo_id  )  ELSE NULL END) AS zm_actions_qty_employee_management_users,
    COUNT(DISTINCT CASE WHEN ((( zm_actions.role  ) = 'admin')) AND (NOT COALESCE(( REGEXP_CONTAINS(lower(zm_actions.email), 'zenklub')  ), FALSE)) THEN COALESCE( zm_actions.user_id , zm_actions.user_pseudo_id  )  ELSE NULL END) AS zm_actions_qty_admin_users,
    COUNT(DISTINCT zm_actions.action_id ) AS zm_actions_qty_actions,
                        ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.monthly_fixed_fee  )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_activation.gmv_activation ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_member.gmv_member ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_session_member.gmv_session_member ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_eligible.gmv_eligible ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.charge_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.value)  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.b2b_paid_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.value)  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.value)  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.charge_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6)) AS core_metrics_gross_revenue_b2b,
                                    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.charge_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.value)  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.b2b_paid_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.value)  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'other'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.value)  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) = 'live'))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.charge_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_variable,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.price   ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_sponsored_sessions,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.charge_value   ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_regular_sessions,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.price   ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_employee_sessions,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.b2b_paid_value   ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_coparticipation_sessions,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.charge_value   ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Distribution')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_variable_distribution_sessions,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.price   ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Partnership')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_partnership_sessions,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (( business_charge.charge_type  ) = 'live')  THEN  business_charge.value  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'live')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'live')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'live')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'live')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_lives,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (( business_charge.charge_type  ) = 'other')  THEN  business_charge.value  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'other')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'other')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'other')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( business_charge.charge_type  ) = 'other')  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_other,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation')))  THEN  business_charge.value  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation')))  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation')))  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation')))  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation')))  THEN  business_charge.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_variable_pack,
                    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.monthly_fixed_fee  )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_activation.gmv_activation ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_eligible.gmv_eligible ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_member.gmv_member ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_session_member.gmv_session_member ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_fixed,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) )  THEN  b2b_saas.monthly_fixed_fee    ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) )  THEN  b2b_saas.uuid   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) )  THEN  b2b_saas.uuid   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) )  THEN  b2b_saas.uuid   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) )  THEN  b2b_saas.uuid   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_fixed_saas,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( gmv_b2b_fixed_eligible.gmv_eligible  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_eligible.uuid   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_eligible.uuid   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_eligible.uuid   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_eligible.uuid   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_fixed_eligible,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( gmv_b2b_fixed_activation.gmv_activation  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_activation.uuid   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_activation.uuid   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_activation.uuid   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( gmv_b2b_fixed_activation.uuid   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS core_metrics_gmv_b2b_fixed_activation,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((( sessions.b2_type  ) = 'B2B')) AND (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  professionalCompensation  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( sessions.b2_type  ) = 'B2B')) AND (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( sessions.b2_type  ) = 'B2B')) AND (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( sessions.b2_type  ) = 'B2B')) AND (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((( sessions.b2_type  ) = 'B2B')) AND (((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo'))))  THEN  sessions.event_id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS sessions_sum_b2b_professional_compensation_value,
            (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.monthly_fixed_fee  )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( IF((DATE(contract.contract_start , 'America/Sao_Paulo')) <= LAST_DAY(core_metrics.date, MONTH) AND ((DATE(contract.contract_end , 'America/Sao_Paulo')) > LAST_DAY(core_metrics.date, MONTH) OR (DATE(contract.contract_end , 'America/Sao_Paulo')) IS NULL), TRUE, FALSE) ))  THEN  (b2b_saas.uuid )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_activation.gmv_activation ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_activation.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_eligible.gmv_eligible ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_eligible.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_member.gmv_member ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( (gmv_b2b_fixed_session_member.gmv_session_member ) ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( (gmv_b2b_fixed_session_member.uuid )  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6)) * - (0.029 + 0.03 + 0.0065) AS sessions_deductions_saas,
            (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.charge_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Regular')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Sponsored')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.value)  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (((( business_charge.charge_type  ) LIKE 'session_pack')) AND (( contract.contract_type   IN ('direct', 'coparticipation'))))  THEN  (business_charge.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.b2b_paid_value )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6)) * - (0.02 + 0.076 + 0.0165) AS sessions_deductions_sessions,
            (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Coparticipation')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) + ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ((((( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )) IS NOT NULL)) AND ((( sessions.session_type ) = 'Corporate Employee')) AND (( sessions.day   < (TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Sao_Paulo'), 'America/Sao_Paulo'), INTERVAL 0 DAY), 'America/Sao_Paulo')))))  THEN  (sessions.event_id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6)) * -0.0205 AS sessions_transactions,
    COUNT(DISTINCT CASE WHEN (( contract.contract_type IN ('direct', 'coparticipation')
      AND ((plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL))) IS TRUE OR daily_validation_code.active IS TRUE) )) AND ( plans.active  ) AND ((( plans.eligible_category  ) = 'standard')) THEN ( COALESCE(
            subscription_validationCode,
            mood_tracker.validation_code,
            content.validation_code,
            wellbeing_index_answer.validation_code,
            rating.validation_code,
            onboarding.validation_code,
            ibc.validation_code,
            journey.validation_code,
            home.validation_code,
            payment_actions.validation_code,
            welcome_actions.validation_code,
            mood_tracker_actions.validation_code,
            session_actions.validation_code,
            access_actions.validation_code,
            typeform.validation_code,
            content_completion.validation_code,
            chat_actions.validation_code,
            triage.validation_code,
            daily_validation_code.code,
            daily_subscriptions.validation_code
            )  )  ELSE NULL END) AS validation_code_qty_b2b_eligible_users_corporate_standard,
    COUNT(DISTINCT CASE WHEN (( contract.contract_type IN ('direct', 'coparticipation')
      AND ((plans.type = 'corporate_subscription' AND
    (daily_subscriptions.status = 'active' OR (daily_subscriptions.status IS NULL AND (COALESCE(
          subscription_subscriptionId,
          wellbeing_index_answer.subscription_id,
          mood_tracker.subscription_id,
          subscription_invoices.subscription_id,
          content.subscription_id,
          rating.subscription_id,
          onboarding.subscription_id,
          client_facts.subscription_id,
          ibc.subscription_id,
          journey.subscription_id,
          home.subscription_id,
          payment_actions.subscription_id,
          welcome_actions.subscription_id,
          mood_tracker_actions.subscription_id,
          session_actions.subscription_id,
          access_actions.subscription_id,
          typeform.subscription_id,
          content_completion.subscription_id,
          chat_actions.subscription_id,
          triage.subscription_id,
          daily_subscriptions.id,
          core_credits_transactions.subscription_id)) IS NOT NULL))) IS TRUE OR daily_validation_code.active IS TRUE) )) AND ( plans.active  ) AND ((( plans.eligible_category  ) = 'standard')) THEN ( IF(sessions.status = 'paid'
          AND (DATE(sessions.day , 'America/Sao_Paulo')) < CURRENT_DATE("America/Sao_Paulo"),
          sessions.event_id,
          NULL)  )  ELSE NULL END) AS sessions_qty_sessions_by_corporate_standard_users
FROM `zenklub-business.core.core_metrics` AS core_metrics
CROSS JOIN core_joinpaths
LEFT JOIN `zenklub-business.core.sessions`  AS sessions ON core_joinpaths.path = 'sessions'
      AND core_metrics.date = (DATE(sessions.day , 'America/Sao_Paulo'))
LEFT JOIN `zenklub-business.core.mood_tracker`  AS mood_tracker ON core_joinpaths.path = 'mood_tracker'
      AND core_metrics.date = mood_tracker.base_date
LEFT JOIN core.content_actions AS content ON core_joinpaths.path = 'content'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(content.base_date ), 'America/Sao_Paulo')))
LEFT JOIN `zenklub-business.core.lk_wellbeing_index_answer`  AS wellbeing_index_answer ON core_joinpaths.path = 'wellbeing_index_answer'
      AND core_metrics.date = (DATE(wellbeing_index_answer.submitted_at , 'America/Sao_Paulo'))
LEFT JOIN `zenklub-business.core.rating`
     AS rating ON core_joinpaths.path = 'rating'
      AND core_metrics.date = (DATE(rating.created_at , 'America/Sao_Paulo'))
LEFT JOIN `zenklub-business.core.onboarding`  AS onboarding ON core_joinpaths.path = 'onboarding'
      AND core_metrics.date = (DATE(onboarding.event_date))
LEFT JOIN client_facts ON core_joinpaths.path = 'client_facts'
      AND core_metrics.date = (DATE(client_facts.base_month , 'America/Sao_Paulo'))
LEFT JOIN `zenklub-business.core.gmv_b2c_fixed_subscriptions` AS subscription_invoices ON core_joinpaths.path = 'subscription_invoices'
      AND core_metrics.date = subscription_invoices.base_date
LEFT JOIN `zenklub-business.support.session_taxonomy`  AS session_reason ON sessions.event_id = session_reason.event_id
LEFT JOIN `zenklub-business.core.ibc_actions`  AS ibc ON core_joinpaths.path = 'ibc'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(ibc.base_date ), 'America/Sao_Paulo')))
LEFT JOIN core.journey_actions  AS journey ON core_joinpaths.path = 'journey'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(journey.base_date ), 'America/Sao_Paulo')))
LEFT JOIN core.home_actions AS home ON core_joinpaths.path = 'home'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(home.base_date ), 'America/Sao_Paulo')))
LEFT JOIN core.payment_actions AS payment_actions ON core_joinpaths.path = 'payment_actions'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(payment_actions.base_date ), 'America/Sao_Paulo')))
LEFT JOIN core.welcome_actions  AS welcome_actions ON core_joinpaths.path = 'welcome_actions'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(welcome_actions.base_date ), 'America/Sao_Paulo')))
LEFT JOIN core.mood_tracker_actions AS mood_tracker_actions ON core_joinpaths.path = 'mood_tracker_actions'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(mood_tracker_actions.base_date ), 'America/Sao_Paulo')))
LEFT JOIN core.session_actions  AS session_actions ON core_joinpaths.path = 'session_actions'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(session_actions.base_date ), 'America/Sao_Paulo')))
LEFT JOIN core.access_actions  AS access_actions ON core_joinpaths.path = 'access_actions'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(access_actions.base_date ), 'America/Sao_Paulo')))
LEFT JOIN `zenklub-business.core.lk_typeform`  AS typeform ON core_joinpaths.path = 'typeform'
      AND core_metrics.date = (DATE(typeform.submitted_at , 'America/Sao_Paulo'))
LEFT JOIN `zenklub-business.core.content_completion` AS content_completion ON core_joinpaths.path = 'content_completion'
      AND core_metrics.date = (DATE(content_completion.updated_at))
LEFT JOIN core.chat_actions  AS chat_actions ON core_joinpaths.path = 'chat_actions'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(chat_actions.base_date ), 'America/Sao_Paulo')))
LEFT JOIN `zenklub-business.core.lk_triage`  AS triage ON core_joinpaths.path = 'triage'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(triage._fivetran_synced ), 'America/Sao_Paulo')))
LEFT JOIN `zenklub-business.core.lk_daily_subscriptions`  AS daily_subscriptions ON core_joinpaths.path = 'daily_subscriptions'
      AND core_metrics.date = daily_subscriptions.base_date
LEFT JOIN core_credits_transactions ON core_joinpaths.path = 'core_credits_transactions'
      AND core_metrics.date = (DATE(core_credits_transactions.created_at))
FULL OUTER JOIN `zenklub-business.core.clients`  AS clients ON clients._id = (COALESCE(
          sessions.profileId,
          mood_tracker.user_id,
          content.user_id,
          wellbeing_index_answer.profile_id,
          rating.profile_id,
          onboarding.user_id,
          client_facts.profileId,
          subscription_invoices.profile_id,
          session_reason.profileId,
          ibc.user_id,
          journey.user_id,
          home.user_id,
          payment_actions.user_id,
          welcome_actions.user_id,
          mood_tracker_actions.user_id,
          session_actions.user_id,
          access_actions.user_id,
          typeform.user_id,
          content_completion.user_id,
          chat_actions.user_id,
          triage.profile_id,
          daily_subscriptions.profile_id,
          core_credits_transactions.profile_id))
LEFT JOIN `zenklub-business.dump.business_charge` AS business_charge ON core_joinpaths.path = 'business_charge'
      AND core_metrics.date = (DATE(charge_date , 'America/Sao_Paulo'))
LEFT JOIN `zenklub-business.core.gmv_b2b_fixed_saas`  AS b2b_saas ON core_joinpaths.path = 'b2b_saas'
      AND core_metrics.date = b2b_saas.base_month
LEFT JOIN `zenklub-business.core.gmv_b2b_fixed_eligible`
     AS gmv_b2b_fixed_eligible ON core_joinpaths.path = 'gmv_b2b_fixed_eligible'
      AND core_metrics.date = gmv_b2b_fixed_eligible.base_month
LEFT JOIN `zenklub-business.core.gmv_b2b_fixed_activation`
     AS gmv_b2b_fixed_activation ON core_joinpaths.path = 'gmv_b2b_fixed_activation'
      AND core_metrics.date = gmv_b2b_fixed_activation.base_month
LEFT JOIN `zenklub-business.core.gmv_b2b_fixed_member`
     AS gmv_b2b_fixed_member ON core_joinpaths.path = 'gmv_b2b_fixed_member'
      AND core_metrics.date = gmv_b2b_fixed_member.base_month
LEFT JOIN `zenklub-business.core.gmv_b2b_fixed_session_member`
     AS gmv_b2b_fixed_session_member ON core_joinpaths.path = 'gmv_b2b_fixed_session_member'
      AND core_metrics.date = gmv_b2b_fixed_session_member.base_month
LEFT JOIN `zenklub-business.core.ndr_b2b_sessions`  AS ndr_b2b_sessions ON core_joinpaths.path = 'ndr_b2b_sessions'
    AND core_metrics.date = ndr_b2b_sessions.cohort_date
LEFT JOIN `zenklub-business.core.gmv_b2b_fixed_saas_daily`  AS daily_gmv_b2b_fixed_saas ON core_joinpaths.path = 'daily_gmv_b2b_fixed_saas'
      AND core_metrics.date = daily_gmv_b2b_fixed_saas.base_payment_date
LEFT JOIN gmv_b2b_fixed_saas_expansion ON core_joinpaths.path = 'gmv_b2b_fixed_saas_expansion'
      AND core_metrics.date = gmv_b2b_fixed_saas_expansion.base_payment_date
LEFT JOIN `zenklub-business.core.ndr_b2b_sessions_by_csm`  AS ndr_b2b_sessions_by_csm ON core_joinpaths.path = 'ndr_b2b_sessions_by_csm'
      AND core_metrics.date = ndr_b2b_sessions_by_csm.cohort_date
LEFT JOIN `zenklub-business.core.daily_validation_code` AS daily_validation_code ON core_joinpaths.path = 'daily_validation_code'
      AND core_metrics.date = daily_validation_code.base_date
LEFT JOIN `zenklub-business.core.gmv_b2b_fixed_saas_daily_by_csm`  AS gmv_b2b_fixed_saas_daily_by_csm ON core_joinpaths.path = 'gmv_b2b_fixed_saas_daily_by_csm'
      AND core_metrics.date = gmv_b2b_fixed_saas_daily_by_csm.cohort_date
LEFT JOIN `zenklub-business.core.contract` AS contract ON contract.id = (COALESCE(
          sessions.contract_id,
          business_charge.contract_id,
          b2b_saas.contract_id,
          gmv_b2b_fixed_eligible.contract_id,
          gmv_b2b_fixed_activation.contract_id,
          gmv_b2b_fixed_member.contract_id,
          gmv_b2b_fixed_session_member.contract_id,
          content.contract_id,
          mood_tracker.contract_id,
          wellbeing_index_answer.contract_id,
          rating.contract_id,
          onboarding.contract_id,
          client_facts.contract_id,
          ibc.contract_id,
          journey.contract_id,
          home.contract_id,
          payment_actions.contract_id,
          welcome_actions.contract_id,
          mood_tracker_actions.contract_id,
          session_actions.contract_id,
          access_actions.contract_id,
          typeform.contract_id,
          ndr_b2b_sessions.contract_id,
          content_completion.contract_id,
          chat_actions.contract_id,
          daily_gmv_b2b_fixed_saas.contract_id,
          triage.contract_id,
          gmv_b2b_fixed_saas_expansion.contract_id,
          ndr_b2b_sessions_by_csm.contract_id,
          daily_validation_code.contract_id,
          daily_subscriptions.contract_id,
          gmv_b2b_fixed_saas_daily_by_csm.contract_id))
LEFT JOIN `zenklub-business.core.zm_actions`
       AS zm_actions ON core_joinpaths.path = 'zm_actions'
      AND core_metrics.date = (DATE(DATETIME(TIMESTAMP(zm_actions.base_date ), 'America/Sao_Paulo')))
LEFT JOIN `zenklub-business.dump.company` AS company ON (COALESCE(
            contract.company_id,
            zm_actions.organization_id,
            CAST(sessions.company_id AS INT64)
            )) = company.id
FULL OUTER JOIN `zenklub-business.dump.plans`
     AS plans ON (COALESCE(
          subscription_planId,
          wellbeing_index_answer.plan_id,
          gmv_b2b_fixed_eligible.plan_id,
          gmv_b2b_fixed_activation.plan_id,
          gmv_b2b_fixed_member.plan_id,
          gmv_b2b_fixed_session_member.plan_id,
          subscription_invoices.plan_id,
          mood_tracker.plan_id,
          content.plan_id,
          rating.plan_id,
          onboarding.plan_id,
          --ibc.plan_id,
          journey.plan_id,
          home.plan_id,
          payment_actions.plan_id,
          welcome_actions.plan_id,
          mood_tracker_actions.plan_id,
          session_actions.plan_id,
          access_actions.plan_id,
          typeform.plan_id,
          content_completion.plan_id,
          chat_actions.plan_id,
          triage.plan_id,
          daily_subscriptions.plan_id,
          daily_validation_code.plan_id,
          core_credits_transactions.plan_id)
) = plans.id
LEFT JOIN `zenklub-business.dump.validation_code` AS validation_code ON (COALESCE(
            subscription_validationCode,
            mood_tracker.validation_code,
            content.validation_code,
            wellbeing_index_answer.validation_code,
            rating.validation_code,
            onboarding.validation_code,
            ibc.validation_code,
            journey.validation_code,
            home.validation_code,
            payment_actions.validation_code,
            welcome_actions.validation_code,
            mood_tracker_actions.validation_code,
            session_actions.validation_code,
            access_actions.validation_code,
            typeform.validation_code,
            content_completion.validation_code,
            chat_actions.validation_code,
            triage.validation_code,
            daily_validation_code.code,
            daily_subscriptions.validation_code
            )) = validation_code.code
      AND (COALESCE(
          subscription_planId,
          wellbeing_index_answer.plan_id,
          gmv_b2b_fixed_eligible.plan_id,
          gmv_b2b_fixed_activation.plan_id,
          gmv_b2b_fixed_member.plan_id,
          gmv_b2b_fixed_session_member.plan_id,
          subscription_invoices.plan_id,
          mood_tracker.plan_id,
          content.plan_id,
          rating.plan_id,
          onboarding.plan_id,
          --ibc.plan_id,
          journey.plan_id,
          home.plan_id,
          payment_actions.plan_id,
          welcome_actions.plan_id,
          mood_tracker_actions.plan_id,
          session_actions.plan_id,
          access_actions.plan_id,
          typeform.plan_id,
          content_completion.plan_id,
          chat_actions.plan_id,
          triage.plan_id,
          daily_subscriptions.plan_id,
          daily_validation_code.plan_id,
          core_credits_transactions.plan_id)
) = validation_code.plan_id
LEFT JOIN `zenklub-business.core.journey_facts`
       AS journey_facts ON core_metrics.date = (DATE(DATETIME(TIMESTAMP(journey_facts.base_date ), 'America/Sao_Paulo')))
    AND (COALESCE(
          sessions.profileId,
          mood_tracker.user_id,
          content.user_id,
          wellbeing_index_answer.profile_id,
          rating.profile_id,
          onboarding.user_id,
          client_facts.profileId,
          subscription_invoices.profile_id,
          session_reason.profileId,
          ibc.user_id,
          journey.user_id,
          home.user_id,
          payment_actions.user_id,
          welcome_actions.user_id,
          mood_tracker_actions.user_id,
          session_actions.user_id,
          access_actions.user_id,
          typeform.user_id,
          content_completion.user_id,
          chat_actions.user_id,
          triage.profile_id,
          daily_subscriptions.profile_id,
          core_credits_transactions.profile_id)) = journey_facts.user_id
    AND (COALESCE(
          content.action_id,
          ibc.action_id,
          journey.action_id,
          home.action_id,
          payment_actions.action_id,
          welcome_actions.action_id,
          mood_tracker_actions.action_id,
          session_actions.action_id,
          access_actions.action_id,
          chat_actions.action_id)) = journey_facts.action_id
WHERE ((( core_metrics.date ) >= ((DATE_ADD(DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), MONTH), INTERVAL -17 MONTH))) AND ( core_metrics.date ) < ((DATE_ADD(DATE_ADD(DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), MONTH), INTERVAL -17 MONTH), INTERVAL 18 MONTH))))) AND ((company.name ) <> 'Mês do RH' AND (company.name ) <> 'Campanhas de Marketing Zenklub') AND ((company.name ) <> 'Zenklub Teste' AND (company.name ) <> 'Zenklub Teste IBC' AND ((company.name ) <> 'Zenklub' AND (company.name ) <> 'Zenklub Demo'))
GROUP BY
    1,
    2,
    3
ORDER BY
    1 DESC
LIMIT 500
