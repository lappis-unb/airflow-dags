

{{ config(
    materialized='table',
    full_refresh=True,
    indexes=[
      {'columns': ['id_proposta', 'titulo_proposta']}
      ]
) }}
select
   a.data_do_voto as data_operacao, 
   a.process_id as id_processo, 
   a.proposal_id as id_proposta,
   a.proposal_title as titulo_proposta,
   a.proposal_status as status_proposta,
   a.process_title as titulo_processo,
   a.proposal_category as eixo_tematico,
   coalesce(b.bounce_rate, 0) as bounce_rate, 
   coalesce(c.qtd_votos, 0) as qtd_votos, 
   coalesce(d.qtd_comentarios, 0) as qtd_comentarios
from
   (
      select
         p.proposal_title,
         p.proposal_status,
         p.process_id,
         pp.process_title,
         p.proposal_id,
         p.proposal_category,
         generate_series(
            p.created_at::date,         -- Data de criação da proposta
            CURRENT_DATE,               -- Até a data atual
            '1 day'::interval
         )::date AS data_do_voto 
      FROM
         {{ ref('proposals') }} p
      join {{ ref('participatory_processes') }} pp 
         on p.process_id = pp.process_id
   ) as a 
   left join
      (
         SELECT
            proposal_id::INTEGER,
            session_date as data_rejeicao,
            SUM(
               CASE
                  WHEN qtd_acoes = 1 THEN 1
                  ELSE 0
               END
            ) * 1.0 / COUNT(*) AS bounce_rate 
         FROM
            (
               SELECT
                  proposal_id,
                  session_date::date AS session_date,
                  visit_id,
                  COUNT(0) AS qtd_acoes 
               FROM
                  {{ ref('visits') }} v
               WHERE
                  proposal_id IS NOT NULL 
                  AND proposal_id ~ '^\d+$'
               GROUP BY
                  session_date::date,
                  visit_id,
                  proposal_id
            ) AS visit_actions 
         GROUP BY
            proposal_id,
            session_date
      ) b 
      on a.proposal_id = b.proposal_id 
      and a.data_do_voto = b.data_rejeicao 
   left join
      (
         select
            v.created_at::date AS data_do_voto,
            v.voted_component_id AS proposal_id,
            COUNT(v.voted_component_id) AS qtd_votos 
         FROM
            {{ ref('votes') }} v
         GROUP BY
            v.created_at::date,
            v.voted_component_id
      ) c 
      on a.proposal_id = c.proposal_id 
      and a.data_do_voto = c.data_do_voto 
   left join
      (
         select
            commented_root_component_id as proposal_id,
            created_at::date data_comentario,
            count(0) qtd_comentarios 
         from
            {{ ref('comments') }} c
         group by
            commented_root_component_id,
            created_at::date
      ) d 
      on a.proposal_id = d.proposal_id 
      and a.data_do_voto = d.data_comentario 
