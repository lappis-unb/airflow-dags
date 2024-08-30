

{{ config(
    materialized='table',
    full_refresh=True,
    indexes=[
        {'columns': ['proposal_id']}
    ],

meta={
	"datasets_trigger": [
		"votes_model",
		"comments_model",
		"participatory_processes_model",
        "proposals_model"
	]
   }

) }}

WITH proposal_votes AS (
    SELECT 
        proposals.proposal_id,
        proposals.proposal_title, 
        proposals.proposal_status,
        proposals.created_at,
        votes.voted_component_id,
        participatory_processes.process_title
    FROM {{ source('silver', 'proposals') }} AS proposals
    JOIN {{ source('silver', 'participatory_processes') }} AS participatory_processes
        ON proposals.process_id = participatory_processes.process_id 
    JOIN {{ source('silver', 'votes') }} AS votes
        ON proposals.proposal_id = votes.voted_component_id
),
proposal_comments AS (
    SELECT 
        proposals.proposal_id,
        proposals.proposal_title, 
        proposals.proposal_status,
        proposals.created_at,
        comments.commented_root_component_id
    FROM {{ source('silver', 'proposals') }} AS proposals
    JOIN {{ source('silver', 'participatory_processes') }} AS participatory_processes
        ON proposals.process_id = participatory_processes.process_id 
    JOIN {{ source('silver', 'comments') }} AS comments
        ON proposals.proposal_id = comments.commented_root_component_id
),
total_votes_count AS (
    SELECT proposal_title, COUNT(*) AS total_votos, MIN(created_at) as data_proposta, MAX(process_title) as processo_participativo
    FROM proposal_votes
    GROUP BY proposal_title
    ORDER BY total_votos DESC
),
total_comments_count AS (
    SELECT proposal_title, COUNT(*) AS total_comentarios
    FROM proposal_comments
    GROUP BY proposal_title
    ORDER BY total_comentarios DESC
)
SELECT 
    total_votes_count.proposal_title AS titulo_proposta, 
    total_votes_count.processo_participativo, 
    total_votes_count.data_proposta, 
    total_votes_count.total_votos, 
    total_comments_count.total_comentarios
FROM total_votes_count
JOIN total_comments_count
ON total_votes_count.proposal_title = total_comments_count.proposal_title