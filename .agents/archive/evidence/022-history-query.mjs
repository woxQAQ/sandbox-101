const scope = { project: '%sandbox-101%', limit: 3 };
const facets = ['ResultInstance', '"configuration" "fence"'];
const evidence = facets.flatMap((facet) =>
  search(facet, scope).map((hit) => ({
    facet,
    session_id: hit.session.id,
    is_invoking: hit.session.is_invoking === true,
    uuid: hit.message.uuid,
    role: hit.message.role,
    content_type: hit.message.content_type,
    is_meta: hit.message.is_meta,
    timestamp: hit.message.timestamp,
    snippet: (hit.message.text ?? '').slice(0, 300),
  })),
);
return {
  query_plan: {
    mode: 'scoped-design-history',
    project: '%sandbox-101%',
    facets,
    max_hits: 6,
    authority: 'Current 47f5 implementation and accepted contracts remain authoritative.',
  },
  evidence,
};
