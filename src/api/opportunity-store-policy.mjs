export function unavailableOpportunityFeed(state = {}) {
  const configured = Boolean(state.configured);
  const warning = state.error || (configured
    ? "Sovereign storage is temporarily unavailable; memory-only results are withheld."
    : "Sovereign API sync is not configured; results are not durable.");
  return { items: [], storage: "unavailable", source: "none", warning };
}

export function opportunityStorageName(state = {}) {
  return state.status === "connected" ? "sovereign-remote-cas" : "unavailable";
}
