# Sports MCP Server Notes

## 2025-03-16

- Goal: stand up an MVP MCP-style service that streams structured JSON slices to downstream agents.  
- Decision: serve data via FastAPI endpoints that mimic MCP tool calls while we flesh out the true protocol handshake.  
- Stub datasets: MLB pitching outings, game metadata, and odds snapshots seeded from static fixtures so the agent can develop against deterministic responses.  
- Upcoming tasks: formalise a dataset registry, bolt on auth (API key header), and replace the static records with warehouse queries once connectors are available.
- Added normalization for string filters (case-insensitive, punctuation/whitespace agnostic) so variants like "OHTANI SHOHEI" match the sample data.
