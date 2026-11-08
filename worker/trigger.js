// Cloudflare Worker: one tap to re-run the cedwyn-graphrag ingest. — Sion's Tab agent
// POST https://<worker>/run with header "X-Trigger-Key: <TRIGGER_KEY>" (optional JSON {"only":"cedwyn-wiki,cedwyn-research"}).
// Secrets to set on the Worker (wrangler secret put ...):
//   GH_TOKEN    a fine-grained GitHub token limited to unclehowell/cedwyn-graphrag with "Actions: read and write"
//   TRIGGER_KEY any long random string; only callers who know it can trigger a run
// GET /status shows the last few runs. The workflow itself waits for running Cedwyn builds and never cancels them.
const REPO = "unclehowell/cedwyn-graphrag", WF = "ingest.yml";
async function gh(env, path, init = {}) {
  return fetch("https://api.github.com/repos/" + REPO + path, { ...init, headers: { "Authorization": "Bearer " + env.GH_TOKEN,
    "Accept": "application/vnd.github+json", "User-Agent": "cedwyn-graphrag-trigger", ...(init.headers || {}) } });
}
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/status") {
      const r = await gh(env, "/actions/workflows/" + WF + "/runs?per_page=5");
      const j = await r.json();
      return Response.json((j.workflow_runs || []).map(x => ({ id: x.id, status: x.status, conclusion: x.conclusion, created: x.created_at, url: x.html_url })));
    }
    if (url.pathname === "/run" && req.method === "POST") {
      if (!env.TRIGGER_KEY || req.headers.get("X-Trigger-Key") !== env.TRIGGER_KEY) return new Response("forbidden", { status: 403 });
      let only = ""; try { only = ((await req.json()) || {}).only || ""; } catch (e) {}
      const r = await gh(env, "/actions/workflows/" + WF + "/dispatches", { method: "POST", body: JSON.stringify({ ref: "main", inputs: { only } }) });
      return new Response(r.status === 204 ? "ingest started" : "GitHub answered " + r.status, { status: r.status === 204 ? 202 : 502 });
    }
    return new Response("cedwyn-graphrag trigger: POST /run, GET /status", { status: 200 });
  }
};
