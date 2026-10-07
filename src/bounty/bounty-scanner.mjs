const SEARCHES = [
  "is:open is:issue bounty",
  "is:open is:issue \"bounty\"",
  "is:open is:issue \"reward\"",
  "is:open is:issue \"$50\"",
  "is:open is:issue \"$100\"",
  "is:open is:issue \"$250\"",
  "is:open is:issue \"$500\""
];

function money(text) {
  const values = String(text || "").match(/\$\s?\d{1,6}(?:,\d{3})*(?:\.\d{2})?/g) || [];
  return values
    .map(v => Number(v.replace(/[$,\s]/g, "")))
    .filter(Number.isFinite);
}

function score(issue) {
  const body = String(issue.body || "");
  const text = (String(issue.title || "") + "\n" + body).toLowerCase();
  const rewards = money(text);
  const reward = rewards.length ? Math.max(...rewards) : 0;

  let value = Math.min(reward, 1000);
  if (/good first issue|first issue/.test(text)) value += 35;
  if (/help wanted/.test(text)) value += 25;
  if (/documentation|docs/.test(text)) value += 10;
  if (/test|testing|unit test/.test(text)) value += 15;
  if (/bug|fix|feature|refactor/.test(text)) value += 15;
  if (/security vulnerability|exploit|private key|credential|secret/.test(text)) value -= 150;
  if (/requires nda|confidential|non-public/.test(text)) value -= 100;

  return {reward, score: value};
}

function normalize(issue, source="github") {
  const {reward, score: rank} = score(issue);
  return {
    source,
    platform: "github",
    repo: issue.repository_url?.split("/repos/")[1] || null,
    number: issue.number,
    title: issue.title,
    htmlUrl: issue.html_url,
    labels: (issue.labels || []).map(label => typeof label === "string" ? label : label.name),
    rewardUsd: reward,
    rewardConfidence: reward ? "text-match" : "none",
    score: rank,
    state: issue.state,
    updatedAt: issue.updated_at,
    comments: issue.comments ?? 0,
    bodyExcerpt: bodyExcerpt(issue.body)
  };
}

function bodyExcerpt(body) {
  return String(body || "").replace(/\s+/g, " ").trim().slice(0, 1200);
}

export class BountyScanner {
  constructor({token="", perQuery=30, maxResults=100}={}) {
    this.token = token;
    this.perQuery = Math.max(5, Math.min(100, Number(perQuery) || 30));
    this.maxResults = Math.max(10, Math.min(200, Number(maxResults) || 100));
  }

  async req(url) {
    const headers = {
      accept: "application/vnd.github+json",
      "user-agent": "OpportunityEngine-BountyScanner/2.0"
    };
    if (this.token) headers.authorization = "Bearer " + this.token;

    const response = await fetch(url, {
      headers,
      signal: AbortSignal.timeout(15000)
    });

    if (!response.ok) throw new Error("GitHub HTTP " + response.status);
    return response.json();
  }

  async search(query) {
    const params = new URLSearchParams({
      q: query,
      per_page: String(this.perQuery),
      sort: "updated",
      order: "desc"
    });
    const data = await this.req("https://api.github.com/search/issues?" + params);
    return Array.isArray(data.items) ? data.items : [];
  }

  async scan() {
    const byUrl = new Map();

    for (const query of SEARCHES) {
      try {
        for (const issue of await this.search(query)) {
          if (!issue.pull_request && issue.html_url) {
            byUrl.set(issue.html_url, normalize(issue));
          }
        }
      } catch (error) {
        // Continue other search lanes. A single GitHub search failure must not
        // take down the engine's other intelligence feeds.
        continue;
      }
    }

    return [...byUrl.values()]
      .sort((a, b) => b.score - a.score || b.rewardUsd - a.rewardUsd)
      .slice(0, this.maxResults);
  }
}
