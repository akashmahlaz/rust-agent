//! Minimal GitHub REST client for the agent.
//!
//! Read-only operations against `https://api.github.com`. The agent's
//! `github_token` is taken from `oauth_accounts.access_token_ciphertext`
//! (plaintext for local dev) when the user has connected GitHub.

use anyhow::{Result, anyhow};
use reqwest::{Client, header};
use serde_json::{Value, json};

const API_BASE: &str = "https://api.github.com";
const USER_AGENT: &str = "operon-agent";

fn auth_headers(token: &str) -> Result<header::HeaderMap> {
    let mut h = header::HeaderMap::new();
    h.insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|e| anyhow!("invalid github token: {e}"))?,
    );
    h.insert(
        header::ACCEPT,
        header::HeaderValue::from_static("application/vnd.github+json"),
    );
    h.insert(
        "X-GitHub-Api-Version",
        header::HeaderValue::from_static("2022-11-28"),
    );
    h.insert(
        header::USER_AGENT,
        header::HeaderValue::from_static(USER_AGENT),
    );
    Ok(h)
}

async fn get_json(client: &Client, token: &str, url: &str) -> Result<Value> {
    let resp = client
        .get(url)
        .headers(auth_headers(token)?)
        .send()
        .await
        .map_err(|e| anyhow!("github request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("github error {status}: {body}"));
    }
    resp.json::<Value>()
        .await
        .map_err(|e| anyhow!("invalid github json: {e}"))
}

async fn send_json(
    client: &Client,
    token: &str,
    method: reqwest::Method,
    url: &str,
    body: &Value,
) -> Result<Value> {
    let resp = client
        .request(method, url)
        .headers(auth_headers(token)?)
        .json(body)
        .send()
        .await
        .map_err(|e| anyhow!("github request failed: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("github error {status}: {text}"));
    }
    if text.trim().is_empty() {
        return Ok(json!({ "ok": true }));
    }
    serde_json::from_str(&text).map_err(|e| anyhow!("invalid github json: {e}"))
}

pub async fn create_repo(
    client: &Client,
    token: &str,
    name: &str,
    private: bool,
    description: Option<&str>,
    auto_init: bool,
    gitignore_template: Option<&str>,
    license_template: Option<&str>,
) -> Result<Value> {
    let mut body = json!({
        "name": name,
        "private": private,
        "auto_init": auto_init,
    });
    if let Some(d) = description { body["description"] = json!(d); }
    if let Some(g) = gitignore_template { body["gitignore_template"] = json!(g); }
    if let Some(l) = license_template { body["license_template"] = json!(l); }
    send_json(client, token, reqwest::Method::POST, &format!("{API_BASE}/user/repos"), &body).await
}

pub async fn create_branch(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    branch: &str,
    from_branch: Option<&str>,
) -> Result<Value> {
    // Resolve the base SHA: use from_branch or repo default branch.
    let base = match from_branch {
        Some(b) => b.to_owned(),
        None => {
            let r = get_repo(client, token, owner, repo).await?;
            r.get("default_branch").and_then(|x| x.as_str()).unwrap_or("main").to_owned()
        }
    };
    let ref_url = format!("{API_BASE}/repos/{owner}/{repo}/git/ref/heads/{base}");
    let base_ref = get_json(client, token, &ref_url).await?;
    let sha = base_ref
        .get("object")
        .and_then(|o| o.get("sha"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| anyhow!("could not resolve base branch sha"))?;
    let body = json!({ "ref": format!("refs/heads/{branch}"), "sha": sha });
    send_json(
        client,
        token,
        reqwest::Method::POST,
        &format!("{API_BASE}/repos/{owner}/{repo}/git/refs"),
        &body,
    )
    .await
}

pub async fn write_file(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    path: &str,
    contents: &str,
    message: &str,
    branch: Option<&str>,
    sha: Option<&str>,
) -> Result<Value> {
    use base64ct::{Base64, Encoding};
    let encoded = Base64::encode_string(contents.as_bytes());
    let mut body = json!({ "message": message, "content": encoded });
    if let Some(b) = branch { body["branch"] = json!(b); }
    if let Some(s) = sha { body["sha"] = json!(s); }
    let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{}", path.trim_start_matches('/'));
    send_json(client, token, reqwest::Method::PUT, &url, &body).await
}

pub async fn delete_file(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    path: &str,
    message: &str,
    sha: &str,
    branch: Option<&str>,
) -> Result<Value> {
    let mut body = json!({ "message": message, "sha": sha });
    if let Some(b) = branch { body["branch"] = json!(b); }
    let url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{}", path.trim_start_matches('/'));
    send_json(client, token, reqwest::Method::DELETE, &url, &body).await
}

pub async fn create_pull_request(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    title: &str,
    head: &str,
    base: &str,
    body_md: Option<&str>,
    draft: bool,
) -> Result<Value> {
    let mut body = json!({ "title": title, "head": head, "base": base, "draft": draft });
    if let Some(b) = body_md { body["body"] = json!(b); }
    send_json(
        client,
        token,
        reqwest::Method::POST,
        &format!("{API_BASE}/repos/{owner}/{repo}/pulls"),
        &body,
    )
    .await
}

pub async fn get_status(client: &Client, token: Option<&str>) -> Result<Value> {
    let Some(token) = token else {
        return Ok(json!({ "connected": false }));
    };
    let user = get_json(client, token, &format!("{API_BASE}/user")).await?;
    Ok(json!({
        "connected": true,
        "login": user.get("login"),
        "name": user.get("name"),
        "avatar_url": user.get("avatar_url"),
        "html_url": user.get("html_url"),
    }))
}

pub async fn list_repos(
    client: &Client,
    token: &str,
    visibility: Option<&str>,
    per_page: u32,
) -> Result<Value> {
    let vis = visibility.unwrap_or("all");
    let url = format!(
        "{API_BASE}/user/repos?per_page={}&visibility={}&sort=updated",
        per_page.min(100).max(1),
        vis
    );
    let v = get_json(client, token, &url).await?;
    let arr = v.as_array().cloned().unwrap_or_default();
    let trimmed: Vec<Value> = arr
        .into_iter()
        .map(|r| {
            json!({
                "full_name": r.get("full_name"),
                "private": r.get("private"),
                "description": r.get("description"),
                "default_branch": r.get("default_branch"),
                "updated_at": r.get("updated_at"),
                "stargazers_count": r.get("stargazers_count"),
                "language": r.get("language"),
                "html_url": r.get("html_url"),
            })
        })
        .collect();
    Ok(json!({ "repos": trimmed }))
}

pub async fn get_repo(client: &Client, token: &str, owner: &str, repo: &str) -> Result<Value> {
    get_json(client, token, &format!("{API_BASE}/repos/{owner}/{repo}")).await
}

pub async fn list_contents(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    path: &str,
    git_ref: Option<&str>,
) -> Result<Value> {
    let p = path.trim_start_matches('/');
    let mut url = format!("{API_BASE}/repos/{owner}/{repo}/contents/{p}");
    if let Some(r) = git_ref {
        url.push_str(&format!("?ref={r}"));
    }
    get_json(client, token, &url).await
}

pub async fn read_file(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    path: &str,
    git_ref: Option<&str>,
) -> Result<Value> {
    let v = list_contents(client, token, owner, repo, path, git_ref).await?;
    // Single file → object with `content` (base64). Directory → array.
    if v.is_array() {
        return Err(anyhow!("path is a directory; use github_list_contents"));
    }
    let encoding = v.get("encoding").and_then(|x| x.as_str()).unwrap_or("");
    let content_b64 = v.get("content").and_then(|x| x.as_str()).unwrap_or("");
    let decoded = if encoding == "base64" {
        let cleaned: String = content_b64.chars().filter(|c| !c.is_whitespace()).collect();
        use base64ct::{Base64, Encoding};
        let bytes = Base64::decode_vec(&cleaned)
            .map_err(|e| anyhow!("base64 decode failed: {e}"))?;
        String::from_utf8_lossy(&bytes).into_owned()
    } else {
        content_b64.to_owned()
    };
    Ok(json!({
        "path": v.get("path"),
        "size": v.get("size"),
        "sha": v.get("sha"),
        "html_url": v.get("html_url"),
        "contents": decoded,
    }))
}

pub async fn search_code(
    client: &Client,
    token: &str,
    query: &str,
    repo: Option<&str>,
    per_page: u32,
) -> Result<Value> {
    let q = if let Some(r) = repo {
        format!("{query}+repo:{r}")
    } else {
        query.to_owned()
    };
    let encoded: String = url::form_urlencoded::byte_serialize(q.as_bytes()).collect();
    let url = format!(
        "{API_BASE}/search/code?q={}&per_page={}",
        encoded,
        per_page.min(50).max(1)
    );
    get_json(client, token, &url).await
}

pub async fn list_branches(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
) -> Result<Value> {
    get_json(
        client,
        token,
        &format!("{API_BASE}/repos/{owner}/{repo}/branches?per_page=100"),
    )
    .await
}

pub async fn list_issues(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    state: Option<&str>,
) -> Result<Value> {
    let s = state.unwrap_or("open");
    get_json(
        client,
        token,
        &format!("{API_BASE}/repos/{owner}/{repo}/issues?state={s}&per_page=50"),
    )
    .await
}

pub async fn list_pull_requests(
    client: &Client,
    token: &str,
    owner: &str,
    repo: &str,
    state: Option<&str>,
) -> Result<Value> {
    let s = state.unwrap_or("open");
    get_json(
        client,
        token,
        &format!("{API_BASE}/repos/{owner}/{repo}/pulls?state={s}&per_page=50"),
    )
    .await
}
