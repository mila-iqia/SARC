"""
User matching viewer GUI.

Loads all users from the SARC database and serves a local web page with two views:
  - All Users: every user with their matching IDs.
  - Match DRAC: pair unmatched DRAC users with unmatched LDAP users and export JSON.
    Exported pairs identify DRAC users by their username ("drac_username"),
    falling back to the CCI ("drac_member") when no username is known.

Usage:
    SARC_CONFIG=config/sarc-prod.yaml SARC_MODE=scraping uv run python scripts/usermatch/main.py
"""

import argparse
import json
import socket
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from sqlmodel import Session, func, select
from tqdm import tqdm

from sarc.config import config
from sarc.db.users import CredentialsDB, UserDB


def load_ignore_list(path: str | Path | None) -> list[dict[str, str]]:
    """Load ignored DRAC users from a JSON file.

    Returns a list of matching ids ({"name": ..., "mid": ...}), where name is
    "drac_username" or "drac_member" (CCI, legacy format). Legacy CCI entries
    are converted to usernames by the page when the user is known.
    If path is None, file doesn't exist, or file contains invalid JSON, returns [].
    """
    if path is None:
        return []
    p = Path(path)
    if not p.is_file():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    result = []
    for item in data:
        if isinstance(item, str):
            result.append({"name": "drac_member", "mid": item})
        elif isinstance(item, dict):
            if "matching_id" in item and isinstance(item["matching_id"], dict):
                name = item["matching_id"].get("name") or "drac_member"
                mid = item["matching_id"].get("mid")
                if mid:
                    result.append({"name": str(name), "mid": str(mid)})
            elif "mid" in item and item["mid"]:
                result.append({"name": "drac_member", "mid": str(item["mid"])})
            elif "drac_member" in item and item["drac_member"]:
                result.append({"name": "drac_member", "mid": str(item["drac_member"])})
    return result


# ---------------------------------------------------------------------------
# HTML page — users data is injected as a JSON literal before serving
# ---------------------------------------------------------------------------

_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>SARC User Viewer</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: ui-monospace, "Cascadia Code", "Source Code Pro", monospace;
    font-size: 13px;
    background: #0f1117;
    color: #c9d1d9;
    padding: 0;
  }

  /* ---- nav ---- */
  .nav {
    display: flex;
    gap: 0;
    background: #161b22;
    border-bottom: 1px solid #30363d;
    padding: 0 24px;
  }
  .nav-tab {
    padding: 12px 18px;
    cursor: pointer;
    color: #8b949e;
    border-bottom: 2px solid transparent;
    font-size: 13px;
    user-select: none;
  }
  .nav-tab:hover { color: #c9d1d9; }
  .nav-tab.active { color: #e6edf3; border-bottom-color: #58a6ff; }

  /* ---- pages ---- */
  .page { display: none; padding: 24px; }
  .page.active { display: block; }

  h2 { font-size: 16px; font-weight: 600; color: #e6edf3; margin-bottom: 14px; }

  /* ---- toolbar ---- */
  .toolbar {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 14px;
  }
  .search-input {
    flex: 1;
    max-width: 400px;
    padding: 6px 10px;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 6px;
    color: #c9d1d9;
    font-family: inherit;
    font-size: 13px;
    outline: none;
  }
  .search-input:focus { border-color: #58a6ff; }
  .count { color: #8b949e; font-size: 12px; }

  /* ---- tables ---- */
  table { width: 100%; border-collapse: collapse; }
  thead th {
    text-align: left;
    padding: 7px 10px;
    background: #161b22;
    border-bottom: 1px solid #30363d;
    color: #8b949e;
    font-weight: 500;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    position: sticky;
    top: 0;
    z-index: 1;
  }
  tbody tr { border-bottom: 1px solid #21262d; }
  tbody tr:hover { background: #161b22; }
  td { padding: 7px 10px; vertical-align: top; }
  td.name  { color: #e6edf3; font-weight: 500; white-space: nowrap; }
  td.email { color: #8b949e; white-space: nowrap; }

  /* ---- badges ---- */
  .badges { display: flex; flex-wrap: wrap; gap: 4px; }
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 2px 7px;
    border-radius: 12px;
    font-size: 11px;
    white-space: nowrap;
  }
  .badge-plugin { font-weight: 600; opacity: 0.75; }
  .badge.mila_ldap   { background: #1c2e4a; border: 1px solid #1f6feb; color: #79c0ff; }
  .badge.drac_member { background: #1c3023; border: 1px solid #238636; color: #56d364; }
  .badge.drac_role   { background: #1c3023; border: 1px solid #2ea043; color: #3fb950; }
  .badge.drac_username { background: #13302e; border: 1px solid #1b8a7e; color: #4fd1c5; }
  .badge.mymila      { background: #2e1c3a; border: 1px solid #8957e5; color: #d2a8ff; }
  .badge.other       { background: #2e2a1c; border: 1px solid #9e6a03; color: #e3b341; }

  /* ---- match screen layout ---- */
  .match-layout {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 24px;
  }
  .match-col h3 {
    font-size: 13px;
    font-weight: 600;
    color: #e6edf3;
    margin-bottom: 10px;
  }
  .match-col-inner {
    border: 1px solid #30363d;
    border-radius: 6px;
    overflow: hidden;
    max-height: 420px;
    overflow-y: auto;
  }

  /* selectable rows */
  tbody tr.selectable { cursor: pointer; }
  tbody tr.selectable:hover { background: #1c2333; }
  tbody tr.selected   { background: #1c2e4a !important; }
  tbody tr.paired     { opacity: 0.35; pointer-events: none; }

  /* ---- pairs section ---- */
  .pairs-header {
    display: flex;
    align-items: center;
    gap: 16px;
    margin-bottom: 10px;
  }
  .pairs-header h3 { font-size: 13px; font-weight: 600; color: #e6edf3; }
  .btn {
    padding: 5px 14px;
    border-radius: 6px;
    font-family: inherit;
    font-size: 12px;
    cursor: pointer;
    border: 1px solid;
    outline: none;
  }
  .btn-primary {
    background: #238636;
    border-color: #2ea043;
    color: #fff;
  }
  .btn-primary:hover { background: #2ea043; }
  .btn-primary:disabled { opacity: 0.4; cursor: not-allowed; }
  .btn-danger {
    background: transparent;
    border-color: #f85149;
    color: #f85149;
    font-size: 11px;
    padding: 2px 8px;
  }
  .btn-danger:hover { background: #3d1217; }
  .btn-danger:hover { background: #3d1217; }
  .btn-ignore {
    background: transparent;
    border-color: #8b949e;
    color: #8b949e;
    font-size: 11px;
    padding: 2px 8px;
  }
  .btn-ignore:hover { background: #21262d; color: #c9d1d9; border-color: #c9d1d9; }
  .pairs-empty { color: #8b949e; font-style: italic; padding: 10px 0; }
  .pair-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 6px 0;
    border-bottom: 1px solid #21262d;
  }
  .pair-cell { flex: 1; }
  .pair-arrow { color: #30363d; font-size: 16px; }
  .pair-name  { color: #e6edf3; }
  .pair-mid   { color: #8b949e; font-size: 11px; }

  .hidden { display: none; }
</style>
</head>
<body>

<nav class="nav">
  <div class="nav-tab active" data-page="page-all">All Users</div>
  <div class="nav-tab"        data-page="page-match">Match DRAC</div>
</nav>

<!-- ================================================================== -->
<!-- PAGE 1 — All users                                                  -->
<!-- ================================================================== -->
<div id="page-all" class="page active">
  <h2>All Users</h2>
  <div class="toolbar">
    <input id="search-all" class="search-input" type="search"
           placeholder="Filter by name, email or match ID…" autofocus>
    <span id="count-all" class="count"></span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Display name</th>
        <th>Email</th>
        <th>Match IDs</th>
      </tr>
    </thead>
    <tbody id="tbody-all"></tbody>
  </table>
</div>

<!-- ================================================================== -->
<!-- PAGE 2 — Match DRAC users                                           -->
<!-- ================================================================== -->
<div id="page-match" class="page">
  <h2>Match DRAC Users</h2>
  <div style="display:flex;align-items:center;gap:16px;margin-bottom:16px;">
    <p style="color:#8b949e;font-size:12px;">
      Click a DRAC-only user on the left, then click the LDAP-only user on the right to pair them.
    </p>
    <button id="btn-autopair" class="btn btn-primary" style="white-space:nowrap;">Auto-pair by email</button>
    <span id="autopair-report" style="color:#8b949e;font-size:12px;"></span>
  </div>

  <div class="match-layout">
    <!-- Left: DRAC-only -->
    <div class="match-col">
      <h3>Unmatched DRAC users <span id="count-drac" class="count"></span></h3>
      <div class="toolbar" style="margin-bottom:8px;">
        <input id="search-drac" class="search-input" type="search"
               placeholder="Filter…" style="max-width:100%;">
      </div>
      <div class="match-col-inner">
        <table>
          <thead>
            <tr><th>Display name</th><th>Email</th><th>DRAC username</th><th>CCI</th><th>Action</th></tr>
          </thead>
          <tbody id="tbody-drac"></tbody>
        </table>
      </div>
    </div>

    <!-- Right: LDAP-only -->
    <div class="match-col">
      <h3>LDAP users without DRAC <span id="count-ldap" class="count"></span></h3>
      <div class="toolbar" style="margin-bottom:8px;">
        <input id="search-ldap" class="search-input" type="search"
               placeholder="Filter…" style="max-width:100%;">
      </div>
      <div class="match-col-inner">
        <table>
          <thead>
            <tr><th>Display name</th><th>Email</th><th>LDAP ID</th></tr>
          </thead>
          <tbody id="tbody-ldap"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- Pairs section -->
  <div class="pairs-header">
    <h3>Pairs</h3>
    <button id="btn-download" class="btn btn-primary" disabled>Download JSON</button>
    <button id="btn-load" class="btn btn-primary">Load from file…</button>
    <button id="btn-reset" class="btn btn-danger">Reset list</button>
    <input id="file-input" type="file" accept=".json,application/json" style="display:none;">
    <span id="load-report" style="color:#8b949e;font-size:12px;"></span>
  </div>
  <div id="pairs-list"></div>

  <!-- Ignored section -->
  <div style="margin-top:24px; border-top:1px solid #30363d; padding-top:16px;">
    <div class="pairs-header">
      <h3>Ignored DRAC Users <span id="count-ignored" class="count"></span></h3>
      <button id="btn-download-ignore" class="btn btn-primary" disabled>Download Ignore List</button>
      <button id="btn-load-ignore" class="btn btn-primary">Load Ignore List…</button>
      <button id="btn-clear-ignore" class="btn btn-danger">Clear Ignore List</button>
      <input id="file-input-ignore" type="file" accept=".json,application/json" style="display:none;">
      <span id="load-ignore-report" style="color:#8b949e;font-size:12px;"></span>
    </div>
    <div id="ignored-list"></div>
  </div>
</div>

<!-- ================================================================== -->
<!-- JS                                                                  -->
<!-- ================================================================== -->
<script>
const USERS = __USERS_DATA__;
const INITIAL_IGNORED = __IGNORED_DATA__;

// ---- navigation --------------------------------------------------------
document.querySelectorAll(".nav-tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".nav-tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById(tab.dataset.page).classList.add("active");
  });
});

// ---- badge helpers -----------------------------------------------------
const PLUGIN_CLASSES = { mila_ldap:"mila_ldap", drac_member:"drac_member",
                          drac_role:"drac_role", mymila:"mymila",
                          drac_username:"drac_username" };
function badgeClass(p) { return PLUGIN_CLASSES[p] || "other"; }
function badge(p, mid) {
  return `<span class="badge ${badgeClass(p)}">
    <span class="badge-plugin">${p}</span>
    <span class="badge-mid">${mid}</span></span>`;
}
function usernameBadge(u) {
  return u && u.drac_username ? badge("drac_username", u.drac_username) : "";
}
function cciBadge(u) {
  return u && u.matching_ids.drac_member ? badge("drac_member", u.matching_ids.drac_member) : "";
}
// Matching id identifying a DRAC user: the username, or the CCI if unknown.
function dracRef(u) {
  return u.drac_username
    ? { name: "drac_username", mid: u.drac_username }
    : { name: "drac_member", mid: u.matching_ids.drac_member };
}
function refKey(ref) { return `${ref.name}:${ref.mid}`; }

// ========================================================================
// PAGE 1 — All users
// ========================================================================
(function() {
  const tbody = document.getElementById("tbody-all");
  const countEl = document.getElementById("count-all");

  function render(q) {
    q = q.trim().toLowerCase();
    let n = 0;
    tbody.innerHTML = USERS.filter(u => {
      const flat = `${u.display_name} ${u.email} ${u.drac_username || ""} ` +
        Object.entries(u.matching_ids).map(([p,m]) => `${p} ${m}`).join(" ");
      return !q || flat.toLowerCase().includes(q);
    }).map(u => {
      n++;
      const badges = Object.entries(u.matching_ids).map(([p,m]) => badge(p,m)).join("") +
        usernameBadge(u);
      return `<tr>
        <td class="name">${u.display_name}</td>
        <td class="email">${u.email}</td>
        <td><div class="badges">${badges}</div></td>
      </tr>`;
    }).join("");
    countEl.textContent = `${n} / ${USERS.length}`;
  }

  document.getElementById("search-all").addEventListener("input", e => render(e.target.value));
  render("");
})();

// ========================================================================
// PAGE 2 — Match DRAC
// ========================================================================
(function() {
  // Split users into the two pools, based on the DRAC username (users may
  // not have a CCI)
  const dracOnly = USERS.filter(u =>
    u.drac_username && !("mila_ldap" in u.matching_ids));
  const ldapOnly = USERS.filter(u =>
    "mila_ldap" in u.matching_ids && !u.drac_username);

  const pairs = [];         // [{drac: user, ldap: user}]
  // Ignored DRAC users: refKey → matching id ({name, mid}), see dracRef
  const ignored = new Map();
  let selectedDrac = null;  // currently selected DRAC row user

  const tbodyDrac = document.getElementById("tbody-drac");
  const tbodyLdap = document.getElementById("tbody-ldap");
  const pairsList = document.getElementById("pairs-list");
  const btnDownload = document.getElementById("btn-download");
  const countDrac = document.getElementById("count-drac");
  const countLdap = document.getElementById("count-ldap");

  const ignoredList = document.getElementById("ignored-list");
  const btnDownloadIgnore = document.getElementById("btn-download-ignore");
  const btnLoadIgnore = document.getElementById("btn-load-ignore");
  const btnClearIgnore = document.getElementById("btn-clear-ignore");
  const fileInputIgnore = document.getElementById("file-input-ignore");
  const loadIgnoreReport = document.getElementById("load-ignore-report");
  const countIgnored = document.getElementById("count-ignored");

  // Set of user IDs already paired (removed from lists)
  const pairedIds = new Set();

  function isPaired(u) { return pairedIds.has(u.id); }
  function isIgnored(u) { return ignored.has(refKey(dracRef(u))); }

  // Add a matching id to the ignore list, converting legacy CCI entries to
  // usernames when the user is known. Returns false if already ignored.
  function addIgnored(ref) {
    if (ref.name === "drac_member") {
      const u = dracOnly.find(user => user.matching_ids.drac_member === ref.mid);
      if (u) ref = dracRef(u);
    }
    const key = refKey(ref);
    if (ignored.has(key)) return false;
    ignored.set(key, { name: ref.name, mid: ref.mid });
    return true;
  }
  (INITIAL_IGNORED || []).forEach(addIgnored);

  // ---- render left column (DRAC-only) ----------------------------------
  function renderDrac(q) {
    q = q.trim().toLowerCase();
    let n = 0;
    const rows = dracOnly.map(u => {
      const mid = u.matching_ids.drac_member || "";
      const hay = `${u.display_name} ${u.email} ${u.drac_username || ""} ${mid}`.toLowerCase();
      const visible = !q || hay.includes(q);
      const hidden = !visible || isPaired(u) || isIgnored(u);
      if (!hidden) n++;
      const cls = [
        "selectable",
        isPaired(u)         ? "paired"   : "",
        u === selectedDrac  ? "selected" : "",
        hidden              ? "hidden"   : "",
      ].filter(Boolean).join(" ");
      return `<tr class="${cls}" data-id="${u.id}">
        <td class="name">${u.display_name}</td>
        <td class="email">${u.email || ""}</td>
        <td>${usernameBadge(u)}</td>
        <td>${cciBadge(u)}</td>
        <td><button class="btn btn-ignore" data-id="${u.id}">Ignore</button></td>
      </tr>`;
    }).join("");
    tbodyDrac.innerHTML = rows;
    countDrac.textContent = `(${n} remaining)`;

    // re-attach click handlers for selection
    tbodyDrac.querySelectorAll("tr.selectable:not(.paired)").forEach(tr => {
      tr.addEventListener("click", () => {
        const user = dracOnly.find(u => u.id === parseInt(tr.dataset.id));
        if (!user) return;
        selectedDrac = (selectedDrac === user) ? null : user;
        renderDrac(document.getElementById("search-drac").value);
      });
    });

    // re-attach click handlers for ignore button
    tbodyDrac.querySelectorAll(".btn-ignore").forEach(btn => {
      btn.addEventListener("click", e => {
        e.stopPropagation();
        const user = dracOnly.find(u => u.id === parseInt(btn.dataset.id));
        if (!user) return;
        if (selectedDrac === user) selectedDrac = null;
        addIgnored(dracRef(user));
        renderAll();
        renderIgnored();
      });
    });
  }

  // ---- render right column (LDAP-only) ---------------------------------
  function renderLdap(q) {
    q = q.trim().toLowerCase();
    let n = 0;
    const rows = ldapOnly.map(u => {
      const mid = u.matching_ids.mila_ldap || "";
      const hay = `${u.display_name} ${u.email} ${mid}`.toLowerCase();
      const visible = !q || hay.includes(q);
      if (visible && !isPaired(u)) n++;
      const cls = [
        "selectable",
        isPaired(u) ? "paired" : "",
        (!visible || isPaired(u)) ? "hidden" : "",
      ].filter(Boolean).join(" ");
      return `<tr class="${cls}" data-id="${u.id}">
        <td class="name">${u.display_name}</td>
        <td class="email">${u.email || ""}</td>
        <td>${badge("mila_ldap", mid)}</td>
      </tr>`;
    }).join("");
    tbodyLdap.innerHTML = rows;
    countLdap.textContent = `(${n} remaining)`;

    // re-attach click handlers
    tbodyLdap.querySelectorAll("tr.selectable:not(.paired)").forEach(tr => {
      tr.addEventListener("click", () => {
        if (!selectedDrac) return;
        const user = ldapOnly.find(u => u.id === parseInt(tr.dataset.id));
        if (!user) return;
        addPair(selectedDrac, user);
      });
    });
  }

  // ---- pairs -----------------------------------------------------------
  function addPair(drac, ldap) {
    pairs.push({ drac, ldap });
    pairedIds.add(drac.id);
    pairedIds.add(ldap.id);
    selectedDrac = null;
    renderAll();
    renderPairs();
  }

  function removePair(idx) {
    const p = pairs.splice(idx, 1)[0];
    pairedIds.delete(p.drac.id);
    pairedIds.delete(p.ldap.id);
    renderAll();
    renderPairs();
  }

  function renderPairs() {
    btnDownload.disabled = pairs.length === 0;
    if (pairs.length === 0) {
      pairsList.innerHTML = '<p class="pairs-empty">No pairs yet.</p>';
      return;
    }
    pairsList.innerHTML = pairs.map((p, i) => `
      <div class="pair-row">
        <div class="pair-cell">
          <div class="pair-name">${p.drac.display_name}</div>
          <div class="pair-mid">${usernameBadge(p.drac)} ${cciBadge(p.drac)}</div>
        </div>
        <div class="pair-arrow">→</div>
        <div class="pair-cell">
          <div class="pair-name">${p.ldap.display_name}</div>
          <div class="pair-mid">${badge("mila_ldap", p.ldap.matching_ids.mila_ldap)}</div>
        </div>
        <button class="btn btn-danger" data-idx="${i}">Remove</button>
      </div>`).join("");

    pairsList.querySelectorAll(".btn-danger").forEach(btn => {
      btn.addEventListener("click", () => removePair(parseInt(btn.dataset.idx)));
    });
  }

  // ---- ignored section --------------------------------------------------
  function renderIgnored() {
    btnDownloadIgnore.disabled = ignored.size === 0;
    countIgnored.textContent = `(${ignored.size})`;
    if (ignored.size === 0) {
      ignoredList.innerHTML = '<p class="pairs-empty">No ignored DRAC users.</p>';
      return;
    }
    ignoredList.innerHTML = Array.from(ignored).map(([key, ref]) => {
      const u = dracOnly.find(user => refKey(dracRef(user)) === key);
      const name = u ? u.display_name : ref.mid;
      const email = u && u.email ? u.email : "";
      const ids = u
        ? `${usernameBadge(u)} ${cciBadge(u)}`
        : badge(ref.name, ref.mid);
      return `<div class="pair-row">
        <div class="pair-cell">
          <div class="pair-name">${name}</div>
          <div class="pair-mid">${email ? email + " • " : ""}${ids}</div>
        </div>
        <button class="btn btn-ignore btn-unignore" data-key="${key}">Unignore</button>
      </div>`;
    }).join("");

    ignoredList.querySelectorAll(".btn-unignore").forEach(btn => {
      btn.addEventListener("click", () => {
        ignored.delete(btn.dataset.key);
        renderAll();
        renderIgnored();
      });
    });
  }

  btnDownloadIgnore.addEventListener("click", () => {
    const data = Array.from(ignored.values()).map(ref => ({ matching_id: ref }));
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "drac_ignored.json";
    a.click();
    URL.revokeObjectURL(a.href);
  });

  btnLoadIgnore.addEventListener("click", () => fileInputIgnore.click());
  fileInputIgnore.addEventListener("change", () => {
    const file = fileInputIgnore.files[0];
    if (!file) return;
    fileInputIgnore.value = "";
    const reader = new FileReader();
    reader.onload = e => {
      let entries;
      try { entries = JSON.parse(e.target.result); }
      catch { loadIgnoreReport.textContent = "Error: invalid JSON file."; return; }
      if (!Array.isArray(entries)) {
        loadIgnoreReport.textContent = "Error: JSON must be an array.";
        return;
      }
      let loaded = 0;
      entries.forEach(entry => {
        let ref = null;
        if (typeof entry === "string") ref = { name: "drac_member", mid: entry };
        else if (typeof entry === "object" && entry !== null) {
          if (entry.matching_id && entry.matching_id.mid)
            ref = { name: entry.matching_id.name || "drac_member", mid: entry.matching_id.mid };
          else if (entry.mid) ref = { name: "drac_member", mid: entry.mid };
          else if (entry.drac_member) ref = { name: "drac_member", mid: entry.drac_member };
        }
        if (ref && addIgnored(ref)) loaded++;
      });
      renderAll();
      renderIgnored();
      loadIgnoreReport.textContent = `Loaded ${loaded} ignored user(s).`;
    };
    reader.readAsText(file);
  });

  btnClearIgnore.addEventListener("click", () => {
    ignored.clear();
    loadIgnoreReport.textContent = "";
    renderAll();
    renderIgnored();
  });

  function renderAll() {
    renderDrac(document.getElementById("search-drac").value);
    renderLdap(document.getElementById("search-ldap").value);
  }

  const btnLoad    = document.getElementById("btn-load");
  const fileInput  = document.getElementById("file-input");
  const loadReport = document.getElementById("load-report");

  // ---- load from file --------------------------------------------------
  btnLoad.addEventListener("click", () => fileInput.click());

  fileInput.addEventListener("change", () => {
    const file = fileInput.files[0];
    if (!file) return;
    fileInput.value = "";  // reset so the same file can be re-loaded
    const reader = new FileReader();
    reader.onload = e => {
      let entries;
      try { entries = JSON.parse(e.target.result); }
      catch { loadReport.textContent = "Error: invalid JSON file."; return; }

      // Build lookup maps: drac_member mid / drac username → user, mila_ldap mid → user
      const dracByMid = {};
      dracOnly.forEach(u => { dracByMid[u.matching_ids.drac_member] = u; });
      const dracByUsername = {};
      dracOnly.forEach(u => { if (u.drac_username) dracByUsername[u.drac_username] = u; });
      const ldapByMid = {};
      ldapOnly.forEach(u => { ldapByMid[u.matching_ids.mila_ldap] = u; });

      let loaded = 0, skipped = 0;
      entries.forEach(entry => {
        const dracMid = entry.matching_id?.mid;
        const ldapMid = entry.known_matches?.find(m => m.name === "mila_ldap")?.mid;
        const drac = entry.matching_id?.name === "drac_username"
          ? dracByUsername[dracMid] : dracByMid[dracMid];
        const ldap = ldapByMid[ldapMid];
        if (!drac || !ldap || isPaired(drac) || isPaired(ldap)) { skipped++; return; }
        pairs.push({ drac, ldap });
        pairedIds.add(drac.id);
        pairedIds.add(ldap.id);
        loaded++;
      });

      selectedDrac = null;
      renderAll();
      renderPairs();
      const s = loaded !== 1 ? "s" : "";
      loadReport.textContent = skipped > 0
        ? `Loaded ${loaded} pair${s} (${skipped} skipped — already paired or not found).`
        : `Loaded ${loaded} pair${s}.`;
    };
    reader.readAsText(file);
  });

  // ---- reset -----------------------------------------------------------
  document.getElementById("btn-reset").addEventListener("click", () => {
    pairs.length = 0;
    pairedIds.clear();
    selectedDrac = null;
    loadReport.textContent = "";
    document.getElementById("autopair-report").textContent = "";
    renderAll();
    renderPairs();
  });

  // ---- download --------------------------------------------------------
  btnDownload.addEventListener("click", () => {
    const data = pairs.map(p => ({
      matching_id: dracRef(p.drac),
      known_matches: [{ name: "mila_ldap", mid: p.ldap.matching_ids.mila_ldap }],
    }));
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "usermatches.json";
    a.click();
    URL.revokeObjectURL(a.href);
  });

  // ---- auto-pair by email ----------------------------------------------
  document.getElementById("btn-autopair").addEventListener("click", () => {
    const eligible = dracOnly.filter(u => !isPaired(u) && !isIgnored(u));
    const ldapByEmail = {};
    ldapOnly.forEach(u => {
      if (u.email && !isPaired(u)) ldapByEmail[u.email.toLowerCase()] = u;
    });

    let matched = 0;
    eligible.forEach(u => {
      if (!u.email) return;
      const ldap = ldapByEmail[u.email.toLowerCase()];
      if (ldap) {
        pairs.push({ drac: u, ldap });
        pairedIds.add(u.id);
        pairedIds.add(ldap.id);
        delete ldapByEmail[u.email.toLowerCase()];
        matched++;
      }
    });

    selectedDrac = null;
    renderAll();
    renderPairs();

    const remaining = dracOnly.filter(u => !isPaired(u) && !isIgnored(u)).length;
    const pct = eligible.length > 0 ? Math.round(matched / eligible.length * 100) : 0;
    const s = matched !== 1 ? "s" : "";
    document.getElementById("autopair-report").textContent =
      `${matched} account${s} automatically paired (${pct}%), ${remaining} remaining`;
  });

  // ---- search wiring ---------------------------------------------------
  document.getElementById("search-drac").addEventListener("input", e => renderDrac(e.target.value));
  document.getElementById("search-ldap").addEventListener("input", e => renderLdap(e.target.value));

  // ---- init ------------------------------------------------------------
  renderAll();
  renderPairs();
  renderIgnored();
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def load_drac_usernames(sess: Session) -> dict[int, str]:
    """Map user ids to their most recent DRAC username."""
    stmt = (
        select(CredentialsDB.user_id, CredentialsDB.username)
        .where(CredentialsDB.domain == "drac")
        .order_by(func.lower(CredentialsDB.valid))
    )
    # Later validity periods overwrite earlier ones.
    return {user_id: username for user_id, username in sess.exec(stmt)}


def load_users() -> list[dict]:
    """Fetch all users from the database with their matching IDs."""
    with config.db.session() as sess:
        drac_usernames = load_drac_usernames(sess)
        db_users = sess.exec(select(UserDB)).all()
        return [
            {
                "id": u.id,
                "display_name": u.display_name,
                "email": u.email,
                "matching_ids": dict(u.matching_ids),
                "drac_username": drac_usernames.get(u.id),
            }
            for u in tqdm(db_users, desc="Loading users", unit="user")
        ]


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_handler(html_bytes: bytes):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html_bytes)))
                self.end_headers()
                self.wfile.write(html_bytes)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            pass  # silence request logs

    return Handler


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--port", type=int, default=0, help="Port to listen on (0 = pick a free port)"
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open the browser automatically",
    )
    parser.add_argument(
        "--ignore-file",
        type=str,
        default=None,
        help="Path to JSON file containing ignored DRAC users",
    )
    args = parser.parse_args()

    print("Loading users from database…")
    users = load_users()
    print(f"Loaded {len(users)} users.")

    ignored = load_ignore_list(args.ignore_file)
    if ignored:
        print(f"Loaded {len(ignored)} ignored DRAC users from {args.ignore_file}.")

    users_json = json.dumps(users, ensure_ascii=False)
    ignored_json = json.dumps(ignored, ensure_ascii=False)
    html_bytes = (
        _HTML.replace("__USERS_DATA__", users_json)
        .replace("__IGNORED_DATA__", ignored_json)
        .encode("utf-8")
    )

    port = args.port or _find_free_port()
    server = HTTPServer(("127.0.0.1", port), _make_handler(html_bytes))
    url = f"http://127.0.0.1:{port}/"
    print(f"Serving at {url}  (Ctrl-C to stop)")

    if not args.no_browser:
        threading.Timer(0.3, webbrowser.open, args=[url]).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
