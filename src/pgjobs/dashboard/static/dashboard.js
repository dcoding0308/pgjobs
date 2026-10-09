"use strict";

// All job data is inserted with textContent, never innerHTML: payloads and
// error messages are untrusted and must not be able to inject markup.

const REFRESH_MS = 2000;
const ACTION_HEADER = "X-Pgjobs-Action";

const state = {
  status: "",
  queue: "",
  selectedId: null,
  lastUpdated: null,
  armedTimer: null,
};

const $ = (id) => document.getElementById(id);

function el(tag, options = {}, children = []) {
  const node = document.createElement(tag);
  if (options.className) node.className = options.className;
  if (options.text !== undefined && options.text !== null) node.textContent = String(options.text);
  if (options.title) node.title = options.title;
  for (const child of children) node.append(child);
  return node;
}

async function getJSON(url, init) {
  const response = await fetch(url, init);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
  return body;
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return "–";
  if (seconds < 1) return "<1s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function relativeTime(iso) {
  if (!iso) return "–";
  const seconds = (Date.now() - new Date(iso).getTime()) / 1000;
  return seconds >= 0 ? `${formatDuration(seconds)} ago` : `in ${formatDuration(-seconds)}`;
}

function showToast(message) {
  const toast = $("toast");
  toast.textContent = message;
  toast.hidden = false;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => (toast.hidden = true), 3000);
}

function setLive(ok, message) {
  $("live-dot").className = `dot ${ok ? "ok" : "err"}`;
  $("live-text").textContent = message;
}

function renderSummary(summary) {
  const totals = { ready: 0, running: 0, succeeded_recently: 0, dead: 0 };
  for (const queue of summary.queues) {
    for (const key of Object.keys(totals)) totals[key] += queue[key];
  }
  $("total-ready").textContent = totals.ready.toLocaleString();
  $("total-running").textContent = totals.running.toLocaleString();
  $("total-done").textContent = totals.succeeded_recently.toLocaleString();
  $("total-dead").textContent = totals.dead.toLocaleString();
  $("total-dead").classList.toggle("has-dead", totals.dead > 0);
  $("done-label").textContent = `Done (${Math.round(summary.recent_window_seconds / 60)} min)`;

  const body = $("queues-body");
  if (summary.queues.length === 0) {
    const cell = el("td", { className: "empty", text: "No jobs yet" });
    cell.colSpan = 7;
    body.replaceChildren(el("tr", {}, [cell]));
  } else {
    body.replaceChildren(
      ...summary.queues.map((q) =>
        el("tr", {}, [
          el("td", { className: "mono", text: q.queue }),
          el("td", { className: "num", text: q.ready.toLocaleString() }),
          el("td", { className: "num", text: q.scheduled.toLocaleString() }),
          el("td", { className: "num", text: q.running.toLocaleString() }),
          el("td", { className: "num", text: q.succeeded_recently.toLocaleString() }),
          el("td", { className: "num", text: q.dead.toLocaleString() }),
          el("td", { className: "num", text: formatDuration(q.oldest_ready_seconds) }),
        ]),
      ),
    );
  }

  const select = $("queue-filter");
  const known = new Set([...select.options].map((option) => option.value));
  for (const q of summary.queues) {
    if (!known.has(q.queue)) select.append(el("option", { text: q.queue }));
  }

  const requeue = $("requeue");
  requeue.hidden = !summary.actions_enabled || totals.dead === 0;
}

function renderJobs(jobs) {
  const body = $("jobs-body");
  if (jobs.length === 0) {
    const cell = el("td", { className: "empty", text: "No matching jobs" });
    cell.colSpan = 7;
    body.replaceChildren(el("tr", {}, [cell]));
    return;
  }
  body.replaceChildren(
    ...jobs.map((job) => {
      const row = el("tr", {}, [
        el("td", { className: "num mono", text: job.id }),
        el("td", { className: "mono", text: job.task_name }),
        el("td", { text: job.queue }),
        el("td", {}, [el("span", { className: `pill ${job.status}`, text: job.status })]),
        el("td", { className: "num", text: `${job.attempts}/${job.max_attempts}` }),
        el("td", { text: relativeTime(job.created_at), title: job.created_at }),
        el("td", { className: "error", text: job.error_summary || "", title: job.error_summary || "" }),
      ]);
      row.tabIndex = 0;
      row.classList.toggle("selected", job.id === state.selectedId);
      row.addEventListener("click", () => openJob(job.id));
      row.addEventListener("keydown", (event) => event.key === "Enter" && openJob(job.id));
      return row;
    }),
  );
}

async function refresh() {
  const params = new URLSearchParams({ limit: "50" });
  if (state.status) params.set("status", state.status);
  if (state.queue) params.set("queue", state.queue);
  try {
    const [summary, jobs] = await Promise.all([
      getJSON("/api/summary"),
      getJSON(`/api/jobs?${params}`),
    ]);
    renderSummary(summary);
    renderJobs(jobs.jobs);
    state.lastUpdated = Date.now();
    setLive(true, "live · updated just now");
    if (state.selectedId !== null) await openJob(state.selectedId, { quiet: true });
  } catch (error) {
    setLive(false, `cannot reach server: ${error.message}`);
  }
}

async function openJob(jobId, { quiet = false } = {}) {
  try {
    const job = await getJSON(`/api/jobs/${encodeURIComponent(jobId)}`);
    state.selectedId = job.id;
    $("drawer-title").textContent = `Job ${job.id}`;
    const facts = [
      ["Task", job.task_name],
      ["Queue", job.queue],
      ["Status", job.status],
      ["Attempts", `${job.attempts} of ${job.max_attempts}`],
      ["Priority", job.priority],
      ["Created", `${job.created_at} (${relativeTime(job.created_at)})`],
      ["Run after", job.run_after],
      ["Finished", job.finished_at || "–"],
      ["Worker", job.locked_by || "–"],
      ["Dedupe key", job.dedupe_key || "–"],
    ];
    $("drawer-facts").replaceChildren(
      ...facts.flatMap(([label, value]) => [el("dt", { text: label }), el("dd", { text: value })]),
    );
    $("drawer-payload").textContent = JSON.stringify(job.payload, null, 2);
    $("drawer-error").textContent = job.last_error || "None";
    $("drawer").hidden = false;
    writeHash();
    for (const row of $("jobs-body").rows) {
      row.classList.toggle("selected", row.cells[0]?.textContent === String(job.id));
    }
  } catch (error) {
    if (!quiet) showToast(error.message);
  }
}

function closeDrawer() {
  state.selectedId = null;
  $("drawer").hidden = true;
  writeHash();
  for (const row of $("jobs-body").rows) row.classList.remove("selected");
}

// Two-step confirmation instead of a blocking confirm() dialog.
async function onRequeueClick() {
  const button = $("requeue");
  if (!button.classList.contains("armed")) {
    button.classList.add("armed");
    button.textContent = state.queue ? `Confirm: retry dead in "${state.queue}"` : "Confirm: retry all dead";
    state.armedTimer = setTimeout(disarm, 4000);
    return;
  }
  disarm();
  button.disabled = true;
  try {
    const params = state.queue ? `?queue=${encodeURIComponent(state.queue)}` : "";
    const result = await getJSON(`/api/requeue-dead${params}`, {
      method: "POST",
      headers: { [ACTION_HEADER]: "1" },
    });
    showToast(`Requeued ${result.requeued} job(s)`);
    await refresh();
  } catch (error) {
    showToast(`Retry failed: ${error.message}`);
  } finally {
    button.disabled = false;
  }
}

function disarm() {
  clearTimeout(state.armedTimer);
  const button = $("requeue");
  button.classList.remove("armed");
  button.textContent = "Retry dead jobs";
}

// The URL hash mirrors the view (e.g. #status=dead&job=42) so it can be shared.
function writeHash() {
  const params = new URLSearchParams();
  if (state.status) params.set("status", state.status);
  if (state.queue) params.set("queue", state.queue);
  if (state.selectedId !== null) params.set("job", String(state.selectedId));
  const hash = params.toString();
  history.replaceState(null, "", hash ? `#${hash}` : location.pathname);
}

function selectStatusTab(status) {
  state.status = status;
  for (const tab of $("status-tabs").querySelectorAll("button")) {
    tab.setAttribute("aria-selected", String(tab.dataset.status === status));
  }
}

function readHash() {
  const params = new URLSearchParams(location.hash.slice(1));
  const status = params.get("status") || "";
  if (["", "queued", "running", "succeeded", "dead"].includes(status)) selectStatusTab(status);
  state.queue = params.get("queue") || "";
  if (state.queue) $("queue-filter").append(el("option", { text: state.queue }));
  $("queue-filter").value = state.queue;
  const job = Number.parseInt(params.get("job") || "", 10);
  if (Number.isSafeInteger(job)) state.selectedId = job;
}

function init() {
  readHash();
  for (const tab of $("status-tabs").querySelectorAll("button")) {
    tab.addEventListener("click", () => {
      selectStatusTab(tab.dataset.status);
      writeHash();
      refresh();
    });
  }
  $("queue-filter").addEventListener("change", (event) => {
    state.queue = event.target.value;
    writeHash();
    refresh();
  });
  $("requeue").addEventListener("click", onRequeueClick);
  $("drawer-close").addEventListener("click", closeDrawer);
  document.addEventListener("keydown", (event) => event.key === "Escape" && closeDrawer());

  refresh();
  setInterval(() => {
    if (!document.hidden) refresh();
  }, REFRESH_MS);
  setInterval(() => {
    if (state.lastUpdated && $("live-dot").classList.contains("ok")) {
      const age = Math.round((Date.now() - state.lastUpdated) / 1000);
      $("live-text").textContent = age < 2 ? "live · updated just now" : `live · updated ${age}s ago`;
    }
  }, 1000);
}

init();
