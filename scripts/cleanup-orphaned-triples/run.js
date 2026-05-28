#!/usr/bin/env node
/**
 * Mu-cli script: cleanup-orphaned-triples
 *
 * Env vars:
 *   MU_APPLICATION_SPARQL_ENDPOINT  used by the template's query/update helpers
 *                                   (default: http://database:8890/sparql)
 *   DEFAULT_GRAPH                   (default: http://mu.semte.ch/graphs/harvesting)
 *   BATCH_SIZE                      (default: 200)
 *
 * Modes:
 *   --analyze   COUNT queries only — fast even on very large graphs.
 *   --dry-run   Enumerate orphaned URIs without deleting (slow on large DBs).
 *   (default)   Perform the actual cleanup in batches.
 *
 * Cleanup steps (in dependency order):
 *   1. Orphaned tasks               — task:Task whose dct:isPartOf job no longer exists
 *   2. Orphaned data containers     — nfo:DataContainer not referenced by any task
 *   3. Orphaned harvesting collections — hrvst:HarvestingCollection not in any container
 *   4. Orphaned remote data objects — nfo:RemoteDataObject not in any collection
 *   5. Orphaned file data objects   — nfo:FileDataObject not referenced by any container
 *   6. Orphaned errors              — oslc:Error not referenced by any task or job
 */

"use strict";

const { unlink } = require("fs/promises");

// Uses native fetch (Node 20+) — no external dependencies needed.
// join_networks: true in config.json makes the stack network reachable.
const ENDPOINT =
  process.env.MU_APPLICATION_SPARQL_ENDPOINT ||
  process.env.HIGH_LOAD_DATABASE_ENDPOINT ||
  "http://database:8890/sparql";
const GRAPH =
  process.env.DEFAULT_GRAPH || "http://mu.semte.ch/graphs/harvesting";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 200;

const ANALYZE  = process.argv.includes("--analyze");
const DRY_RUN  = process.argv.includes("--dry-run");

if (DRY_RUN && ANALYZE) {
  console.error("Pass either --analyze or --dry-run, not both.");
  process.exit(1);
}

// ── Predicates ────────────────────────────────────────────────────────────────
const P = {
  isPartOf:                "http://purl.org/dc/terms/isPartOf",
  hasPart:                 "http://purl.org/dc/terms/hasPart",
  resultsContainer:        "http://redpencil.data.gift/vocabularies/tasks/resultsContainer",
  inputContainer:          "http://redpencil.data.gift/vocabularies/tasks/inputContainer",
  hasFile:                 "http://redpencil.data.gift/vocabularies/tasks/hasFile",
  hasHarvestingCollection: "http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection",
  error:                   "http://redpencil.data.gift/vocabularies/tasks/error",
  dataSource:              "http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource",
  nieUrl:                  "http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url",
  subject:                 "http://purl.org/dc/terms/subject",
};

// ── Types ─────────────────────────────────────────────────────────────────────
const T = {
  task:                "http://redpencil.data.gift/vocabularies/tasks/Task",
  job:                 "http://vocab.deri.ie/cogs#Job",
  scheduledJob:        "http://vocab.deri.ie/cogs#ScheduledJob",
  dataContainer:       "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#DataContainer",
  fileDataObject:      "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject",
  remoteDataObject:    "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#RemoteDataObject",
  error:               "http://open-services.net/ns/core#Error",
  harvestingCollection:"http://lblod.data.gift/vocabularies/harvesting/HarvestingCollection",
};

// ── SPARQL helpers ────────────────────────────────────────────────────────────

async function sparqlFetch(params) {
  let res;
  try {
    res = await fetch(ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/sparql-results+json" },
      body: new URLSearchParams(params),
    });
  } catch (e) {
    console.error(`fetch to ${ENDPOINT} failed: ${e.message}`);
    if (e.cause) console.error(`  cause: ${e.cause.message || e.cause}`);
    throw e;
  }
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`SPARQL request failed (${res.status}): ${body}`);
  }
  return res;
}

async function query(q) {
  const res = await sparqlFetch({ query: q });
  return res.json();
}

async function update(q) {
  if (DRY_RUN) return;
  await sparqlFetch({ update: q });
}

function urisFromResult(result, varName) {
  return (result.results && result.results.bindings || []).map((b) => b[varName].value);
}

async function countQuery(q) {
  const result = await query(q);
  return parseInt((result.results.bindings[0] || {}).count && result.results.bindings[0].count.value || "0");
}

async function fetchOrphanedBatch(q, varName) {
  const result = await query(`${q}\nLIMIT ${BATCH_SIZE}`);
  return urisFromResult(result, varName);
}

async function batchDeleteSubjects(uris) {
  if (!uris.length) return;
  const values = uris.map((u) => `<${u}>`).join(" ");
  await update(`
    DELETE { GRAPH <${GRAPH}> { ?s ?p ?o } }
    WHERE  { GRAPH <${GRAPH}> { ?s ?p ?o . VALUES ?s { ${values} } } }
  `);
}

// ── Orphan detection (MINUS = hash join, faster than FILTER NOT EXISTS) ───────

const ORPHANED_TASK_FILTER = `
  GRAPH <${GRAPH}> { ?task a <${T.task}> ; <${P.isPartOf}> ?job }
  MINUS { GRAPH <${GRAPH}> { ?job a ?t . FILTER(?t IN (<${T.job}>, <${T.scheduledJob}>)) } }
`;
// Anchor on outgoing predicates rather than rdf:type — predicate indexes are far
// more selective than a full type scan on a large graph.  Empty containers/collections/
// RDOs with only a type triple are left behind and caught on the next run.
const ORPHANED_CONTAINER_FILTER = `
  GRAPH <${GRAPH}> {
    { ?container <${P.hasFile}> ?file }
    UNION
    { ?container <${P.hasHarvestingCollection}> ?collection }
  }
  MINUS { GRAPH <${GRAPH}> { ?task <${P.resultsContainer}> ?container } }
  MINUS { GRAPH <${GRAPH}> { ?task <${P.inputContainer}> ?container } }
`;
const ORPHANED_COLLECTION_FILTER = `
  GRAPH <${GRAPH}> { ?collection <${P.hasPart}> ?rdo }
  MINUS { GRAPH <${GRAPH}> { ?container <${P.hasHarvestingCollection}> ?collection } }
`;
const ORPHANED_RDO_FILTER = `
  GRAPH <${GRAPH}> { ?rdo <${P.nieUrl}> ?url }
  MINUS { GRAPH <${GRAPH}> { ?collection <${P.hasPart}> ?rdo } }
`;
// Only target virtual files — those that a physical disk file points to via
// nie:dataSource. Physical files themselves are never the target of task:hasFile
// so a naive "no container" check would incorrectly flag every physical file
// on disk as orphaned.
// Also exclude files referenced via dct:subject (e.g. delta producer graph dumps
// stored as dcat:Distribution → dct:subject → nfo:FileDataObject in this same graph).
const ORPHANED_FILE_FILTER = `
  GRAPH <${GRAPH}> { ?diskFile <${P.dataSource}> ?file }
  MINUS { GRAPH <${GRAPH}> { ?container <${P.hasFile}> ?file } }
  MINUS { GRAPH <${GRAPH}> { ?distribution <${P.subject}> ?file } }
`;
const ORPHANED_ERROR_FILTER = `
  GRAPH <${GRAPH}> { ?error a <${T.error}> }
  MINUS { GRAPH <${GRAPH}> { ?subject <${P.error}> ?error } }
`;

// ── Analysis ──────────────────────────────────────────────────────────────────

async function analyze() {
  console.log("\nRunning analysis (COUNT queries only — no data fetched or modified)...\n");

  const steps = [
    ["Orphaned tasks",                "task",       ORPHANED_TASK_FILTER],
    ["Orphaned data containers",      "container",  ORPHANED_CONTAINER_FILTER],
    ["Orphaned harvesting collections","collection",ORPHANED_COLLECTION_FILTER],
    ["Orphaned remote data objects",  "rdo",        ORPHANED_RDO_FILTER],
    ["Orphaned file data objects",    "file",       ORPHANED_FILE_FILTER],
    ["Orphaned errors",               "error",      ORPHANED_ERROR_FILTER],
  ];

  let hasOrphans = false;
  for (const [label, varName, filter] of steps) {
    const count = await countQuery(
      `SELECT (COUNT(DISTINCT ?${varName}) AS ?count) WHERE { ${filter} }`,
    );
    const flag = count > 0 ? "  <-- needs cleanup" : "";
    console.log(`  ${label.padEnd(35)} ${String(count).padStart(8)}${flag}`);
    if (count > 0) hasOrphans = true;
  }

  console.log();
  if (!hasOrphans) {
    console.log("No orphaned data found.");
  } else {
    console.log("Run without --analyze to perform the cleanup.");
    console.log("Run with --dry-run to enumerate the URIs (slow on large databases).");
  }
}

// ── Cascade helpers ───────────────────────────────────────────────────────────

async function deletePhysicalFile(uri) {
  const path = uri.replace("share://", "/share/");
  if (DRY_RUN) { console.log(`    [dry-run] would delete ${path}`); return; }
  try {
    await unlink(path);
  } catch (e) {
    console.warn(`    could not delete ${path}: ${e.message}`);
  }
}

async function cleanContainerBatch(containerUris) {
  // Files are intentionally NOT deleted here. Removing the container subject
  // (and its task:hasFile triples) orphans the files, and step 5 picks them
  // up in its own batched loop — avoiding unbounded SELECTs for file counts
  // that could be millions.
  const values = containerUris.map((u) => `<${u}>`).join(" ");

  // Delete RDOs inside harvesting collections (server-side join)
  await update(`
    DELETE { GRAPH <${GRAPH}> { ?rdo ?p ?o } }
    WHERE {
      GRAPH <${GRAPH}> {
        VALUES ?container { ${values} }
        ?container <${P.hasHarvestingCollection}> ?collection .
        ?collection <${P.hasPart}> ?rdo .
        ?rdo ?p ?o .
      }
    }
  `);

  // Delete collections
  await update(`
    DELETE { GRAPH <${GRAPH}> { ?collection ?p ?o } }
    WHERE {
      GRAPH <${GRAPH}> {
        VALUES ?container { ${values} }
        ?container <${P.hasHarvestingCollection}> ?collection .
        ?collection ?p ?o .
      }
    }
  `);

  // Delete containers (removes task:hasFile links, orphaning the files for step 5)
  await update(`
    DELETE { GRAPH <${GRAPH}> { ?container ?p ?o } }
    WHERE {
      GRAPH <${GRAPH}> {
        VALUES ?container { ${values} }
        ?container ?p ?o .
      }
    }
  `);
}

// ── Cleanup steps ─────────────────────────────────────────────────────────────

async function cleanOrphanedTasks() {
  console.log("\n=== Step 1: Orphaned tasks ===");
  const q = `SELECT DISTINCT ?task WHERE { ${ORPHANED_TASK_FILTER} }`;
  let total = 0;
  while (true) {
    const tasks = await fetchOrphanedBatch(q, "task");
    if (!tasks.length) break;
    const values = tasks.map((u) => `<${u}>`).join(" ");

    for (const pred of [P.resultsContainer, P.inputContainer]) {
      const containers = urisFromResult(await query(`
        SELECT DISTINCT ?c WHERE {
          GRAPH <${GRAPH}> { VALUES ?task { ${values} } ?task <${pred}> ?c }
        }
      `), "c");
      if (containers.length) await cleanContainerBatch(containers);
    }

    const errors = urisFromResult(await query(`
      SELECT DISTINCT ?e WHERE {
        GRAPH <${GRAPH}> { VALUES ?task { ${values} } ?task <${P.error}> ?e }
      }
    `), "e");
    await batchDeleteSubjects(errors);
    await batchDeleteSubjects(tasks);
    total += tasks.length;
    console.log(`  removed ${total} tasks so far...`);
  }
  console.log(`  done — ${total} orphaned tasks removed`);
}

async function cleanOrphanedContainers() {
  // Use server-side DELETE with an inner subquery LIMIT instead of SELECT + client loop.
  // This way the client only sends an UPDATE and receives a tiny HTTP 200 — no large
  // result set travels over the network, so the fetch timeout is irrelevant.
  console.log("\n=== Step 2: Orphaned data containers ===");
  const batchFilter = `{ SELECT DISTINCT ?container WHERE { ${ORPHANED_CONTAINER_FILTER} } LIMIT ${BATCH_SIZE} }`;
  let total = 0;
  while (true) {
    const exists = await query(`ASK { ${ORPHANED_CONTAINER_FILTER} }`);
    if (!exists.boolean) break;

    await update(`
      DELETE { GRAPH <${GRAPH}> { ?rdo ?p ?o } }
      WHERE { ${batchFilter} GRAPH <${GRAPH}> {
        ?container <${P.hasHarvestingCollection}> ?collection .
        ?collection <${P.hasPart}> ?rdo . ?rdo ?p ?o .
      } }
    `);
    await update(`
      DELETE { GRAPH <${GRAPH}> { ?collection ?p ?o } }
      WHERE { ${batchFilter} GRAPH <${GRAPH}> {
        ?container <${P.hasHarvestingCollection}> ?collection . ?collection ?p ?o .
      } }
    `);
    await update(`
      DELETE { GRAPH <${GRAPH}> { ?container ?p ?o } }
      WHERE { ${batchFilter} GRAPH <${GRAPH}> { ?container ?p ?o . } }
    `);

    total += BATCH_SIZE;
    console.log(`  removed ~${total} containers so far...`);
  }
  console.log(`  done — ~${total} orphaned containers removed`);
}

async function cleanOrphanedHarvestingCollections() {
  console.log("\n=== Step 3: Orphaned harvesting collections ===");
  const q = `SELECT DISTINCT ?collection WHERE { ${ORPHANED_COLLECTION_FILTER} }`;
  let total = 0;
  while (true) {
    const collections = await fetchOrphanedBatch(q, "collection");
    if (!collections.length) break;
    const values = collections.map((u) => `<${u}>`).join(" ");
    // Delete RDOs server-side — avoids pulling potentially large result sets
    await update(`
      DELETE { GRAPH <${GRAPH}> { ?rdo ?p ?o } }
      WHERE {
        GRAPH <${GRAPH}> {
          VALUES ?c { ${values} }
          ?c <${P.hasPart}> ?rdo .
          ?rdo ?p ?o .
        }
      }
    `);
    await batchDeleteSubjects(collections);
    total += collections.length;
    console.log(`  removed ${total} collections so far...`);
  }
  console.log(`  done — ${total} orphaned harvesting collections removed`);
}

async function cleanOrphanedRemoteDataObjects() {
  console.log("\n=== Step 4: Orphaned remote data objects ===");
  const q = `SELECT DISTINCT ?rdo WHERE { ${ORPHANED_RDO_FILTER} }`;
  let total = 0;
  while (true) {
    const rdos = await fetchOrphanedBatch(q, "rdo");
    if (!rdos.length) break;
    await batchDeleteSubjects(rdos);
    total += rdos.length;
    console.log(`  removed ${total} remote data objects so far...`);
  }
  console.log(`  done — ${total} orphaned remote data objects removed`);
}

async function cleanOrphanedFiles() {
  console.log("\n=== Step 5: Orphaned file data objects ===");
  // ORPHANED_FILE_FILTER already selects both ?file (virtual) and ?diskFile (physical)
  const q = `SELECT DISTINCT ?file ?diskFile WHERE { ${ORPHANED_FILE_FILTER} }`;
  let total = 0;
  while (true) {
    const result = await query(`${q}\nLIMIT ${BATCH_SIZE}`);
    const rows = result.results && result.results.bindings || [];
    if (!rows.length) break;
    const fileUris    = rows.map((r) => r.file.value);
    const diskUris    = rows.map((r) => r.diskFile.value);
    for (const uri of diskUris) await deletePhysicalFile(uri);
    await batchDeleteSubjects(diskUris);
    await batchDeleteSubjects(fileUris);
    total += rows.length;
    console.log(`  removed ${total} file pairs so far...`);
  }
  console.log(`  done — ${total} orphaned file pairs removed`);
}

async function cleanOrphanedErrors() {
  console.log("\n=== Step 6: Orphaned errors ===");
  const q = `SELECT DISTINCT ?error WHERE { ${ORPHANED_ERROR_FILTER} }`;
  let total = 0;
  while (true) {
    const errors = await fetchOrphanedBatch(q, "error");
    if (!errors.length) break;
    await batchDeleteSubjects(errors);
    total += errors.length;
    console.log(`  removed ${total} errors so far...`);
  }
  console.log(`  done — ${total} orphaned errors removed`);
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log("=== Orphaned triples cleanup ===");
  console.log(`Endpoint   : ${ENDPOINT}`);
  console.log(`Graph      : ${GRAPH}`);
  console.log(`Batch size : ${BATCH_SIZE}`);

  console.log("Checking connectivity...");
  await query("ASK { ?s ?p ?o }");
  console.log("Connectivity OK");

  if (ANALYZE) { await analyze(); return; }

  if (DRY_RUN) {
    console.log("Mode       : DRY RUN — queries run, no data modified");
    console.warn("WARNING: --dry-run fetches all orphaned URIs. Slow on large databases.");
  }

  await cleanOrphanedTasks();
  await cleanOrphanedContainers();
  await cleanOrphanedHarvestingCollections();
  await cleanOrphanedRemoteDataObjects();
  await cleanOrphanedFiles();
  await cleanOrphanedErrors();

  console.log("\n=== Done ===");
}

main().catch((e) => {
  console.error("Fatal:", e.message);
  process.exit(1);
});
