import { sparqlEscapeUri, sparqlEscapeDateTime } from "mu";
import { querySudo as query, updateSudo as update } from "@lblod/mu-auth-sudo";
import {
  DEFAULT_GRAPH,
  MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS,
  HIGH_LOAD_DATABASE_ENDPOINT,
} from "../constants";
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};

function cleanupUrl(u) {
  let url = new URL(u);
  url.pathname = "";
  url.search = "";
  return url.toString();
}

export async function getJobWithStatusAndBeforeDate(status, date) {
  const q = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/> 
  select distinct ?job ?jobId where {
    graph <${DEFAULT_GRAPH}> {
      ?job a ?type;
           mu:uuid ?jobId;
           <http://www.w3.org/ns/adms#status> <${status}>;
           <http://purl.org/dc/terms/modified> ?modified.
      filter (?modified < ${sparqlEscapeDateTime(date)} && ?type in(<http://vocab.deri.ie/cogs#Job>,<http://vocab.deri.ie/cogs#ScheduledJob>))

    }
  
  }`;
  let res = await query(q, {}, connectionOptions);
  return res.results.bindings.map((r) =>  {return  {jobUri: r.job.value, jobId: r.jobId.value}});
}
export async function getSuccessfulJobs() {
  const jobsToClean = [];
  let maxDaysToKeepSuccessFulJobs = new Date();
  maxDaysToKeepSuccessFulJobs.setDate(
    maxDaysToKeepSuccessFulJobs.getDate() - MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS,
  );
  let jobsMap = new Map();
  const selectAllJobsAndCollectingContainer = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    select distinct ?job ?jobId ?modified ?dataContainer where {

     graph <${DEFAULT_GRAPH}>{
       ?job a ?type; 
       <http://www.w3.org/ns/adms#status> <http://redpencil.data.gift/id/concept/JobStatus/success>;
        mu:uuid ?jobId;
       <http://purl.org/dc/terms/modified> ?modified.
       ?tasks <http://purl.org/dc/terms/isPartOf> ?job;
   	      <http://redpencil.data.gift/vocabularies/tasks/operation> <http://lblod.data.gift/id/jobs/concept/TaskOperation/collecting>;
               <http://www.w3.org/ns/adms#status> <http://redpencil.data.gift/id/concept/JobStatus/success>.
       ?tasks <http://redpencil.data.gift/vocabularies/tasks/resultsContainer> ?dataContainer.
         filter (?type in(<http://vocab.deri.ie/cogs#Job>,<http://vocab.deri.ie/cogs#ScheduledJob>))
     
     }
    }

`;
  const response = await query(
    selectAllJobsAndCollectingContainer,
    {},
    connectionOptions,
  );
  for (const job of response?.results?.bindings) {
    const jobUri = job.job.value;
    const jobId = job.jobId.value;
    const modified = new Date(job.modified.value);

    const dataContainer = job.dataContainer.value;
    const queryRootUrl = `
        select distinct ?rootUrl where {

        graph <${DEFAULT_GRAPH}> {
             ${sparqlEscapeUri(dataContainer)}  <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?remoteDataObject.
             ?remoteDataObject <http://purl.org/dc/terms/created> ?dataObjectCreated.
             ?remoteDataObject <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url> ?rootUrl.
             
        }

        }order by ?dataObjectCreated  limit 1
`;
    let res = await query(queryRootUrl, {}, connectionOptions);
    // we assume a target url exists
    // if not, we skip the job

    if (res.results?.bindings?.length === 1) {
      let rootUrl = cleanupUrl(res.results.bindings[0].rootUrl.value);

      if (!jobsMap.has(rootUrl)) {
        jobsMap.set(rootUrl, []);
      }
      let jobsPerRootUrl = jobsMap.get(rootUrl);
      jobsPerRootUrl.push({
        jobUri,
        jobId,
        modified,
      });
    }
  }

  for (let [rootUrl, jobs] of jobsMap.entries()) {
    jobs.sort((a, b) => a.modified - b.modified);
    const mostRecentJob = jobs.pop();
    console.log(
      `keeping ${mostRecentJob.jobUri} because it's the most recent one for ${rootUrl} (date: ${mostRecentJob.modified.toISOString()})`,
    );
    while (jobs.length) {
      const j = jobs.pop();
      if (j.modified < maxDaysToKeepSuccessFulJobs) {
        jobsToClean.push({ jobId: j.jobId, jobUri: j.jobUri });
      } else {
        console.log(
          `keeping ${j.jobUri} as its date '${j.modified.toISOString()}' is greater than ${maxDaysToKeepSuccessFulJobs.toISOString()}`,
        );
      }
    }
  }
  return jobsToClean;
}

export async function* getFilesForJob(jobUri) {
  const limit = 5000;
  let offset = 0;
  let bindings;
  do {
    const res = await query(`
      SELECT ?file ?fileOnDisk
      WHERE {
      { SELECT distinct ?file ?fileOnDisk WHERE {
        graph <${DEFAULT_GRAPH}> {
          ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)}.
          { ?task <http://redpencil.data.gift/vocabularies/tasks/resultsContainer> ?container }
          UNION
          { ?task <http://redpencil.data.gift/vocabularies/tasks/inputContainer> ?container }
          ?container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file.
          ?fileOnDisk <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?file.
        }
      } ORDER BY ?file }
    } LIMIT ${limit} OFFSET ${offset}
    `, {}, connectionOptions);
    bindings = res.results.bindings;
    for (const r of bindings) {
      yield { file: r.file.value, fileOnDisk: r.fileOnDisk.value };
    }
    offset += limit;
  } while (bindings.length === limit);
}

export async function deleteContainersForJob(jobUri) {
  const containerPreds = [
    "http://redpencil.data.gift/vocabularies/tasks/resultsContainer",
    "http://redpencil.data.gift/vocabularies/tasks/inputContainer",
  ];
  for (const containerPred of containerPreds) {
    // Delete physical file metadata
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?fileOnDisk ?p ?o } }
      WHERE {
        GRAPH <${DEFAULT_GRAPH}> {
          ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)} .
          ?task <${containerPred}> ?container .
          ?container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file .
          ?fileOnDisk <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?file .
          ?fileOnDisk ?p ?o .
        }
      }
    `, {}, connectionOptions);
    // Delete virtual file metadata
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?file ?p ?o } }
      WHERE {
        GRAPH <${DEFAULT_GRAPH}> {
          ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)} .
          ?task <${containerPred}> ?container .
          ?container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file .
          ?file ?p ?o .
        }
      }
    `, {}, connectionOptions);
    // Delete remote data objects in harvesting collections
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?rdo ?p ?o } }
      WHERE {
        GRAPH <${DEFAULT_GRAPH}> {
          ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)} .
          ?task <${containerPred}> ?container .
          ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection .
          ?collection <http://purl.org/dc/terms/hasPart> ?rdo .
          ?rdo ?p ?o .
        }
      }
    `, {}, connectionOptions);
    // Delete harvesting collections
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?collection ?p ?o } }
      WHERE {
        GRAPH <${DEFAULT_GRAPH}> {
          ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)} .
          ?task <${containerPred}> ?container .
          ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection .
          ?collection ?p ?o .
        }
      }
    `, {}, connectionOptions);
    // Delete container
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?container ?p ?o } }
      WHERE {
        GRAPH <${DEFAULT_GRAPH}> {
          ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)} .
          ?task <${containerPred}> ?container .
          ?container ?p ?o .
        }
      }
    `, {}, connectionOptions);
  }
}

export async function deleteErrorsForJob(jobUri) {
  // Errors on tasks
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?error ?p ?o } }
    WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)} .
        ?task <http://redpencil.data.gift/vocabularies/tasks/error> ?error .
        ?error ?p ?o .
      }
    }
  `, {}, connectionOptions);
  // Errors directly on the job
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?error ?p ?o } }
    WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        ${sparqlEscapeUri(jobUri)} <http://redpencil.data.gift/vocabularies/tasks/error> ?error .
        ?error ?p ?o .
      }
    }
  `, {}, connectionOptions);
}

export async function genericDelete(subject) {
  const q = `delete where {
      graph <${DEFAULT_GRAPH}>{
        ${sparqlEscapeUri(subject)} ?p ?o.
        optional {
          ?m ?n ${sparqlEscapeUri(subject)}; ?mm ?mo
        }
  }}`;

  await update(q, {}, connectionOptions);
}

// ── Orphan cleanup ─────────────────────────────────────────────────────────────
// Cleans up metadata left behind by previously failed cleanup runs.
// All container/collection/RDO deletes are server-side; only file pairs need
// the URI in JS so the physical disk file can be unlinked.

const ORPHAN_BATCH = 200;

export async function getOrphanedTaskBatch() {
  const res = await query(`
    SELECT DISTINCT ?task WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        ?task a <http://redpencil.data.gift/vocabularies/tasks/Task> ;
              <http://purl.org/dc/terms/isPartOf> ?job .
      }
      MINUS {
        GRAPH <${DEFAULT_GRAPH}> {
          ?job a ?t .
          FILTER(?t IN (<http://vocab.deri.ie/cogs#Job>, <http://vocab.deri.ie/cogs#ScheduledJob>))
        }
      }
    } LIMIT ${ORPHAN_BATCH}
  `, {}, connectionOptions);
  return res.results.bindings.map(b => b.task.value);
}

export async function deleteOrphanedTaskBatch(taskUris) {
  const values = taskUris.map(sparqlEscapeUri).join(" ");
  for (const pred of [
    "http://redpencil.data.gift/vocabularies/tasks/resultsContainer",
    "http://redpencil.data.gift/vocabularies/tasks/inputContainer",
  ]) {
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?rdo ?p ?o } }
      WHERE { GRAPH <${DEFAULT_GRAPH}> {
        VALUES ?task { ${values} }
        ?task <${pred}> ?container .
        ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection .
        ?collection <http://purl.org/dc/terms/hasPart> ?rdo . ?rdo ?p ?o .
      } }
    `, {}, connectionOptions);
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?collection ?p ?o } }
      WHERE { GRAPH <${DEFAULT_GRAPH}> {
        VALUES ?task { ${values} }
        ?task <${pred}> ?container .
        ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection .
        ?collection ?p ?o .
      } }
    `, {}, connectionOptions);
    await update(`
      DELETE { GRAPH <${DEFAULT_GRAPH}> { ?container ?p ?o } }
      WHERE { GRAPH <${DEFAULT_GRAPH}> {
        VALUES ?task { ${values} }
        ?task <${pred}> ?container . ?container ?p ?o .
      } }
    `, {}, connectionOptions);
  }
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?error ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> {
      VALUES ?task { ${values} }
      ?task <http://redpencil.data.gift/vocabularies/tasks/error> ?error . ?error ?p ?o .
    } }
  `, {}, connectionOptions);
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?task ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> { VALUES ?task { ${values} } ?task ?p ?o . } }
  `, {}, connectionOptions);
}

export async function getOrphanedContainerBatch() {
  const res = await query(`
    SELECT DISTINCT ?container WHERE {
      GRAPH <${DEFAULT_GRAPH}> {
        { ?container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file }
        UNION
        { ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?col }
      }
      MINUS { GRAPH <${DEFAULT_GRAPH}> { ?task <http://redpencil.data.gift/vocabularies/tasks/resultsContainer> ?container } }
      MINUS { GRAPH <${DEFAULT_GRAPH}> { ?task <http://redpencil.data.gift/vocabularies/tasks/inputContainer> ?container } }
    } LIMIT ${ORPHAN_BATCH}
  `, {}, connectionOptions);
  return res.results.bindings.map(b => b.container.value);
}

export async function deleteOrphanedContainerBatch(containerUris) {
  const values = containerUris.map(sparqlEscapeUri).join(" ");
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?rdo ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> {
      VALUES ?container { ${values} }
      ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection .
      ?collection <http://purl.org/dc/terms/hasPart> ?rdo . ?rdo ?p ?o .
    } }
  `, {}, connectionOptions);
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?collection ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> {
      VALUES ?container { ${values} }
      ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection .
      ?collection ?p ?o .
    } }
  `, {}, connectionOptions);
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?container ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> { VALUES ?container { ${values} } ?container ?p ?o . } }
  `, {}, connectionOptions);
}

export async function getOrphanedCollectionBatch() {
  const res = await query(`
    SELECT DISTINCT ?collection WHERE {
      GRAPH <${DEFAULT_GRAPH}> { ?collection <http://purl.org/dc/terms/hasPart> ?rdo }
      MINUS { GRAPH <${DEFAULT_GRAPH}> { ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection } }
    } LIMIT ${ORPHAN_BATCH}
  `, {}, connectionOptions);
  return res.results.bindings.map(b => b.collection.value);
}

export async function deleteOrphanedCollectionBatch(collectionUris) {
  const values = collectionUris.map(sparqlEscapeUri).join(" ");
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?rdo ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> {
      VALUES ?collection { ${values} }
      ?collection <http://purl.org/dc/terms/hasPart> ?rdo . ?rdo ?p ?o .
    } }
  `, {}, connectionOptions);
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?collection ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> { VALUES ?collection { ${values} } ?collection ?p ?o . } }
  `, {}, connectionOptions);
}

export async function getOrphanedRDOBatch() {
  const res = await query(`
    SELECT DISTINCT ?rdo WHERE {
      GRAPH <${DEFAULT_GRAPH}> { ?rdo <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url> ?url }
      MINUS { GRAPH <${DEFAULT_GRAPH}> { ?collection <http://purl.org/dc/terms/hasPart> ?rdo } }
    } LIMIT ${ORPHAN_BATCH}
  `, {}, connectionOptions);
  return res.results.bindings.map(b => b.rdo.value);
}

export async function getOrphanedFileBatch() {
  const res = await query(`
    SELECT DISTINCT ?file ?diskFile WHERE {
      GRAPH <${DEFAULT_GRAPH}> { ?diskFile <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?file }
      MINUS { GRAPH <${DEFAULT_GRAPH}> { ?container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file } }
      MINUS { GRAPH <${DEFAULT_GRAPH}> { ?distribution <http://purl.org/dc/terms/subject> ?file } }
    } LIMIT ${ORPHAN_BATCH}
  `, {}, connectionOptions);
  return res.results.bindings.map(b => ({ file: b.file.value, diskFile: b.diskFile.value }));
}

export async function getOrphanedErrorBatch() {
  const res = await query(`
    SELECT DISTINCT ?error WHERE {
      GRAPH <${DEFAULT_GRAPH}> { ?error a <http://open-services.net/ns/core#Error> }
      MINUS { GRAPH <${DEFAULT_GRAPH}> { ?subject <http://redpencil.data.gift/vocabularies/tasks/error> ?error } }
    } LIMIT ${ORPHAN_BATCH}
  `, {}, connectionOptions);
  return res.results.bindings.map(b => b.error.value);
}

export async function deleteSubjectsBatch(uris) {
  if (!uris.length) return;
  const values = uris.map(sparqlEscapeUri).join(" ");
  await update(`
    DELETE { GRAPH <${DEFAULT_GRAPH}> { ?s ?p ?o } }
    WHERE { GRAPH <${DEFAULT_GRAPH}> { VALUES ?s { ${values} } ?s ?p ?o . } }
  `, {}, connectionOptions);
}

export async function deleteFileInDb(f) {
  const deleteByPredicate = async (pred) => await update(`
  delete where {
      graph <${DEFAULT_GRAPH}> {
        ?s <${pred}> ${sparqlEscapeUri(f)}; ?p ?o
      }
  }`, {}, connectionOptions);
  await deleteByPredicate("http://redpencil.data.gift/vocabularies/tasks/hasFile");
  await deleteByPredicate("http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource");
  await deleteByPredicate("http://oscaf.sourceforge.net/ndo.html#copiedFrom");
  await deleteByPredicate("http://purl.org/dc/terms/hasPart");
  await update(`
    delete where {
        graph <${DEFAULT_GRAPH}> {
          ${sparqlEscapeUri(f)} ?p ?o
        }
    }`, {}, connectionOptions);
}
