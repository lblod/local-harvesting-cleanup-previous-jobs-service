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
  const q = `select distinct ?job where {
    graph <${DEFAULT_GRAPH}> {
      ?job a ?type;
           <http://www.w3.org/ns/adms#status> <${status}>;
           <http://purl.org/dc/terms/modified> ?modified.
      filter (?modified < ${sparqlEscapeDateTime(date)} && ?type in(<http://vocab.deri.ie/cogs#Job>,<http://vocab.deri.ie/cogs#ScheduledJob>))

    }
  
  }`;
  let res = await query(q, {}, connectionOptions);
  return res.results.bindings.map((r) => r.job.value);
}
export async function getSuccessfulJobs() {
  const jobsToClean = [];
  let maxDaysToKeepSuccessFulJobs = new Date();
  maxDaysToKeepSuccessFulJobs.setDate(
    maxDaysToKeepSuccessFulJobs.getDate() - MAX_DAYS_TO_KEEP_SUCCESSFUL_JOBS,
  );
  let jobsMap = new Map();
  const selectAllJobsAndCollectingContainer = `

    select distinct ?job ?modified ?dataContainer where {

     graph <${DEFAULT_GRAPH}>{
       ?job a ?type; 
       <http://www.w3.org/ns/adms#status> <http://redpencil.data.gift/id/concept/JobStatus/success>;
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
        jobsToClean.push(j.jobUri);
      } else {
        console.log(
          `keeping ${j.jobUri} as its date '${j.modified.toISOString()}' is greater than ${maxDaysToKeepSuccessFulJobs.toISOString()}`,
        );
      }
    }
  }
  return jobsToClean;
}

export async function countFileForJob(jobUri) {
  const q = `
    SELECT (COUNT(?file) as ?files)
    WHERE {
      graph <${DEFAULT_GRAPH}>{
        ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)}.
        ?task <http://redpencil.data.gift/vocabularies/tasks/resultsContainer> ?container.
        ?container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file.
      }
    }
`;
  const res = await query(q, {}, connectionOptions);
  return res.results.bindings[0].files.value;
}

export async function getFilesForJob(jobUri, nbFiles) {
  let offset = 0;
  let out = [];
  const limit = 5000;

  while (offset < nbFiles) {
    const q = `
      SELECT ?file ?fileOnDisk
      WHERE {
      { SELECT distinct ?file ?fileOnDisk WHERE {
        graph <${DEFAULT_GRAPH}> {
          ?task <http://purl.org/dc/terms/isPartOf> ${sparqlEscapeUri(jobUri)}.
          ?task <http://redpencil.data.gift/vocabularies/tasks/resultsContainer> ?container.
          ?container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file.
          ?fileOnDisk <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?file.
        }
      } ORDER BY ?file ?source }
    } LIMIT ${limit} OFFSET ${offset}
    `;
    const res = await query(q, {}, connectionOptions);
    const files = res.results.bindings.map((r) => {
      return {
        file: r.file.value,
        fileOnDisk: r.fileOnDisk.value,
      };
    });
    out = [...out, ...files];
    offset += limit;
  }
  return out;
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
