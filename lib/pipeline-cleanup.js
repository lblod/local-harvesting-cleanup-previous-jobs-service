import { appendTaskError, loadExtractionTask, updateTaskStatus } from "./task";
import {
  MAX_DAYS_TO_KEEP_BUSY_JOBS,
  MAX_DAYS_TO_KEEP_FAILED_JOBS,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from "../constants";
import { rm, unlink } from "fs/promises";
import {
  deleteContainersForJob,
  deleteErrorsForJob,
  deleteOrphanedCollectionBatch,
  deleteOrphanedContainerBatch,
  deleteOrphanedTaskBatch,
  deleteSubjectsBatch,
  genericDelete,
  getFilesForJob,
  getJobWithStatusAndBeforeDate,
  getOrphanedCollectionBatch,
  getOrphanedContainerBatch,
  getOrphanedErrorBatch,
  getOrphanedFileBatch,
  getOrphanedRDOBatch,
  getOrphanedTaskBatch,
  getSuccessfulJobs,
} from "./queries";
export async function run(deltaEntry) {
  const task = await loadExtractionTask(deltaEntry);
  if (!task) return;

  try {
    await updateTaskStatus(task, STATUS_BUSY);
    let maxDaysToKeepFailedJobs = new Date();
    maxDaysToKeepFailedJobs.setDate(
      maxDaysToKeepFailedJobs.getDate() - MAX_DAYS_TO_KEEP_FAILED_JOBS,
    );
    let maxDaysToKeepBusyJobs = new Date();
    maxDaysToKeepBusyJobs.setDate(
      maxDaysToKeepBusyJobs.getDate() - MAX_DAYS_TO_KEEP_BUSY_JOBS,
    );

    let jobsToClean = [
      ...(await getSuccessfulJobs()),
      ...(await getJobWithStatusAndBeforeDate(
        STATUS_FAILED,
        maxDaysToKeepFailedJobs,
      )),
      ...(await getJobWithStatusAndBeforeDate(
        STATUS_BUSY,
        maxDaysToKeepBusyJobs,
      )),
    ];

    while (jobsToClean.length) {
      const {jobUri, jobId} = jobsToClean.pop();
      console.log(`cleaning job ${jobUri}...`);
      let fileCount = 0;
      for await (const f of getFilesForJob(jobUri)) {
        await removeFile(f);
        fileCount++;
      }
      console.log(`done cleaning up ${fileCount} files`);
      await deleteContainersForJob(jobUri);
      await deleteErrorsForJob(jobUri);
      await genericDelete(jobUri);
      await removeJobDirectory(jobId);
      console.log(`job ${jobUri} deleted`);
    }

    await cleanOrphans();
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

async function removeJobDirectory(jobId) {
  try {
    const directory = `/share/${jobId}`;
    console.log(`attempting to delete job directory ${directory} for jobId ${jobId}...`);
    await rm(directory,{ recursive: true, force: true });
  }catch (e) {
    console.error(`could not delete job directory ${directory} for jobId ${jobId}`);
  }
}

async function cleanOrphans() {
  console.log("cleaning orphaned metadata...");

  let batch;

  do {
    batch = await getOrphanedTaskBatch();
    if (batch.length) await deleteOrphanedTaskBatch(batch);
  } while (batch.length > 0);

  do {
    batch = await getOrphanedContainerBatch();
    if (batch.length) await deleteOrphanedContainerBatch(batch);
  } while (batch.length > 0);

  do {
    batch = await getOrphanedCollectionBatch();
    if (batch.length) await deleteOrphanedCollectionBatch(batch);
  } while (batch.length > 0);

  do {
    batch = await getOrphanedRDOBatch();
    if (batch.length) await deleteSubjectsBatch(batch);
  } while (batch.length > 0);

  let fileBatch;
  do {
    fileBatch = await getOrphanedFileBatch();
    for (const { diskFile } of fileBatch) {
      try { await unlink(diskFile.replace("share://", "/share/")); }
      catch (e) { console.error(`could not delete ${diskFile}: ${e.message}`); }
    }
    await deleteSubjectsBatch(fileBatch.map(f => f.diskFile));
    await deleteSubjectsBatch(fileBatch.map(f => f.file));
  } while (fileBatch.length > 0);

  do {
    batch = await getOrphanedErrorBatch();
    if (batch.length) await deleteSubjectsBatch(batch);
  } while (batch.length > 0);

  console.log("orphaned metadata cleanup done");
}

async function removeFile(file) {
  try {
    let path = file.fileOnDisk.replace("share://", "/share/");
    await unlink(path);
  } catch (e) {
    console.error(`could not delete ${JSON.stringify(file)}. ${e}`);
  }
}
