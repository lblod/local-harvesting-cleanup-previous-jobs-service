import { appendTaskError, loadExtractionTask, updateTaskStatus } from "./task";
import {
  MAX_DAYS_TO_KEEP_BUSY_JOBS,
  MAX_DAYS_TO_KEEP_FAILED_JOBS,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from "../constants";
import { unlink } from "fs/promises";
import {
  countFileForJob,
  genericDelete,
  getFilesForJob,
  deleteFileInDb,
  getJobWithStatusAndBeforeDate,
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
      const jobUri = jobsToClean.pop();
      const nbFiles = await countFileForJob(jobUri);
      console.log(`cleaning job ${jobUri} with ${nbFiles} files...`);
      const files = await getFilesForJob(jobUri, nbFiles);
      for (let f of files) {
        await removeFile(f);
      }
      console.log(`done cleaning up files`);
      await genericDelete(jobUri);
      console.log(`job ${jobUri} deleted`);
    }
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (e) {
    console.error(e);
    if (task) {
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

async function removeFile(file) {
  try {
    let path = file.fileOnDisk.replace("share://", "/share/");
    await unlink(path);
  } catch (e) {
    console.error(`could not delete ${JSON.stringify(file)}. ${e}`);
  }
  try {
    await deleteFileInDb(file.file);
  } catch (e) {
    console.error(`could not delete file in db: ${JSON.stringify(file)}. ${e}`);
  }
}
