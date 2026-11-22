/** @typedef {{id:number, title:string, payload:object, author:string}} BusinessCase */
/** @typedef {{id:number, case_id:number, status:'queued'|'running'|'complete'|'failed'}} Run */
/** @typedef {{id:number, run_id:number, digest:string, report:?{summary:string, metrics:object, sections:Array}, reviewer:?string, review_note:?string, reviewed_at:?string, archive_key:?string, archived_at:?string, archive_error:?string}} Evidence */
