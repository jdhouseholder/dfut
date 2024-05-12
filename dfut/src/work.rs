use crate::{
    d_scheduler::worker_service::DoWorkRequest,
    global_scheduler::global_scheduler_service::ScheduleRequest,
};

pub trait IntoWork {
    fn into_work(&self) -> Work;
}

#[derive(Debug, Clone)]
pub struct Work {
    pub fn_name: String,
    pub args: Vec<u8>,
}

impl<'a> Into<ScheduleRequest> for &'a Work {
    fn into(self) -> ScheduleRequest {
        ScheduleRequest {
            fn_name: self.fn_name.clone(),
        }
    }
}

impl Work {
    pub fn into_do_work_request(self, task_id: u64) -> DoWorkRequest {
        DoWorkRequest {
            task_id,
            fn_name: self.fn_name,
            args: self.args,
        }
    }
}
