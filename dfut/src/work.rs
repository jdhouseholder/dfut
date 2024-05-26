pub trait IntoWork {
    fn into_work(&self) -> Work;
}

#[derive(Debug, Clone)]
pub struct Work {
    pub fn_name: String,
    pub args: Vec<u8>,
}
