use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Seq {
    m: HashMap<String, u64>,
}

impl Seq {
    pub fn next(&mut self, address: &str) -> u64 {
        match self.m.get_mut(address) {
            Some(next_request_id) => {
                let id = *next_request_id;
                *next_request_id += 1;
                id
            }
            None => {
                self.m.insert(address.to_string(), 1);
                0
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_works() {
        let mut seq = Seq::default();
        assert_eq!(0, seq.next("0"));
        assert_eq!(1, seq.next("0"));
        assert_eq!(0, seq.next("1"));
        assert_eq!(0, seq.next("2"));
        assert_eq!(2, seq.next("0"));
        assert_eq!(1, seq.next("1"));
        assert_eq!(1, seq.next("2"));
        assert_eq!(2, seq.next("2"));
        assert_eq!(3, seq.next("2"));
    }
}
