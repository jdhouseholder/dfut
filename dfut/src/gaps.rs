use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq)]
pub enum GapState {
    New,
    Seen,
}
impl GapState {
    #[allow(unused)]
    pub fn is_new(&self) -> bool {
        match self {
            GapState::New => true,
            GapState::Seen => false,
        }
    }

    pub fn is_seen(&self) -> bool {
        match self {
            GapState::New => false,
            GapState::Seen => true,
        }
    }
}

#[derive(Debug, Default)]
pub struct Gaps {
    next_id: u64,
    gaps: HashSet<u64>,
}

impl Gaps {
    pub fn add(&mut self, id: u64) -> GapState {
        match u64::cmp(&id, &self.next_id) {
            Ordering::Equal => {
                self.next_id += 1;
                GapState::New
            }
            Ordering::Less => {
                if self.gaps.contains(&id) {
                    self.gaps.remove(&id);
                    GapState::New
                } else {
                    GapState::Seen
                }
            }
            Ordering::Greater => {
                for i in self.next_id..id {
                    self.gaps.insert(i);
                }
                self.next_id = id;
                GapState::New
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_works() {
        let mut gaps = Gaps::default();
        assert_eq!(gaps.add(0), GapState::New);
        assert_eq!(gaps.add(1), GapState::New);
        assert_eq!(gaps.add(1), GapState::Seen);
        assert_eq!(gaps.add(5), GapState::New);
        assert_eq!(gaps.add(3), GapState::New);
        assert_eq!(gaps.add(2), GapState::New);
        assert_eq!(gaps.add(3), GapState::Seen);
        assert_eq!(gaps.add(4), GapState::New);
        assert_eq!(gaps.add(6), GapState::New);
    }
}
