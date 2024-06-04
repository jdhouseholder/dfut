use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

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
    max: Option<u64>,
    gaps: BTreeSet<u64>,
}

impl Gaps {
    pub fn add(&mut self, id: u64) -> GapState {
        match self.max {
            Some(max) => match u64::cmp(&id, &max) {
                Ordering::Equal => GapState::Seen,
                Ordering::Less => {
                    if self.gaps.remove(&id) {
                        GapState::New
                    } else {
                        GapState::Seen
                    }
                }
                Ordering::Greater => {
                    if let Some(max) = self.max {
                        if id > max + 1 {
                            for i in (max + 1)..id {
                                self.gaps.insert(i);
                            }
                        }
                    }
                    self.max = Some(id);
                    GapState::New
                }
            },
            None => {
                for i in 0..id {
                    self.gaps.insert(i);
                }
                self.max = Some(id);
                GapState::New
            }
        }
    }

    pub fn clear(&mut self) {
        self.max = None;
        self.gaps.clear();
    }
}
#[derive(Debug, Default)]
pub struct LifetimeScopedGaps {
    lifetime_id: u64,
    gaps: Gaps,
}

impl LifetimeScopedGaps {
    pub fn reset(&mut self, lifetime_id: u64) {
        self.lifetime_id = lifetime_id;
        self.gaps.clear();
    }

    pub fn add(&mut self, request_id: u64) -> GapState {
        self.gaps.add(request_id)
    }

    pub fn lifetime_id(&self) -> u64 {
        self.lifetime_id
    }
}

#[derive(Debug, Clone, Default)]
pub struct AddressToGaps {
    address_to_gaps: Arc<Mutex<HashMap<String, LifetimeScopedGaps>>>,
}

impl AddressToGaps {
    pub fn have_seen_request_id(
        &self,
        parent_address: &str,
        parent_lifetime_id: u64,
        request_id: u64,
    ) -> bool {
        let mut address_to_gaps = self.address_to_gaps.lock().unwrap();
        let gaps = address_to_gaps
            .entry(parent_address.to_string())
            .or_default();
        if gaps.lifetime_id() < parent_lifetime_id {
            gaps.reset(parent_lifetime_id);
        }
        if gaps.lifetime_id() > parent_lifetime_id {
            // TODO: This would mean that there is a system bug.
            unreachable!()
        }

        gaps.add(request_id).is_seen()
    }

    pub fn try_reset(&self, address: &str, lifetime_id: u64) {
        if let Some(gaps) = self.address_to_gaps.lock().unwrap().get_mut(address) {
            gaps.reset(lifetime_id);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn add_works() {
        let mut gaps = Gaps::default();
        for _ in 0..2 {
            assert_eq!(gaps.add(0), GapState::New);
            assert_eq!(gaps.add(1), GapState::New);
            assert_eq!(gaps.add(1), GapState::Seen);
            assert_eq!(gaps.add(5), GapState::New);
            assert_eq!(gaps.add(3), GapState::New);
            assert_eq!(gaps.add(2), GapState::New);
            assert_eq!(gaps.add(3), GapState::Seen);
            assert_eq!(gaps.add(4), GapState::New);
            assert_eq!(gaps.add(6), GapState::New);
            gaps.clear();
        }
    }
}
