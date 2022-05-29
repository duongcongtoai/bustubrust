use super::common::RID;

#[derive(Copy, Clone)]
pub struct Txn {
    lv: IsolationLevel,
    two2pl: TwoPLState,
}

#[derive(Copy, Clone)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadComitted,
    RepeatableRead,
    Serializable,
}
pub enum TwoPLState {
    Growing,
    Shrinking,
    Committed,
    Aborted,
}

impl Txn {
    pub fn isolation_level(&self) -> IsolationLevel {
        self.lv
    }
    pub fn abort(&mut self) {
        self.two2pl = TwoPLState::Aborted;
    }
    pub fn state(&self) -> TwoPLState {
        self.two2pl
    }
    pub fn s_locked(&self, rid: RID) -> bool {}
    pub fn x_locked(&self, rid: RID) -> bool {}
}
