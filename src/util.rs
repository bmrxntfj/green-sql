use core::ops::{Deref,DerefMut};
use std::cmp::PartialEq;

#[inline]
pub fn if_or<T>(f:bool, true_result: T, false_result: T) -> T
{
    match f {
        true => true_result,
        false => false_result,
    }
}

#[inline]
pub fn if_or_default<T>(f: bool, true_result: T) -> T
where
    T:Default
{
    match f {
        true => true_result,
        false => T::default(),
    }
}

#[derive(Debug)]
pub struct Observer<T>(pub T,pub T,bool);

impl<T:Default+PartialEq> Observer<T> {
    pub fn new(value:T)->Self{
        Self(T::default(), value,false)
    }
    pub fn neq(&self)->bool{
        self.2 &&self.0!=self.1
    }
}
impl<T> Deref for Observer<T>{
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Observer<T>{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.2=true;
        &mut self.0
    }
}