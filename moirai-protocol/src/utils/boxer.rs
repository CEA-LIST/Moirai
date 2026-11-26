/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//  
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
// 
//  SPDX-License-Identifier: Apache-2.0
//  
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

//! A trait for boxing and unboxing values.

pub trait Boxer<Boxed> {
    fn boxer(self) -> Boxed;

    fn with_boxed<R>(&self, f: impl FnOnce(&Boxed) -> R) -> R
    where
        Self: Clone,
    {
        f(&self.clone().boxer())
    }
}

impl<T> Boxer<T> for T {
    fn boxer(self) -> T {
        self
    }

    fn with_boxed<R>(&self, f: impl FnOnce(&T) -> R) -> R
    where
        Self: Clone,
    {
        f(self)
    }
}

impl<T> Boxer<T> for Box<T> {
    fn boxer(self) -> T {
        *self
    }

    fn with_boxed<R>(&self, f: impl FnOnce(&T) -> R) -> R
    where
        Self: Clone,
    {
        f(self)
    }
}

impl<T> Boxer<Box<T>> for T {
    fn boxer(self) -> Box<T> {
        Box::new(self)
    }
}
