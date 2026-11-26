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

use std::fmt::Debug;

pub trait QueryOperation {
    type Response;

    /// Optional observation metadata. Defaults keep query implementations valid
    /// when another dependency enables `test_utils` through feature unification.
    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        Vec::new()
    }
}

#[derive(Debug)]
pub struct Read<V>(std::marker::PhantomData<V>);

impl<V> QueryOperation for Read<V> {
    type Response = V;

    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        "Read"
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        vec![]
    }
}

impl<V> Read<V> {
    pub const fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V> Default for Read<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ReadStable<V>(std::marker::PhantomData<V>);

impl<V> QueryOperation for ReadStable<V> {
    type Response = V;

    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        "ReadStable"
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        vec![]
    }
}

impl<V> ReadStable<V> {
    pub const fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V> Default for ReadStable<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct Contains<V>(pub V);

impl<V> QueryOperation for Contains<V>
where
    V: Debug + Clone,
{
    type Response = bool;

    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        "Contains"
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        let args = format!("{:?}", self.0);
        vec![args]
    }
}

pub struct Get<K, Q> {
    pub key: K,
    pub nested_query: Q,
}

impl<K, Q> Get<K, Q>
where
    K: Clone,
{
    pub fn new(key: &K, nested_query: Q) -> Self {
        Self {
            key: key.clone(),
            nested_query,
        }
    }
}

impl<K, Q> QueryOperation for Get<K, Q>
where
    Q: QueryOperation,
    K: Debug,
{
    type Response = Option<Q::Response>;

    #[cfg(feature = "test_utils")]
    fn name() -> &'static str {
        "Get"
    }

    #[cfg(feature = "test_utils")]
    fn args(&self) -> Vec<String> {
        let key = format!("{:?}", self.key);
        vec![
            key,
            format!("{}({:?})", Q::name(), self.nested_query.args()),
        ]
    }
}
